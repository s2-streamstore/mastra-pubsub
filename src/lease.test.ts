import type {
	AppendAck,
	AppendInput,
	ReadBatch,
	ReadInput,
	ReadRecord,
	S2Basin,
} from "@s2-dev/streamstore";
import { FencingTokenMismatchError } from "@s2-dev/streamstore";
import { describe, expect, it, vi } from "vitest";

import {
	isControlRecord,
	MAX_LEASE_TTL_MS,
	S2LeaseProvider,
	threadTopic,
} from "./lease.js";

type Headers = ReadonlyArray<readonly [string, string]>;

/** The thread stream a lease key shares with that thread's events. */
const streamNameFor = (key: string) => `mastra/durable/${threadTopic(key)}`;

/** Minimal S2 stream fake enforcing fencing tokens like the service. */
class FakeStream {
	readonly records: Array<ReadRecord<"string">> = [];
	fence = "";
	readCount = 0;
	casConflicts = 0;
	/** Records returned per read, to exercise multi-batch scans. */
	batchLimit = 1_000;
	/** Runs once at the start of the next append (to simulate a racing writer). */
	beforeAppendOnce?: () => void;

	push(body: string, headers: Headers = [], timestamp = new Date()): void {
		this.records.push({
			seqNum: this.records.length,
			body,
			headers,
			timestamp,
		});
		if (headers.length === 1 && headers[0]?.[0] === "") {
			this.fence = body;
		}
	}

	async append(input: AppendInput): Promise<AppendAck> {
		const hook = this.beforeAppendOnce;
		this.beforeAppendOnce = undefined;
		hook?.();
		if (input.fencingToken !== undefined && input.fencingToken !== this.fence) {
			this.casConflicts++;
			throw new FencingTokenMismatchError({
				message: "fencing token mismatch",
				expectedFencingToken: this.fence,
			});
		}
		const start = this.records.length;
		for (const record of input.records) {
			this.push(
				String(record.body),
				(record.headers ?? []).map(
					(header) => [String(header[0]), String(header[1])] as const,
				),
			);
		}
		const position = { seqNum: this.records.length, timestamp: new Date() };
		return {
			start: { seqNum: start, timestamp: new Date() },
			end: position,
			tail: position,
		};
	}

	async checkTail(): Promise<{
		tail: { seqNum: number; timestamp: Date };
	}> {
		return {
			tail: { seqNum: this.records.length, timestamp: new Date() },
		};
	}

	async read(input?: ReadInput): Promise<ReadBatch<"string">> {
		this.readCount++;
		const start = this.startPosition(input);
		return {
			records: this.records.slice(start, start + this.batchLimit),
			tail: { seqNum: this.records.length, timestamp: new Date() },
		};
	}

	private startPosition(input?: ReadInput): number {
		const from = input?.start?.from;
		if (!from) return 0;
		if ("seqNum" in from) return from.seqNum;
		if ("tailOffset" in from) {
			return Math.max(0, this.records.length - from.tailOffset);
		}
		// Records are timestamp-ordered, so start at the first one in the window.
		const since =
			from.timestamp instanceof Date
				? from.timestamp.getTime()
				: from.timestamp;
		const index = this.records.findIndex(
			(record) => record.timestamp.getTime() >= since,
		);
		return index === -1 ? this.records.length : index;
	}
}

class FakeBasin {
	readonly byName = new Map<string, FakeStream>();

	stream(name: string): FakeStream {
		let stream = this.byName.get(name);
		if (!stream) {
			stream = new FakeStream();
			this.byName.set(name, stream);
		}
		return stream;
	}
}

function setup() {
	const basin = new FakeBasin();
	const stream = (key: string) => basin.stream(streamNameFor(key));
	// Separate providers model separate processes with no shared local state.
	const provider = () =>
		new S2LeaseProvider(basin as unknown as S2Basin, "mastra/durable/");
	return { basin, stream, provider, a: provider() };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** An appended Mastra event, which lease reads must scan past. */
const pushEvent = (stream: FakeStream, i: number) =>
	stream.push(JSON.stringify({ id: `e${i}`, type: "chunk" }));

describe("S2LeaseProvider", () => {
	it("acquires a free lease and reports the owner", async () => {
		const { a, stream } = setup();
		expect(await a.acquireLease("k", "a", 1_000)).toEqual({
			acquired: true,
			owner: "a",
		});
		expect(await a.getLeaseOwner("k")).toBe("a");
		// Inline owners need one fence record and no lease-state read.
		expect(stream("k").records).toHaveLength(1);
		expect(stream("k").fence).not.toBe("");
		expect(stream("k").readCount).toBe(0);
	});

	it("round-trips a UUID owner entirely through the fencing token", async () => {
		const { a, stream, provider } = setup();
		const owner = "550e8400-e29b-41d4-a716-446655440000";

		expect(await a.acquireLease("k", owner, 60_000)).toEqual({
			acquired: true,
			owner,
		});
		expect(stream("k").fence).toHaveLength(36);
		expect(await provider().getLeaseOwner("k")).toBe(owner);
		expect(stream("k").readCount).toBe(0);
	});

	it("acquires against a hot event tail without reading or matching its position", async () => {
		const { a, stream } = setup();
		for (let i = 0; i < 1_000; i++) pushEvent(stream("k"), i);

		expect(await a.acquireLease("k", "a", 60_000)).toEqual({
			acquired: true,
			owner: "a",
		});
		expect(stream("k").readCount).toBe(0);
		expect(stream("k").records).toHaveLength(1_001);
	});

	it("admits exactly one of two concurrent acquirers", async () => {
		const { provider, stream } = setup();
		const [first, second] = await Promise.all([
			provider().acquireLease("k", "a", 60_000),
			provider().acquireLease("k", "b", 60_000),
		]);

		expect([first.acquired, second.acquired].filter(Boolean)).toHaveLength(1);
		const winner = first.acquired ? "a" : "b";
		expect(first.acquired ? second.owner : first.owner).toBe(winner);
		expect(stream("k").readCount).toBe(0);
	});

	it("falls back to a tagged record for a long custom owner", async () => {
		const { a, stream, provider } = setup();
		const owner = "custom-owner-id-longer-than-sixteen-bytes";

		expect(await a.acquireLease("k", owner, 60_000)).toEqual({
			acquired: true,
			owner,
		});
		expect(stream("k").records).toHaveLength(2);
		expect(
			stream("k").records.some((record) =>
				(record.headers ?? []).some(([name]) => name === "mastra-lease"),
			),
		).toBe(true);
		expect(await provider().getLeaseOwner("k")).toBe(owner);
		expect(stream("k").readCount).toBeGreaterThan(0);
	});

	it("stores lease state in the thread stream for the key", async () => {
		const { a, basin } = setup();
		await a.acquireLease("resource-1\0thread-1", "a", 1_000);
		expect([...basin.byName.keys()]).toEqual([
			"mastra/durable/agent.thread-stream.resource-1%00thread-1",
		]);
	});

	it("refuses a held lease and names the holder", async () => {
		const { a, provider } = setup();
		await a.acquireLease("k", "a", 1_000);
		expect(await provider().acquireLease("k", "b", 1_000)).toEqual({
			acquired: false,
			owner: "a",
		});
	});

	it("lets the same owner re-acquire (refreshing the TTL)", async () => {
		const { a } = setup();
		await a.acquireLease("k", "a", 1_000);
		expect(await a.acquireLease("k", "a", 1_000)).toEqual({
			acquired: true,
			owner: "a",
		});
	});

	it("treats an expired lease as free", async () => {
		const { a, provider } = setup();
		await a.acquireLease("k", "a", 20);
		await sleep(30);
		expect(await a.getLeaseOwner("k")).toBeUndefined();
		expect((await provider().acquireLease("k", "b", 1_000)).acquired).toBe(
			true,
		);
	});

	it("renews only while held by the caller", async () => {
		const { a, provider } = setup();
		await a.acquireLease("k", "a", 1_000);
		expect(await a.renewLease("k", "a", 1_000)).toBe(true);
		expect(await provider().renewLease("k", "b", 1_000)).toBe(false);
		await a.acquireLease("expired", "a", 20);
		await sleep(30);
		expect(await a.renewLease("expired", "a", 1_000)).toBe(false);
	});

	it("renews under the fencing token already in the stream", async () => {
		const { a, stream } = setup();
		await a.acquireLease("k", "a", 1_000);
		const { fence } = stream("k");
		expect(await a.renewLease("k", "a", 1_000)).toBe(true);
		// Renewal atomically rotates the self-contained token.
		expect(stream("k").records).toHaveLength(2);
		expect(stream("k").fence).not.toBe(fence);
		expect(stream("k").readCount).toBe(0);
	});

	it("renews, transfers, and releases from a different process", async () => {
		const { a, stream, provider } = setup();
		await a.acquireLease("k", "a", 60_000);
		// The stream is the only state, so a process that wrote none of it can
		// still drive the lease it owns.
		const other = provider();
		expect(await other.renewLease("k", "a", 60_000)).toBe(true);
		expect(await other.transferLease("k", "a", "b", 60_000)).toBe(true);
		await other.releaseLease("k", "b");
		expect(await a.getLeaseOwner("k")).toBeUndefined();
		expect(stream("k").readCount).toBe(0);
	});

	it("fails a renewal fenced off between its read and its append", async () => {
		const { a, stream } = setup();
		await a.acquireLease("k", "a", 60_000);
		// Another process recovers the thread and fences `a` off mid-renewal.
		stream("k").beforeAppendOnce = () => {
			stream("k").push("stolen-token", [["", "fence"]]);
			stream("k").push(
				JSON.stringify({
					owner: "b",
					expiresAt: Date.now() + 60_000,
					token: "stolen-token",
				}),
				[["mastra-lease", "1"]],
			);
		};

		expect(await a.renewLease("k", "a", 60_000)).toBe(false);
		expect(stream("k").casConflicts).toBe(1);
		expect(await a.getLeaseOwner("k")).toBe("b");
	});

	it("release frees the lease; non-owner release is a no-op", async () => {
		const { a, provider } = setup();
		await a.acquireLease("k", "a", 1_000);
		await provider().releaseLease("k", "b");
		expect(await a.getLeaseOwner("k")).toBe("a");
		await a.releaseLease("k", "a");
		expect(await a.getLeaseOwner("k")).toBeUndefined();
		expect((await provider().acquireLease("k", "b", 1_000)).acquired).toBe(
			true,
		);
	});

	it("transfers atomically from the current holder", async () => {
		const { a } = setup();
		await a.acquireLease("k", "a", 1_000);
		expect(await a.transferLease("k", "a", "b", 1_000)).toBe(true);
		expect(await a.getLeaseOwner("k")).toBe("b");
		// `a` no longer holds it, so a second transfer fails.
		expect(await a.transferLease("k", "a", "c", 1_000)).toBe(false);
	});

	it("does not scan interleaved events for an inline lease", async () => {
		const { a, stream, provider } = setup();
		await a.acquireLease("k", "a", 60_000);
		stream("k").batchLimit = 2;
		for (let i = 0; i < 10; i++) pushEvent(stream("k"), i);

		expect(await provider().getLeaseOwner("k")).toBe("a");
		expect(await provider().acquireLease("k", "b", 60_000)).toEqual({
			acquired: false,
			owner: "a",
		});
		expect(stream("k").readCount).toBe(0);
	});

	it("keeps token lookup constant-time when events bury the lease", async () => {
		const { a, stream, provider } = setup();
		await a.acquireLease("k", "a", 60_000);
		for (let i = 0; i < 600; i++) pushEvent(stream("k"), i);

		expect(await provider().getLeaseOwner("k")).toBe("a");
		expect(stream("k").readCount).toBe(0);
	});

	it("ignores legacy lease state older than the fallback window", async () => {
		const { a, stream } = setup();
		// An unexpired lease is always written within its TTL, so a record this
		// old cannot be an active lease whatever its `expiresAt` claims.
		stream("k").push(
			"stale-token",
			[["", "fence"]],
			new Date(Date.now() - 10 * 60_000),
		);
		stream("k").push(
			JSON.stringify({
				owner: "stale",
				expiresAt: Date.now() + 60_000,
				token: "stale-token",
			}),
			[["mastra-lease", "1"]],
			new Date(Date.now() - 10 * 60_000),
		);
		// Bury it past the bounded tail scan so the time window decides.
		for (let i = 0; i < 600; i++) pushEvent(stream("k"), i);

		expect(await a.getLeaseOwner("k")).toBeUndefined();
		expect((await a.acquireLease("k", "a", 1_000)).acquired).toBe(true);
	});

	it("retries a lost CAS race and re-evaluates the new state", async () => {
		const { a, stream } = setup();
		// A racing writer takes and releases the lease right before our append
		// lands, making our condition stale. The retry re-reads and still acquires.
		stream("k").beforeAppendOnce = () => {
			stream("k").push("other-token", [["", "fence"]]);
			stream("k").push(
				JSON.stringify({ owner: null, expiresAt: 0, token: "other-token" }),
				[["mastra-lease", "1"]],
			);
		};
		expect(await a.acquireLease("k", "a", 1_000)).toEqual({
			acquired: true,
			owner: "a",
		});
		expect(stream("k").casConflicts).toBe(1);
	});

	it("loses to a racing writer who took the lease", async () => {
		const { a, stream } = setup();
		stream("k").beforeAppendOnce = () => {
			stream("k").push("winner-token", [["", "fence"]]);
			stream("k").push(
				JSON.stringify({
					owner: "b",
					expiresAt: Date.now() + 60_000,
					token: "winner-token",
				}),
				[["mastra-lease", "1"]],
			);
		};
		expect(await a.acquireLease("k", "a", 1_000)).toEqual({
			acquired: false,
			owner: "b",
		});
	});

	it("ignores malformed lease records instead of trusting them", async () => {
		const { a, stream } = setup();
		stream("k").push("malformed-token", [["", "fence"]]);
		stream("k").push("not-json", [["mastra-lease", "1"]]);
		expect(await a.getLeaseOwner("k")).toBeUndefined();
		expect((await a.acquireLease("k", "a", 1_000)).acquired).toBe(true);
	});

	it("clamps a TTL longer than the lookback window, warning once", async () => {
		const { basin, stream } = setup();
		const warn = vi.fn();
		const logger = { warn } as unknown as ConstructorParameters<
			typeof S2LeaseProvider
		>[2];
		const p = new S2LeaseProvider(
			basin as unknown as S2Basin,
			"mastra/durable/",
			logger,
		);

		const before = Date.now();
		const owner = "owner-longer-than-sixteen-bytes";
		await p.acquireLease("k", owner, 10 * 60_000);
		const state = JSON.parse(
			stream("k").records.find((r) =>
				(r.headers ?? []).some(([h]) => h === "mastra-lease"),
			)?.body as string,
		) as { expiresAt: number };
		// Expiry reflects the clamp, not the requested ten minutes.
		expect(state.expiresAt - before).toBeLessThanOrEqual(MAX_LEASE_TTL_MS + 50);

		// Repeat offences stay quiet so renewals cannot flood the log.
		await p.renewLease("k", owner, 10 * 60_000);
		expect(warn).toHaveBeenCalledTimes(1);
		expect(warn.mock.calls[0]?.[0]).toContain("Clamping lease TTL");
	});

	it("keeps an inline lease readable behind an arbitrarily hot tail", async () => {
		const { a, stream, provider } = setup();
		await a.acquireLease("k", "a", MAX_LEASE_TTL_MS);
		for (let i = 0; i < 600; i++) pushEvent(stream("k"), i);
		expect(await provider().getLeaseOwner("k")).toBe("a");
		expect(await provider().acquireLease("k", "b", 1_000)).toEqual({
			acquired: false,
			owner: "a",
		});
		expect(stream("k").readCount).toBe(0);
	});

	it("treats a multi-header record as an event, not a command", () => {
		// Command records carry exactly one header with an empty name; a producer
		// using an empty header name alongside others is still publishing an event.
		expect(isControlRecord({ headers: [["", "fence"]] })).toBe(true);
		expect(isControlRecord({ headers: [["mastra-lease", "1"]] })).toBe(true);
		expect(
			isControlRecord({
				headers: [
					["", "x"],
					["content-type", "application/json"],
				],
			}),
		).toBe(false);
		expect(isControlRecord({ headers: [] })).toBe(false);
	});
});
