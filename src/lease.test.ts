import type {
	AppendAck,
	AppendInput,
	ReadRecord,
	S2Basin,
} from "@s2-dev/streamstore";
import { FencingTokenMismatchError } from "@s2-dev/streamstore";
import { describe, expect, it } from "vitest";

import { isControlRecord, S2LeaseProvider, threadTopic } from "./lease.js";

type Headers = ReadonlyArray<readonly [string, string]>;

/** The thread stream a lease key shares with that thread's events. */
const streamNameFor = (key: string) => `mastra/durable/${threadTopic(key)}`;

/** Minimal S2 stream fake enforcing fencing tokens like the service. */
class FakeStream {
	readonly records: Array<ReadRecord<"string">> = [];
	fence = "";
	casConflicts = 0;
	/** Runs once at the start of the next append (to simulate a racing writer). */
	beforeAppendOnce?: () => void;
	/** Runs after the next mismatch has captured the old token. */
	afterMismatchOnce?: () => void;

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
			const actual = this.fence;
			const afterMismatch = this.afterMismatchOnce;
			this.afterMismatchOnce = undefined;
			afterMismatch?.();
			throw new FencingTokenMismatchError({
				message: "fencing token mismatch",
				expectedFencingToken: actual,
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

/** An appended Mastra event sharing the lease stream. */
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
		// Token-only leases need one fence record; the fake exposes no read API.
		expect(stream("k").records).toHaveLength(1);
		expect(stream("k").fence).not.toBe("");
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
	});

	it("acquires against a hot event tail without reading or matching its position", async () => {
		const { a, stream } = setup();
		for (let i = 0; i < 1_000; i++) pushEvent(stream("k"), i);

		expect(await a.acquireLease("k", "a", 60_000)).toEqual({
			acquired: true,
			owner: "a",
		});
		expect(stream("k").records).toHaveLength(1_001);
	});

	it("admits exactly one of two concurrent acquirers", async () => {
		const { provider } = setup();
		const [first, second] = await Promise.all([
			provider().acquireLease("k", "a", 60_000),
			provider().acquireLease("k", "b", 60_000),
		]);

		expect([first.acquired, second.acquired].filter(Boolean)).toHaveLength(1);
		const winner = first.acquired ? "a" : "b";
		expect(first.acquired ? second.owner : first.owner).toBe(winner);
	});

	it("rejects an owner that cannot fit in the token", async () => {
		const { a, stream } = setup();
		const owner = "custom-owner-id-longer-than-sixteen-bytes";

		await expect(a.acquireLease("k", owner, 60_000)).rejects.toThrow(
			"lease owner must be a canonical lowercase UUID or at most 16 UTF-8 bytes",
		);
		expect(stream("k").records).toHaveLength(0);
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
	});

	it("renews, transfers, and releases from a different process", async () => {
		const { a, provider } = setup();
		await a.acquireLease("k", "a", 60_000);
		// The stream is the only state, so a process that wrote none of it can
		// still drive the lease it owns.
		const other = provider();
		expect(await other.renewLease("k", "a", 60_000)).toBe(true);
		expect(await other.transferLease("k", "a", "b", 60_000)).toBe(true);
		await other.releaseLease("k", "b");
		expect(await a.getLeaseOwner("k")).toBeUndefined();
	});

	it("fails a renewal fenced off after observing the token", async () => {
		const { a, stream, provider } = setup();
		await a.acquireLease("k", "a", 60_000);
		await provider().acquireLease("stolen", "b", 60_000);
		const stolenToken = stream("stolen").fence;
		// The probe returns `a`'s token, then another process installs `b`'s
		// token before the renewal compare-and-swap.
		stream("k").afterMismatchOnce = () => {
			stream("k").push(stolenToken, [["", "fence"]]);
		};

		expect(await a.renewLease("k", "a", 60_000)).toBe(false);
		expect(stream("k").casConflicts).toBe(2);
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

	it("does not scan interleaved events for a lease", async () => {
		const { a, stream, provider } = setup();
		await a.acquireLease("k", "a", 60_000);
		for (let i = 0; i < 10; i++) pushEvent(stream("k"), i);

		expect(await provider().getLeaseOwner("k")).toBe("a");
		expect(await provider().acquireLease("k", "b", 60_000)).toEqual({
			acquired: false,
			owner: "a",
		});
	});

	it("keeps token lookup constant-time when events bury the lease", async () => {
		const { a, stream, provider } = setup();
		await a.acquireLease("k", "a", 60_000);
		for (let i = 0; i < 600; i++) pushEvent(stream("k"), i);

		expect(await provider().getLeaseOwner("k")).toBe("a");
	});

	it("retries a lost CAS race and re-evaluates the new state", async () => {
		const { a, stream, provider } = setup();
		await provider().acquireLease("expired", "other", 0);
		const expiredToken = stream("expired").fence;
		// A racing writer installs an already-expired token before our initial
		// append. The mismatch returns it and the retry replaces it.
		stream("k").beforeAppendOnce = () => {
			stream("k").push(expiredToken, [["", "fence"]]);
		};
		expect(await a.acquireLease("k", "a", 1_000)).toEqual({
			acquired: true,
			owner: "a",
		});
		expect(stream("k").casConflicts).toBe(1);
	});

	it("loses to a racing writer who took the lease", async () => {
		const { a, stream, provider } = setup();
		await provider().acquireLease("winner", "b", 60_000);
		const winnerToken = stream("winner").fence;
		stream("k").beforeAppendOnce = () => {
			stream("k").push(winnerToken, [["", "fence"]]);
		};
		expect(await a.acquireLease("k", "a", 1_000)).toEqual({
			acquired: false,
			owner: "b",
		});
	});

	it("replaces an unrecognized token without reading records", async () => {
		const { a, stream } = setup();
		stream("k").push("malformed-token", [["", "fence"]]);
		expect(await a.getLeaseOwner("k")).toBeUndefined();
		expect((await a.acquireLease("k", "a", 1_000)).acquired).toBe(true);
		expect(stream("k").fence).not.toBe("malformed-token");
	});

	it("supports TTLs longer than the old record-scan window", async () => {
		const { a } = setup();
		expect(await a.acquireLease("k", "a", 10 * 60_000)).toEqual({
			acquired: true,
			owner: "a",
		});
		expect(await a.getLeaseOwner("k")).toBe("a");
	});

	it("keeps a lease readable behind an arbitrarily hot tail", async () => {
		const { a, stream, provider } = setup();
		await a.acquireLease("k", "a", 60_000);
		for (let i = 0; i < 600; i++) pushEvent(stream("k"), i);
		expect(await provider().getLeaseOwner("k")).toBe("a");
		expect(await provider().acquireLease("k", "b", 1_000)).toEqual({
			acquired: false,
			owner: "a",
		});
	});

	it("treats a multi-header record as an event, not a command", () => {
		// Command records carry exactly one header with an empty name; a producer
		// using an empty header name alongside others is still publishing an event.
		expect(isControlRecord({ headers: [["", "fence"]] })).toBe(true);
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
