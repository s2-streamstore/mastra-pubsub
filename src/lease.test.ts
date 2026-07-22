import type {
	AppendAck,
	AppendInput,
	ReadBatch,
	ReadInput,
	S2Basin,
} from "@s2-dev/streamstore";
import { SeqNumMismatchError } from "@s2-dev/streamstore";
import { describe, expect, it } from "vitest";

import { S2LeaseProvider } from "./lease.js";

/** Minimal S2 stream fake that enforces `matchSeqNum` like the real service. */
class FakeStream {
	readonly records: Array<{ seqNum: number; body: unknown }> = [];
	casConflicts = 0;
	/** Runs once at the start of the next append (to simulate a racing writer). */
	beforeAppendOnce?: () => void;

	push(body: unknown): void {
		this.records.push({ seqNum: this.records.length, body });
	}

	async append(input: AppendInput): Promise<AppendAck> {
		const hook = this.beforeAppendOnce;
		this.beforeAppendOnce = undefined;
		hook?.();
		if (
			input.matchSeqNum !== undefined &&
			input.matchSeqNum !== this.records.length
		) {
			this.casConflicts++;
			throw new SeqNumMismatchError({
				message: "seqNum mismatch",
				expectedSeqNum: this.records.length,
			});
		}
		for (const record of input.records) this.push(record.body);
		const position = { seqNum: this.records.length, timestamp: new Date() };
		return { start: position, end: position, tail: position };
	}

	async read(_input?: ReadInput): Promise<ReadBatch<"string">> {
		// The provider only ever reads the last record (tailOffset: 1).
		return {
			records: this.records.slice(-1).map((r) => ({
				seqNum: r.seqNum,
				body: String(r.body),
				headers: [],
				timestamp: new Date(),
			})),
			tail: { seqNum: this.records.length, timestamp: new Date() },
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
	const provider = new S2LeaseProvider(
		basin as unknown as S2Basin,
		"mastra/durable/lease/",
	);
	const stream = (key: string) => basin.stream(`mastra/durable/lease/${key}`);
	return { provider, stream };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

describe("S2LeaseProvider", () => {
	it("acquires a free lease and reports the owner", async () => {
		const { provider } = setup();
		expect(await provider.acquireLease("k", "a", 1_000)).toEqual({
			acquired: true,
			owner: "a",
		});
		expect(await provider.getLeaseOwner("k")).toBe("a");
	});

	it("refuses a held lease and names the holder", async () => {
		const { provider } = setup();
		await provider.acquireLease("k", "a", 1_000);
		expect(await provider.acquireLease("k", "b", 1_000)).toEqual({
			acquired: false,
			owner: "a",
		});
	});

	it("lets the same owner re-acquire (refreshing the TTL)", async () => {
		const { provider } = setup();
		await provider.acquireLease("k", "a", 1_000);
		expect(await provider.acquireLease("k", "a", 1_000)).toEqual({
			acquired: true,
			owner: "a",
		});
	});

	it("treats an expired lease as free", async () => {
		const { provider } = setup();
		await provider.acquireLease("k", "a", 20);
		await sleep(30);
		expect(await provider.getLeaseOwner("k")).toBeUndefined();
		expect((await provider.acquireLease("k", "b", 1_000)).acquired).toBe(true);
	});

	it("renews only while held by the caller", async () => {
		const { provider } = setup();
		await provider.acquireLease("k", "a", 1_000);
		expect(await provider.renewLease("k", "a", 1_000)).toBe(true);
		expect(await provider.renewLease("k", "b", 1_000)).toBe(false);
		await provider.acquireLease("expired", "a", 20);
		await sleep(30);
		expect(await provider.renewLease("expired", "a", 1_000)).toBe(false);
	});

	it("release frees the lease; non-owner release is a no-op", async () => {
		const { provider } = setup();
		await provider.acquireLease("k", "a", 1_000);
		await provider.releaseLease("k", "b");
		expect(await provider.getLeaseOwner("k")).toBe("a");
		await provider.releaseLease("k", "a");
		expect(await provider.getLeaseOwner("k")).toBeUndefined();
		expect((await provider.acquireLease("k", "b", 1_000)).acquired).toBe(true);
	});

	it("transfers atomically from the current holder", async () => {
		const { provider } = setup();
		await provider.acquireLease("k", "a", 1_000);
		expect(await provider.transferLease("k", "a", "b", 1_000)).toBe(true);
		expect(await provider.getLeaseOwner("k")).toBe("b");
		// `a` no longer holds it, so a second transfer fails.
		expect(await provider.transferLease("k", "a", "c", 1_000)).toBe(false);
	});

	it("retries a lost CAS race and re-evaluates the new state", async () => {
		const { provider, stream } = setup();
		// A racing writer releases the lease right before our append lands,
		// making our matchSeqNum stale. The retry re-reads and still acquires.
		stream("k").beforeAppendOnce = () =>
			stream("k").push(JSON.stringify({ owner: null, expiresAt: 0 }));
		expect(await provider.acquireLease("k", "a", 1_000)).toEqual({
			acquired: true,
			owner: "a",
		});
		expect(stream("k").casConflicts).toBe(1);
	});

	it("loses to a racing writer who took the lease", async () => {
		const { provider, stream } = setup();
		const winner = JSON.stringify({
			owner: "b",
			expiresAt: Date.now() + 60_000,
		});
		stream("k").beforeAppendOnce = () => stream("k").push(winner);
		expect(await provider.acquireLease("k", "a", 1_000)).toEqual({
			acquired: false,
			owner: "b",
		});
	});

	it("ignores malformed lease records instead of trusting them", async () => {
		const { provider, stream } = setup();
		stream("k").push("not-json");
		expect(await provider.getLeaseOwner("k")).toBeUndefined();
		expect((await provider.acquireLease("k", "a", 1_000)).acquired).toBe(true);
	});
});
