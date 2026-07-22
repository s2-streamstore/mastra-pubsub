import type { Event, EventCallback } from "@mastra/core/events";
import { noopLogger } from "@mastra/core/logger";
import type {
	AppendAck,
	AppendInput,
	ReadBatch,
	ReadInput,
	ReadRecord,
	ReadSession,
	S2,
	S2RequestOptions,
	StreamPosition,
} from "@s2-dev/streamstore";
import { afterEach, describe, expect, it, vi } from "vitest";

import { S2PubSub, type S2PubSubOptions } from "./pubsub.js";

interface FakeSession {
	cursor: number;
	controller: ReadableStreamDefaultController<ReadRecord<"string">>;
	closed: boolean;
	closeWithError: (error: unknown) => void;
}

class FakeStream {
	readonly records: Array<ReadRecord<"string">> = [];
	readonly sessions = new Set<FakeSession>();
	readCount = 0;
	sessionCount = 0;
	trimPoint = 0;
	appendError?: Error;

	async checkTail(
		options?: S2RequestOptions,
	): Promise<{ tail: StreamPosition }> {
		if (options?.signal?.aborted)
			throw new DOMException("Aborted", "AbortError");
		return { tail: this.position(this.records.length) };
	}

	async append(input: AppendInput): Promise<AppendAck> {
		if (this.appendError) throw this.appendError;
		const start = this.records.length;
		for (const appendRecord of input.records) {
			if (typeof appendRecord.body !== "string") {
				throw new Error("FakeStream only supports string records");
			}
			const record: ReadRecord<"string"> = {
				seqNum: this.records.length,
				body: appendRecord.body,
				headers: [],
				timestamp: new Date(),
			};
			this.records.push(record);
			for (const session of [...this.sessions]) {
				if (!session.closed && record.seqNum >= session.cursor) {
					session.cursor = record.seqNum + 1;
					session.controller.enqueue(record);
				}
			}
		}
		return {
			start: this.position(start),
			end: this.position(this.records.length),
			tail: this.position(this.records.length),
		};
	}

	async read(input?: ReadInput): Promise<ReadBatch<"string">> {
		this.readCount++;
		const start = this.startPosition(input);
		return {
			records: this.records.slice(Math.max(start, this.trimPoint)),
			tail: this.position(this.records.length),
		};
	}

	async readSession(
		input?: ReadInput,
		options?: S2RequestOptions,
	): Promise<ReadSession<"string">> {
		if (options?.signal?.aborted)
			throw new DOMException("Aborted", "AbortError");
		this.sessionCount++;
		let state!: FakeSession;
		let abortHandler: (() => void) | undefined;
		const session = new ReadableStream<ReadRecord<"string">>({
			start: (controller) => {
				state = {
					cursor: this.startPosition(input),
					controller,
					closed: false,
					closeWithError: (error) => {
						if (state.closed) return;
						state.closed = true;
						this.sessions.delete(state);
						controller.error(error);
					},
				};
				for (const record of this.records.slice(
					Math.max(state.cursor, this.trimPoint),
				)) {
					state.cursor = record.seqNum + 1;
					controller.enqueue(record);
				}
				this.sessions.add(state);
				abortHandler = () =>
					state.closeWithError(new DOMException("Aborted", "AbortError"));
				options?.signal?.addEventListener("abort", abortHandler, {
					once: true,
				});
			},
			cancel: () => {
				state.closed = true;
				this.sessions.delete(state);
				if (abortHandler) {
					options?.signal?.removeEventListener("abort", abortHandler);
				}
			},
		});

		return Object.assign(session, {
			nextReadPosition: () => this.position(state.cursor),
			lastObservedTail: () => this.position(this.records.length),
			[Symbol.asyncDispose]: async () => session.cancel("disposed"),
		}) as ReadSession<"string">;
	}

	async close(): Promise<void> {}

	failReaders(error: unknown): void {
		for (const session of [...this.sessions]) {
			session.closeWithError(error);
		}
	}

	reset(): void {
		for (const session of [...this.sessions]) {
			if (!session.closed) {
				session.closed = true;
				session.controller.close();
			}
		}
		this.sessions.clear();
		this.records.length = 0;
	}

	private startPosition(input?: ReadInput): number {
		const from = input?.start?.from;
		if (!from) return 0;
		if ("seqNum" in from) {
			return input?.start?.clamp
				? Math.min(from.seqNum, this.records.length)
				: from.seqNum;
		}
		if ("tailOffset" in from) {
			return Math.max(0, this.records.length - from.tailOffset);
		}
		return 0;
	}

	private position(seqNum: number): StreamPosition {
		return { seqNum, timestamp: new Date() };
	}
}

class FakeBasin {
	readonly byName = new Map<string, FakeStream>();
	deleteError?: Error;
	readonly streams = {
		delete: async ({ stream }: { stream: string }) => {
			if (this.deleteError) throw this.deleteError;
			this.stream(stream).reset();
		},
	};

	stream(name: string): FakeStream {
		let stream = this.byName.get(name);
		if (!stream) {
			stream = new FakeStream();
			this.byName.set(name, stream);
		}
		return stream;
	}
}

const instances: S2PubSub[] = [];
type Logger = NonNullable<S2PubSubOptions["logger"]>;

function createTestLogger() {
	return { ...noopLogger, error: vi.fn() } satisfies Logger;
}

function createPubSub(basin: FakeBasin, logger?: Logger) {
	const client = { basin: () => basin } as unknown as S2;
	const pubsub = new S2PubSub(
		{ client, basin: "test-basin" },
		logger ? { logger } : undefined,
	);
	instances.push(pubsub);
	return pubsub;
}

function event(i: number): Omit<Event, "id" | "createdAt" | "index"> {
	return { type: "chunk", data: { i }, runId: "run-1" };
}

async function waitFor(
	predicate: () => boolean,
	timeoutMs = 1_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!predicate()) {
		if (Date.now() >= deadline)
			throw new Error("Timed out waiting for condition");
		await new Promise((resolve) => setTimeout(resolve, 5));
	}
}

afterEach(async () => {
	await Promise.all(instances.splice(0).map((instance) => instance.close()));
});

describe("S2PubSub read-session live delivery", () => {
	it("delivers live records across separate PubSub instances", async () => {
		const basin = new FakeBasin();
		const publisher = createPubSub(basin);
		const subscriber = createPubSub(basin);
		const received: Event[] = [];
		const cb: EventCallback = (value) => received.push(value);

		await subscriber.subscribe("agent.stream.run-1", cb);
		await publisher.publish("agent.stream.run-1", event(0));
		await waitFor(() => received.length === 1);

		expect(received[0]?.index).toBe(0);
		expect(received[0]?.data).toEqual({ i: 0 });
		await subscriber.unsubscribe("agent.stream.run-1", cb);
	});

	it("replays and follows live through one S2 read session", async () => {
		const basin = new FakeBasin();
		const publisher = createPubSub(basin);
		const subscriber = createPubSub(basin);
		const indexes: number[] = [];
		const cb: EventCallback = (value) => indexes.push(value.index ?? -1);

		await publisher.publish("agent.stream.run-1", event(0));
		await publisher.publish("agent.stream.run-1", event(1));
		await subscriber.subscribeFromOffset("agent.stream.run-1", 1, cb);
		await publisher.publish("agent.stream.run-1", event(2));
		await waitFor(() => indexes.length === 2);

		expect(indexes).toEqual([1, 2]);
		const stream = basin.stream("mastra/durable/agent.stream.run-1");
		expect(stream.sessionCount).toBe(1);
		expect(stream.readCount).toBe(0);
		await subscriber.unsubscribe("agent.stream.run-1", cb);
	});

	it("starts a live-only subscriber at the current tail", async () => {
		const basin = new FakeBasin();
		const publisher = createPubSub(basin);
		const subscriber = createPubSub(basin);
		const indexes: number[] = [];
		const cb: EventCallback = (value) => indexes.push(value.index ?? -1);

		await publisher.publish("agent.stream.run-1", event(0));
		await subscriber.subscribe("agent.stream.run-1", cb);
		await publisher.publish("agent.stream.run-1", event(1));
		await waitFor(() => indexes.length === 1);

		expect(indexes).toEqual([1]);
		await subscriber.unsubscribe("agent.stream.run-1", cb);
	});

	it("reconnects from the next sequence number without a gap", async () => {
		const basin = new FakeBasin();
		const logger = createTestLogger();
		const publisher = createPubSub(basin);
		const subscriber = createPubSub(basin, logger);
		const indexes: number[] = [];
		const cb: EventCallback = (value) => indexes.push(value.index ?? -1);
		const stream = basin.stream("mastra/durable/agent.stream.run-1");

		await subscriber.subscribe("agent.stream.run-1", cb);
		await publisher.publish("agent.stream.run-1", event(0));
		await waitFor(() => indexes.length === 1);
		stream.failReaders(new Error("connection lost"));
		await waitFor(() => stream.sessionCount >= 2);
		await publisher.publish("agent.stream.run-1", event(1));
		await waitFor(() => indexes.length === 2);

		expect(indexes).toEqual([0, 1]);
		expect(logger.error).toHaveBeenCalledOnce();
		await subscriber.unsubscribe("agent.stream.run-1", cb);
	});

	it("keeps localOnly events in the publishing process", async () => {
		const basin = new FakeBasin();
		const local = createPubSub(basin);
		const remote = createPubSub(basin);
		const localEvents: Event[] = [];
		const remoteEvents: Event[] = [];
		const localCb: EventCallback = (value) => localEvents.push(value);
		const remoteCb: EventCallback = (value) => remoteEvents.push(value);

		await local.subscribe("agent.stream.run-1", localCb);
		await remote.subscribe("agent.stream.run-1", remoteCb);
		await local.publish("agent.stream.run-1", event(0), { localOnly: true });
		await waitFor(() => localEvents.length === 1);
		await new Promise((resolve) => setTimeout(resolve, 20));

		expect(remoteEvents).toHaveLength(0);
		expect(await local.getHistory("agent.stream.run-1", 0)).toHaveLength(0);
		await local.unsubscribe("agent.stream.run-1", localCb);
		await remote.unsubscribe("agent.stream.run-1", remoteCb);
	});

	it("rejects failed S2 appends without local fallback", async () => {
		const basin = new FakeBasin();
		const logger = createTestLogger();
		const pubsub = createPubSub(basin, logger);
		const received: Event[] = [];
		const cb: EventCallback = (value) => received.push(value);
		const stream = basin.stream("mastra/durable/agent.stream.run-1");

		await pubsub.subscribe("agent.stream.run-1", cb);
		stream.appendError = new Error("append failed");
		await expect(
			pubsub.publish("agent.stream.run-1", event(0)),
		).rejects.toThrow("append failed");

		expect(received).toHaveLength(0);
		expect(await pubsub.getHistory("agent.stream.run-1", 0)).toHaveLength(0);
		expect(logger.error).not.toHaveBeenCalled();
		await pubsub.unsubscribe("agent.stream.run-1", cb);
	});

	it("logs rejected async subscriber callbacks without stopping delivery", async () => {
		const basin = new FakeBasin();
		const logger = createTestLogger();
		const pubsub = createPubSub(basin, logger);
		const cb: EventCallback = async () => {
			throw new Error("callback failed");
		};

		await pubsub.subscribe("agent.stream.run-1", cb);
		await pubsub.publish("agent.stream.run-1", event(0));
		await waitFor(() => logger.error.mock.calls.length === 1);

		expect(logger.error).toHaveBeenCalledWith(
			"[S2PubSub] Subscriber callback failed for agent.stream.run-1",
			expect.objectContaining({ message: "callback failed" }),
		);
		await pubsub.unsubscribe("agent.stream.run-1", cb);
	});

	it("rejects malformed persisted records instead of replaying empty events", async () => {
		const basin = new FakeBasin();
		const pubsub = createPubSub(basin);
		basin.stream("mastra/durable/agent.stream.run-1").records.push({
			seqNum: 0,
			body: "not-json",
			headers: [],
			timestamp: new Date(),
		});

		await expect(pubsub.getHistory("agent.stream.run-1", 0)).rejects.toThrow(
			"Invalid S2PubSub record in agent.stream.run-1 at seqNum 0",
		);
	});

	it("rejects a replay whose requested records were trimmed", async () => {
		const basin = new FakeBasin();
		const publisher = createPubSub(basin);
		const subscriber = createPubSub(basin);
		const stream = basin.stream("mastra/durable/agent.stream.run-1");

		await publisher.publish("agent.stream.run-1", event(0));
		await publisher.publish("agent.stream.run-1", event(1));
		stream.trimPoint = 1;

		await expect(publisher.getHistory("agent.stream.run-1", 0)).rejects.toThrow(
			"expected seqNum 0, received 1",
		);
		await expect(
			subscriber.subscribeFromOffset("agent.stream.run-1", 0, () => {}),
		).rejects.toThrow("expected seqNum 0, received 1");
	});

	it("cancels the S2 session after the final unsubscribe", async () => {
		const basin = new FakeBasin();
		const publisher = createPubSub(basin);
		const subscriber = createPubSub(basin);
		const received: Event[] = [];
		const cb: EventCallback = (value) => received.push(value);
		const stream = basin.stream("mastra/durable/agent.stream.run-1");

		await subscriber.subscribe("agent.stream.run-1", cb);
		expect(stream.sessions.size).toBe(1);
		await subscriber.unsubscribe("agent.stream.run-1", cb);
		expect(stream.sessions.size).toBe(0);
		await publisher.publish("agent.stream.run-1", event(0));
		await new Promise((resolve) => setTimeout(resolve, 20));

		expect(received).toHaveLength(0);
	});

	it("logs clearTopic failures without rejecting", async () => {
		const basin = new FakeBasin();
		const logger = createTestLogger();
		const pubsub = createPubSub(basin, logger);
		basin.deleteError = new Error("delete failed");

		await expect(
			pubsub.clearTopic("agent.stream.run-1"),
		).resolves.toBeUndefined();
		expect(logger.error).toHaveBeenCalledWith(
			"[S2PubSub] Failed to clear topic agent.stream.run-1",
			expect.objectContaining({ message: "delete failed" }),
		);
	});

	it("rejects consumer groups for S2-backed topics", async () => {
		const pubsub = createPubSub(new FakeBasin());
		await expect(
			pubsub.subscribe("agent.stream.run-1", () => {}, { group: "workers" }),
		).rejects.toThrow("does not support consumer groups");
	});
});
