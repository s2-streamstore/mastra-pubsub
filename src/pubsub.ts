import { randomUUID } from "node:crypto";
import EventEmitter from "node:events";
import { MastraServerCache } from "@mastra/core/cache";
import type {
	CachingPubSubOptions,
	Event,
	EventCallback,
	LeaseProvider,
	SubscribeOptions,
} from "@mastra/core/events";
import {
	CachingPubSub,
	EventEmitterPubSub,
	type PubSub,
} from "@mastra/core/events";
import type {
	ReadBatch,
	ReadRecord,
	S2Basin,
	S2Endpoints,
	S2EndpointsInit,
	S2Stream,
} from "@s2-dev/streamstore";
import {
	AppendInput,
	AppendRecord,
	RangeNotSatisfiableError,
	S2,
	S2Error,
} from "@s2-dev/streamstore";
import { isControlRecord, S2LeaseProvider } from "./lease.js";

export interface S2PubSubConfig {
	/** An S2 client (takes precedence over `accessToken`). */
	readonly client?: S2;
	/** An S2 access token, used to build a client when `client` is omitted. */
	readonly accessToken?: string;
	/** Basin for durable streams. Requires create-on-append and create-on-read. */
	readonly basin: string;
	/** Endpoint overrides, such as for `s2-lite`. */
	readonly endpoints?: S2Endpoints | S2EndpointsInit;
}

type Logger = CachingPubSubOptions["logger"];

export interface S2PubSubOptions {
	/** Local transport. Defaults to `EventEmitterPubSub`. */
	readonly inner?: PubSub;
	/** S2 stream-name prefix. Defaults to `mastra/durable/`. */
	readonly streamPrefix?: string;
	/**
	 * Only topics with this prefix use S2. Defaults to `agent.`, covering
	 * per-run streams and the per-thread streams that also hold lease state.
	 */
	readonly topicPrefix?: string;
	/** Error logger. Defaults to `console.error`. */
	readonly logger?: Logger;
}

interface Subscription {
	readonly topic: string;
	readonly callback: EventCallback;
	readonly stream: S2Stream;
	readonly abortController: AbortController;
	/** Whether readiness waits for the first replayed record. */
	readonly isReplay: boolean;
	/** Next sequence number, or undefined before the first live record. */
	nextSeqNum: number | undefined;
	/** Current reader, retained for cancellation. */
	activeReader?: ReadableStreamDefaultReader<ReadRecord<"string">>;
	/** Resolves when the subscription is ready. */
	readonly ready: PromiseWithResolvers<void>;
	/** Whether `ready` has settled. */
	readySettled: boolean;
	/** Background read loop. */
	task: Promise<void>;
}

class S2RecordDecodeError extends Error {
	constructor(topic: string, seqNum: number, cause: unknown) {
		super(`Invalid S2PubSub record in ${topic} at seqNum ${seqNum}`, { cause });
		this.name = "S2RecordDecodeError";
	}
}

class S2RecordEncodeError extends TypeError {
	constructor(cause: unknown) {
		super("S2PubSub event could not be serialized as a valid Mastra event", {
			cause,
		});
		this.name = "S2RecordEncodeError";
	}
}

/** Raised when the requested replay position has been trimmed. */
export class S2ReplayGapError extends Error {
	constructor(
		readonly topic: string,
		readonly expectedSeqNum: number,
		readonly actualSeqNum: number,
	) {
		super(
			`S2PubSub replay gap in ${topic}: expected seqNum ${expectedSeqNum}, received ${actualSeqNum}`,
		);
		this.name = "S2ReplayGapError";
	}
}

/** Whether a stream is missing or being deleted. */
function isGone(error: unknown): boolean {
	return (
		error instanceof S2Error &&
		(error.status === 404 || error.code === "stream_deletion_pending")
	);
}

/** Whether an operation was aborted. */
function isAbortError(error: unknown): boolean {
	return error instanceof Error && error.name === "AbortError";
}

/** Serialize an event, rejecting anything that would poison its persisted stream. */
function encodeEvent(event: Event): string {
	try {
		const body = JSON.stringify(event);
		if (body === undefined) {
			throw new TypeError("event did not produce JSON");
		}
		// JSON.stringify can silently omit required values such as `data:
		// undefined`. Validate the exact bytes before appending so every record we
		// write is guaranteed to be readable by this adapter.
		decodeEvent(body);
		return body;
	} catch (error) {
		throw new S2RecordEncodeError(error);
	}
}

/** Whether a value is a plain JSON object. */
function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function logError(logger: Logger, message: string, error: unknown): void {
	if (logger) {
		logger.error(message, error);
	} else {
		console.error(message, error);
	}
}

/** Decode and validate one serialized Mastra event. */
function decodeEvent(body: string): Omit<Event, "index"> {
	const decoded: unknown = JSON.parse(body);
	if (!isRecord(decoded)) {
		throw new TypeError("record body must be a JSON object");
	}
	if (
		typeof decoded.id !== "string" ||
		typeof decoded.type !== "string" ||
		typeof decoded.runId !== "string" ||
		!("data" in decoded) ||
		(typeof decoded.createdAt !== "string" &&
			typeof decoded.createdAt !== "number") ||
		(decoded.deliveryAttempt !== undefined &&
			typeof decoded.deliveryAttempt !== "number")
	) {
		throw new TypeError("record body is not a valid Mastra event");
	}

	const createdAt = new Date(decoded.createdAt);
	if (Number.isNaN(createdAt.getTime())) {
		throw new TypeError("record createdAt is invalid");
	}

	return {
		id: decoded.id,
		type: decoded.type,
		data: decoded.data,
		runId: decoded.runId,
		createdAt,
		deliveryAttempt: decoded.deliveryAttempt ?? 1,
	};
}

/** Decode and validate an event, using `seqNum` as its index. */
function eventFromRecord(
	record: Pick<ReadRecord<"string">, "body" | "seqNum">,
	topic: string,
): Event {
	try {
		return { ...decodeEvent(record.body), index: record.seqNum };
	} catch (error) {
		throw new S2RecordDecodeError(topic, record.seqNum, error);
	}
}

/** Wait for a retry delay or cancellation. */
function abortableDelay(ms: number, signal: AbortSignal): Promise<void> {
	if (signal.aborted) return Promise.resolve();
	return new Promise((resolve) => {
		const timer = setTimeout(finish, ms);
		function finish() {
			clearTimeout(timer);
			signal.removeEventListener("abort", finish);
			resolve();
		}
		signal.addEventListener("abort", finish, { once: true });
	});
}

/** Disabled cache required by the `CachingPubSub` base class. */
class UnusedServerCache extends MastraServerCache {
	constructor() {
		super({ name: "s2-pubsub-unused-cache" });
	}

	private fail(): never {
		throw new Error(
			"S2PubSub invariant violated: Mastra attempted to use the disabled cache path",
		);
	}

	async get(): Promise<never> {
		this.fail();
	}
	async listLength(): Promise<never> {
		this.fail();
	}
	async set(): Promise<never> {
		this.fail();
	}
	async listPush(): Promise<never> {
		this.fail();
	}
	async listFromTo(): Promise<never> {
		this.fail();
	}
	async delete(): Promise<never> {
		this.fail();
	}
	async clear(): Promise<never> {
		this.fail();
	}
	async increment(): Promise<never> {
		this.fail();
	}
}

/**
 * Emitter for the default local transport, without a listener ceiling.
 *
 * Every subscription registers one local listener (see `subscribeAt`) and drops
 * it on unsubscribe, so the count tracks concurrent subscribers per topic —
 * routinely past Node's default of ten when several clients observe one run.
 * That is fan-out, not a leak, and the warning it produces is noise.
 */
function localEmitter(): EventEmitter {
	const emitter = new EventEmitter();
	emitter.setMaxListeners(0);
	return emitter;
}

/** Resolve the configured S2 client. */
function resolveS2Client(config: S2PubSubConfig): S2 {
	if (!config.basin) {
		throw new Error("S2PubSub: `basin` is required");
	}
	if (config.client) return config.client;
	if (config.accessToken) {
		return new S2({
			accessToken: config.accessToken,
			endpoints: config.endpoints,
		});
	}
	throw new Error("S2PubSub: provide either `client` or `accessToken`");
}

/** Durable Mastra PubSub backed by S2 read sessions. */
export class S2PubSub extends CachingPubSub {
	private readonly basin: S2Basin;
	private readonly local: PubSub;
	private readonly streamPrefix: string;
	private readonly topicPrefix: string;
	private readonly s2Logger?: Logger;
	/** Active subscriptions by topic and callback. */
	private readonly subscriptions = new Map<
		string,
		Map<EventCallback, Subscription>
	>();
	/** Lazily created lease provider. */
	private leaseProvider?: S2LeaseProvider;

	constructor(config: S2PubSubConfig, options: S2PubSubOptions = {}) {
		const s2 = resolveS2Client(config);
		// Handles non-durable and local-only events.
		const local =
			options.inner ??
			new EventEmitterPubSub(localEmitter(), {
				logger: options.logger,
			});
		// S2 is the replay store, so the inherited cache is unused.
		super(local, new UnusedServerCache(), { logger: options.logger });
		this.basin = s2.basin(config.basin);
		this.local = local;
		this.streamPrefix = options.streamPrefix ?? "mastra/durable/";
		this.topicPrefix = options.topicPrefix ?? "agent.";
		this.s2Logger = options.logger;
	}

	get supportsNativeBatching(): boolean {
		return false;
	}

	/** Return the S2-backed lease provider. */
	getLeaseProvider(): LeaseProvider {
		// Leases live in the thread streams they coordinate, under this prefix.
		this.leaseProvider ??= new S2LeaseProvider(this.basin, this.streamPrefix);
		return this.leaseProvider;
	}

	/** Append an event to its S2 stream. */
	async publish(
		topic: string,
		event: Omit<Event, "id" | "createdAt" | "index">,
		options?: { localOnly?: boolean },
	): Promise<void> {
		if (!this.shouldPersist(topic) || options?.localOnly) {
			await this.local.publish(topic, event, options);
			return;
		}

		const persisted: Event = {
			...event,
			id: randomUUID(),
			createdAt: new Date(),
			deliveryAttempt: 1,
		};
		await this.stream(topic).append(
			AppendInput.create([
				AppendRecord.string({ body: encodeEvent(persisted) }),
			]),
		);
	}

	/** Follow events appended after the subscription opens. */
	async subscribe(
		topic: string,
		callback: EventCallback,
		options?: SubscribeOptions,
	): Promise<void> {
		if (!this.shouldPersist(topic)) {
			await this.local.subscribe(topic, callback, options);
			return;
		}
		this.assertSupportedOptions(options);
		// Start at the current tail.
		await this.subscribeAt(
			topic,
			undefined,
			false,
			callback,
			this.stream(topic),
		);
	}

	/** Replay from the beginning, then follow live events. */
	async subscribeWithReplay(
		topic: string,
		callback: EventCallback,
	): Promise<void> {
		await this.subscribeFromOffset(topic, 0, callback);
	}

	/** Replay from an offset, then follow live events. */
	async subscribeFromOffset(
		topic: string,
		offset: number,
		callback: EventCallback,
	): Promise<void> {
		if (!this.shouldPersist(topic)) {
			await this.local.subscribeFromOffset(topic, offset, callback);
			return;
		}
		const nextSeqNum = Math.max(0, offset);
		const stream = this.stream(topic);
		// Replay subscriptions wait for their first record before becoming ready.
		const { tail } = await stream.checkTail();
		const isReplay = nextSeqNum < tail.seqNum;
		await this.subscribeAt(topic, nextSeqNum, isReplay, callback, stream);
	}

	async unsubscribe(topic: string, callback: EventCallback): Promise<void> {
		if (!this.shouldPersist(topic)) {
			await this.local.unsubscribe(topic, callback);
			return;
		}
		await this.stopSubscription(topic, callback);
	}

	/**
	 * Read retained events from an offset.
	 *
	 * Offsets are S2 sequence numbers, and the filtered-out lease and command
	 * records consume them too. Resume from `last.index + 1`, not the count.
	 */
	async getHistory(topic: string, offset: number = 0): Promise<Event[]> {
		if (!this.shouldPersist(topic)) {
			return this.local.getHistory(topic, offset);
		}
		const stream = this.stream(topic);
		const events: Event[] = [];
		let cursor = Math.max(0, offset);

		// Read to the tail and reject gaps caused by trimming.
		while (true) {
			let batch: ReadBatch<"string">;
			try {
				batch = await stream.read({ start: { from: { seqNum: cursor } } });
			} catch (error) {
				if (isGone(error)) break;
				// An offset past the tail has no history.
				if (
					error instanceof RangeNotSatisfiableError &&
					error.tail &&
					cursor >= error.tail.seqNum
				) {
					break;
				}
				throw error;
			}
			if (batch.records.length === 0) {
				// An empty batch before the tail indicates a trim gap.
				if (batch.tail && cursor < batch.tail.seqNum) {
					throw new S2ReplayGapError(topic, cursor, batch.tail.seqNum);
				}
				break;
			}
			for (const record of batch.records) {
				if (record.seqNum < cursor) continue;
				if (record.seqNum > cursor) {
					throw new S2ReplayGapError(topic, cursor, record.seqNum);
				}
				cursor = record.seqNum + 1;
				// S2 command records share the stream; they are not events.
				if (isControlRecord(record)) continue;
				events.push(eventFromRecord(record, topic));
			}
			if (batch.tail && cursor >= batch.tail.seqNum) break;
		}
		return events;
	}

	async flush(): Promise<void> {
		// S2 appends are already awaited by publish.
		await this.local.flush();
	}

	/** Stop subscriptions and clear the topic. */
	async clearTopic(topic: string): Promise<void> {
		await this.stopTopic(topic);
		try {
			await this.local.clearTopic(topic);
		} catch (error) {
			logError(
				this.s2Logger,
				`[S2PubSub] Failed to clear local state for ${topic}`,
				error,
			);
		}
		if (!this.shouldPersist(topic)) return;
		try {
			await this.basin.streams.delete({ stream: this.streamName(topic) });
		} catch (error) {
			if (!isGone(error)) {
				logError(
					this.s2Logger,
					`[S2PubSub] Failed to clear topic ${topic}`,
					error,
				);
			}
		}
	}

	/** Stop all active S2 subscriptions. */
	async close(): Promise<void> {
		await Promise.all(
			[...this.subscriptions.keys()].map((topic) => this.stopTopic(topic)),
		);
	}

	/** Whether a topic is S2-backed. */
	private shouldPersist(topic: string): boolean {
		return topic.startsWith(this.topicPrefix);
	}

	/** Return the S2 stream name for a topic. */
	private streamName(topic: string): string {
		return `${this.streamPrefix}${topic}`;
	}

	/** Return the S2 stream for a topic. */
	private stream(topic: string): S2Stream {
		return this.basin.stream(this.streamName(topic));
	}

	/** Reject options unsupported by S2-backed topics. */
	private assertSupportedOptions(options?: SubscribeOptions): void {
		if (options?.group) {
			throw new Error(
				"S2PubSub does not support consumer groups on persisted topics",
			);
		}
	}

	/** Register a callback and start its read loop. */
	private async subscribeAt(
		topic: string,
		nextSeqNum: number | undefined,
		isReplay: boolean,
		callback: EventCallback,
		stream: S2Stream,
	): Promise<void> {
		const existing = this.subscriptions.get(topic)?.get(callback);
		if (existing) {
			await existing.ready.promise;
			return;
		}

		const state: Subscription = {
			topic,
			callback,
			stream,
			abortController: new AbortController(),
			isReplay,
			nextSeqNum,
			ready: Promise.withResolvers<void>(),
			readySettled: false,
			task: Promise.resolve(),
		};
		const byCallback = this.subscriptions.get(topic) ?? new Map();
		this.subscriptions.set(topic, byCallback);
		// Reserve this callback before starting asynchronous registration so
		// concurrent subscribe calls share this state and its readiness promise.
		byCallback.set(callback, state);
		state.task = this.runSubscription(state);
		await state.ready.promise;
	}

	/** Resolve or reject subscription readiness once. */
	private settleReady(state: Subscription, error?: unknown): void {
		if (state.readySettled) return;
		state.readySettled = true;
		if (error === undefined) state.ready.resolve();
		else state.ready.reject(error);
	}

	/** Register local delivery, then read records and reconnect from the cursor. */
	private async runSubscription(state: Subscription): Promise<void> {
		const { signal } = state.abortController;
		let reconnectAttempt = 0;
		try {
			// Persisted-topic subscribers also receive explicit local-only events.
			try {
				await this.local.subscribe(state.topic, state.callback);
			} catch (error) {
				this.settleReady(state, error);
				return;
			}

			while (!signal.aborted) {
				let reader: Subscription["activeReader"];
				try {
					// New live subscriptions start at the tail; reconnects resume exactly.
					const from =
						state.nextSeqNum === undefined
							? { tailOffset: 0 }
							: { seqNum: state.nextSeqNum };
					const session = await state.stream.readSession(
						{ start: { from } },
						{ signal },
					);
					reader = session.getReader();
					state.activeReader = reader;
					// Replay subscriptions become ready after their first record.
					if (!state.isReplay) this.settleReady(state);

					while (!signal.aborted) {
						const result = await reader.read();
						if (result.done) break;
						const record = result.value;
						// The first live record establishes the expected position.
						if (state.nextSeqNum !== undefined) {
							if (record.seqNum < state.nextSeqNum) continue;
							// A forward jump indicates trimmed records.
							if (record.seqNum > state.nextSeqNum) {
								throw new S2ReplayGapError(
									state.topic,
									state.nextSeqNum,
									record.seqNum,
								);
							}
						}
						state.nextSeqNum = record.seqNum + 1;
						reconnectAttempt = 0;
						// S2 command records share the stream; they are not events.
						if (!isControlRecord(record)) {
							this.invokeSubscriber(
								state,
								eventFromRecord(record, state.topic),
							);
						}
						this.settleReady(state);
					}
				} catch (error) {
					if (signal.aborted || isAbortError(error)) break;
					// Permanent replay errors stop the subscription.
					if (
						error instanceof S2RecordDecodeError ||
						error instanceof S2ReplayGapError
					) {
						if (!state.readySettled) {
							this.settleReady(state, error);
							return;
						}
						logError(
							this.s2Logger,
							`[S2PubSub] Persisted stream is not replayable for ${state.topic}; subscription stopped`,
							error,
						);
						return;
					}
					if (isGone(error)) return;
					// Fail before readiness; reconnect after readiness.
					if (!state.readySettled) {
						this.settleReady(state, error);
						return;
					}
					logError(
						this.s2Logger,
						`[S2PubSub] Read session failed for ${state.topic}; reconnecting from ${
							state.nextSeqNum === undefined
								? "the live tail"
								: `seqNum ${state.nextSeqNum}`
						}`,
						error,
					);
				} finally {
					if (state.activeReader === reader) state.activeReader = undefined;
					if (reader) {
						try {
							reader.releaseLock();
						} catch {
							// Cancellation may release it first.
						}
					}
				}

				if (signal.aborted) break;
				// Exponential backoff capped at five seconds.
				const delayMs = Math.min(250 * 2 ** reconnectAttempt, 5_000);
				reconnectAttempt++;
				await abortableDelay(delayMs, signal);
			}
		} finally {
			if (!state.readySettled) {
				this.settleReady(
					state,
					new Error(
						`S2 read session stopped before subscribing to ${state.topic}`,
					),
				);
			}
			this.removeSubscription(state);
			await this.local
				.unsubscribe(state.topic, state.callback)
				.catch((error) => {
					logError(
						this.s2Logger,
						`[S2PubSub] Failed to remove local subscription for ${state.topic}`,
						error,
					);
				});
			await state.stream.close().catch((error) => {
				logError(
					this.s2Logger,
					`[S2PubSub] Failed to close read session for ${state.topic}`,
					error,
				);
			});
		}
	}

	/** Invoke a subscriber without blocking later records. */
	private invokeSubscriber(state: Subscription, event: Event): void {
		const onError = (error: unknown) => {
			logError(
				this.s2Logger,
				`[S2PubSub] Subscriber callback failed for ${state.topic}`,
				error,
			);
		};
		try {
			void Promise.resolve(state.callback(event)).catch(onError);
		} catch (error) {
			onError(error);
		}
	}

	/** Remove a subscription without deleting a newer replacement. */
	private removeSubscription(state: Subscription): void {
		const topicSubscriptions = this.subscriptions.get(state.topic);
		if (topicSubscriptions?.get(state.callback) !== state) return;
		topicSubscriptions.delete(state.callback);
		if (topicSubscriptions.size === 0) {
			this.subscriptions.delete(state.topic);
		}
	}

	/** Stop one subscription and wait for cleanup. */
	private async stopSubscription(
		topic: string,
		callback: EventCallback,
	): Promise<void> {
		const state = this.subscriptions.get(topic)?.get(callback);
		if (!state) {
			await this.local.unsubscribe(topic, callback);
			return;
		}
		this.removeSubscription(state);
		state.abortController.abort();
		if (state.activeReader) {
			await state.activeReader
				.cancel("S2PubSub subscription ended")
				.catch(() => {});
		}
		await state.task;
	}

	/** Stop every subscription on a topic. */
	private async stopTopic(topic: string): Promise<void> {
		const states = [...(this.subscriptions.get(topic)?.values() ?? [])];
		await Promise.all(
			states.map((state) => this.stopSubscription(topic, state.callback)),
		);
	}
}
