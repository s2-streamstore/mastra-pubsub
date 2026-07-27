import type { CachingPubSubOptions, LeaseProvider } from "@mastra/core/events";
import type {
	ReadRecord,
	ReadStart,
	S2Basin,
	S2Stream,
} from "@s2-dev/streamstore";
import {
	AppendInput,
	AppendRecord,
	FencingTokenMismatchError,
	RangeNotSatisfiableError,
	randomToken,
	S2Error,
	SeqNumMismatchError,
} from "@s2-dev/streamstore";

type Logger = CachingPubSubOptions["logger"];

/** Mastra's `AGENT_THREAD_STREAM_TOPIC_PREFIX`. */
const THREAD_TOPIC_PREFIX = "agent.thread-stream.";

/** Header marking a record as lease state. */
const LEASE_HEADER_NAME = "mastra-lease";

/** Maximum CAS attempts per operation. */
const CAS_ATTEMPTS = 3;

/** Records scanned back from the tail before falling back to time. */
const TAIL_LOOKBACK_RECORDS = 500;

/**
 * Longest TTL this provider supports, and the basis for {@link LOOKBACK_MS}.
 *
 * Reads conclude "no lease" when the lookback window holds no lease state, on
 * the grounds that anything older has expired. That holds only while every
 * writer's TTL fits the window, so longer TTLs are clamped rather than allowed
 * to hide unexpired state behind a burst of events.
 */
export const MAX_LEASE_TTL_MS = 60_000;

/**
 * Time window scanned for lease state when the record lookback misses.
 *
 * A fixed multiple of the maximum TTL, not of the caller's TTL: the window has
 * to cover whatever TTL the *incumbent* used, which the reader cannot know. The
 * surplus absorbs clock skew between writers and readers.
 */
const LOOKBACK_MS = MAX_LEASE_TTL_MS * 4;

interface LeaseState {
	readonly owner: string | null;
	readonly expiresAt: number;
	/** Fencing token guarding the next update. */
	readonly token: string;
}

/** Precondition for replacing the current lease state. */
type LeaseCondition =
	| { readonly fencingToken: string }
	| { readonly matchSeqNum: number };

/** The topic Mastra publishes a lease key's thread events to. */
export function threadTopic(key: string): string {
	return `${THREAD_TOPIC_PREFIX}${encodeURIComponent(key)}`;
}

/** Whether a record is an S2 command: exactly one header, with an empty name. */
function isCommandRecord(
	record: Pick<ReadRecord<"string">, "headers">,
): boolean {
	const headers = record.headers ?? [];
	return headers.length === 1 && headers[0]?.[0] === "";
}

/** Whether a record is lease state or an S2 command, rather than an event. */
export function isControlRecord(
	record: Pick<ReadRecord<"string">, "headers">,
): boolean {
	return isCommandRecord(record) || isLeaseRecord(record);
}

/** Whether the stream is missing or being deleted. */
function isGone(error: unknown): boolean {
	return (
		error instanceof S2Error &&
		(error.status === 404 || error.code === "stream_deletion_pending")
	);
}

/** Parse lease state, treating invalid data as no lease. */
function decodeLease(body: string): LeaseState | undefined {
	try {
		const value: unknown = JSON.parse(body);
		if (value === null || typeof value !== "object") return undefined;
		const { owner, expiresAt, token } = value as Record<string, unknown>;
		if (owner !== null && typeof owner !== "string") return undefined;
		if (typeof expiresAt !== "number") return undefined;
		if (typeof token !== "string") return undefined;
		return { owner, expiresAt, token };
	} catch {
		return undefined;
	}
}

/** The owner of an unexpired lease. */
function holderOf(lease: LeaseState | undefined): string | undefined {
	if (!lease?.owner || lease.expiresAt <= Date.now()) return undefined;
	return lease.owner;
}

/** Encode lease state as a tagged record. */
function leaseRecord(state: LeaseState) {
	return AppendRecord.string({
		body: JSON.stringify(state),
		headers: [[LEASE_HEADER_NAME, "1"]],
	});
}

/**
 * Distributed S2 leases stored in the thread stream they coordinate.
 *
 * A lease key identifies both a thread and its topic, so lease state lives in
 * that thread's stream rather than one of its own; readers skip it (see
 * {@link isControlRecord}). Updates are conditional on a fencing token so event
 * appends never conflict with them, and each change of owner rotates the token
 * to fence the previous owner off.
 *
 * The stream is the only state, so nothing is cached between calls. Expiry is
 * wall-clock, so keep process clocks synchronized and TTLs at or under
 * {@link MAX_LEASE_TTL_MS}; longer ones are clamped.
 */
export class S2LeaseProvider implements LeaseProvider {
	/** Whether a clamped TTL has already been reported. */
	private warnedAboutTtl = false;

	constructor(
		private readonly basin: S2Basin,
		private readonly streamPrefix: string,
		private readonly logger?: Logger,
	) {}

	/**
	 * Clamp a TTL to the readable maximum, reporting the first clamp.
	 *
	 * Clamping rather than throwing is deliberate: Mastra treats a failed
	 * `acquireLease` as success, so rejecting an over-long TTL would hand every
	 * caller the same lease. A short TTL only costs liveness — the fencing token
	 * still admits one writer at a time.
	 */
	private ttl(ttlMs: number): number {
		if (!Number.isFinite(ttlMs) || ttlMs < 0) return 0;
		if (ttlMs <= MAX_LEASE_TTL_MS) return ttlMs;
		if (!this.warnedAboutTtl) {
			this.warnedAboutTtl = true;
			const message = `[S2LeaseProvider] Clamping lease TTL ${ttlMs}ms to ${MAX_LEASE_TTL_MS}ms, the longest this provider can read back reliably. Lower MASTRA_AGENT_THREAD_LEASE_TTL_MS to keep renewals ahead of expiry.`;
			if (this.logger) this.logger.warn(message);
			else console.warn(message);
		}
		return MAX_LEASE_TTL_MS;
	}

	async acquireLease(
		key: string,
		owner: string,
		ttlMs: number,
	): Promise<{ acquired: boolean; owner?: string }> {
		const ttl = this.ttl(ttlMs);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const { lease, tail } = await this.read(key);
			const holder = holderOf(lease);
			// Re-acquiring an owned lease refreshes its TTL.
			if (holder && holder !== owner) return { acquired: false, owner: holder };
			if (await this.claim(key, owner, ttl, conditionFor(lease, tail))) {
				return { acquired: true, owner };
			}
		}
		// Name the winner after repeated CAS conflicts.
		const { lease } = await this.read(key);
		return { acquired: false, owner: holderOf(lease) };
	}

	async getLeaseOwner(key: string): Promise<string | undefined> {
		const { lease } = await this.read(key);
		return holderOf(lease);
	}

	async releaseLease(key: string, owner: string): Promise<void> {
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const { lease } = await this.read(key);
			if (!lease || holderOf(lease) !== owner) return;
			const released = { owner: null, expiresAt: 0, token: lease.token };
			if (await this.update(key, released)) return;
		}
	}

	async renewLease(
		key: string,
		owner: string,
		ttlMs: number,
	): Promise<boolean> {
		const ttl = this.ttl(ttlMs);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const { lease } = await this.read(key);
			if (!lease || holderOf(lease) !== owner) return false;
			// That token proves we still hold it: a takeover would have rotated it.
			if (await this.update(key, leaseFor(owner, ttl, lease.token))) {
				return true;
			}
		}
		return false;
	}

	async transferLease(
		key: string,
		fromOwner: string,
		toOwner: string,
		ttlMs: number,
	): Promise<boolean> {
		const ttl = this.ttl(ttlMs);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const { lease, tail } = await this.read(key);
			if (!lease || holderOf(lease) !== fromOwner) return false;
			if (await this.claim(key, toOwner, ttl, conditionFor(lease, tail))) {
				return true;
			}
		}
		return false;
	}

	/** The stream carrying this key's thread events. */
	private stream(key: string): S2Stream {
		return this.basin.stream(`${this.streamPrefix}${threadTopic(key)}`);
	}

	/** Locate the newest lease state and the stream tail. */
	private async read(
		key: string,
	): Promise<{ tail: number; lease?: LeaseState }> {
		// An active lease renews every ttl/3, so its state is usually within a
		// bounded window of the tail.
		const recent = await this.scan(key, {
			from: { tailOffset: TAIL_LOOKBACK_RECORDS },
		});
		if (recent.lease || recent.from === 0) return recent;
		// Otherwise scan by time: expiresAt is writeTime + ttl, so state older
		// than this window belongs to an expired lease.
		return await this.scan(key, {
			from: { timestamp: Date.now() - LOOKBACK_MS },
			clamp: true,
		});
	}

	/** Read from `start` to the tail, keeping the newest lease state seen. */
	private async scan(
		key: string,
		start: ReadStart,
	): Promise<{ tail: number; from: number; lease?: LeaseState }> {
		const stream = this.stream(key);
		let lease: LeaseState | undefined;
		let from = 0;
		let cursor: number | undefined;
		let tail = 0;
		try {
			while (true) {
				const batch = await stream.read({
					start: cursor === undefined ? start : { from: { seqNum: cursor } },
				});
				if (cursor === undefined) from = batch.records[0]?.seqNum ?? 0;
				for (const record of batch.records) {
					if (isLeaseRecord(record)) lease = decodeLease(record.body);
					cursor = record.seqNum + 1;
				}
				tail = Math.max(batch.tail?.seqNum ?? 0, cursor ?? 0);
				// Read to the tail; events behind the cursor can hide newer state.
				if (batch.records.length === 0 || cursor === undefined) break;
				if (cursor >= tail) break;
			}
		} catch (error) {
			if (isGone(error)) return { tail: 0, from: 0 };
			if (error instanceof RangeNotSatisfiableError) {
				return { tail: error.tail?.seqNum ?? tail, from, lease };
			}
			throw error;
		}
		return { tail, from, lease };
	}

	/** Take ownership, rotating the token to fence off the previous owner. */
	private async claim(
		key: string,
		owner: string,
		ttlMs: number,
		condition: LeaseCondition,
	): Promise<boolean> {
		const state = leaseFor(owner, ttlMs, randomToken(16));
		return await this.append(
			key,
			[AppendRecord.fence(state.token), leaseRecord(state)],
			condition,
		);
	}

	/** Rewrite the state under the token it already carries. */
	private async update(key: string, state: LeaseState): Promise<boolean> {
		return await this.append(key, [leaseRecord(state)], {
			fencingToken: state.token,
		});
	}

	/** Append conditionally; `false` means our view of the lease was stale. */
	private async append(
		key: string,
		records: readonly AppendRecord[],
		condition: LeaseCondition,
	): Promise<boolean> {
		try {
			await this.stream(key).append(AppendInput.create(records, condition));
			return true;
		} catch (error) {
			if (
				error instanceof SeqNumMismatchError ||
				error instanceof FencingTokenMismatchError
			) {
				return false;
			}
			throw error;
		}
	}
}

/** State for `owner`, expiring `ttlMs` from now. */
function leaseFor(owner: string, ttlMs: number, token: string): LeaseState {
	return { owner, expiresAt: Date.now() + ttlMs, token };
}

/** Whether a record holds lease state. */
function isLeaseRecord(record: Pick<ReadRecord<"string">, "headers">): boolean {
	return (record.headers ?? []).some(([name]) => name === LEASE_HEADER_NAME);
}

/** Condition on the current token, or on the tail if there is no lease yet. */
function conditionFor(
	lease: LeaseState | undefined,
	tail: number,
): LeaseCondition {
	return lease ? { fencingToken: lease.token } : { matchSeqNum: tail };
}
