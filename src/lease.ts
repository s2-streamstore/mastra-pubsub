import { Buffer } from "node:buffer";
import { randomBytes } from "node:crypto";
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
} from "@s2-dev/streamstore";

type Logger = CachingPubSubOptions["logger"];

/** Mastra's `AGENT_THREAD_STREAM_TOPIC_PREFIX`. */
const THREAD_TOPIC_PREFIX = "agent.thread-stream.";

/** Header marking a legacy or oversized-owner lease state record. */
const LEASE_HEADER_NAME = "mastra-lease";

/** Maximum CAS attempts per operation. */
const CAS_ATTEMPTS = 3;

/** Records scanned for legacy state before falling back to a time window. */
const LEGACY_TAIL_LOOKBACK_RECORDS = 500;

/** A token that this provider never installs, used to observe the current token. */
const TOKEN_PROBE = "!";

/** Inline token binary layout. Base64url expands the 27-byte maximum to 36 bytes. */
const TOKEN_MAGIC = 0xa5;
const TOKEN_KIND_UUID = 1;
const TOKEN_KIND_UTF8 = 2;
const TOKEN_EXPIRY_OFFSET = 2;
const TOKEN_EXPIRY_BYTES = 6;
const TOKEN_NONCE_OFFSET = TOKEN_EXPIRY_OFFSET + TOKEN_EXPIRY_BYTES;
const TOKEN_NONCE_BYTES = 3;
const TOKEN_OWNER_OFFSET = TOKEN_NONCE_OFFSET + TOKEN_NONCE_BYTES;
const MAX_INLINE_OWNER_BYTES = 16;
const MAX_INLINE_TOKEN_BYTES = TOKEN_OWNER_OFFSET + MAX_INLINE_OWNER_BYTES;
const UUID_PATTERN =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/**
 * Longest TTL this provider supports.
 *
 * Inline lease tokens can represent longer expiries, but legacy and oversized
 * owner IDs use the bounded record fallback. One limit keeps both formats
 * interoperable and preserves the existing operational contract.
 */
export const MAX_LEASE_TTL_MS = 60_000;

/**
 * Legacy state older than this cannot still be active because every supported
 * TTL is at most {@link MAX_LEASE_TTL_MS}.
 */
const LEGACY_LOOKBACK_MS = MAX_LEASE_TTL_MS * 4;

interface LeaseState {
	readonly owner: string | null;
	readonly expiresAt: number;
	/** Fencing token guarding the next update. */
	readonly token: string;
}

interface LeaseSnapshot {
	readonly token: string;
	readonly lease?: LeaseState;
}

interface LeaseUpdate {
	readonly records: readonly AppendRecord[];
}

type SwapResult =
	| { readonly swapped: true }
	| { readonly swapped: false; readonly actualToken: string };

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

/** Parse record-backed lease state, treating invalid data as no lease. */
function decodeLeaseRecord(body: string): LeaseState | undefined {
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

/** Encode lease state as a tagged compatibility record. */
function leaseRecord(state: LeaseState): AppendRecord {
	return AppendRecord.string({
		body: JSON.stringify(state),
		headers: [[LEASE_HEADER_NAME, "1"]],
	});
}

/** Convert a canonical UUID to its compact 16-byte representation. */
function uuidBytes(owner: string): Buffer | undefined {
	if (!UUID_PATTERN.test(owner)) return undefined;
	return Buffer.from(owner.replaceAll("-", ""), "hex");
}

/** Convert compact UUID bytes back to the exact canonical owner form. */
function uuidFromBytes(bytes: Buffer): string {
	const hex = bytes.toString("hex");
	return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(
		12,
		16,
	)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Encode the complete lease into an S2 fencing token.
 *
 * S2 limits tokens to 36 UTF-8 bytes. A 27-byte binary payload base64url
 * encodes to exactly 36 bytes and fits a UUID (or a short UTF-8 owner), expiry,
 * format marker, and nonce.
 */
function encodeInlineLease(
	owner: string,
	expiresAt: number,
): LeaseState | undefined {
	const compactUuid = uuidBytes(owner);
	const utf8Owner = compactUuid ? undefined : Buffer.from(owner, "utf8");
	const ownerBytes = compactUuid ?? utf8Owner;
	if (
		!ownerBytes ||
		ownerBytes.length === 0 ||
		ownerBytes.length > MAX_INLINE_OWNER_BYTES ||
		(!compactUuid && ownerBytes.toString("utf8") !== owner)
	) {
		return undefined;
	}

	const bytes = Buffer.alloc(TOKEN_OWNER_OFFSET + ownerBytes.length);
	bytes[0] = TOKEN_MAGIC;
	bytes[1] = compactUuid ? TOKEN_KIND_UUID : TOKEN_KIND_UTF8;
	bytes.writeUIntBE(
		Math.max(0, Math.floor(expiresAt)),
		TOKEN_EXPIRY_OFFSET,
		TOKEN_EXPIRY_BYTES,
	);
	randomBytes(TOKEN_NONCE_BYTES).copy(bytes, TOKEN_NONCE_OFFSET);
	ownerBytes.copy(bytes, TOKEN_OWNER_OFFSET);

	return {
		owner,
		expiresAt: Math.floor(expiresAt),
		token: bytes.toString("base64url"),
	};
}

/** Decode lease state embedded in a fencing token. */
function decodeInlineLease(token: string): LeaseState | undefined {
	if (!token || token.length > 36) return undefined;
	let bytes: Buffer;
	try {
		bytes = Buffer.from(token, "base64url");
	} catch {
		return undefined;
	}
	if (
		bytes.toString("base64url") !== token ||
		bytes.length <= TOKEN_OWNER_OFFSET ||
		bytes.length > MAX_INLINE_TOKEN_BYTES ||
		bytes[0] !== TOKEN_MAGIC
	) {
		return undefined;
	}

	const kind = bytes[1];
	const ownerBytes = bytes.subarray(TOKEN_OWNER_OFFSET);
	let owner: string;
	if (kind === TOKEN_KIND_UUID) {
		if (ownerBytes.length !== MAX_INLINE_OWNER_BYTES) return undefined;
		owner = uuidFromBytes(ownerBytes);
	} else if (kind === TOKEN_KIND_UTF8) {
		owner = ownerBytes.toString("utf8");
		if (
			!owner ||
			!Buffer.from(owner, "utf8").equals(ownerBytes) ||
			ownerBytes.length > MAX_INLINE_OWNER_BYTES
		) {
			return undefined;
		}
	} else {
		return undefined;
	}

	return {
		owner,
		expiresAt: bytes.readUIntBE(TOKEN_EXPIRY_OFFSET, TOKEN_EXPIRY_BYTES),
		token,
	};
}

/** Build a token-only update when possible, otherwise a compatible record pair. */
function leaseUpdate(owner: string, ttlMs: number): LeaseUpdate {
	const expiresAt = Math.floor(Date.now() + ttlMs);
	const inline = encodeInlineLease(owner, expiresAt);
	if (inline) {
		return {
			records: [AppendRecord.fence(inline.token)],
		};
	}

	const lease = {
		owner,
		expiresAt,
		token: `~${randomToken(16)}`,
	} satisfies LeaseState;
	return {
		records: [AppendRecord.fence(lease.token), leaseRecord(lease)],
	};
}

/**
 * Distributed S2 leases stored in the thread stream they coordinate.
 *
 * For canonical UUIDs and owner IDs up to 16 UTF-8 bytes, the fencing token is
 * the complete lease register: owner, expiry, and a nonce. Conditional-append
 * failures return the current token, so acquisition and renewal do not read
 * event records or retain process-local state.
 *
 * Longer custom owner IDs and tokens written by older versions use tagged
 * state records as a compatibility fallback. Event readers skip both those
 * records and S2 command records via {@link isControlRecord}.
 */
export class S2LeaseProvider implements LeaseProvider {
	/** Whether a clamped TTL has already been reported. */
	private warnedAboutTtl = false;

	constructor(
		private readonly basin: S2Basin,
		private readonly streamPrefix: string,
		private readonly logger?: Logger,
	) {}

	/** Clamp a TTL to the supported maximum, reporting the first clamp. */
	private ttl(ttlMs: number): number {
		if (!Number.isFinite(ttlMs) || ttlMs < 0) return 0;
		if (ttlMs <= MAX_LEASE_TTL_MS) return ttlMs;
		if (!this.warnedAboutTtl) {
			this.warnedAboutTtl = true;
			const message = `[S2LeaseProvider] Clamping lease TTL ${ttlMs}ms to ${MAX_LEASE_TTL_MS}ms. Lower MASTRA_AGENT_THREAD_LEASE_TTL_MS to keep renewals ahead of expiry.`;
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
		// The S2 fencing token starts empty, so a free lease is one append.
		const initial = await this.swap(key, "", leaseUpdate(owner, ttl).records);
		if (initial.swapped) return { acquired: true, owner };

		let current = await this.snapshotFromToken(key, initial.actualToken);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const holder = holderOf(current.lease);
			if (holder && holder !== owner) {
				return { acquired: false, owner: holder };
			}
			// Empty, expired, or already ours: replace exactly what S2 returned.
			const desired = leaseUpdate(owner, ttl);
			const result = await this.swap(key, current.token, desired.records);
			if (result.swapped) return { acquired: true, owner };
			current = await this.snapshotFromToken(key, result.actualToken);
		}

		// Name the winner after repeated CAS conflicts.
		return { acquired: false, owner: holderOf(current.lease) };
	}

	async getLeaseOwner(key: string): Promise<string | undefined> {
		const current = await this.currentLease(key);
		return holderOf(current.lease);
	}

	async releaseLease(key: string, owner: string): Promise<void> {
		let current = await this.currentLease(key);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			if (holderOf(current.lease) !== owner) return;
			const result = await this.swap(key, current.token, [
				AppendRecord.fence(""),
			]);
			if (result.swapped) return;
			current = await this.snapshotFromToken(key, result.actualToken);
		}
	}

	async renewLease(
		key: string,
		owner: string,
		ttlMs: number,
	): Promise<boolean> {
		const ttl = this.ttl(ttlMs);
		let current = await this.currentLease(key);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			if (holderOf(current.lease) !== owner) return false;
			const desired = leaseUpdate(owner, ttl);
			const result = await this.swap(key, current.token, desired.records);
			if (result.swapped) return true;
			current = await this.snapshotFromToken(key, result.actualToken);
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
		let current = await this.currentLease(key);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			if (holderOf(current.lease) !== fromOwner) return false;
			const desired = leaseUpdate(toOwner, ttl);
			const result = await this.swap(key, current.token, desired.records);
			if (result.swapped) return true;
			current = await this.snapshotFromToken(key, result.actualToken);
		}
		return false;
	}

	/** The stream carrying this key's thread events. */
	private stream(key: string): S2Stream {
		return this.basin.stream(`${this.streamPrefix}${threadTopic(key)}`);
	}

	/**
	 * Observe the current fencing token without reading stream records.
	 *
	 * TOKEN_PROBE is outside the provider's token formats, so this append always
	 * fails its condition and S2 returns the actual token without side effects.
	 */
	private async observeToken(key: string): Promise<string> {
		const result = await this.swap(key, TOKEN_PROBE, [
			AppendRecord.fence(TOKEN_PROBE),
		]);
		return result.swapped ? TOKEN_PROBE : result.actualToken;
	}

	/** Resolve the current lease, decoding the fencing token whenever possible. */
	private async currentLease(key: string): Promise<LeaseSnapshot> {
		return await this.snapshotFromToken(key, await this.observeToken(key));
	}

	/** Resolve one token, consulting records only for legacy/oversized formats. */
	private async snapshotFromToken(
		key: string,
		token: string,
	): Promise<LeaseSnapshot> {
		if (!token) return { token };
		const inline = decodeInlineLease(token);
		if (inline) return { token, lease: inline };

		const legacy = await this.readRecordBackedLease(key);
		return {
			token,
			lease: legacy?.token === token ? legacy : undefined,
		};
	}

	/** Locate legacy record-backed state within a fixed tail snapshot. */
	private async readRecordBackedLease(
		key: string,
	): Promise<LeaseState | undefined> {
		const stream = this.stream(key);
		let targetTail: number;
		try {
			targetTail = (await stream.checkTail()).tail.seqNum;
		} catch (error) {
			if (isGone(error)) return undefined;
			throw error;
		}

		const recentStart = Math.max(0, targetTail - LEGACY_TAIL_LOOKBACK_RECORDS);
		const recent = await this.scanRecords(
			key,
			{ from: { seqNum: recentStart } },
			targetTail,
		);
		if (recent || recentStart === 0) return recent;

		return await this.scanRecords(
			key,
			{
				from: { timestamp: Date.now() - LEGACY_LOOKBACK_MS },
				clamp: true,
			},
			targetTail,
		);
	}

	/** Scan a compatibility range up to, but never beyond, a captured tail. */
	private async scanRecords(
		key: string,
		start: ReadStart,
		targetTail: number,
	): Promise<LeaseState | undefined> {
		const stream = this.stream(key);
		let lease: LeaseState | undefined;
		let cursor: number | undefined;
		try {
			while (cursor === undefined || cursor < targetTail) {
				const batch = await stream.read({
					start: cursor === undefined ? start : { from: { seqNum: cursor } },
				});
				let advanced = false;
				for (const record of batch.records) {
					if (record.seqNum >= targetTail) break;
					if (isLeaseRecord(record)) {
						lease = decodeLeaseRecord(record.body);
					}
					cursor = record.seqNum + 1;
					advanced = true;
				}
				if (!advanced) break;
			}
		} catch (error) {
			if (isGone(error) || error instanceof RangeNotSatisfiableError) {
				return lease;
			}
			throw error;
		}
		return lease;
	}

	/** Atomically replace an exact fencing token, returning the winner on loss. */
	private async swap(
		key: string,
		expectedToken: string,
		records: readonly AppendRecord[],
	): Promise<SwapResult> {
		try {
			await this.stream(key).append(
				AppendInput.create(records, { fencingToken: expectedToken }),
			);
			return { swapped: true };
		} catch (error) {
			if (error instanceof FencingTokenMismatchError) {
				return { swapped: false, actualToken: error.expectedFencingToken };
			}
			throw error;
		}
	}
}

/** Whether a record holds compatibility lease state. */
function isLeaseRecord(record: Pick<ReadRecord<"string">, "headers">): boolean {
	return (record.headers ?? []).some(([name]) => name === LEASE_HEADER_NAME);
}
