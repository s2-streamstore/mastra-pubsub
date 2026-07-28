import { Buffer } from "node:buffer";
import { randomBytes } from "node:crypto";
import type { LeaseProvider } from "@mastra/core/events";
import type { ReadRecord, S2Basin, S2Stream } from "@s2-dev/streamstore";
import {
	AppendInput,
	AppendRecord,
	FencingTokenMismatchError,
} from "@s2-dev/streamstore";

/** Mastra's `AGENT_THREAD_STREAM_TOPIC_PREFIX`. */
const THREAD_TOPIC_PREFIX = "agent.thread-stream.";

/** Maximum CAS attempts per operation. */
const CAS_ATTEMPTS = 3;

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
const MAX_OWNER_BYTES = 16;
const MAX_TOKEN_BYTES = TOKEN_OWNER_OFFSET + MAX_OWNER_BYTES;
const MAX_EXPIRY = 2 ** (TOKEN_EXPIRY_BYTES * 8) - 1;
const UUID_PATTERN =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

interface LeaseState {
	readonly owner: string;
	readonly expiresAt: number;
	/** Fencing token guarding the next update. */
	readonly token: string;
}

interface LeaseSnapshot {
	readonly token: string;
	readonly lease?: LeaseState;
}

type SwapResult =
	| { readonly swapped: true }
	| { readonly swapped: false; readonly actualToken: string };

/** The topic Mastra publishes a lease key's thread events to. */
export function threadTopic(key: string): string {
	return `${THREAD_TOPIC_PREFIX}${encodeURIComponent(key)}`;
}

/** Whether a record is an S2 command rather than an event. */
export function isControlRecord(
	record: Pick<ReadRecord<"string">, "headers">,
): boolean {
	const headers = record.headers ?? [];
	return headers.length === 1 && headers[0]?.[0] === "";
}

/** The owner of an unexpired lease. */
function holderOf(lease: LeaseState | undefined): string | undefined {
	if (!lease || lease.expiresAt <= Date.now()) return undefined;
	return lease.owner;
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
function encodeLease(owner: string, expiresAt: number): LeaseState {
	const compactUuid = uuidBytes(owner);
	const utf8Owner = compactUuid ? undefined : Buffer.from(owner, "utf8");
	const ownerBytes = compactUuid ?? utf8Owner;
	if (
		!ownerBytes ||
		ownerBytes.length === 0 ||
		ownerBytes.length > MAX_OWNER_BYTES ||
		(!compactUuid && ownerBytes.toString("utf8") !== owner)
	) {
		throw new RangeError(
			"lease owner must be a canonical lowercase UUID or at most 16 UTF-8 bytes",
		);
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
function decodeLease(token: string): LeaseState | undefined {
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
		bytes.length > MAX_TOKEN_BYTES ||
		bytes[0] !== TOKEN_MAGIC
	) {
		return undefined;
	}

	const kind = bytes[1];
	const ownerBytes = bytes.subarray(TOKEN_OWNER_OFFSET);
	let owner: string;
	if (kind === TOKEN_KIND_UUID) {
		if (ownerBytes.length !== MAX_OWNER_BYTES) return undefined;
		owner = uuidFromBytes(ownerBytes);
	} else if (kind === TOKEN_KIND_UTF8) {
		owner = ownerBytes.toString("utf8");
		if (
			!owner ||
			!Buffer.from(owner, "utf8").equals(ownerBytes) ||
			ownerBytes.length > MAX_OWNER_BYTES
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

/** Build the single fencing command that installs a new lease. */
function leaseUpdate(owner: string, ttlMs: number): readonly AppendRecord[] {
	const expiresAt = Math.floor(Date.now() + Math.max(0, ttlMs));
	if (!Number.isFinite(expiresAt) || expiresAt > MAX_EXPIRY) {
		throw new RangeError("lease expiry exceeds the fencing-token format");
	}
	return [AppendRecord.fence(encodeLease(owner, expiresAt).token)];
}

/** Decode a token into the state it guards. Unknown formats are unowned. */
function snapshot(token: string): LeaseSnapshot {
	return { token, lease: decodeLease(token) };
}

/**
 * Distributed S2 leases stored in the thread stream they coordinate.
 *
 * The fencing token is the complete lease register: owner, expiry, and a nonce.
 * Conditional-append failures return the current token, so lease operations do
 * not read event records or retain process-local state.
 *
 * Owners must be canonical lowercase UUIDs or at most 16 UTF-8 bytes. There is
 * deliberately no record-backed or legacy-token fallback.
 */
export class S2LeaseProvider implements LeaseProvider {
	constructor(
		private readonly basin: S2Basin,
		private readonly streamPrefix: string,
	) {}

	async acquireLease(
		key: string,
		owner: string,
		ttlMs: number,
	): Promise<{ acquired: boolean; owner?: string }> {
		const desired = leaseUpdate(owner, ttlMs);
		// The S2 fencing token starts empty, so a free lease is one append.
		const initial = await this.swap(key, "", desired);
		if (initial.swapped) return { acquired: true, owner };

		let current = snapshot(initial.actualToken);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const holder = holderOf(current.lease);
			if (holder && holder !== owner) {
				return { acquired: false, owner: holder };
			}
			// Empty, expired, or already ours: replace exactly what S2 returned.
			const result = await this.swap(
				key,
				current.token,
				leaseUpdate(owner, ttlMs),
			);
			if (result.swapped) return { acquired: true, owner };
			current = snapshot(result.actualToken);
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
			current = snapshot(result.actualToken);
		}
	}

	async renewLease(
		key: string,
		owner: string,
		ttlMs: number,
	): Promise<boolean> {
		let current = await this.currentLease(key);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			if (holderOf(current.lease) !== owner) return false;
			const result = await this.swap(
				key,
				current.token,
				leaseUpdate(owner, ttlMs),
			);
			if (result.swapped) return true;
			current = snapshot(result.actualToken);
		}
		return false;
	}

	async transferLease(
		key: string,
		fromOwner: string,
		toOwner: string,
		ttlMs: number,
	): Promise<boolean> {
		let current = await this.currentLease(key);
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			if (holderOf(current.lease) !== fromOwner) return false;
			const result = await this.swap(
				key,
				current.token,
				leaseUpdate(toOwner, ttlMs),
			);
			if (result.swapped) return true;
			current = snapshot(result.actualToken);
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
		return snapshot(await this.observeToken(key));
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
