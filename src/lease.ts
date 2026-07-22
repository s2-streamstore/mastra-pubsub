import type { LeaseProvider } from "@mastra/core/events";
import type { S2Basin } from "@s2-dev/streamstore";
import {
	AppendInput,
	AppendRecord,
	RangeNotSatisfiableError,
	S2Error,
	SeqNumMismatchError,
} from "@s2-dev/streamstore";

/** Lease state stored in the stream's last record. */
interface LeaseState {
	readonly owner: string | null;
	readonly expiresAt: number;
}

/** Maximum CAS attempts per operation. */
const CAS_ATTEMPTS = 3;

/** Whether the lease stream is missing or being deleted. */
function isGone(error: unknown): boolean {
	return (
		error instanceof S2Error &&
		(error.status === 404 || error.code === "stream_deletion_pending")
	);
}

/** Parse a lease record, treating invalid data as no lease. */
function decodeLease(body: string): LeaseState | undefined {
	try {
		const value: unknown = JSON.parse(body);
		if (value === null || typeof value !== "object") return undefined;
		const { owner, expiresAt } = value as Record<string, unknown>;
		if (owner !== null && typeof owner !== "string") return undefined;
		if (typeof expiresAt !== "number") return undefined;
		return { owner, expiresAt };
	} catch {
		return undefined;
	}
}

/** Return the current owner of an active lease. */
function holderOf(lease: LeaseState | undefined): string | undefined {
	if (!lease?.owner || lease.expiresAt <= Date.now()) return undefined;
	return lease.owner;
}

/**
 * Distributed S2 leases using one stream per key.
 *
 * Conditional appends make updates atomic. Expiry uses wall-clock time, so
 * processes should keep their clocks synchronized. Old records are trimmed.
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
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const { tail, lease } = await this.read(key);
			const holder = holderOf(lease);
			// Re-acquiring an owned lease refreshes its TTL.
			if (holder && holder !== owner) return { acquired: false, owner: holder };
			if (await this.write(key, tail, owner, ttlMs)) {
				return { acquired: true, owner };
			}
		}
		// Report the winner after repeated CAS conflicts.
		const { lease } = await this.read(key);
		return { acquired: false, owner: holderOf(lease) };
	}

	async getLeaseOwner(key: string): Promise<string | undefined> {
		const { lease } = await this.read(key);
		return holderOf(lease);
	}

	async releaseLease(key: string, owner: string): Promise<void> {
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const { tail, lease } = await this.read(key);
			if (holderOf(lease) !== owner) return;
			if (await this.write(key, tail, null, 0)) return;
		}
	}

	async renewLease(
		key: string,
		owner: string,
		ttlMs: number,
	): Promise<boolean> {
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const { tail, lease } = await this.read(key);
			if (holderOf(lease) !== owner) return false;
			if (await this.write(key, tail, owner, ttlMs)) return true;
		}
		return false;
	}

	async transferLease(
		key: string,
		fromOwner: string,
		toOwner: string,
		ttlMs: number,
	): Promise<boolean> {
		for (let attempt = 0; attempt < CAS_ATTEMPTS; attempt++) {
			const { tail, lease } = await this.read(key);
			if (holderOf(lease) !== fromOwner) return false;
			if (await this.write(key, tail, toOwner, ttlMs)) return true;
		}
		return false;
	}

	private streamName(key: string): string {
		return `${this.streamPrefix}${key}`;
	}

	/** Read the lease and its CAS position. */
	private async read(
		key: string,
	): Promise<{ tail: number; lease?: LeaseState }> {
		try {
			const batch = await this.basin
				.stream(this.streamName(key))
				.read({ start: { from: { tailOffset: 1 } } });
			const last = batch.records.at(-1);
			const tail = Math.max(
				batch.tail?.seqNum ?? 0,
				last ? last.seqNum + 1 : 0,
			);
			return { tail, lease: last ? decodeLease(last.body) : undefined };
		} catch (error) {
			if (isGone(error)) return { tail: 0 };
			if (error instanceof RangeNotSatisfiableError) {
				return { tail: error.tail?.seqNum ?? 0 };
			}
			throw error;
		}
	}

	/** Conditionally write the lease and trim its previous state. */
	private async write(
		key: string,
		tail: number,
		owner: string | null,
		ttlMs: number,
	): Promise<boolean> {
		const state: LeaseState = {
			owner,
			expiresAt: owner === null ? 0 : Date.now() + ttlMs,
		};
		const record = AppendRecord.string({ body: JSON.stringify(state) });
		const records = tail > 0 ? [AppendRecord.trim(tail), record] : [record];
		try {
			await this.basin
				.stream(this.streamName(key))
				.append(AppendInput.create(records, { matchSeqNum: tail }));
			return true;
		} catch (error) {
			if (error instanceof SeqNumMismatchError) return false;
			throw error;
		}
	}
}
