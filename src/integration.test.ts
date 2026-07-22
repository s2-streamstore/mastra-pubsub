/**
 * Integration tests for {@link S2PubSub}.
 *
 * These require an S2 access token and are skipped otherwise:
 * ```bash
 * S2_ACCESS_TOKEN=... npm test
 * ```
 *
 * To run against a local s2-lite instead of the hosted service, also set the
 * endpoint env vars:
 * ```bash
 * S2_ACCESS_TOKEN=... \
 *   S2_ACCOUNT_ENDPOINT=http://localhost:4243 \
 *   S2_BASIN_ENDPOINT=http://localhost:4243 \
 *   npm test
 * ```
 */
import type { Event, EventCallback } from "@mastra/core/events";
import { S2, S2Environment } from "@s2-dev/streamstore";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { S2PubSub } from "./pubsub.js";

const accessToken = process.env.S2_ACCESS_TOKEN;
const describeIf = accessToken ? describe : describe.skip;

const makeBasinName = (): string =>
	`mastra-s2ps-${Math.random().toString(36).slice(2, 10)}`.slice(0, 48);
const topic = () => `agent.stream.${Math.random().toString(36).slice(2)}`;
const makeEvent = (
	type: string,
	data: unknown,
): Omit<Event, "id" | "createdAt" | "index"> => ({
	type,
	data,
	runId: "integration-run",
});

describeIf("S2PubSub Integration", () => {
	let s2: S2;
	let basinName: string;
	let ps: S2PubSub;
	let observer: S2PubSub;

	beforeAll(async () => {
		// Honors S2_ACCOUNT_ENDPOINT / S2_BASIN_ENDPOINT for s2-lite (see file header).
		const environment = S2Environment.parse();
		if (!environment.accessToken) {
			throw new Error("S2_ACCESS_TOKEN is required for integration tests");
		}
		s2 = new S2({ ...environment, accessToken: environment.accessToken });
		basinName = makeBasinName();
		await s2.basins.create({
			basin: basinName,
			config: { createStreamOnAppend: true, createStreamOnRead: true },
		});
		ps = new S2PubSub({ client: s2, basin: basinName });
		observer = new S2PubSub({ client: s2, basin: basinName });
	});

	afterAll(async () => {
		if (!s2 || !basinName) return;
		await Promise.all([ps?.close(), observer?.close()]);
		try {
			await s2.basins.delete({ basin: basinName });
		} catch {
			// best-effort cleanup
		}
	});

	it("delivers live records to another PubSub instance through S2", async () => {
		const t = topic();
		let resolveEvent!: (event: Event) => void;
		let rejectEvent!: (error: Error) => void;
		const received = new Promise<Event>((resolve, reject) => {
			resolveEvent = resolve;
			rejectEvent = reject;
		});
		const callback: EventCallback = (event) => resolveEvent(event);

		await observer.subscribe(t, callback);
		await ps.publish(t, makeEvent("chunk", { i: 0 }));
		const timeout = setTimeout(
			() => rejectEvent(new Error("timed out waiting for S2")),
			5_000,
		);
		const receivedEvent = await received.finally(() => clearTimeout(timeout));

		expect(receivedEvent.index).toBe(0);
		expect(receivedEvent.data).toEqual({ i: 0 });
		await observer.unsubscribe(t, callback);
	});

	it("persists a durable topic and replays with index == position", async () => {
		const t = topic();
		await ps.publish(t, makeEvent("chunk", { i: 0 }));
		await ps.publish(t, makeEvent("chunk", { i: 1 }));
		await ps.publish(t, makeEvent("chunk", { i: 2 }));

		const history = await ps.getHistory(t, 0);
		expect(history.map((e) => (e.data as { i: number }).i)).toEqual([0, 1, 2]);
		expect(history.map((e) => e.index)).toEqual([0, 1, 2]);
		expect((await ps.getHistory(t, 1)).map((e) => e.index)).toEqual([1, 2]);
	});

	it("replays from an offset and follows new records on the same session", async () => {
		const t = topic();
		await ps.publish(t, makeEvent("chunk", { i: 0 }));
		await ps.publish(t, makeEvent("chunk", { i: 1 }));
		const indexes: number[] = [];
		const callback: EventCallback = (event) => indexes.push(event.index ?? -1);

		await observer.subscribeFromOffset(t, 1, callback);
		await ps.publish(t, makeEvent("chunk", { i: 2 }));

		const deadline = Date.now() + 5_000;
		while (indexes.length < 2 && Date.now() < deadline) {
			await new Promise((resolve) => setTimeout(resolve, 20));
		}
		expect(indexes).toEqual([1, 2]);
		await observer.unsubscribe(t, callback);
	});

	// Skipped on s2-lite: concurrent create-on-append races to a spurious 404
	// (s2-streamstore/s2#641). Cloud is unaffected, so this still runs there.
	it.skipIf(process.env.S2_LITE)(
		"assigns distinct, gap-free indices under concurrent publishes",
		async () => {
			const t = topic();
			await Promise.all([
				ps.publish(t, makeEvent("chunk", { i: 0 })),
				ps.publish(t, makeEvent("chunk", { i: 1 })),
				ps.publish(t, makeEvent("chunk", { i: 2 })),
				ps.publish(t, makeEvent("chunk", { i: 3 })),
			]);
			const idx = (await ps.getHistory(t, 0))
				.map((e) => e.index)
				.sort((a, b) => (a ?? 0) - (b ?? 0));
			expect(idx).toEqual([0, 1, 2, 3]);
		},
	);

	it("does not persist non-durable topics", async () => {
		await ps.publish("workflows", makeEvent("wf", { x: 1 }));
		expect(await ps.getHistory("workflows", 0)).toEqual([]);
	});

	it("coordinates a lease between two providers through S2", async () => {
		const leaseA = ps.getLeaseProvider();
		const leaseB = observer.getLeaseProvider();
		const key = `it-${Math.random().toString(36).slice(2)}`;

		expect(await leaseA.acquireLease(key, "a", 30_000)).toEqual({
			acquired: true,
			owner: "a",
		});
		expect(await leaseB.acquireLease(key, "b", 30_000)).toEqual({
			acquired: false,
			owner: "a",
		});
		expect(await leaseB.renewLease(key, "b", 30_000)).toBe(false);
		expect(await leaseA.renewLease(key, "a", 30_000)).toBe(true);
		expect(await leaseA.transferLease(key, "a", "b", 30_000)).toBe(true);
		expect(await leaseB.getLeaseOwner(key)).toBe("b");
		await leaseB.releaseLease(key, "b");
		expect(await leaseA.getLeaseOwner(key)).toBeUndefined();
	});

	it("clearTopic requests stream deletion without throwing", async () => {
		const t = topic();
		await ps.publish(t, makeEvent("chunk", {}));
		expect(await ps.getHistory(t, 0)).toHaveLength(1);
		await expect(ps.clearTopic(t)).resolves.toBeUndefined();
		// Repeat should still be fine
		await expect(ps.clearTopic(t)).resolves.toBeUndefined();
	});
});
