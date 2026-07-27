---
"@s2-dev/mastra-pubsub": minor
---

Store lease state in the thread stream it coordinates instead of a separate stream per lease key.

- `S2LeaseProvider` now uses the S2 fencing token itself as a compact, strongly consistent lease register for UUID and short owner IDs. The token carries the owner, expiry, and a nonce, and fencing-token mismatch responses expose the current state without replaying event records. Longer custom owner IDs and legacy tokens use a bounded tagged-record compatibility path.
- Every acquire, renewal, transfer, and release conditionally rotates the exact current token, fencing off stale owners without `matchSeqNum` contention from ordinary event appends. The stream remains the only state — nothing is cached in the process.
- `S2PubSub` filters lease and S2 command records out of `getHistory` and live subscriptions. They still consume sequence numbers, so resume from `last.index + 1` rather than the event count.
- `topicPrefix` now defaults to `agent.`, so per-thread topics (thread coordination, broadcast parts, lease state) are durable alongside per-run topics.
- Lease TTLs are clamped to `MAX_LEASE_TTL_MS` (60s), warning once, so inline and compatibility formats share one operational contract and legacy state is never considered active beyond its bounded fallback window.
- The default local transport no longer caps listeners. One local listener is registered per subscription, so several observers on a single run used to trip Node's ten-listener warning even though the listeners were released on unsubscribe.
