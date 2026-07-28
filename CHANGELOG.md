# @s2-dev/mastra-pubsub

## 0.2.0

### Minor Changes

- 4509da1: Store lease state in the thread stream it coordinates instead of a separate stream per lease key.

  - `S2LeaseProvider` now uses the S2 fencing token itself as a compact, strongly consistent lease register. The token carries the owner, expiry, and a nonce, and fencing-token mismatch responses expose the current state without replaying event records.
  - Every acquire, renewal, transfer, and release conditionally rotates the exact current token, fencing off stale owners without `matchSeqNum` contention from ordinary event appends. The stream remains the only state — nothing is cached in the process.
  - `S2PubSub` filters S2 command records out of `getHistory` and live subscriptions. They still consume sequence numbers, so resume from `last.index + 1` rather than the event count.
  - `topicPrefix` now defaults to `agent.`, so per-thread topics (thread coordination, broadcast parts, lease state) are durable alongside per-run topics.
  - Lease owners must be canonical lowercase UUIDs or at most 16 UTF-8 bytes. Other owner formats and previously written record-backed leases are intentionally unsupported.
  - The default local transport no longer caps listeners. One local listener is registered per subscription, so several observers on a single run used to trip Node's ten-listener warning even though the listeners were released on unsubscribe.

### Patch Changes

- a92875c: Validate the exact serialized event before appending it so unsupported values cannot create an unreadable record that poisons the topic.
