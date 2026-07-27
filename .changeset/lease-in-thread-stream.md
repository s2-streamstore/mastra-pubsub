---
"@s2-dev/mastra-pubsub": minor
---

Store lease state in the thread stream it coordinates instead of a separate stream per lease key.

- `S2LeaseProvider` now writes tagged lease records into `<streamPrefix>agent.thread-stream.<key>`, guarding updates with an S2 fencing token instead of `matchSeqNum` so event appends never conflict with them. Ownership changes rotate the token, fencing off the previous owner. The stream remains the only state — nothing is cached in the process.
- `S2PubSub` filters lease and S2 command records out of `getHistory` and live subscriptions. They still consume sequence numbers, so resume from `last.index + 1` rather than the event count.
- `topicPrefix` now defaults to `agent.`, so per-thread topics (thread coordination, broadcast parts, lease state) are durable alongside per-run topics.
- Lease TTLs are clamped to `MAX_LEASE_TTL_MS` (60s), warning once. Reads size their lookback window from that bound rather than the caller's TTL, since a reader cannot know which TTL the incumbent used; without the clamp a longer TTL could hide unexpired lease state behind a burst of events and hand the lease to a second owner.
- The default local transport no longer caps listeners. One local listener is registered per subscription, so several observers on a single run used to trip Node's ten-listener warning even though the listeners were released on unsubscribe.
