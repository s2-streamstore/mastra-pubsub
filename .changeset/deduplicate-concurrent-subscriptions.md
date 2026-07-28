---
"@s2-dev/mastra-pubsub": patch
---

Deduplicate concurrent subscriptions that register the same callback for a topic, preventing duplicate S2 readers, duplicate event delivery, and an untracked reader after unsubscribe.
