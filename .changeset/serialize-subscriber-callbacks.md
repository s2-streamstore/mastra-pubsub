---
"@s2-dev/mastra-pubsub": patch
---

Await subscriber callbacks per subscription to preserve event order and apply backpressure instead of starting unbounded concurrent async work.
