---
"@s2-dev/mastra-pubsub": patch
---

Validate the exact serialized event before appending it so unsupported values cannot create an unreadable record that poisons the topic.
