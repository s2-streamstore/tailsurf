---
"@tailsurf/protocol": minor
"@tailsurf/client": minor
---

Add reconnecting durable writers with a fixed sent-but-unacknowledged window and one transcript reassembly limit. Export protocol-owned writer, heartbeat, and initial-link limits. Replace the configurable retry policy with one bounded-operation attempt count and fixed backoff behavior. Clarify the WebSocket progress timeout and derive silent-read detection from protocol heartbeats.
