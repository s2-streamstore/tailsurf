---
"@tailsurf/protocol": minor
"@tailsurf/client": minor
---

Add reconnecting durable writers with a fixed sent-but-unacknowledged window and one transcript reassembly limit. Export the writer window from the protocol package and use it as the client implementation's single source of truth.
