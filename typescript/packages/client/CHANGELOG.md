# @tailsurf/client

## 0.4.0

### Minor Changes

- 9512b52: Breaking JSON shape change for REST appends and SSE reads. A record's payload sits under exactly one key: `text` for UTF-8 or `bytes` for canonical base64url, with the key implying the format and an explicit `format` covering cross cases. Writer identity groups as one optional `writer: {id, seq_num}` object on appends; read records carry `writer: {id, seq_num}` and omit `part` when unsplit. `compactRecordData` is replaced by `compactRecordPayload`, with `resolvedRecordFormat` and `recordPayloadBytes` helpers.

### Patch Changes

- Updated dependencies [9512b52]
  - @tailsurf/protocol@0.4.0

## 0.3.0

### Minor Changes

- 3a2b451: Add the required `web_origin` field to stream creation and link creation responses. `createLink` now returns a `CreateLinkResponse` carrying `webOrigin` alongside the credential, and `CreateStreamResponse` exposes `webOrigin`. Clients present stream links against the server-supplied origin, so no separate web-URL configuration is needed. Requires a server that returns `web_origin` when minting link credentials.

### Patch Changes

- Updated dependencies [3a2b451]
  - @tailsurf/protocol@0.3.0

## 0.2.0

### Minor Changes

- d6c23fe: Add reconnecting durable writers with a fixed sent-but-unacknowledged record and payload-byte window and one transcript reassembly limit. Export protocol-owned writer, heartbeat, and initial-link limits. Replace the configurable retry policy with one bounded-operation attempt count and fixed backoff behavior. Name the HTTP request and WebSocket progress timeouts by what they bound. Derive silent-read detection from protocol heartbeats. Remove raw frame-limit re-exports from the high-level client package.

### Patch Changes

- Updated dependencies [d6c23fe]
  - @tailsurf/protocol@0.2.0
