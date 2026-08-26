---
"@tailsurf/protocol": minor
"@tailsurf/client": minor
---

Breaking JSON shape change for REST appends and SSE reads. A record's payload sits under exactly one key: `text` for UTF-8 or `bytes` for canonical base64url, with the key implying the format and an explicit `format` covering cross cases. Writer identity groups as one optional `writer: {id, seq_num}` object on appends; read records carry `writer: {id, seq_num}` and omit `part` when unsplit. `compactRecordData` is replaced by `compactRecordPayload`, with `resolvedRecordFormat` and `recordPayloadBytes` helpers.
