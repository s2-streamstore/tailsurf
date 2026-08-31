# Tailsurf protocol

This document defines the public TSF v1 wire contract across REST, Server-Sent Events (SSE), and WebSocket transports. It does not define SDK APIs or local client policy.

[openapi.yaml](openapi.yaml) is the machine-readable REST and SSE contract. The hosted service at [tail.surf](https://tail.surf) implements this contract.

## Stream model

A stream has an immutable kind. A `records` stream is one append-only sequence of physical records. A `terminal` stream is a terminal session with independent input and output record sequences.

The default kind is `records`.

Clients omit `kind` when creating a record stream. They send `kind: terminal` when creating a terminal session.

Servers include `kind` in stream metadata and creation responses. Clients treat a missing response field as `records` for compatibility with record-only servers.

A stream's complete retained history remains readable until the stream expires or an owner deletes it.

The service assigns each physical record a zero-based `seq_num` in append order. Sequence numbers are contiguous. The stream's `next_seq_num` is the sequence number that the next appended record will receive.

Writers identify physical records with `(client_writer_id, writer_seq_num)`. A client writer ID is 16 bytes. A writer sequence number is an unsigned 64-bit integer chosen by that writer. A writer normally increments it for each new record.

A retry uses the same writer identity, writer sequence number, and payload. Retries can create physical duplicates. Logical transcript consumers suppress a record when its writer sequence number is not greater than the highest value already accepted for that writer.

One logical record may span multiple physical records. Each part has a zero-based index and a final bit.

## Identifiers

A `stream_id` is a uniformly random 160-bit value encoded as 32 lowercase Crockford base32 characters. It is generated independently of idempotency keys and link credentials.

A `link_id` is a client-chosen name scoped to one stream. It contains 1 to 64 lowercase ASCII letters, digits, or hyphens. It cannot start or end with a hyphen.

A link secret is a 24-byte server-minted credential encoded as exactly 32 unpadded base64url characters. Clients treat it as opaque. The server stores only credential hashes. It returns a secret only when creating the link or replaying that creation exactly.

A stream has an optional title. A title contains 1 to 120 well-formed Unicode code points. It cannot have leading or trailing whitespace, control characters, or line breaks. Titles are mutable and need not be unique. A title is display metadata, not an identifier.

## Permissions

Links belong to one stream.

- `o` grants owner, read, and write permission.
- `r` grants read permission.
- `w` grants write permission.
- `rw` grants read and write permission.

Owner permission cannot be combined with another permission.

Owner permission is required to change stream metadata, create or revoke links, and delete a stream.

Private reads require read permission. Public reads require no link. All writes require write permission.

## Stream links

A stream page uses `/s/{stream_id}`.

A terminal page uses `/t/{stream_id}`. The distinct path names the terminal workspace. Read-capable browser workspaces verify it against the immutable stream kind.

A link secret is carried in one URL fragment parameter:

```txt
https://tail.surf/s/{stream_id}#r={secret}
```

The fragment key is one of `o`, `r`, `w`, or `rw`. It declares the permission the client should use. The server resolves the secret's authoritative permissions.

The fragment may also contain a sequence anchor. `at` is a canonical decimal integer from zero through `9007199254740991`:

```txt
https://tail.surf/s/{stream_id}#r={secret}&at=50
```

A fragment may contain at most one credential and one `at` parameter. Either may appear alone. Duplicate or unknown parameters are invalid.

Browser stream URLs do not accept query parameters. API read controls such as `seq_num`, `count`, and `wait` belong on data-plane read URLs.

`at` identifies a record to highlight. It is not an API read selector. A client may read earlier records to provide context.

Changing the fragment key does not change the secret's authority. The server rejects any operation that the secret does not authorize.

Browsers do not send URL fragments in HTTP requests. Clients extract the secret and send it in a bearer header or WebSocket opening frame.

Terminal links use the same fragment format:

```txt
https://tail.surf/t/{stream_id}#rw={secret}
```

The API origin and web origin are independent deployment settings. Responses that mint credentials include the canonical `web_origin` used to present stream links. A stream link never selects the API backend.

## REST API

Versioned routes use `/api/v1`. The hosted REST base URL is `https://tail.surf/api/v1`.

| Method | Path | Authorization |
| --- | --- | --- |
| `POST` | `/streams` | No link; subject to deployment policy |
| `GET` | `/streams/{stream_id}` | Read permission for private streams |
| `PATCH` | `/streams/{stream_id}` | Owner |
| `DELETE` | `/streams/{stream_id}` | Owner |
| `GET` | `/streams/{stream_id}/links` | Owner |
| `PUT` | `/streams/{stream_id}/links/{link_id}` | Owner |
| `DELETE` | `/streams/{stream_id}/links/{link_id}` | Owner |
| `POST` | `/streams/{stream_id}/records` | Write |
| `GET` | `/streams/{stream_id}/records` | Read permission for private streams |

Authorized requests carry a link secret as `Authorization: Bearer <secret>`. The credential is scoped to one stream. It is not account authorization.

Routes under `/api/v1/internal` are operator-only. They are not part of the client protocol.

Stream creation does not use a link secret and may be disabled by deployment policy.

### Creation retries

Record stream and link creation accept an optional `Idempotency-Key`. Terminal stream creation requires it because the operation provisions metadata and two physical streams. A key is 32 random bytes encoded as exactly 43 unpadded base64url characters. One logical creation uses the same key, method, path, and body for every retry.

Omitting the header requests one-shot record stream or link creation. Retrying record stream creation can create another stream. Retrying link creation cannot recover a committed credential and may conflict with the existing Link ID.

An exact stream-creation replay returns the same Stream ID and initial credentials while the stream remains active. Reusing the key with a different request returns `409 conflict`.

Replay matching uses the original request, not current stream metadata. A title or visibility change does not affect it.

The mapping remains through deleted-stream tombstone retention. Reuse conflicts after the stream leaves `active`. The key becomes new again only after the tombstone and mapping are purged.

An anonymous stream-creation key is sensitive recovery material. Anyone with the key and complete request can recover the initial link secrets. Clients must not log it or expose it in URLs.

Link creation also requires an owner credential. An idempotency key does not grant authority.

An exact link-creation replay returns the same credential while the link row is retained. It does not reactivate an inactive link. Once cleanup removes the row, the request is a new creation.

### Metadata and links

The server rejects unknown request fields. Clients ignore unknown response fields. [openapi.yaml](openapi.yaml) defines the exact OpenAPI 3.1 schemas.

Stream metadata includes `kind`, `created_at`, and `expires_at`. The timestamps use RFC 3339. Responses that create a stream or link also include `web_origin`.

Link creation uses `PUT /streams/{stream_id}/links/{link_id}`. The body carries `permissions` and an optional RFC 3339 `expires_at`.

A request that would mint a different credential or change the attributes of a retained Link ID returns `409 conflict`.

The Link ID is immutable. It identifies the link in owner interfaces, management paths, authorization state, rate limits, and derived writer identity.

Stream creation defaults to the `records` kind, private visibility, and expiry 10 days after creation. A request creates one to three initial, non-expiring links. Each link has a unique `link_id` and permissions. At least one must be an owner.

Stream creation accepts an optional `title`. Creation and metadata responses always contain `title`, using `null` for an untitled stream.

An owner changes a title with `PATCH /streams/{stream_id}` and `{ "title": "..." }`. Sending `{ "title": null }` clears it. Omitting `title` preserves it.

Set `expires_in_seconds` from 1 through 864,000 to request the initial lifetime.

Creation and metadata responses include the absolute `expires_at`. An owner renews an active stream by sending a later absolute `expires_at`. A renewal can extend expiry to at most 10 days from the request. Renewal after expiry is not allowed.

A stream may have at most 16 active links. An expired link stops counting immediately and cannot authorize a request.

Every active stream keeps at least one non-expiring owner link. The server refuses to revoke the last one.

The link inventory identifies the `authorizing_link_id`. It never includes link secrets or hashes.

Each inventory entry contains its Link ID, permissions, status, and timestamps. Status is `active`, `expired`, or `revoked`. Entries are ordered newest first.

The page `limit` is at most 100. A non-empty page may include a non-empty `next_cursor`. Link IDs do not repeat within a page.

Title visibility follows stream visibility. A public title is public. A private title requires read permission. A write-only link cannot read it.

## Records

### JSON representation

A JSON record carries its payload under exactly one key. `text` contains UTF-8. `bytes` contains canonical unpadded base64url.

The payload key implies the presentation format. `text` means `transcript`. `bytes` means `bytes`. An explicit `format` supplies the cross cases, such as transcript data that is not valid UTF-8.

Requests may use either valid payload representation. Read responses use whichever representation produces smaller JSON.

An omitted `part` means final part zero, which is an unsplit record.

Each decoded record is at most 512 KiB.

Sequence numbers, writer sequence numbers, and Unix epoch millisecond timestamps in REST and SSE bodies are canonical decimal strings.

### HTTP append

`POST /streams/{stream_id}/records` appends one atomic batch. The body carries one to 128 records, an optional `writer`, and an optional `expected_next_seq_num`.

The `writer` object contains a stable 16-byte `id` and the writer sequence number assigned to the first record. The ID is exactly 22 canonical unpadded base64url characters. Sequence numbers advance by one within the batch.

Omitting `writer` requests a one-shot append. The server mints a random writer identity. Retrying that request may append a duplicate.

`expected_next_seq_num` requires the stream's next sequence number to match before the append.

The decoded batch payload is at most 900 KiB. The encoded body is at most 1,300,000 bytes. The writer sequence range must end before `18446744073709551615`.

The response returns the half-open durable range as `start_seq_num` and `end_seq_num`. Its length equals the submitted record count.

An ambiguous response does not guarantee that nothing was appended. A safe retry uses the same writer identity, writer sequence numbers, and data.

### Read query

SSE and WebSocket reads use the same query. The query accepts at most one start selector:

| Selector | Start position |
| --- | --- |
| `seq_num` | First record whose sequence number is at least the value |
| `timestamp` | First record whose timestamp is at least the Unix epoch millisecond value |
| `tail_offset` | That many records before the tail observed when the selector resolves, clamped to sequence zero |

Omitting a selector means `tail_offset=0`.

An absolute sequence beyond the current tail remains pending. Records below that sequence are filtered, and the server does not report a caught-up position below it.

A timestamp selector resolves at the first matching record or at an observed tail. Timestamp regressions after resolution do not change physical sequence order.

The query also accepts these controls:

| Control | Meaning |
| --- | --- |
| `count` | Maximum physical records delivered across the logical read |
| `until` | Exclusive stop timestamp in Unix epoch milliseconds |
| `wait` | Seconds to wait at the tail before ending, from 0 through 60 |
| `rate` | Playback speed multiplier from 0.1 through 100 |

Selector and `until` values are canonical decimal integers no greater than `9007199254740991`. `count` is a canonical decimal unsigned 64-bit integer.

A `count` of zero completes without reading records. Records below `until` are delivered. Records at or above it are not.

When `wait` is omitted, a read with no `count` or `until` follows indefinitely. A read with either stop control defaults to `wait=0`.

`wait=0` drains through the tail observed by the connection and ends. A positive value waits at the tail for that many seconds. A finite read ends when its stop condition is met or its wait ends.

Paced reads require `count`, `until`, or `wait=0`. The first record is immediate. Later records follow their scaled timestamp gaps. Each gap is capped at five seconds. Timestamp regressions do not move playback backward.

Playback deadlines accumulate from the first record. Processing and transport delay do not add replay drift. A reconnect starts a new playback epoch.

A reconnect does not retain the previous connection's tail boundary. It may observe records appended after the earlier connection began.

These controls follow [S2 read semantics](https://s2.dev/docs/sdk/reading), except `rate`, which is a TSF extension. TSF does not expose S2's `clamp` or `bytes` controls.

### SSE reads

`GET /streams/{stream_id}/records` requires `Accept: text/event-stream`. Unsupported, duplicate, or conflicting query parameters fail before backend work.

The first event is `stream_metadata`. Its data matches the REST stream metadata object.

`read_batch` events contain up to 1,000 records and 1 MiB of decoded record data. A complete event is at most 2 MiB including its terminator.

Each record contains `seq_num`, `timestamp_ms`, and a `writer` object with the server-derived ID and writer-local sequence number. The payload appears under `text` or `bytes`. An omitted `part` means an unsplit record. An omitted `format` follows the payload key.

Every `read_batch` and `caught_up` event has an ID of `v1,<next_seq_num>,<consumed_count>`. Both values are canonical decimal unsigned 64-bit integers. `consumed_count` is the number of physical records delivered in this logical read and cannot exceed `next_seq_num`.

The event ID is resume state, not a credential. `caught_up` establishes a safe resume position. SSE comments are heartbeats.

`Last-Event-ID` overrides the start position and preserves the consumed count. The original URL remains unchanged. Its selector and controls are still validated. A resumed request enforces the unconsumed `count` remainder.

A zero-count read carries its terminal cursor on `stream_metadata`. A resume after exhausting `count` returns HTTP 204.

A retryable interruption aborts the response. A client resumes with the unchanged URL and the latest event ID.

A non-retryable failure after the response opens sends a terminal `error` event and ends the response. Its data is `{ "error": { "code": "...", "message": "..." } }`. It has no resume ID.

## Terminal sessions

A terminal stream has independent `input` and `output` logs. Each log follows the ordinary record ordering, durability, retry, and read semantics.

The generic read route selects terminal output. The generic write route selects terminal input. Explicit routes select either log for hosts and specialized clients.

The explicit terminal routes are WebSocket-only. Generic HTTP append and SSE read routes select terminal input and output using the same defaults as generic WebSocket routes.

```txt
/api/v1/streams/{stream_id}/terminal/input/read
/api/v1/streams/{stream_id}/terminal/input/write
/api/v1/streams/{stream_id}/terminal/output/read
/api/v1/streams/{stream_id}/terminal/output/write
```

Read permission authorizes output reads. Write permission authorizes input writes. Owner permission authorizes every terminal route. Public visibility authorizes output reads without a link.

Only an owner can read the input log or write the output log. Terminal routes reject `records` streams.

Terminal events use unsplit byte records. Every payload begins with a version byte and a type byte. The version is `0x01`.

Input event types are:

| Type | Name | Body |
| --- | --- | --- |
| `0x01` | `data` | PTY input bytes |
| `0x02` | `resize` | Columns and rows as big-endian `uint16` values |

Output event types are:

| Type | Name | Body |
| --- | --- | --- |
| `0x01` | `data` | PTY output bytes |
| `0x02` | `resize` | Accepted columns and rows as big-endian `uint16` values |
| `0x03` | `started` | Initial columns and rows as big-endian `uint16` values |
| `0x04` | `exited` | Signed exit status as a big-endian `int32` value |
| `0x05` | `heartbeat` | Empty |

Terminal dimensions are between 1 and 1,000 columns, between 1 and 500 rows, and at most 131,072 cells. Fixed-width events reject truncation and trailing bytes. Unknown versions and types are terminal protocol errors. Servers validate the record envelope and direction-specific event before append.

The host writes one `started` event before other output. It writes data, resize, and heartbeat events while the child runs. A clean exit ends with one `exited` event. The browser rejects an event before `started` or a second `started`, and stops reading at `exited`.

The `tsf terminal` host claims sequence zero before starting its PTY. This prevents a second CLI host from attaching to the same output log. It applies a requested resize before publishing the output resize event.

## History export

A history export reads from sequence zero with `wait=0` and writes `application/x-ndjson`. It finishes when the read catches up.

The first line has `type` set to `tailsurf_export`, `version` set to `2`, and the `stream_id`.

Each later line has `type` set to `record` and uses the read-record shape. Sequence values are decimal strings.

A valid UTF-8 transcript record exports under `text`. Opaque bytes and invalid UTF-8 transcript records export under `bytes` as canonical unpadded base64url.

A resumed export starts at the next sequence number and may observe a newer tail. Records appended during an interrupted export can therefore be included. Stream expiry or owner deletion can terminate an export.

## Stream lifecycle

Stream lifecycle states are `active`, `deleting`, and `deleted`.

At `expires_at`, new reads, writes, management requests, and renewal fail.

Established SSE and WebSocket sessions reauthorize on an absolute deadline no more than 60 seconds after their previous authorization. Expiry, link revocation, deletion, and a public-to-private visibility change take effect by that deadline. Reconnecting does not extend it.

The service later moves an expired stream to `deleting`, revokes its active links, and removes its stored records.

Deleting an active stream durably changes it to `deleting` and revokes its active links. The stream changes to `deleted` only after its records are removed.

A `204` deletion response means the request is durable. Record removal may still be pending.

`deleting` and `deleted` are tombstone states. Later reads and writes fail. Deleted tombstones remain for 90 days.

Inactive link metadata remains for 30 days. Link inventory omits rows after cleanup.

## Errors and retries

REST errors use one stable shape:

```json
{"error":{"code":"bad_request","message":"human-readable detail","request_id":"request identifier"}}
```

Codes use lowercase `snake_case` and may drive client behavior. Messages are for display and debugging.

Requests consume short-term and rolling budgets. HTTP rate denials return `429 rate_limited` with `Retry-After`.

Exhausted client, stream, or link allowances return `403 free_plan_limit`. Free-plan expiry denials use the same error. Exhausted global capacity returns `503 overloaded`.

A failed append sequence precondition returns `412 sequence_mismatch` and includes `actual_next_seq_num`.

Response fields may be added when they are optional. Clients must ignore unknown optional fields. Removing a field or changing its meaning is a compatibility change.

## WebSocket data plane

### Upgrade

Data-plane routes are:

```txt
/api/v1/streams/{stream_id}/write
/api/v1/streams/{stream_id}/read
```

Clients offer `Sec-WebSocket-Protocol: tsf.v1`. The server echoes `tsf.v1` after a successful upgrade.

Connection attempts consume request capacity before the upgrade. A short-term denial returns HTTP `429` with `Retry-After`. Allowance and capacity denials use the REST status codes above.

When a browser sends an `Origin` header, the server accepts only the configured web origin.

Read URLs accept the shared read query. The server validates it before upgrade. Write URLs reject every query parameter.

Messages are binary. One WebSocket message contains exactly one TSF frame. Text messages are protocol errors.

All integer fields are unsigned and big-endian.

The first byte is the operation ID.

Record batch frames contain one or more records. Each record starts with a `uint32` body length that excludes its own four bytes.

`AppendBatch` carries at most 128 records. `ReadBatch` carries at most 1,000 records with contiguous physical sequence numbers. Both carry at most 1 MiB of aggregate record data, excluding headers.

### Client frames

| Operation | ID | Body |
| --- | --- | --- |
| `OpenRead` | `0x01` | flags, then an optional 32-byte encoded link secret |
| `OpenWrite` | `0x02` | flags, 16-byte client writer ID, optional expected next sequence `uint64`, then a 32-byte encoded link secret |
| `AppendBatch` | `0x03` | records containing body length `uint32`, writer sequence `uint64`, part `uint32`, format `uint8`, and data |

`OpenRead` starts with one flags byte. Bit `0x01` means that a link secret follows as exactly 32 canonical unpadded base64url bytes.

`OpenWrite` starts with one flags byte. Bit `0x01` means that an expected next sequence follows the client writer ID.

Unknown flags, truncation, trailing bytes, and malformed or non-canonical credentials are protocol errors.

### Server frames

| Operation | ID | Body |
| --- | --- | --- |
| `Ready` | `0x80` | empty |
| `AppendAck` | `0x81` | writer start `uint64`, writer end `uint64`, durable start `uint64`, durable end `uint64` |
| `ReadBatch` | `0x82` | records containing body length `uint32`, sequence `uint64`, timestamp milliseconds `uint64`, 16-byte writer ID, writer sequence `uint64`, part `uint32`, format `uint8`, and data |
| `Heartbeat` | `0x83` | empty |
| `CaughtUp` | `0x84` | next sequence `uint64`, last-record timestamp milliseconds `uint64` |
| `StreamMetadata` | `0x85` | UTF-8 JSON matching the REST stream metadata schema |

An empty stream has position `(0, 0)`. A non-empty stream may also have a last timestamp of zero. The next sequence disambiguates them.

Malformed lengths and unknown formats are protocol errors.

### Binary record format

Record data may contain at most 512 KiB.

The format byte is a presentation hint. It does not transform bytes.

- `0x00` means opaque bytes.
- `0x01` means transcript text.

Transcript consumers preserve payload bytes. They do not add separators or remove newlines.

The part header uses bit 31 as the final bit. Bits 0 through 30 contain the part index. An unsplit record is final part zero.

### Writes

A writer opens the session before appending:

```txt
-> OpenWrite
<- Ready
```

The writer sends `OpenWrite` immediately after the WebSocket opens. Its optional expected-next-sequence field requires the initial stream position to match. The match then advances across the session.

A mismatch closes the socket with `1008 sequence_mismatch`.

After `Ready`, the writer sends `AppendBatch` frames. `AppendAck` reports half-open contiguous writer and durable sequence ranges after durability.

An acknowledgement may cover several records. The server splits acknowledgements when writer or durable sequence numbers are not contiguous.

An `AppendBatch` frame is not atomic. A failure can leave a durable prefix. The server acknowledges only durable contiguous ranges.

To recover after an interruption, a writer preserves the link secret, client writer ID, writer sequence numbers, and payloads. It resends only the unacknowledged suffix.

If `Ready` was received, the recovering writer omits the initial sequence precondition. This avoids rejecting recovery when durability succeeded but its acknowledgement was lost.

Each read record carries a server-scoped writer ID derived from the stream, authorized Link ID, and client writer ID. Reconnecting with the same write link preserves that read-side identity. Different write links cannot claim the same identity.

Durability remains uncertain when an acknowledgement is lost before a terminal failure or cancellation. Submitting those records under a new writer identity may duplicate them.

### Reads

A reader puts the shared read query in the WebSocket URL. It sends `OpenRead` immediately after the socket opens. Anonymous public readers omit the link secret. Private readers include a read-capable secret.

```txt
-> OpenRead
<- Ready
<- StreamMetadata
```

The server sends `Ready` only after accepting the request and authorization. An authorization failure closes without `Ready`.

`StreamMetadata` follows `Ready`. It contains `stream_id`, `title`, `visibility`, `created_at`, and `expires_at`. Clients ignore unknown fields.

The server then sends `ReadBatch`, `CaughtUp`, and `Heartbeat` frames. A reader sends no frames after `OpenRead`.

Every read record includes its durable sequence and timestamp. An otherwise idle connection sends a heartbeat every 20 seconds.

`CaughtUp` means that every preceding record below its `next_seq_num` has been delivered. It also contains the last-record timestamp.

The server sends `CaughtUp` when the observed caught-up position differs from the last one sent. It is not a periodic heartbeat.

To continue after an interruption, a reader starts at the sequence after its last record. If no later record followed the latest `CaughtUp`, it uses that frame's `next_seq_num`. If neither exists, it uses the original selector.

A resumed finite read preserves `until` and reduces `count` by the number of records already consumed. A normal close ends the logical read.

When an unbounded backing read ends or fails transiently, the server closes with `1013 upstream_unavailable`. A finite read closes normally when its condition is met or it catches up.

A transcript consumer reassembles parts in delivery order. A read that starts in the middle of a split record skips that partial logical record. Consumers should bound incomplete reassembly according to their resource limits.

### Close behavior

The server uses these WebSocket closes:

- `1000 normal` for normal completion.
- `1001 server_shutdown` for runtime shutdown.
- `1002 protocol_error` for invalid frames or role flow.
- `1008 unauthorized`, `forbidden`, or `free_plan_limit` for permanent policy failures.
- `1011 backend_error` or `internal_error` for server failures.
- `1013 overloaded` when bounded read or write backpressure cannot recover, or global capacity is exhausted.
- `1013 rate_limited` for temporary established-socket rate pressure.
- `1013 upstream_unavailable` when an unbounded backing read ends or fails transiently.

Close reasons under `1013` are temporary. Protocol and policy failures under `1002` and `1008` are permanent.

## Compatibility

REST and SSE use the `/api/v1` route prefix. A breaking change requires a new route version.

Binary clients and the server select the exact `tsf.v1` subprotocol. A breaking binary change requires a new subprotocol.

Additive REST and SSE fields must remain ignorable by v1 clients. Additive binary behavior must remain ignorable under `tsf.v1`. Otherwise it requires capability negotiation or a new version.

Changes to permissions, routes, status codes, close behavior, record reconstruction, URL fragments, or the history export format are public compatibility changes.

## Conformance

A conforming implementation follows the schemas and frame layouts in this document. It preserves the defined sequence, acknowledgement, resume, reconstruction, and retry behavior.

It rejects malformed frames, invalid identifiers, conflicting read parameters, and unsupported protocol flow as specified above.
