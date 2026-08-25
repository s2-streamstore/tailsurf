# Tailsurf protocol

This document defines the public TSF v1 contract across REST, Server-Sent Events (SSE), and WebSocket transports. [openapi.yaml](openapi.yaml) is the machine-readable REST and SSE contract.

The official TypeScript and Rust implementations live in this repository. The hosted service at [tail.surf](https://tail.surf) implements this contract.

Canonical, language-neutral fixtures live in [`rust/fixtures`](../rust/fixtures) and [`typescript/packages/protocol/fixtures`](../typescript/packages/protocol/fixtures).

## Stream model

A stream is an append-only sequence of physical records. Its complete history remains readable until the stream expires or an owner deletes it.

The service assigns each physical record a zero-based `seq_num` in append order. Sequence numbers are contiguous. The stream's `next_seq_num` is the sequence number that the next appended record will receive.

Writers submit physical records with `(client_writer_id, writer_seq_num)`. A client writer ID is 16 bytes. A writer sequence number is an unsigned 64-bit integer chosen by that writer. Official writers increment it for each new record.

Retries reuse the same identity and sequence number. The Rust and TypeScript `LogicalTranscript` implementations suppress sequence numbers that are not greater than the highest value already seen for that writer.

One logical record may span multiple physical records. Each part has a zero-based index and a final bit.

## Identifiers

A `stream_id` is a uniformly random 160-bit value encoded as 32 lowercase Crockford base32 characters. It is generated independently of idempotency keys and link credentials.

A `link_id` is a client-chosen name scoped to one stream. It contains 1 to 64 lowercase ASCII letters, digits, or hyphens. It cannot start or end with a hyphen.

A link secret is a 24-byte server-minted credential encoded as exactly 32 unpadded base64url characters. Clients treat it as opaque. The server stores only credential hashes. It returns a secret only in the link's creation response or an exact idempotent replay described below.

A stream has an optional title. A title contains 1 to 120 well-formed Unicode code points. It cannot have leading or trailing whitespace, control characters, or line breaks. Titles are mutable and need not be unique. A title is display metadata, not an identifier.

## Permissions

Links belong to one stream.

- `o` grants owner, read, and write permission.
- `r` grants read permission.
- `w` grants write permission.
- `rw` grants read and write permission.

Owner permission cannot be combined with another permission.

Owner permission is required to change stream metadata, create or revoke links, and delete a stream.

Private reads require effective read permission. Public reads require no link. All writes require effective write permission.

## Stream links

A stream page uses `/s/{stream_id}`.

A link secret is carried in one URL fragment parameter:

```txt
https://tail.surf/s/{stream_id}#r={secret}
```

The fragment key is one of `o`, `r`, `w`, or `rw`. It declares the permission the client should use. A stream link may contain at most one secret. The server resolves authoritative permissions from the secret.

The browser can also carry a sequence anchor in the fragment. `at` is a canonical decimal integer from zero through `9007199254740991`:

```txt
https://tail.surf/s/{stream_id}#r={secret}&at=50
```

The fragment may contain at most one credential and at most one `at` parameter. Either may appear alone. Duplicate and unknown fragment parameters are invalid.

Browser stream URLs do not accept query parameters. API read controls such as `seq_num`, `count`, and `wait` belong only on data-plane read URLs. The browser derives a contextual API read start from `at` without adding transport state to the stream URL.

`at` identifies the record to highlight. It is not the API read selector. The browser can read earlier records to provide context around the anchor.

Changing the fragment key does not change the secret's authority. The client may use the declaration immediately to select its mode. The server rejects an operation when the secret's effective permissions do not allow it. Clients do not need a remote permission preflight before presenting the stream UI or copying the link.

Browsers do not send URL fragments in HTTP requests. Clients extract the secret and send it in a bearer header or WebSocket opening frame.

The API origin and web origin are configured independently on the server. Responses that mint link credentials carry the deployment's canonical `web_origin`, and clients present stream links against it. A stream link never selects the API backend.

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

Authorized REST requests carry a link secret as `Authorization: Bearer <secret>`. The credential is scoped to one stream. It is not global or account authorization.

Routes under `/api/v1/internal` are operator-only. They require an internal bearer secret and are not a client API.

Private reads use a read-capable link. Stream and link management use an owner link. Stream creation does not use a link secret and may be disabled by deployment policy.

### Creation retries

Stream and link creation accept an optional `Idempotency-Key`. A supplied key must be generated from 32 random bytes and encoded as exactly 43 unpadded base64url characters. A client generates it once per logical creation and reuses the same method, path, and body for every retry.

Omitting the header requests one-shot creation. Retrying stream creation then creates a new stream. Retrying link creation cannot recover a committed credential and may conflict with the existing Link ID.

Raw HTTP clients may omit the header only when they accept that ambiguity. The official TypeScript and Rust clients always supply it.

For stream creation, an exact supplied-key and request replay returns the same Stream ID and initial credentials while the mapped stream remains active. Reusing the key with a different request returns `409 conflict`.

Each initial link carries a client-chosen Link ID and permissions. An exact retry returns the same link secrets.

An exact retry is recognized by the original creation request content, not current stream metadata. Retries keep resolving after the title or visibility changes.

The mapping remains through deleted-stream tombstone retention. Reuse conflicts after the stream leaves `active`. Once the tombstone and mapping are purged, the key no longer identifies the previous creation.

An anonymous stream-creation key is sensitive recovery material. Anyone with the key and complete request can replay creation and recover the initial link secrets. Clients do not log it or expose it in URLs. They retain it until the returned credentials are stored safely.

Link creation also requires an owner link secret. A supplied idempotency key is not independently authorizing.

### Metadata and links

The server rejects unknown request body fields. Official SDK decoders ignore unknown response fields. [openapi.yaml](openapi.yaml) defines the exact OpenAPI 3.1 schemas.

Stream metadata includes `created_at` and `expires_at` as RFC 3339 timestamps.

Stream creation and link creation responses include `web_origin`, the deployment's canonical origin for presenting the minted stream links.

Sequence numbers, writer sequence numbers, and Unix epoch millisecond record timestamps in REST and SSE bodies are canonical decimal strings. Query parameters use their documented decimal forms.

Link creation uses `PUT /streams/{stream_id}/links/{link_id}`. The request may carry an idempotency key. The body carries `permissions` and an optional RFC 3339 `expires_at`.

Repeating the same supplied key, path, and body returns the same credential while the link row is retained. An exact replay does not reactivate an inactive retained link. After cleanup removes the row, the request is handled as a new link creation.

A request that would mint a different credential or change the attributes of a retained Link ID returns `409 conflict`.

The Link ID is the immutable human and machine identity used in owner interfaces, management paths, authorization state, rate limits, and derived writer identity.

Stream creation defaults to private visibility and expiry 10 days after creation. A request creates between one and three initial, non-expiring links through `links`. Each item contains a unique `link_id` and `permissions`. At least one must be an owner. The Rust and TypeScript SDKs add a link named `owner` when needed. The CLI also adds `reader` for private streams.

Stream creation accepts an optional `title`. Creation and metadata responses always contain `title`, using `null` for an untitled stream.

An owner changes a title with `PATCH /streams/{stream_id}` and `{ "title": "..." }`. Sending `{ "title": null }` clears it. Omitting `title` preserves it. A title change does not affect the Stream ID, URL, links, permissions, expiry, or established sessions.

Set `expires_in_seconds` to an integer from 1 through 864,000 to request the initial lifetime.

Creation and metadata responses include the absolute RFC 3339 `expires_at` timestamp. An owner renews an active stream with `PATCH /streams/{stream_id}` and a later absolute `expires_at`. A renewal can extend expiry to at most 10 days from the request. Renewal after expiry is not allowed.

One stream may have at most 16 active links. An expired link stops counting immediately and cannot authorize a request. A separately created link credential is returned only by its creation response. Initial link credentials are returned again only for an exact replay with a supplied idempotency key. Link expiry is an RFC 3339 timestamp.

Every active stream keeps at least one non-expiring owner link. The server refuses to revoke the last one.

The link inventory contains `authorizing_link_id` and link metadata. The top-level ID identifies the bearer credential used for the request. Link entries contain `link_id`, permissions, an `active`, `expired`, or `revoked` status, and timestamps. They never include link secrets or hashes. Entries are ordered newest first. `limit` is at most 100. A non-empty page may carry a non-empty `next_cursor` for the next page. Link IDs do not repeat within a page.

Title visibility follows stream metadata visibility. A public stream title is public. A private stream title requires read permission. A write-only link cannot read it.

## HTTP data plane

`POST /streams/{stream_id}/records` appends one atomic batch. The body carries a stable `client_writer_id`, `writer_start_seq_num`, one to 128 records, and an optional `expected_next_seq_num`. The client writer ID is 16 bytes encoded as exactly 22 canonical unpadded base64url characters. The precondition compares the stream's next sequence number.

Each record contains an optional part header, a presentation format, and byte-preserving data encoded as UTF-8 or base64url. An omitted part header means final part zero, which is an unsplit record. The Rust and TypeScript SDKs use whichever JSON representation is smaller.

Each decoded record is at most 512 KiB. The decoded batch payload is at most 900 KiB. The encoded body is at most 1,300,000 bytes. The writer sequence range must end before `u64::MAX`.

The response returns the half-open durable range as `start_seq_num` and `end_seq_num`. Its length equals the submitted record count.

An ambiguous append response does not imply physical exactly-once delivery. A retry reuses identical writer identity, writer sequence numbers, and data. It may create physical duplicates. Logical transcript readers suppress those duplicates by writer sequence.

`GET /streams/{stream_id}/records` requires `Accept: text/event-stream`. It accepts one of `seq_num`, `timestamp`, or `tail_offset`. It also accepts `count`, exclusive `until`, `rate`, and `wait`. An omitted selector means `tail_offset=0`. Unsupported, duplicate, and conflicting query parameters fail before backend work.

`seq_num`, `timestamp`, `tail_offset`, `count`, `until`, and `wait` follow [S2 read semantics](https://s2.dev/docs/sdk/reading). `rate` is a TSF extension. TSF does not expose S2's `clamp` or `bytes` parameters. Start resolution below defines TSF's fixed clamping behavior.

An SSE response sends `stream_metadata` first. Its data is the same stream metadata object returned by REST.

`read_batch` events contain up to 1,000 records and 1 MiB of decoded record data. Completed events are at most 2 MiB including their terminator. Clients separately cap an unterminated event at 2 MiB so acceptance does not depend on transport chunking.

The Rust and TypeScript SDKs validate decoded record and batch limits, contiguous sequences, requested stop conditions, and event cursor progression. `caught_up` establishes a safe reconnect position. SSE comments are heartbeats.

Every `read_batch` and `caught_up` event has a strict versioned CSV ID. Its form is `v1,<next_seq_num>,<consumed_count>`. The consumed count is the number of physical records delivered in this logical read and cannot exceed the next sequence number. Both fields are canonical decimal u64 values. The cursor is not an authorization credential.

`Last-Event-ID` is authoritative resume state. The original URL remains unchanged and its selector and options are still strictly validated. The URL `count` is the total physical record count. A resumed request enforces only the unconsumed remainder.

A count-zero read carries its terminal cursor on `stream_metadata`. A resume that has exhausted `count` returns HTTP 204 so native `EventSource` stops reconnecting.

Public readers can use native `EventSource`. Private readers use streaming `fetch` with a bearer header on every request.

Retryable interruptions abort the response. The Rust and TypeScript SDKs reconnect with the unchanged URL and latest event ID. They honor server retry hints up to two seconds.

A non-retryable failure after the response opens sends a terminal `error` event and then ends the response. Its data has the form `{ "error": { "code": "...", "message": "..." } }`. It has no resume ID.

The SDK HTTP request timeout bounds each opening handshake through `stream_metadata`. It does not time out an established event body.

## Browser history export

The tail.surf web app reads from sequence zero with `wait=0` and writes `application/x-ndjson`. It finishes when the read catches up.

The first line has `type` set to `tailsurf_export`, `version` set to `1`, and the `stream_id`.

Each later line has `type` set to `record`. It preserves `seq_num`, `timestamp_ms`, `writer_id`, `writer_seq_num`, `part`, `format`, `encoding`, and `data`. Sequence values are decimal strings. The writer ID is 32 lowercase hexadecimal characters. `part` contains `index` and `is_final`. `format` is `bytes` or `transcript`.

A valid UTF-8 transcript record uses `encoding: "utf8"` and its text as `data`. Opaque bytes and invalid UTF-8 transcript records use `encoding: "base64"` and standard padded base64 data.

A reconnect resumes at the next sequence number and can observe a newer tail. Records appended during an interrupted export can therefore be included. Stream expiry or owner deletion can terminate an export.

## Stream lifecycle

Stream lifecycle states are `active`, `deleting`, and `deleted`.

At `expires_at`, new reads, writes, management requests, and renewal fail. Established SSE and WebSocket sessions reauthorize on an absolute deadline no more than 60 seconds after their previous authorization. Expiry, link revocation, deletion, and a public-to-private visibility change take effect by that deadline. Reconnecting does not extend it.

The service later moves an expired stream to `deleting`, revokes its active links, and removes its stored records.

Deleting an active stream durably changes it to `deleting` and revokes its active links.

The stream changes to `deleted` only after its stored records are removed. A `204` response means the deletion request is durable. Removal may still be pending.

`deleting` and `deleted` are tombstone states. Later reads and writes fail. Deleted tombstones remain in metadata storage for 90 days.

Inactive link metadata remains for 30 days. Link inventory omits rows after cleanup.

## Errors and retries

REST errors use one stable shape:

```json
{"error":{"code":"bad_request","message":"human-readable detail","request_id":"request identifier"}}
```

Codes use lowercase `snake_case` and may drive client behavior. Messages are for display and debugging.

Requests consume short-term and rolling request budgets. HTTP rate denials return `429 rate_limited` with `Retry-After`.

Exhausted client, stream, or link allowances return `403 free_plan_limit`. Exhausted global capacity returns `503 overloaded`.

Free-plan expiry denials also return `403 free_plan_limit`.

Response fields may be added when they are optional. Removing fields or changing field meaning is a compatibility change.

The Rust and TypeScript SDKs buffer at most 2 MiB for one successful REST response. They inspect at most 64 KiB from an error response. Larger responses fail without retaining the remaining body.

The official SDKs use one attempt-count option for bounded operations. Rust names it `bounded_operation_attempts`. TypeScript names it `boundedOperationAttempts`. The count includes the initial attempt. Established durable writers recover without an attempt limit.

The SDKs own the retry schedule. Exponential backoff starts at 200 milliseconds and caps at two seconds. Client-controlled delays are jittered and never exceed that cap. Explicit server retry hints are not jittered and use the same cap.

Official SDK defaults bound REST requests and SSE opening handshakes at 10 seconds. WebSocket connection and progress bounds are 10 and 30 seconds. These are separate caller options because they govern different operations.

Rust names the timeout options `http_request_timeout`, `websocket_connect_timeout`, and `websocket_progress_timeout`. TypeScript names them `httpRequestTimeoutMs`, `webSocketConnectTimeoutMs`, and `webSocketProgressTimeoutMs`.

The server sends a WebSocket heartbeat after 20 seconds without another read frame. SDK readers reconnect after three silent heartbeat intervals. The silent-read cutoff is fixed client behavior.

Caller-configured timeouts cannot exceed 2,147,483,647 milliseconds. This keeps behavior consistent with JavaScript timers.

A failed append sequence precondition uses `412 sequence_mismatch` and includes `actual_next_seq_num`. Official SDK HTTP errors expose that value with the request ID and retry delay.

## WebSocket upgrade

Data-plane routes are:

```txt
/api/v1/streams/{stream_id}/write
/api/v1/streams/{stream_id}/read
```

Clients must offer `Sec-WebSocket-Protocol: tsf.v1`. The server echoes `tsf.v1` after a successful upgrade.

Connection attempts consume request capacity before the upgrade. A short-term denial rejects the upgrade with HTTP `429` and `Retry-After`. Free allowance and global capacity denials use the REST status codes above.

When a browser sends an `Origin` header, the server rejects origins other than the configured web origin.

Read WebSocket URLs accept the same read query as SSE. The server validates the query before upgrade. Write WebSocket URLs reject every query parameter.

Messages are binary. One WebSocket message contains exactly one TSF frame. Text messages are protocol errors.

All binary integer fields are unsigned and big-endian.

The first byte is the operation ID.

Record batch frames contain one or more records. Each record starts with a big-endian `u32` body length. The length excludes its own four bytes. `AppendBatch` carries at most 128 records. `ReadBatch` carries at most 1,000 records with contiguous physical sequence numbers. Both carry at most 1 MiB of aggregate record data, excluding headers.

## Client frames

| Operation | ID | Body |
| --- | --- | --- |
| `OpenRead` | `0x01` | flags, then an optional 32-byte encoded link secret |
| `OpenWrite` | `0x02` | flags, client writer ID as 16 bytes, optional expected next sequence `u64`, then a 32-byte encoded link secret |
| `AppendBatch` | `0x03` | records containing body length `u32`, writer sequence `u64`, part `u32`, format `u8`, and data |

`OpenRead` starts with one flags byte. Bit `0x01` indicates that a link secret follows as exactly 32 canonical unpadded base64url bytes. Unknown flags, truncation, trailing bytes, and malformed or non-canonical credentials are protocol errors.

`OpenWrite` starts with one flags byte. Bit `0x01` indicates that an expected next sequence follows the client writer ID. Unknown bits are protocol errors.

## Server frames

| Operation | ID | Body |
| --- | --- | --- |
| `Ready` | `0x80` | empty |
| `AppendAck` | `0x81` | writer start `u64`, writer end `u64`, durable start `u64`, durable end `u64` |
| `ReadBatch` | `0x82` | records containing body length `u32`, sequence `u64`, timestamp milliseconds `u64`, 16-byte writer ID, writer sequence `u64`, part `u32`, format `u8`, and data |
| `Heartbeat` | `0x83` | empty |
| `CaughtUp` | `0x84` | next sequence `u64`, last-record timestamp milliseconds `u64` |
| `StreamMetadata` | `0x85` | UTF-8 JSON matching the REST stream metadata schema |

An empty stream has position `(0, 0)`. A non-empty stream can also have a last timestamp of zero. The next sequence disambiguates the cases. Malformed lengths, malformed client credentials, and unknown formats are protocol errors.

## Record format

Record data may contain at most 512 KiB.

The format byte is a presentation hint. It does not transform bytes.

- `0x00` means opaque bytes.
- `0x01` means transcript text.

Transcript readers preserve payload bytes. They do not add separators or remove newlines.

The part header uses bit 31 as the final bit. Bits 0 through 30 contain the part index.

An unsplit record is final part zero.

## Writes

A writer opens first:

```txt
-> OpenWrite
<- Ready
```

The writer sends `OpenWrite` immediately after the WebSocket opens. It can require an initial stream position with the optional expected-next-sequence field. The server applies that match across the socket's session.

The Rust and TypeScript SDKs repeat the initial precondition while retrying before `Ready`. After `Ready`, reconnects preserve the link secret, client writer ID, and unacknowledged records but omit the initial precondition. This avoids rejecting a retry when an acknowledgement was lost after durability.

A mismatch closes the socket with `1008 sequence_mismatch`. SDK durable writers surface a terminal sequence-mismatch error without retrying.

The writer may then send `AppendBatch` frames. `AppendAck` reports half-open contiguous writer and durable sequence ranges after durability.

An acknowledgement may cover several records. The server splits acknowledgement ranges when writer or durable sequence numbers are not contiguous.

An append frame is not atomic. The server acknowledges durable contiguous ranges. The sequence match advances across the session.

The TypeScript and Rust durable writers create a fresh client writer ID and start its sequence at zero. They retain that identity and sequence progress across reconnects.

The writers assign one contiguous sequence range to each submission in submission order. Concurrent submissions do not interleave their records.

One TypeScript `appendBatch` call or Rust `AppendBatch` is a sequencing and completion unit. It may span multiple append frames. It is not an atomic service append.

SDK durable writers queue submitted input without a separate record, byte, or logical-record admission limit. They send queued records through a fixed window of 1,024 records and 5 MiB of payload. The window contains only sent records that have not been acknowledged.

The writers retain acknowledged progress, complete finished submissions, and resend only the unacknowledged suffix after reconnecting. Retryable interruptions keep recovering with the same writer identity, sequence numbers, and payloads until acknowledgement or explicit cancellation.

`OpenWrite` carries the client writer ID. Each read record carries a server-scoped writer ID derived from the stream, authorized Link ID, and client writer ID. Reconnects with the same write link preserve that derived identity. Different write links cannot claim the same read-side identity.

An append can become durable before its acknowledgement is lost. A non-retryable failure or explicit cancellation can therefore report unknown durability. A terminal failure can leave a durable prefix even when the submission reports an error. Submitting uncertain records under a new writer identity may duplicate them.

TypeScript exposes `abort`. Rust exposes `abort` and cancels when the writer or its close future is dropped. `close` waits for accepted records through retryable outages.

## Reads

A reader puts its request in the WebSocket URL query. It sends `OpenRead` immediately after the WebSocket opens. Anonymous public readers omit its link secret. Private readers include a read-capable link secret.

```txt
-> OpenRead
<- Ready
<- StreamMetadata
```

The server sends `Ready` only after it accepts the request and authorization. An authorization failure closes without `Ready`.

`StreamMetadata` follows `Ready` for every authorized read. It contains `stream_id`, `title`, `visibility`, `created_at`, and `expires_at`. Readers ignore unknown JSON fields.

The server then sends `ReadBatch`, `CaughtUp`, and `Heartbeat` frames. Every read record includes its durable sequence and timestamp. An otherwise idle connection sends a heartbeat every 20 seconds.

`CaughtUp` means every preceding record below its captured next sequence has been delivered. It contains the next sequence number and last-record timestamp. An empty stream uses `(0, 0)`. Timestamp zero remains valid for a non-empty stream.

`CaughtUp` is not a periodic heartbeat. The server emits it when a newly observed caught-up position differs from the last one it sent.

The Rust and TypeScript SDKs retain the latest caught-up position.

A reader must not send frames after `OpenRead`.

The read query contains at most one start selector:

- A sequence selector starts at the first sequence greater than or equal to the value.
- A timestamp selector starts at the first timestamp greater than or equal to the value.
- A tail-offset selector starts that many records before the current tail and saturates at sequence zero.
- An omitted selector means tail offset zero. The Rust and TypeScript SDKs make the same default explicit in their request URL.

Selector and `until` values may not exceed `9007199254740991` so they remain exact JavaScript integers. `count` retains the full `u64` range.

An absolute sequence start beyond the current tail remains pending. The server filters records below that sequence and does not emit `CaughtUp` below it.

A timestamp selector resolves on its first matching record or an observed empty-batch tail. Later timestamp regressions remain in physical sequence order. The Rust and TypeScript SDKs reject batches and caught-up positions that do not continue the requested state.

Each record in `ReadBatch` carries its absolute sequence. After delivering a record, an official client reconnects with `seq_num` set to that sequence plus one.

An empty underlying batch produces `CaughtUp` without a preceding record. Its `next_seq_num` becomes the reconnect position.

A connection that ends before either a record or `CaughtUp` establishes a position retries its original selector.

`count` bounds the physical records delivered across the TSF session. A count of zero completes without reading any data. `until` is a Unix epoch millisecond timestamp. Records with timestamps below it are delivered. Records at or above it are not. The timestamp condition remains unchanged across reconnects.

`wait` is an integer number of seconds from zero through 60. An omitted `wait` follows indefinitely when `count` and `until` are also absent. A read with `count` or `until` defaults to `wait=0`.

`wait=0` drains through the tail observed by that connection and ends. A positive value long-polls at the tail for that many seconds before ending. With the default `wait=0`, `count` and `until` end at their condition or the current tail, whichever comes first.

A reconnect starts at the next sequence number. It does not retain a fixed tail boundary. The new connection can observe and deliver records appended after the previous connection began.

The `rate` query value is a floating-point multiplier from `0.1` through `100`. `1` is recorded speed. `2` is twice recorded speed. `0.5` is half speed. TypeScript stores it as a `number`. Rust stores it as an `f64`.

Paced reads require `count`, `until`, or `wait=0`. Each connection uses accumulated deadlines from a monotonic clock. The first record is immediate. Processing and transport time do not accumulate as replay drift.

Each scaled timestamp gap is capped at five seconds. Timestamp regressions do not move the clock backward. Reconnects start a new playback epoch.

Official read sessions resume from the next sequence after transient interruption. A normal close ends the read.

An unbounded backing read that ends or fails transiently closes with `1013 upstream_unavailable`. The Rust and TypeScript SDKs resume from their latest record or `CaughtUp` position. Each SDK applies the fixed bounded backoff.

A finite read that reaches its condition or catches up closes normally.

A successful reader reconnect resets the retry budget, even when the stream remains idle. The budget only bounds consecutive connection failures.

Official transcript readers reassemble parts in delivery order. A read that starts in the middle of a split record skips that partial logical record.

Rust and TypeScript transcript readers use one 16 MiB reassembly-byte limit. Rust names it `max_reassembly_bytes`. TypeScript names it `maxReassemblyBytes`. It bounds bytes retained across unfinished split records and the size of one completed split-record assembly. Unsplit records borrow their input payload and do not consume this budget.

## Close behavior

The server uses these WebSocket closes:

- `1000 normal` for normal completion.
- `1001 server_shutdown` for runtime shutdown.
- `1002 protocol_error` for invalid frames or role flow.
- `1008 unauthorized`, `forbidden`, or `free_plan_limit` for permanent policy failures.
- `1011 backend_error` or `internal_error` for server failures.
- `1013 overloaded` when bounded read or write backpressure cannot recover, or global capacity is exhausted.
- `1013 rate_limited` for temporary established-socket rate pressure.
- `1013 upstream_unavailable` when an unbounded backing read ends or fails transiently.

The Rust and TypeScript SDKs retry `1013` and other bounded transient failures. Protocol and policy failures surface immediately.

## Compatibility

REST and SSE use the `/api/v1` route prefix. A breaking change requires a new route version.

Binary clients and the server select the exact `tsf.v1` subprotocol. A breaking binary change requires a new subprotocol.

Additive REST and SSE behavior must remain ignorable by v1 clients. Additive binary behavior must remain ignorable under `tsf.v1`. Otherwise it needs explicit capability negotiation or a new version.

Changes to permissions, routes, status codes, close behavior, record reconstruction, URL fragments, or the history export format are public compatibility changes.

## Conformance

TypeScript and Rust must agree on:

- frame vectors and malformed-frame rejection
- IDs and permission strings
- URL fragment parsing
- REST request and response schemas
- read-query mapping and reconnect state
- close codes and retry classes
- record splitting, reconstruction, and duplicate suppression
- sequence and acknowledgement mapping

This repository requires its Rust and TypeScript fixture copies to remain byte-identical. The hosted service runs the released TypeScript packages and Rust CLI against its implementation.
