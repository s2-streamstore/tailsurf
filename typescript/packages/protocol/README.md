# `@tailsurf/protocol`

`@tailsurf/protocol` contains the low-level TypeScript implementation of the public TSF v1 contract.

It exports IDs, permissions, stream URLs, REST and SSE schemas, read-query helpers, binary WebSocket frames, terminal event codecs, and logical-record reconstruction.

Most applications should use `@tailsurf/client` instead.

The package is ESM-only and supports Node.js 22 or newer.

## Install

```sh
npm install @tailsurf/protocol
```

## Example

```ts
import { buildStreamLink, parseStreamId } from "@tailsurf/protocol";

function readLink(rawStreamId: string, linkSecret: string): URL {
  const streamId = parseStreamId(rawStreamId);
  return buildStreamLink("https://tail.surf", streamId, "r", linkSecret);
}
```

The package also exports its language-neutral fixtures at `@tailsurf/protocol/fixtures/v1.json` and `@tailsurf/protocol/fixtures/rest-v1.json`.

`LogicalRecordAssembler` uses one 16 MiB `maxReassemblyBytes` limit. It bounds bytes retained across unfinished split records and the size of one completed split-record assembly. Unsplit records borrow their input payload and do not consume this budget.

SDK durable writers use the shared `MAX_WRITER_IN_FLIGHT_RECORDS` and `MAX_WRITER_IN_FLIGHT_PAYLOAD_BYTES` window. It bounds records that have been sent but not acknowledged.

## Compatibility

Protocol fixtures are shared with the Rust implementation in the `tailsurf` repository. CI rejects any byte-level drift between the two copies.

Additive fields in server responses are accepted where the public contract is forward-compatible. Client requests and binary frames remain strict.

## License

MIT
