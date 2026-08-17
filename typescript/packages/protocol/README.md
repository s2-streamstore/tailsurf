# `@tailsurf/protocol`

`@tailsurf/protocol` contains the low-level TypeScript implementation of the public TSF v1 contract.

It exports IDs, permissions, stream URLs, REST and SSE schemas, read-query helpers, binary WebSocket frames, and transcript reconstruction.

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

## Compatibility

Protocol fixtures are shared with the Rust implementation in the `tailsurf` repository. CI rejects any byte-level drift between the two copies.

Additive fields in server responses are accepted where the public contract is forward-compatible. Client requests and binary frames remain strict.

## License

MIT
