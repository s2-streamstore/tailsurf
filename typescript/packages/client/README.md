# `@tailsurf/client`

`@tailsurf/client` is the supported TypeScript API for tail.surf. It works in modern browsers and Node.js 22 or newer.

It includes REST operations, resumable SSE reads, and reconnecting WebSocket readers and writers.

The package is ESM-only.

## Install

```sh
npm install @tailsurf/client
```

## Quickstart

```ts
import { TsfClient, buildStreamLink } from "@tailsurf/client";

const client = new TsfClient();
const stream = await client.createStream({
  title: "Production deploy",
  visibility: "public",
});
const owner = stream.links.find((link) => link.permissions === "o");

if (owner === undefined) {
  throw new Error("owner link missing");
}

const writer = await client.connectWriter({
  streamId: stream.streamId,
  linkSecret: owner.secret,
});
await writer.append({ data: "deploy started\n" });
await writer.close();

const ownerUrl = buildStreamLink(
  "https://tail.surf",
  stream.streamId,
  owner.permissions,
  owner.secret,
);
console.log(ownerUrl.href);
```

The default API origin is `https://tail.surf`. Set `apiOrigin` when using another deployment.

## Read

Read sessions are async iterables. Breaking out of the loop closes the session.

```ts
const session = await client.connectReader({
  streamId: stream.streamId,
  start: { type: "seqNum", seqNum: 0n },
  stop: { waitSeconds: 0 },
});

for await (const record of session) {
  console.log(new TextDecoder().decode(record.data));
}
```

Omit `stop` to follow new records. Use `connectSseReader` for a resumable HTTP event stream.

## Write

`connectWriter` keeps one writer identity across reconnects. Concurrent append calls are coalesced into bounded batches. `close` rejects new appends and waits for accepted appends to settle.

```ts
const writer = await client.connectWriter({
  streamId: stream.streamId,
  linkSecret: owner.secret,
});

try {
  await Promise.all([
    writer.append({ data: "first\n" }),
    writer.append({ data: "second\n" }),
  ]);
} finally {
  await writer.close();
}
```

## Retries and errors

REST mutations use idempotency keys. Pass a caller-owned key as the second argument when a creation must survive page reloads.

```ts
await client.createStream(request, { idempotencyKey });
```

Transient REST and connection failures use the configured bounded `retryPolicy`. Client failures extend `TsfClientError`. HTTP failures are `TsfHttpError` and include the status, request ID, retry hint, and structured API code when the server provides them.

## Runtime configuration

The client uses global `fetch`, `crypto`, and `WebSocket` implementations. Override `fetch` or `webSocketFactory` for tests and custom runtimes.

Common IDs, permissions, stream-link helpers, record types, and transcript reconstruction are re-exported from this package. Use `@tailsurf/protocol` directly for raw frame codecs, wire schemas, or compatibility fixtures.

## License

MIT
