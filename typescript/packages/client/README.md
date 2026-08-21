# `@s2-dev/tailsurf-client`

`@s2-dev/tailsurf-client` is the supported TypeScript API for tail.surf. It works in modern browsers and Node.js 22 or newer.

It includes REST operations, resumable SSE reads, and reconnecting WebSocket readers and writers.

The package is ESM-only.

## Install

```sh
npm install @s2-dev/tailsurf-client
```

## Quickstart

```ts
import { TsfClient, buildStreamLink } from "@s2-dev/tailsurf-client";

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

## Manage

Management methods require an owner link secret. `listLinks` returns one page. `listAllLinks` follows pagination and validates the complete inventory.

## Write

`connectWriter` creates a fresh writer identity and starts its sequence at zero. It keeps that identity and sequence progress across reconnects.

Concurrent append calls receive contiguous sequence ranges in call order. The writer coalesces them into bounded protocol frames. It retains acknowledged progress and resends only the unacknowledged suffix after a reconnect.

Retryable interruptions keep recovering until the records are acknowledged. This preserves the exact writer identity, sequence numbers, and payloads needed for logical deduplication. `close` waits through retryable outages. Call `abort` to stop recovery immediately.

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

`appendBatch` is one sequencing and Promise unit. It may span several protocol frames. It is not an atomic service append. A terminal failure can leave a durable prefix even when the Promise rejects.

`writer_durability_unknown` means a non-retryable failure or explicit cancellation left an accepted append without a recovered acknowledgement. Submitting that record under a new writer identity may duplicate it.

`appendLogical` splits data above the 512 KiB physical-record limit into contiguous parts.

The writer queues submitted input and sends it through a fixed socket window of 128 records and 5 MiB. A submission may be larger than that window.

```ts
const writer = await client.connectWriter({
  streamId: stream.streamId,
  linkSecret: owner.secret,
});

await writer.appendLogical({ data: largeTranscriptRecord });
await writer.close();
```

## Retries and errors

REST mutations use idempotency keys. Pass a caller-owned key as the second argument when a creation must survive page reloads.

```ts
await client.createStream(request, { idempotencyKey });
```

Transient REST failures, initial connections, and readers use the configured bounded `retryPolicy`. An established durable writer uses its backoff without an attempt limit. Client failures extend `TsfClientError`. HTTP failures are `TsfHttpError` and include the status, request ID, retry hint, and structured API code when the server provides them.

## Runtime configuration

The client uses global `fetch`, `crypto`, and `WebSocket` implementations. Override `fetch` or `webSocketFactory` for tests and custom runtimes.

Common IDs, permissions, stream-link helpers, record types, and transcript reconstruction are re-exported from this package. Use `@s2-dev/tailsurf-protocol` directly for raw frame codecs, wire schemas, or compatibility fixtures.

## License

MIT
