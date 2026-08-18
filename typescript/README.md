# TypeScript SDK

This workspace contains the public TypeScript implementation of the TSF protocol.

- `@s2-dev/tailsurf-client` is the supported high-level API for browsers and Node.js.
- `@s2-dev/tailsurf-protocol` contains lower-level schemas, codecs, primitives, and compatibility fixtures.

The packages use independent versions. A client release declares the protocol versions it supports.

Both packages are ESM-only. Their Node.js baseline is 22.

## Development

Install dependencies and run every check:

```sh
pnpm install --frozen-lockfile --strict-peer-dependencies
pnpm exec playwright install chromium
pnpm check
```

`pnpm test:packages` packs both packages, installs the tarballs into a temporary consumer, type-checks Node.js and browser consumers, and runs Node.js and Chromium smoke tests.

`pnpm test:fixtures` requires the Rust and TypeScript fixture copies to remain byte-identical.
