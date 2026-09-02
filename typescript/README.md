# TypeScript SDK

This workspace contains the public TypeScript implementation of the TSF protocol.

- `@tailsurf/client` is the supported high-level API for browsers and Node.js.
- `@tailsurf/protocol` contains lower-level schemas, codecs, primitives, and conformance fixtures.
- `tailsurf` is an alias that re-exports `@tailsurf/client`.

The packages use independent versions. A client release declares the protocol versions it supports.

The packages are ESM-only. Their Node.js baseline is 22.

## Development

Install dependencies and run every check:

```sh
pnpm install --frozen-lockfile --strict-peer-dependencies
pnpm exec playwright install chromium
pnpm check
```

`pnpm test:packages` packs the packages, installs the tarballs into a temporary consumer, type-checks Node.js and browser consumers, and runs Node.js and Chromium smoke tests.

`pnpm check` validates the TypeScript codecs against the canonical fixtures in `../rust/fixtures` and verifies the published fixture exports.

See [TypeScript releases](RELEASING.md) before changing a published package.
