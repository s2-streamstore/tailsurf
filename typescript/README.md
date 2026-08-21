# TypeScript SDK

This workspace contains the public TypeScript implementation of the TSF protocol.

- `@tailsurf/client` is the supported high-level API for browsers and Node.js.
- `@tailsurf/protocol` contains lower-level schemas, codecs, primitives, and compatibility fixtures.

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

## Releasing

Add a changeset to a pull request that changes a published package:

```sh
pnpm changeset
```

Select the affected packages, their semantic version bumps, and a short user-facing summary.

Changesets maintains a TypeScript release pull request on `main`. Merging that pull request publishes every new package version to npm.
