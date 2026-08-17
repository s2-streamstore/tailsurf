# Development

The repository root is a virtual Cargo workspace. The CLI is the default workspace member, and its binary is named `tsf`.

- `cli`: CLI package and integration tests.
- `rust`: Rust SDK and common crate with API types, stream URL parsing, permissions, IDs, and binary frame encoding.
- `typescript/packages/client`: Supported high-level TypeScript API for browsers and Node.js.
- `typescript/packages/protocol`: Low-level TypeScript schemas, codecs, primitives, and fixtures.

Language-neutral TSF v1 frame vectors live in both protocol packages. Forward-compatible REST and SSE examples do too. `typescript/scripts/verify-fixtures.mjs` requires the Rust and TypeScript copies to remain byte-identical.

## Local installation

Install the CLI from the checkout:

```sh
cargo install --path cli
```

## Local service

Point the clients at a local API and web application:

```sh
export TSF_API_URL=http://127.0.0.1:8787
export TSF_WEB_URL=http://localhost:3000
```

`TSF_API_URL` is the API origin. The SDK appends the versioned `/api/v1` namespace.

## Checks

Run the workspace checks:

```sh
cargo +nightly fmt --all --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
scripts/verify-packages.sh
```

The package verifier builds the exact SDK and CLI archives that would be published. It extracts both archives, patches the CLI registry dependency to the packaged SDK, and checks every packaged target.

Run the TypeScript checks:

```sh
cd typescript
pnpm install --frozen-lockfile --strict-peer-dependencies
pnpm exec playwright install chromium
pnpm check
```

The TypeScript package verifier builds the exact npm tarballs, installs them in a temporary consumer, type-checks Node.js and browser consumers, and runs Node.js and Chromium smoke tests.
