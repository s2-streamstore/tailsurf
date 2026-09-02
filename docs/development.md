# Development

The repository root is a virtual Cargo workspace. The CLI is the default workspace member, and its binary is named `tsf`.

- `cli`: CLI package and integration tests.
- `rust`: Rust SDK and common crate with API types, stream URL parsing, permissions, IDs, and binary frame encoding.
- `typescript`: TypeScript workspace containing the high-level client and low-level protocol packages.

Language-neutral TSF v1 frame vectors and REST examples live in `rust/fixtures`. Both implementations test against these files. The TypeScript package build copies them into its published `dist` tree.

## Local installation

Install the CLI from the checkout:

```sh
cargo install --path cli
```

## Local service

Point the CLI at a local service:

```sh
export TSF_ORIGIN=http://127.0.0.1:8787
```

`--origin` (env `TSF_ORIGIN`) is the service origin. It defaults to `https://tail.surf`. The SDK appends the versioned `/api/v1` namespace.

Stream links use the `web_origin` the service returns when it mints credentials. The server configures it with `TSF_WEB_ORIGIN`, so a split local web and API deployment prints correct links without extra CLI configuration.

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

The TypeScript workspace has its own checks and package tests. See [TypeScript SDK development](../typescript/README.md#development).
