# Development

This repository is a Rust workspace with two crates:

- `tailsurf`: SDK and common crate with API types, stream URL parsing, permissions, IDs, and binary frame encoding.
- `tailsurf-cli`: CLI shell for stream workflows and URL validation. Its binary is named `tsf`.

Language-neutral TSF v3 frame vectors live in `tailsurf/fixtures/v3.json`. They are packaged with the SDK and exercised by both the Rust and TypeScript implementations.

## Local installation

Install the CLI from the checkout:

```sh
cargo install --path tailsurf-cli
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
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
scripts/verify-packages.sh
```

The package verifier builds the exact SDK and CLI archives that would be published. It extracts both archives, patches the CLI registry dependency to the packaged SDK, and checks every packaged target.
