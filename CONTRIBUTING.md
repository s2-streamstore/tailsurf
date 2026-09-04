# Contributing

Thanks for helping improve Tailsurf.

Bug reports and feature requests use [GitHub Issues](https://github.com/s2-streamstore/tailsurf/issues). Never post owner, write, or private read links, link secrets, idempotency keys, or private stream content.

Report vulnerabilities through [SECURITY.md](SECURITY.md), not Issues.

## Pull requests

PR titles must follow [Conventional Commits](https://www.conventionalcommits.org). CI validates the title, and squash merges use it as the commit subject. Mark breaking changes with `!`, such as `feat!:`.

Rust SDK and CLI versions are managed by release-plz. Do not bump versions or edit `CHANGELOG.md` by hand.

Use nightly Cargo for dependency changes so that the repository publication cooldown applies:

```sh
cargo +nightly add <crate>
cargo +nightly update
cargo +nightly update -p <crate>
cargo +nightly remove <crate>
cargo +nightly generate-lockfile
```

Do not use stable Cargo for dependency changes or edit dependency declarations and `Cargo.lock` directly.

A change to a published TypeScript package needs a changeset. Run `pnpm changeset` in `typescript/` and commit the result. See [RELEASING.md](typescript/RELEASING.md).

The Rust and TypeScript protocol fixture copies must remain byte-identical. Change both together.

CI runs on maintainer approval for first-time contributors, so a run may not start immediately.

## Checks

Formatting uses nightly rustfmt. Run the Rust workspace checks from the repository root:

```sh
cargo +nightly fmt --all --check
cargo test --locked --workspace --all-targets
cargo clippy --locked --workspace --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --locked --workspace --no-deps
scripts/verify-packages.sh
```

Run the TypeScript checks from `typescript/`:

```sh
pnpm install --frozen-lockfile --strict-peer-dependencies
pnpm exec playwright install chromium
pnpm check
```

See [Development](docs/development.md) for workspace layout and local-service setup.
