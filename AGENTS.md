# Agent Instructions

- Use `--locked` for Cargo commands that build, check, test, run, document, fetch, or read metadata.
- Use `cargo add`, `cargo update`, `cargo remove`, or `cargo generate-lockfile` for dependency changes so the repository publication cooldown applies.
- Avoid editing dependency declarations or `Cargo.lock` directly; if you need to, ask for explicit permission first and flag the risk.
- Do not run `cargo install` as part of a coding task.
- Run Rust formatting with `cargo +nightly fmt --all --check`.
- Run Rust tests with `cargo test --locked --workspace --all-targets`.
- Run Rust linting with `cargo clippy --locked --workspace --all-targets -- -D warnings`.
