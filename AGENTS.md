# Agent Instructions

- Use `--locked` for Cargo commands that build, check, test, run, document, fetch, or read metadata.
- Use `cargo +nightly add`, `cargo +nightly update`, `cargo +nightly remove`, or `cargo +nightly generate-lockfile` for dependency changes.
- Do not use stable Cargo for dependency changes.
- Do not edit dependency declarations or `Cargo.lock` directly.
- Do not run `cargo install` as part of a coding task.
- Run Rust formatting with `cargo +nightly fmt --all --check`.
- Run Rust tests with `cargo test --locked --workspace --all-targets`.
- Run Rust linting with `cargo clippy --locked --workspace --all-targets -- -D warnings`.
