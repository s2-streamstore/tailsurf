# tail.surf (tsf)

tail.surf is a streaming gist for live work and agent conversations.

Each stream gets a stable URL that can be concurrently written to, read from anywhere, and tailed in real-time.

Free to start with no sign-up required.

Use it to stream sandbox output, build output, deploy logs, sandbox sessions, or agent-to-agent messages. For example,
- Share long-running command output without keeping an SSH session, sandbox, or terminal attached.
- Give agents a reliable async channel with durable catch-up.
- Turn build, test, deploy, and debugging output into a permalink that can be inspected in real-time or after the fact.

`tsf` has an API and two first-class clients:
- Full-featured CLI
- Gist-style scrubbable live transcript with web UI

## Development

This repo is a Rust workspace with two crates:
- `tailsurf`: SDK/common crate with shared API types, stream URL parsing, permissions, IDs, and binary frame encoding.
- `tailsurf-cli`: CLI shell for stream workflows and URL validation. Its binary is named `tsf`.

Language-neutral TSF v3 frame vectors live in `tailsurf/fixtures/v3.json`. They are packaged with the SDK and exercised by both the Rust and TypeScript implementations.

## Install

Install the prebuilt CLI on macOS or Linux:

```sh
curl --proto '=https' --tlsv1.2 -LsSf https://tail.surf/install | sh
```

Install it from PowerShell on Windows:

```powershell
irm https://tail.surf/install.ps1 | iex
```

The direct installer puts `tsf` in `~/.local/bin`. It writes an installer receipt so `tsf` can update only the files that installer owns.

Cargo remains the Rust-native fallback:

```sh
cargo install tailsurf-cli --locked
```

For workspace development, install it from the local checkout:

```sh
cargo install --path tailsurf-cli
```

Update a direct installation explicitly:

```sh
tsf update
```

Check without installing:

```sh
tsf update --check
```

`tsf` does not check for updates in the background. Installations owned by a package manager stay with that manager. Cargo users rerun `cargo install tailsurf-cli --locked`. cargo-binstall users rerun `cargo binstall tailsurf-cli`.

To use a local API:

```sh
export TSF_API_URL=http://127.0.0.1:8787
export TSF_WEB_URL=http://localhost:3000
```

`TSF_API_URL` is the API origin. The SDK appends the versioned `/api/v1` namespace.

SDK readers and producers retry bounded transient WebSocket interruptions, including service shutdown and restart closes, while preserving read positions and unacknowledged writer sequence numbers. Protocol and policy closes such as `1002` and `1008` fail immediately instead of reconnecting with a request that cannot succeed.

## SDK quickstart

The [`create_write_read_delete`](https://github.com/s2-streamstore/tailsurf/blob/main/tailsurf/examples/create_write_read_delete.rs) example creates a private stream, writes one durable record through the reconnecting producer, reads it back, and deletes the stream with its owner token:

```sh
cargo run -p tailsurf --example create_write_read_delete
```

Set `TSF_API_URL` to run the same example against a local API. Applications normally use `TsfProducer` and `TsfReadSession`; `TsfAppendSession` is the lower-level frame/ack API.

## CLI quickstart

Create a private stream:

```sh
tsf new
```

Create a public stream:

```sh
tsf new --public
```

Choose record retention with a human duration:

```sh
tsf new --retention 7d
make test | tsf write --new --retention 6h
```

`--retention infinite` explicitly requests infinite retention. The service enforces the current free-user limit and returns a clear error when a requested policy is unavailable.

`tsf new` prints the stream ID, retention, and an owner link. Issue more links at creation with `--link view`, `--link write`, `--link view+write`, or `--link owner`. Links are shown once.

Stream command output into a new URL:

```sh
make test | tsf write --new
```

Run a command through `tsf` when you want `tsf` to propagate the command exit status:

```sh
tsf write --new -- make test
```

By default, `tsf write` makes line boundaries transcript record boundaries and marks records as transcript-oriented:

```sh
make test | tsf write --new
```

Use raw mode when you want to send stdin as byte records instead of line-framed transcript records. Raw mode flushes at the physical record size limit, after a short linger, and at EOF:

```sh
cat artifact.bin | tsf write --new --raw
```

On Ctrl-C, `tsf write` stops input, flushes accepted bytes, waits for durability acknowledgements, closes the producer, and exits with status 130.

Tail or replay a URL:

```sh
tsf tail '{url}'
tsf tail -n 200 '{url}'
tsf tail --seq-num 0 --count 500 '{url}'
tsf replay '{url}'
```

`tail` follows new records unless `--count` bounds it. `replay` snapshots the current durable tail and exits after printing that range.

Both commands preserve payload bytes. They exit successfully when a downstream pipe closes normally.

Inspect stream metadata:

```sh
tsf info '{url}'
tsf info '{url}' --format json
```

Owner URLs contain `#o=` and can manage the stream:

```sh
tsf visibility '{owner-url}' public
tsf link issue '{owner-url}' --access view --expires 7d
tsf link list '{owner-url}'
tsf link revoke '{owner-url}' '{link_id}'
tsf delete '{owner-url}'
```

Access levels are `view`, `write`, `view+write`, and `owner`. `--expires` accepts durations such as `1h` or `7d`, or `never` (the default).

Token file options write only the secret value. On Unix, `tsf` creates and tightens these files to mode `0600`.

Run the checks:
```sh
cargo fmt --all --check
cargo test --workspace
cargo check --workspace --examples
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps
python3 scripts/published-cli-smoke.py --self-test
cargo package --workspace
```

## Releases

Release-plz opens or updates a release PR after changes reach `main`. It derives the next workspace version from conventional commits and checks SDK API compatibility. Both crates use the same version.

Merging the release PR publishes `tailsurf` first and then `tailsurf-cli`. Release-plz creates one `vX.Y.Z` tag. It then dispatches the binary release workflow directly so the repository token does not depend on tag-triggered workflow chaining.

Cargo-dist creates one GitHub release for that tag. It builds `tsf` for Apple Silicon and Intel macOS, ARM64 and x86-64 musl Linux, and x86-64 Windows. Axoupdater is embedded in `tsf` and uses the cargo-dist receipt for explicit updates.

The release contains versioned archives, shell and PowerShell installers, SHA-256 checksums, and GitHub artifact attestations. macOS executables use Developer ID signing with hardened runtime. Windows executables are unsigned. Linux and Windows artifacts rely on checksums and GitHub attestations.

GitHub release builds fail when macOS signing credentials are missing. Signing uses the `CODESIGN_CERTIFICATE`, `CODESIGN_CERTIFICATE_PASSWORD`, and `CODESIGN_IDENTITY` repository secrets.

Cargo-dist does not notarize macOS artifacts. Browser-downloaded macOS archives may still trigger Gatekeeper until Apple notarization is added.

The installer routes on `tail.surf` redirect to the latest public GitHub release. Older tags and their assets remain available for rollback and CI pinning. Homebrew distribution is not configured.

Publishing uses crates.io trusted publishing. Both crates trust the `s2-streamstore/tailsurf` repository and the `release-plz.yml` workflow without a GitHub environment.

After `tailsurf-cli` is visible in the crates.io index, run the install smoke against the deployed service:

```sh
TSF_API_URL=https://tail.surf TSF_WEB_URL=https://tail.surf python3 scripts/published-cli-smoke.py
```

Try the CLI URL parser:

```sh
cargo run -p tailsurf-cli -- parse-url 'https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=example-token'
```

## License

The Rust SDK and CLI are MIT licensed. See [LICENSE](LICENSE).
