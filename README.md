# tail.surf (tsf)

tail.surf is a streaming gist for live work and agent conversations.

Each stream gets a stable URL that can be concurrently written to, read from anywhere, and tailed in real-time.

Free to start with no sign-up required.

Use it to stream sandbox output, build output, deploy logs, sandbox sessions, or agent-to-agent messages. For example,
- Share long-running command output without keeping an SSH session, sandbox, or terminal attached.
- Give agents a reliable async channel with durable catch-up.
- Turn build, test, deploy, and debugging output into a permalink that can be inspected in real-time or after the fact.

`tsf` has an API and 2 first-class clients:
- Full-featured CLI
- Gist-style scrubbable live transcript with web UI

## Development

This repo is a Rust workspace with two crates:
- `tailsurf`: SDK/common crate with shared API types, stream URL parsing, permissions, IDs, and binary frame encoding.
- `tailsurf-cli`: CLI shell for stream workflows and URL validation. Its binary is named `tsf`.

Language-neutral TSF v3 frame vectors live in `tailsurf/fixtures/v3.json`. They are packaged with the SDK and exercised by both the Rust and TypeScript implementations.

## Install

Install the CLI from crates.io:

```sh
cargo install tailsurf-cli
```

For workspace development, install it from the local checkout:

```sh
cargo install --path tailsurf-cli
```

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

`tsf new` prints the stream retention in seconds along with the generated URLs.

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

Tail or replay a URL:

```sh
tsf tail '{url}'
tsf replay '{url}'
```

Owner URLs contain `#o=` and can manage the stream:

```sh
tsf visibility '{owner-url}' public
tsf token issue '{owner-url}' --token r
tsf token list '{owner-url}'
tsf token revoke '{owner-url}' '{token_id}'
tsf delete '{owner-url}'
```

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

Package and publish order:

```sh
cargo package --workspace
cargo publish -p tailsurf
# Wait for tailsurf 0.1.0 to appear in the crates.io index before packaging/publishing the CLI.
cargo package -p tailsurf-cli
cargo publish -p tailsurf-cli
```

Use `cargo package --workspace` as the clean-tree preflight because it verifies both crates from the workspace source. After publishing `tailsurf`, wait for `tailsurf 0.1.0` to appear in the crates.io index before packaging or publishing `tailsurf-cli` by itself, because the packaged CLI resolves its SDK dependency from crates.io rather than the local workspace path.

After `tailsurf-cli` is visible in the crates.io index, run the install smoke against the deployed service:

```sh
TSF_CLI_VERSION=0.1.0 TSF_API_URL=https://tail.surf TSF_WEB_URL=https://tail.surf python3 scripts/published-cli-smoke.py
```

Try the CLI URL parser:
```sh
cargo run -p tailsurf-cli -- parse-url 'https://tail.surf/s/0123456789abcdefghjkmnpqrstvwxyz#r=example-token'
```

## License

The Rust SDK and CLI are MIT licensed. See [LICENSE](LICENSE).
