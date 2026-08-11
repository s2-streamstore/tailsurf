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

Update a direct installation explicitly:

```sh
tsf update
```

Check without installing:

```sh
tsf update --check
```

`tsf` does not check for updates in the background. Installations owned by a package manager stay with that manager. Cargo users rerun `cargo install tailsurf-cli --locked`. cargo-binstall users rerun `cargo binstall tailsurf-cli`.

## SDK quickstart

The [`create_write_read_delete`](https://github.com/s2-streamstore/tailsurf/blob/main/tailsurf/examples/create_write_read_delete.rs) example creates a private stream, writes one durable record through the reconnecting producer, reads it back, and deletes the stream with its owner token:

```sh
cargo run -p tailsurf --example create_write_read_delete
```

Set `TSF_API_URL` to use a non-default API origin. The SDK appends the versioned `/api/v1` namespace.

Applications normally use `TsfProducer` and `TsfReadSession`; `TsfAppendSession` is the lower-level frame/ack API.

SDK readers and producers retry bounded transient WebSocket interruptions while preserving read positions and unacknowledged writer sequence numbers. Protocol and policy closes fail immediately.

REST authorization is stream-scoped. Read methods accept an optional read-capable stream token because public streams need none. Management methods require an owner token on each call. The client never retains one stream credential as implicit authorization for later REST requests.

The default producer window is capped at the service's hard writer-queue contract: 128 records and 5 MiB of payload. Applications may configure smaller windows.

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
make test | tsf write --retention 6h
```

`--retention infinite` explicitly requests infinite retention. The service enforces the current free-user limit and returns a clear error when a requested policy is unavailable.

`tsf new` prints the stream ID, retention, and an owner link. Issue more links at creation with `--link view`, `--link write`, `--link view+write`, or `--link owner`. Links are shown once.

Stream command output into a new URL:

```sh
make test | tsf write
```

`tsf write` creates a stream when no URL is supplied. It prints the view URL to stdout. Creation details, the owner link, and durability status go to stderr.

Run a command through `tsf` when you want `tsf` to propagate the command exit status:

```sh
tsf write -- make test
```

By default, `tsf write` makes line boundaries transcript record boundaries and marks records as transcript-oriented:

```sh
make test | tsf write
make test | tsf write '{write-url}'
```

One logical line is limited to 16 MiB by default. This is the same default used by `tail` and `replay`. Set `--max-logical-record-bytes` on both the writer and reader only when a larger application-specific limit is required.

Use raw mode when you want to send stdin as byte records instead of line-framed transcript records. Raw mode flushes at the physical record size limit, after a short linger, and at EOF:

```sh
cat artifact.bin | tsf write --raw
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

## Development

See [Development](docs/development.md) for the workspace layout, local-service setup, checks, and diagnostics. Release maintainers should also read [Release operations](docs/release-operations.md).

## License

The Rust SDK and CLI are MIT licensed. See [LICENSE](LICENSE).
