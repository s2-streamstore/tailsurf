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

Installer-owned binaries may contact GitHub Releases and print an update hint after a successful interactive command against `https://tail.surf`. The check runs at most once per day, waits at most 500 milliseconds, and never installs an update. Set `TSF_NO_UPDATE_CHECK` or `DO_NOT_TRACK` to disable hints. CI and non-interactive commands do not check.

Installations owned by a package manager do not check or print the hint. Cargo users rerun `cargo install tailsurf-cli --locked`. cargo-binstall users rerun `cargo binstall tailsurf-cli`.

## SDK quickstart

The [`create_write_read_delete`](https://github.com/s2-streamstore/tailsurf/blob/main/tailsurf/examples/create_write_read_delete.rs) example creates a private stream, writes one durable record through the reconnecting producer, reads it back, and deletes the stream with its owner link:

```sh
cargo run -p tailsurf --example create_write_read_delete
```

Set `TSF_API_URL` to use a non-default API origin. The SDK appends the versioned `/api/v1` namespace.

Applications normally use `TsfProducer` and `TsfReadSession`; `TsfAppendSession` is the lower-level frame/ack API.

SDK readers and producers retry bounded transient WebSocket interruptions while preserving read positions and unacknowledged writer sequence numbers. Protocol and policy closes fail immediately.

REST authorization is stream-scoped. Read methods accept an optional link secret because public streams need none. Management methods require an owner link secret on each call. The client never retains a link secret as implicit authorization for later REST requests.

The permission in a link fragment selects the intended client mode. The server resolves authoritative permissions from the secret. Changing the fragment cannot elevate permission, and clients do not need a remote permission preflight before choosing their initial mode.

The default producer window is capped at the service's hard writer-queue contract: 128 records and 5 MiB of payload. Applications may configure smaller windows.

## CLI quickstart

Create a private stream:

```sh
tsf new --title 'Production deploy'
```

Create a public stream:

```sh
tsf new --public
```

Choose a shorter initial lifetime with a human duration:

```sh
tsf new --expires 7d
make test | tsf --title 'Test run' --expires 6h
```

Streams expire after 10 days by default. Their complete history remains readable until expiry.

`tsf new` prints the title, Stream ID, expiry, and an owner link. The title is optional. Issue more labeled links at creation with `--link 'read=Build reader'`, `--link 'write=Deploy writer'`, `--link 'read-write=Operator'`, or `--link 'owner=Backup owner'`. Links are shown once.

Stream command output into a new stream:

```sh
make test | tsf
```

Bare `tsf` captures piped input in a new stream. With terminal input and no command or capture options, it prints help.

Private capture creates an owner link and a read link. The owner link writes the captured data. The read link is the single stdout result. Creation details, the owner link, and durability status go to stderr.

Public capture creates only an owner link. It prints the public stream URL to stdout.

Use `--to` to capture into an existing stream. It accepts a write-capable link and creates no links.

Run a command through `tsf` when you want `tsf` to propagate the command exit status:

```sh
tsf -- make test
```

By default, `tsf` makes line boundaries transcript record boundaries and marks records as transcript-oriented:

```sh
make test | tsf
make test | tsf --to '{write-link}'
```

One logical line is limited to 16 MiB by default. This is the same default used by `tail` and `replay`. Set `--max-logical-record-bytes` on both the writer and reader only when a larger application-specific limit is required.

Use raw mode when you want to send stdin as byte records instead of line-framed transcript records. Raw mode flushes at the physical record size limit, after a short linger, and at EOF:

```sh
cat artifact.bin | tsf --raw
```

On Ctrl-C, `tsf` stops input, flushes accepted bytes, waits for durability acknowledgements, closes the producer, and exits with status 130.

Tail or replay a link or public stream URL:

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

Owner links contain `#o=` and can manage the stream:

```sh
tsf visibility '{owner-link}' public
tsf title '{owner-link}' 'Production deploy — west'
tsf title '{owner-link}' --clear
tsf renew '{owner-link}' --expires 7d
tsf link issue '{owner-link}' 'Deploy reader' --permission read --expires 7d
tsf link list '{owner-link}'
tsf link rename '{owner-link}' '{link_id}' 'CI reader'
tsf link revoke '{owner-link}' '{link_id}'
tsf delete '{owner-link}'
```

Renewal extends an active stream from the current time. Link permissions are `read`, `write`, `read-write`, and `owner`. Link expiry accepts durations such as `1h` or `7d`, or `never` by default.

A stream title contains 1 to 120 Unicode code points. Leading or trailing whitespace, control characters, and line breaks are rejected. Titles may be duplicated, changed, or cleared. The immutable Stream ID remains the stream identity and URL component.

Every link has a required owner-visible label. Labels contain 1 to 64 Unicode code points. Leading or trailing whitespace, control characters, and line breaks are rejected. Labels may be renamed and do not need to be unique.

Each link also has an immutable generated Link ID. The CLI uses it for rename and revoke commands. Renaming does not change the secret, permissions, expiry, or established sessions.

Link file options write only the secret value. On Unix, `tsf` creates and tightens these files to mode `0600`.

## Development

See [Development](docs/development.md) for the workspace layout, local-service setup, checks, and diagnostics. Release maintainers should also read [Release operations](docs/release-operations.md).

## License

The Rust SDK and CLI are MIT licensed. See [LICENSE](LICENSE).
