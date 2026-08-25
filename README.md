# tail.surf (`tsf`)

[tail.surf](https://tail.surf) is a streaming gist for live work and agent conversations.

Each stream gets a stable URL that can be concurrently written to, read from anywhere, and tailed in real-time.

Free to start with no sign-up required.

Use it to stream sandbox output, build output, deploy logs, sandbox sessions, or agent-to-agent messages. For example:

- Share long-running command output without keeping an SSH session, sandbox, or terminal attached.
- Give agents a reliable async channel with durable catch-up.
- Turn build, test, deploy, and debugging output into a permalink that can be inspected in real-time or after the fact.

Open [tail.surf](https://tail.surf) for the scrubbable live transcript, or install the `tsf` CLI below.

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

Installer-owned binaries may contact GitHub Releases and print an update hint after a successful interactive command against `https://tail.surf`. Successful checks run at most once per day. Failed checks retry after one hour. Each request waits at most three seconds and never installs an update. Set `TSF_NO_UPDATE_CHECK` or `DO_NOT_TRACK` to disable hints. CI and non-interactive commands do not check.

Installations owned by a package manager do not check or print the hint. Cargo users rerun `cargo install tailsurf-cli --locked`. cargo-binstall users rerun `cargo binstall tailsurf-cli`.

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
make test | tsf new --title 'Test run' --expires 6h
```

Streams expire after 10 days by default. Their complete history remains readable until expiry.

`tsf new` prints the title, Stream ID, expiry, and initial links. Private streams get a read link and an owner link. Public streams get a public URL and an owner link. The title is optional.

Create custom links with `--link LINK_ID=PERMISSION`. Link IDs are short semantic names such as `deploy-bot`. Permissions are `read`, `write`, `read-write`, and `owner`. The short forms `r`, `w`, `rw`, and `o` are also accepted. A stream may have up to three initial links, including defaults. Links are shown once.

Stream command output into a new stream:

```sh
make test | tsf
```

Bare `tsf` captures piped input in a new stream. With terminal input and no subcommand, it prints help.

Creation details and links go to stdout. Durability progress goes to stderr.

Use `write` to send input to an existing stream. It accepts a write-capable link and creates no links.

Run a command through `tsf` when you want `tsf` to propagate the command exit status:

```sh
tsf new -- make test
```

By default, `tsf` makes line boundaries transcript record boundaries and marks records as transcript-oriented:

```sh
make test | tsf
make test | tsf write '{write-link}'
```

The writer splits a logical line into physical records and paces them through its fixed in-flight window. `tail` and `replay` retain a 16 MiB reassembly safety bound by default. Raise `--max-reassembly-bytes` when reading larger logical records.

Use raw mode when you want to send stdin as byte records instead of line-framed transcript records. Raw mode flushes at the physical record size limit, after a short linger, and at EOF:

```sh
cat artifact.bin | tsf new --raw
cat artifact.bin | tsf write '{write-link}' --raw
```

On Ctrl-C, `tsf` stops input, flushes accepted bytes, waits for durability acknowledgements, closes the writer, and exits with status 130. Press Ctrl-C again to stop immediately without waiting for pending acknowledgements.

Tail or replay a link or public stream URL:

```sh
tsf tail '{url}'
tsf tail -n 200 '{url}'
tsf tail --seq 0 --count 500 '{url}'
tsf tail --since 15m '{url}'
tsf replay '{url}'
tsf tail --sse '{url}'
```

`--last` or `-n` starts relative to the durable tail. `--seq` starts at an absolute sequence number. `--since` accepts a duration or RFC 3339 timestamp. `--count` bounds the number of records.

`tail` follows new records unless `--count` bounds it. `replay` drains through the tail observed by its connection and exits.

`--sse` uses the resumable HTTP event-stream transport. The default binary WebSocket transport remains best for interactive CLI use.

Both commands preserve payload bytes. They exit successfully when a downstream pipe closes normally.

Inspect stream metadata:

```sh
tsf info '{url}'
tsf info '{url}' --json
```

Owner links contain `#o=` and can manage the stream:

```sh
tsf visibility '{owner-link}' public
tsf title set '{owner-link}' 'Production deploy — west'
tsf title clear '{owner-link}'
tsf renew '{owner-link}' 7d
tsf link create '{owner-link}' 'deploy-reader=read' --expires 7d
tsf link list '{owner-link}'
tsf link revoke '{owner-link}' 'deploy-reader'
tsf delete '{owner-link}'
```

Deletion asks for confirmation on a terminal. Scripts must pass `--yes`.

Renewal extends an active stream from the current time. Link expiry accepts durations such as `1h` or `7d`, or `never` by default.

A stream title contains 1 to 120 Unicode code points. Leading or trailing whitespace, control characters, and line breaks are rejected. Titles may be duplicated, changed, or cleared. The immutable Stream ID remains the stream identity and URL component.

Every link has a client-chosen immutable Link ID. Link IDs contain 1 to 64 lowercase ASCII letters, digits, or hyphens. They cannot start or end with a hyphen. Link IDs are unique within a stream.

Link file options write complete URLs. Any command that accepts a link also accepts `@PATH` to read one complete URL from a file. On Unix, `tsf` creates and tightens link files to mode `0600`.

Commands with structured output accept `--json`.

## SDKs

The repository also contains public Rust and TypeScript SDKs.

- [`rust`](rust/README.md) is the `tailsurf` Rust SDK crate.
- [`typescript`](typescript/README.md) contains `@tailsurf/client` and the lower-level `@tailsurf/protocol` package.

Install the TypeScript client with `npm install @tailsurf/client`. Rust API documentation is available on [docs.rs](https://docs.rs/tailsurf).

## Protocol

The [TSF v1 protocol](docs/protocol.md) defines the public REST, SSE, and WebSocket contract the SDKs and service implement. [openapi.yaml](docs/openapi.yaml) is the machine-readable REST and SSE contract.

## Development

See [Development](docs/development.md) for the workspace layout, local-service setup, checks, and diagnostics. Release maintainers should also read [Release operations](docs/release-operations.md).

## Trust and support

- [Trust](https://tail.surf/trust), [Privacy](https://tail.surf/privacy), and [Terms](https://tail.surf/terms) cover the hosted service.
- Report vulnerabilities through [SECURITY.md](SECURITY.md).
- [GitHub Issues](https://github.com/s2-streamstore/tailsurf/issues) is the public support path. Never post an owner, write, or private read link.

## License

The SDKs and CLI are MIT licensed. See [LICENSE](LICENSE).
