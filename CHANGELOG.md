# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- add reconnecting Rust and TypeScript durable writers that preserve writer identity, sequence numbers, and unacknowledged payloads
- add TypeScript logical-record splitting

### Changed

- [**breaking**] keep transcript cardinality guards internal and expose only the reassembly-byte limit
- [**breaking**] replace configurable writer retention and submission limits with one fixed 1,024-record and 5 MiB sent-but-unacknowledged window
- [**breaking**] simplify the Rust writer to immediate queued submission with durability tickets
- [**breaking**] remove the CLI logical-record write limit and keep only the read-side reassembly safety bound
- [**breaking**] replace configurable retry schedules with one bounded-operation attempt count, rename the WebSocket operation timeout to progress timeout, and derive silent-read detection from protocol heartbeats

## [0.11.0](https://github.com/s2-streamstore/tailsurf/compare/v0.10.0...v0.11.0) - 2026-08-17

### Added

- [**breaking**] align read options with query protocol ([#41](https://github.com/s2-streamstore/tailsurf/pull/41))

## [0.10.0](https://github.com/s2-streamstore/tailsurf/compare/v0.9.0...v0.10.0) - 2026-08-17

### Added

- accept server-minted link secrets ([#40](https://github.com/s2-streamstore/tailsurf/pull/40))

### Other

- consolidate duplicated validation, simplify SSE internals, fix hot-path scan ([#38](https://github.com/s2-streamstore/tailsurf/pull/38))

## [0.9.0](https://github.com/s2-streamstore/tailsurf/compare/v0.8.0...v0.9.0) - 2026-08-16

### Added

- add split_logical_record as the write-side half of LogicalTranscript ([#37](https://github.com/s2-streamstore/tailsurf/pull/37))

### Other

- [**breaking**] validate link secrets at construction and keep errors typed end to end ([#36](https://github.com/s2-streamstore/tailsurf/pull/36))
- consolidate duplicated validation, backoff, and CLI label logic ([#35](https://github.com/s2-streamstore/tailsurf/pull/35))
- simplify core and test infrastructure ([#32](https://github.com/s2-streamstore/tailsurf/pull/32))

## [0.8.0](https://github.com/s2-streamstore/tailsurf/compare/v0.7.1...v0.8.0) - 2026-08-16

### Fixed

- [**breaking**] tighten wire parsing, bound no-progress SSE reconnects, trim hot-path work ([#29](https://github.com/s2-streamstore/tailsurf/pull/29))

### Other

- [**breaking**] deliver zero-copy read batches through transcript assembly ([#31](https://github.com/s2-streamstore/tailsurf/pull/31))

## [0.7.1](https://github.com/s2-streamstore/tailsurf/compare/v0.7.0...v0.7.1) - 2026-08-15

### Fixed

- *(cli)* allow concurrent writes during capture ([#27](https://github.com/s2-streamstore/tailsurf/pull/27))

## [0.7.0](https://github.com/s2-streamstore/tailsurf/compare/v0.6.0...v0.7.0) - 2026-08-14

### Other

- share text validation, stream query building, and CLI helpers ([#25](https://github.com/s2-streamstore/tailsurf/pull/25))
- [**breaking**] minimize socket startup roundtrips ([#24](https://github.com/s2-streamstore/tailsurf/pull/24))

## [0.6.0](https://github.com/s2-streamstore/tailsurf/compare/v0.5.0...v0.6.0) - 2026-08-13

### Added

- unify links and add stream titles ([#22](https://github.com/s2-streamstore/tailsurf/pull/22))

## [0.5.0](https://github.com/s2-streamstore/tailsurf/compare/v0.4.0...v0.5.0) - 2026-08-12

### Added

- add renewable stream expiry ([#21](https://github.com/s2-streamstore/tailsurf/pull/21))
- *(cli)* make piped input to bare tsf behave like tsf write ([#16](https://github.com/s2-streamstore/tailsurf/pull/16))
- *(cli)* renew active streams with `tsf renew`

### Other

- [**breaking**] clean up stream token extraction, record traits, base64 helpers, and ack dispatch ([#15](https://github.com/s2-streamstore/tailsurf/pull/15))
- require nightly Rustfmt ([#17](https://github.com/s2-streamstore/tailsurf/pull/17))

### Changed

- [**breaking**] replace record retention with fixed, owner-renewable stream expiry

## [0.4.0](https://github.com/s2-streamstore/tailsurf/compare/v0.3.0...v0.4.0) - 2026-08-11

### Added

- *(cli)* hint when updates are available

### Fixed

- *(cli)* restore explicit installer updates
- align client authorization and write limits

### Other

- [**breaking**] trim CLI and SDK surface

## [0.3.0](https://github.com/s2-streamstore/tailsurf/compare/v0.2.0...v0.3.0) - 2026-08-11

### Other

- Reduce hot path allocations and syscalls ([#8](https://github.com/s2-streamstore/tailsurf/pull/8))
- keep create recovery keys out of argv guidance
- expose recoverable stream creation
- preserve reader reconnect state
- allow transcript completion at part limit
- bound resumable reader reconnects
- canonicalize stream share URLs
- omit authorization from stream creation
- bound transcript pending parts
- keep owner access on stream creation
- validate canonical stream tokens
- retry stream creation idempotently
- stabilize tail offsets across reconnects
- bound transcript reassembly state
- Reset read idle timeouts on heartbeats and simplify the client ([#7](https://github.com/s2-streamstore/tailsurf/pull/7))
- Make tsf write create streams by default
- print create recovery before file writes

### Changed

- [**breaking**] `tsf write` creates a stream when no URL is supplied; remove `--new`

## [0.2.0](https://github.com/s2-streamstore/tailsurf/compare/v0.1.0...v0.2.0) - 2026-08-10

### Added

- self-update via tsf update, cargo-dist release profile
- [**breaking**] link-centric CLI output, commands, and leaner defaults
- add CLI stream retention control
- durability summary at the end of tsf write

### Fixed

- simplify CLI lifecycle and reads

### Other

- separate maintainer guidance from readme
- notarize macOS release binaries
- simplify release signing
- Initial public release
- keep stdin open through interrupt
- synchronize interrupt handling
