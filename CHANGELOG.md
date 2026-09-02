# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.6](https://github.com/s2-streamstore/tailsurf/compare/v0.14.5...v0.14.6) - 2026-09-02

### Fixed

- *(cli)* preserve checkpoints across terminal modes ([#93](https://github.com/s2-streamstore/tailsurf/pull/93))

## [0.14.5](https://github.com/s2-streamstore/tailsurf/compare/v0.14.4...v0.14.5) - 2026-09-01

### Other

- *(cli)* keep alternate-screen terminal checkpoints compact ([#91](https://github.com/s2-streamstore/tailsurf/pull/91))

## [0.14.4](https://github.com/s2-streamstore/tailsurf/compare/v0.14.3...v0.14.4) - 2026-09-01

### Added

- *(protocol)* add native terminal checkpoints ([#88](https://github.com/s2-streamstore/tailsurf/pull/88))

## [0.14.3](https://github.com/s2-streamstore/tailsurf/compare/v0.14.2...v0.14.3) - 2026-09-01

### Added

- *(cli)* publish terminal state checkpoints ([#86](https://github.com/s2-streamstore/tailsurf/pull/86))

## [0.14.2](https://github.com/s2-streamstore/tailsurf/compare/v0.14.1...v0.14.2) - 2026-09-01

### Added

- add multiplayer terminal protocol and CLI ([#83](https://github.com/s2-streamstore/tailsurf/pull/83))

## [0.14.1](https://github.com/s2-streamstore/tailsurf/compare/v0.14.0...v0.14.1) - 2026-08-29

### Fixed

- *(cli)* bound backlog drained after interrupt ([#81](https://github.com/s2-streamstore/tailsurf/pull/81))

## [0.14.0](https://github.com/s2-streamstore/tailsurf/compare/v0.13.0...v0.14.0) - 2026-08-26

### Added

- [**breaking**] flatten record JSON and group writer identity ([#77](https://github.com/s2-streamstore/tailsurf/pull/77))
- *(cli)* [**breaking**] records carry no newline delimiter ([#79](https://github.com/s2-streamstore/tailsurf/pull/79))

### Other

- rename --raw to --bytes ([#72](https://github.com/s2-streamstore/tailsurf/pull/72))

## [0.13.0](https://github.com/s2-streamstore/tailsurf/compare/v0.12.2...v0.13.0) - 2026-08-25

### Added

- [**breaking**] one --origin flag; stream links use the server-supplied web origin ([#66](https://github.com/s2-streamstore/tailsurf/pull/66))
- publish tailsurf as an alias for @tailsurf/client ([#67](https://github.com/s2-streamstore/tailsurf/pull/67))

### Other

- rustfmt rest_fixtures imports ([#69](https://github.com/s2-streamstore/tailsurf/pull/69))
- add contributor guidance and repair release changelog plumbing ([#64](https://github.com/s2-streamstore/tailsurf/pull/64))
- publish the TSF v1 protocol specification and OpenAPI contract ([#63](https://github.com/s2-streamstore/tailsurf/pull/63))
- add security policy and hosted-service trust links ([#60](https://github.com/s2-streamstore/tailsurf/pull/60))

## [0.12.2](https://github.com/s2-streamstore/tailsurf/compare/v0.12.1...v0.12.2) - 2026-08-22

This release contains no functional changes.

## [0.12.1](https://github.com/s2-streamstore/tailsurf/compare/v0.12.0...v0.12.1) - 2026-08-22

### Fixed

- *(cli)* allow forced interrupt during write shutdown ([#57](https://github.com/s2-streamstore/tailsurf/pull/57))
- *(cli)* retry timed out update checks ([#55](https://github.com/s2-streamstore/tailsurf/pull/55))

## [0.12.0](https://github.com/s2-streamstore/tailsurf/compare/v0.11.0...v0.12.0) - 2026-08-22

### Added

- [**breaking**] batch durable writes with actor sequencing and frame pacing ([#46](https://github.com/s2-streamstore/tailsurf/pull/46))
- publish TypeScript SDK packages ([#43](https://github.com/s2-streamstore/tailsurf/pull/43))
- add reconnecting Rust and TypeScript durable writers that preserve writer identity, sequence numbers, and unacknowledged payloads
- add TypeScript logical-record splitting

### Changed

- [**breaking**] keep transcript cardinality guards internal and expose only the reassembly-byte limit
- [**breaking**] replace configurable writer retention and submission limits with one fixed 1,024-record and 5 MiB sent-but-unacknowledged window
- [**breaking**] simplify the Rust writer to immediate queued submission with durability tickets
- [**breaking**] remove the CLI logical-record write limit and keep only the read-side reassembly safety bound
- [**breaking**] replace configurable retry schedules with one bounded-operation attempt count, rename the WebSocket operation timeout to progress timeout, and derive silent-read detection from protocol heartbeats
- [**breaking**] name the HTTP request timeout and durable writer options consistently across SDKs
- [**breaking**] name Rust's low-level write-session options after the session they configure
- [**breaking**] measure the durable writer byte window in exact payload bytes and reserve accounted-byte terminology for service limits
- [**breaking**] stop re-exporting raw frame limits from the high-level TypeScript client package

### Other

- coalesce writer submissions and decode SSE batches directly ([#52](https://github.com/s2-streamstore/tailsurf/pull/52))
- [**breaking**] simplify client policy and writer limits ([#50](https://github.com/s2-streamstore/tailsurf/pull/50))
- restore SDK and npm workflows ([#47](https://github.com/s2-streamstore/tailsurf/pull/47))
- move npm packages to tailsurf scope ([#48](https://github.com/s2-streamstore/tailsurf/pull/48))
- use s2-dev npm scope ([#45](https://github.com/s2-streamstore/tailsurf/pull/45))

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
