# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
