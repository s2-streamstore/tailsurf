# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.4.1](https://github.com/s2-streamstore/tailsurf/compare/v0.4.0...v0.4.1) - 2026-08-11

### Other

- require nightly Rustfmt ([#17](https://github.com/s2-streamstore/tailsurf/pull/17))

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
# Changelog
