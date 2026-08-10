# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
