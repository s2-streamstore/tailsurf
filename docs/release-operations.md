# Release operations

The Rust SDK and CLI share one version. Release-plz and dist coordinate crates.io publication and binary distribution.

## Release flow

Release-plz opens or updates a release PR after changes reach `main`. It derives the next workspace version from conventional commits and checks SDK API compatibility.

Merging the release PR publishes `tailsurf` first and then `tailsurf-cli`. Release-plz creates one `vX.Y.Z` tag. It sends a repository dispatch for the binary release. Repository dispatch always loads the release workflow from the default branch.

Dist creates one GitHub release for the tag. It builds `tsf` for Apple Silicon and Intel macOS, ARM64 and x86-64 musl Linux, and x86-64 Windows.

The release contains versioned archives, shell and PowerShell installers, SHA-256 checksums, and GitHub artifact attestations. Older tags and assets remain available for rollback and CI pinning.

The installer routes on `tail.surf` redirect to the latest public GitHub release. Homebrew distribution is not configured.

Publishing uses crates.io trusted publishing. Both crates trust the `s2-streamstore/tailsurf` repository, the `release-plz.yml` workflow, and the `crates-io` GitHub environment.

Release builds, macOS signing and notarization, hosting, and artifact attestations run on GitHub-hosted runners. The release workflow accepts only tags reachable from the default branch. It carries the validated commit SHA through every build and checks that the tag has not moved before hosting.

## Updates

Direct installations update by rerunning the installer. Cargo installations update by rerunning `cargo install tailsurf-cli --locked`. Other package-manager installations remain owned by their package manager.

## Platform trust

macOS executables use Developer ID signing with hardened runtime and Apple notarization. Apple must accept both signed macOS binaries before dist can publish the release.

Windows executables are unsigned. Linux and Windows artifacts rely on SHA-256 checksums and GitHub attestations.

## Protected environments

Two GitHub environments are required before publishing.

Configure the `crates-io` environment with required reviewers and allow deployments only from the protected default branch. Both crates.io trusted-publisher configurations name this environment exactly.

Configure the `release` environment with required reviewers and allow deployments only from the protected default branch. This environment protects binary builds, signing, notarization, and hosting.

Prevent reviewers from approving their own deployments. Protect `v*` tags from updates and deletion. Store no release secrets at repository or organization scope.

## Required release secrets

The `release` environment stores these macOS signing secrets:

- `CODESIGN_CERTIFICATE`
- `CODESIGN_CERTIFICATE_PASSWORD`
- `CODESIGN_IDENTITY`

It also stores these Apple notarization secrets:

- `APPLE_NOTARY_ISSUER_ID`
- `APPLE_NOTARY_KEY_ID`
- `APPLE_NOTARY_PRIVATE_KEY`

Release builds fail when any required signing or notarization credential is missing. These values are not repository-level or organization-level secrets.

## Validate without publishing

Send a repository dispatch with the `dry-run` payload:

```sh
printf '%s\n' '{"event_type":"release","client_payload":{"tag":"dry-run"}}' | gh api --method POST repos/s2-streamstore/tailsurf/dispatches --input -
```

The dry run requires `release` environment approval. It builds every platform and submits the macOS binaries to Apple. It does not host artifacts or announce a release.

After a release, wait until `tailsurf-cli` is visible in the crates.io index. Then run the install smoke against the deployed service:

```sh
TSF_API_URL=https://tail.surf TSF_WEB_URL=https://tail.surf python3 scripts/published-cli-smoke.py
```
