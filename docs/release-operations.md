# Release operations

The Rust SDK and CLI share one version. Release-plz and dist coordinate crates.io publication and binary distribution.

## Release flow

Release-plz opens or updates a release PR after changes reach `main`. It derives the next workspace version from conventional commits and checks SDK API compatibility.

Merging the release PR publishes `tailsurf` first and then `tailsurf-cli`. Release-plz creates one `vX.Y.Z` tag. It dispatches the binary release workflow directly so release creation does not depend on workflows triggered by the repository token.

Dist creates one GitHub release for the tag. It builds `tsf` for Apple Silicon and Intel macOS, ARM64 and x86-64 musl Linux, and x86-64 Windows.

The release contains versioned archives, shell and PowerShell installers, SHA-256 checksums, and GitHub artifact attestations. Older tags and assets remain available for rollback and CI pinning.

The installer routes on `tail.surf` redirect to the latest public GitHub release. Homebrew distribution is not configured.

Publishing uses crates.io trusted publishing. Both crates trust the `s2-streamstore/tailsurf` repository and the `release-plz.yml` workflow without a GitHub environment.

Linux and Windows binaries build on Blacksmith runners. Release orchestration, macOS signing and notarization, hosting, and artifact attestations run on GitHub-hosted runners.

## Updates

Axoupdater is embedded in `tsf`. Direct installations use the dist installer receipt for explicit updates through `tsf update`.

Cargo installations remain owned by Cargo. Other package-manager installations remain owned by their package manager.

The CLI does not update automatically or check for updates in the background.

## Platform trust

macOS executables use Developer ID signing with hardened runtime and Apple notarization. Apple must accept both signed macOS binaries before dist can publish the release.

Windows executables are unsigned. Linux and Windows artifacts rely on SHA-256 checksums and GitHub attestations.

## Required repository secrets

macOS signing uses:

- `CODESIGN_CERTIFICATE`
- `CODESIGN_CERTIFICATE_PASSWORD`
- `CODESIGN_IDENTITY`

Apple notarization uses:

- `APPLE_NOTARY_ISSUER_ID`
- `APPLE_NOTARY_KEY_ID`
- `APPLE_NOTARY_PRIVATE_KEY`

Release builds fail when any required signing or notarization credential is missing.

## Validate without publishing

Run the dist workflow with the `dry-run` tag:

```sh
gh workflow run release.yml --repo s2-streamstore/tailsurf --ref main -f tag=dry-run
```

The dry run builds every platform and submits the macOS binaries to Apple. It does not host artifacts or announce a release.

After a release, wait until `tailsurf-cli` is visible in the crates.io index. Then run the install smoke against the deployed service:

```sh
TSF_API_URL=https://tail.surf TSF_WEB_URL=https://tail.surf python3 scripts/published-cli-smoke.py
```
