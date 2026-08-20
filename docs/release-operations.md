# Release operations

The Rust SDK and CLI share one version. The TypeScript packages use independent versions.

## Release flow

Release-plz opens or updates a Rust SDK and CLI release PR after relevant changes reach `main`. Pushes that only change `typescript/` do not run Release-plz. It derives the next workspace version from conventional commits and checks SDK API compatibility.

Merging the release PR publishes `tailsurf` first and then `tailsurf-cli`. Release-plz creates one `vX.Y.Z` tag. It sends a repository dispatch for the binary release. Repository dispatch always loads the release workflow from the default branch.

Dist creates one GitHub release for the tag. It builds `tsf` for Apple Silicon and Intel macOS, ARM64 and x86-64 musl Linux, and x86-64 Windows.

The release contains versioned archives, shell and PowerShell installers, SHA-256 checksums, and GitHub artifact attestations. Older tags and assets remain available for rollback and CI pinning.

The installer routes on `tail.surf` redirect to the latest public GitHub release. Homebrew distribution is not configured.

Publishing uses crates.io trusted publishing. Both crates trust the `s2-streamstore/tailsurf` repository, the `release-plz.yml` workflow, and the `crates-io` GitHub environment.

## TypeScript releases

`@s2-dev/tailsurf-protocol` and `@s2-dev/tailsurf-client` are released independently from their package manifests. Publish the protocol before a client version that depends on it.

Change the selected package version in its `package.json`. Run `pnpm check` from `typescript`. Merge the change to `main`, then dispatch `publish-npm.yml` for that package.

The workflow rejects versions already present on npm. It packs and tests both packages before publishing the selected tarball from the `npm` GitHub environment.

Each npm package trusts `publish-npm.yml` in `s2-streamstore/tailsurf` with the `npm` environment. Trusted publishing uses GitHub OIDC and creates provenance. A new package name can use the optional `NPM_TOKEN` repository secret for its first publication. Delete that secret after the trusted publisher is configured.

Published npm versions are immutable. Fix a bad release with a new version. Deprecate the bad version on npm when consumers should not select it.

## Protocol and service upgrades

Public protocol changes start in this repository. Update the Rust and TypeScript implementations and both fixture copies in one pull request.

Additive response fields can ship in the service first when every released client ignores them. Request changes, frame changes, and stricter validation require compatible client releases before the service uses them.

Publish `@s2-dev/tailsurf-protocol` first. Publish `@s2-dev/tailsurf-client` when its supported protocol range or behavior changes. Publish the Rust SDK and CLI through the Rust release flow when they change.

After the required client versions are public, update `tailsurf-web` to exact released versions and run its full check, cross-client test, and browser suite. Deploy the service only after those checks pass.

Keep the preceding service version deployable until the new clients and service have completed production probes. Roll the service back independently if the public wire behavior remains compatible.

Release builds, macOS signing and notarization, hosting, and artifact attestations run on GitHub-hosted runners. The release workflow accepts only tags reachable from the default branch. It carries the validated commit SHA through every build and checks that the tag has not moved before hosting.

## Updates

Axoupdater is embedded in `tsf`. Direct installations use the dist installer receipt for explicit updates through `tsf update`.

`tsf update` and `tsf update --check` contact GitHub Releases. The installer receipt stays local.

After a successful service command against `https://tail.surf`, an installer-owned binary may check GitHub Releases and print a generic hint. The check runs only when stderr is a terminal. It runs at most once per 24 hours and times out after 500 milliseconds. Failures are ignored. The cache attempt happens before the request, so an unwritable cache disables the check. `CI`, `TSF_NO_UPDATE_CHECK`, and `DO_NOT_TRACK` disable automatic checks. The hint never installs an update.

The updater refuses installations it does not own. Cargo installations remain owned by Cargo. Other package-manager installations remain owned by their package manager.

## Platform trust

macOS executables use Developer ID signing with hardened runtime and Apple notarization. Apple must accept both signed macOS binaries before dist can publish the release.

Windows executables are unsigned. Linux and Windows artifacts rely on SHA-256 checksums and GitHub attestations.

## Release environments

Release workflows use two GitHub environments.

The `crates-io` environment identifies trusted-publisher jobs. Both crates.io trusted-publisher configurations name this environment exactly.

The `release` environment identifies binary build, signing, notarization, and hosting jobs.

Both environments accept deployments only from `main`. Both require approval from `shikhar`. Self-approval is allowed because the repository has one maintainer. Administrators cannot bypass approval.

Pull requests run the `Rust SDK and CLI (stable)`, `TypeScript SDK (Node and browser)`, `Rust SDK and CLI (MSRV 1.95)`, `Validate conventional PR title`, and `Plan Rust binary release` checks.

The default branch accepts only squash merges from pull requests. Review threads must be resolved. Force pushes and deletion are blocked.

Release tags matching `v*` cannot be rewritten or deleted.

GitHub Actions may use GitHub-owned actions and an explicit third-party repository allowlist. The allowlist includes the transitive actions required by Release-plz. Every action must use a full commit SHA. The default workflow token is read-only and cannot approve pull requests.

## Required release secrets

Repository Actions secrets store these macOS signing credentials:

- `CODESIGN_CERTIFICATE`
- `CODESIGN_CERTIFICATE_PASSWORD`
- `CODESIGN_IDENTITY`

They also store these Apple notarization credentials:

- `APPLE_NOTARY_ISSUER_ID`
- `APPLE_NOTARY_KEY_ID`
- `APPLE_NOTARY_PRIVATE_KEY`

The reusable notarization workflow receives its three credentials through an explicit secret map. Release builds fail when any required signing or notarization credential is missing.

The environments store no duplicate secrets. Their approval rules gate the jobs that consume repository credentials.

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
