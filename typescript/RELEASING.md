# TypeScript releases

`@tailsurf/protocol` and `@tailsurf/client` have independent versions. Changesets records which packages change and the semantic version bump for each package.

## Propose a release

Add a changeset to any pull request that changes a published package:

```sh
cd typescript
pnpm changeset
```

Select each affected package. Choose patch, minor, or major. Write a short user-facing summary.

Changesets updates a dependent package when its `workspace:` range must change. Changes that do not affect a published package do not need a changeset.

## Publish

`publish-npm.yml` collects pending changesets into a pull request named `chore: release TypeScript SDK packages`. The pull request updates package versions and changelogs.

Merging the release pull request tests both packages and publishes every new version in dependency order. The workflow creates a package tag and GitHub release for each published version.

## Trust and recovery

Each npm package trusts `publish-npm.yml` in `s2-streamstore/tailsurf` with the `npm` GitHub environment. Trusted publishing uses GitHub OIDC and creates provenance. The repository stores no npm publishing token.

Published npm versions are immutable. Fix a bad release with a new version. Deprecate the bad version on npm when consumers should not select it.
