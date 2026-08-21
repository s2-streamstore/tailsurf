# Changesets

Add a changeset to any pull request that changes a published TypeScript package.

Run this command from `typescript`:

```sh
pnpm changeset
```

Select each affected package. Choose patch, minor, or major. Write a short user-facing summary.

The release workflow collects changesets into one release pull request. Merging that pull request publishes the new package versions.
