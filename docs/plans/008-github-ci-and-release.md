# Plan 008: GitHub CI and main-only releases

**Status:** Completed

## Feature completed

GitHub Actions continuously validates pull requests and direct pushes to
`main`. Version-tagged commits that are reachable from `main` are published as
GitHub releases, while tags created from other branches are rejected.

## Depends on

Plans 001 through 007.

## Implementation

- [x] Add a CI workflow triggered by pushes to `main` and by pull requests,
  avoiding duplicate checks for feature-branch pushes.
- [x] Set up the Go version from the project requirements and cache module
  dependencies.
- [x] Build all packages with `go build ./...`.
- [x] Run the regular tests, race-detector tests, and `go vet ./...`.
- [x] Add a release workflow triggered by version tags matching `v*`.
- [x] Verify that a release tag points to a commit reachable from `origin/main`
  before publishing it.
- [x] Re-run the build, tests, and vet checks in the release workflow.
- [x] Create a GitHub release with generated release notes using the pushed
  version tag. Since Fabrik is a library, GitHub's source archives are the
  release artifacts.

## Release usage

After the version commit is on `main`, create and push a version tag:

```sh
git checkout main
git tag v0.1.0
git push origin v0.1.0
```

## Completion criteria

- Every pull request and direct push to `main` runs build, test, race, and vet
  checks.
- A tag on a non-`main` commit cannot publish a release.
- A valid version tag on `main` creates a GitHub release with generated notes.
- The release workflow validates the project before publishing.
