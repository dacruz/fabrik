# Plan 008: GitHub CI and main-only releases

**Status:** Completed

## Feature completed

GitHub Actions continuously validates pull requests and direct pushes to
`main`. Every merged pull request creates the next minor version tag and
publishes a GitHub release.

## Depends on

Plans 001 through 007.

## Implementation

- [x] Add a CI workflow triggered by pushes to `main` and by pull requests,
  avoiding duplicate checks for feature-branch pushes.
- [x] Set up the Go version from the project requirements and cache module
  dependencies.
- [x] Build all packages with `go build ./...`.
- [x] Run the regular tests, race-detector tests, and `go vet ./...`.
- [x] Add a release workflow triggered by merged pull requests targeting
  `main`.
- [x] Determine the next minor semantic version, starting at `v0.1.0`.
- [x] Create and push the version tag from the merged commit.
- [x] Re-run the build, tests, and vet checks in the release workflow.
- [x] Create a GitHub release with generated release notes using the pushed
  version tag. Since Fabrik is a library, GitHub's source archives are the
  release artifacts.

## Release usage

After a pull request is merged into `main`, the release workflow automatically
creates the next minor version tag and GitHub release.

## Completion criteria

- Every pull request and direct push to `main` runs build, test, race, and vet
  checks.
- Every merged pull request targeting `main` creates a minor version release.
- The release tag points to the merged commit and includes generated notes.
- The release workflow validates the project before publishing.
