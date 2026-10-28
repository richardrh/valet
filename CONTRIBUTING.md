# Contributing

## Setup

You need Go at the version in `go.mod`. You do not need cgo, Python or Arrow C++.

```sh
go build ./cmd/valet
```

## Before you push

CI runs these. Run them locally first:

```sh
gofmt -l .                                   # must print nothing
go vet ./...
go run honnef.co/go/tools/cmd/staticcheck@2026.2.2 ./...
go test -race ./...
go test -tags integration ./integration      # CLI acceptance tests
bash .github/scripts/test-s3-compatible.sh minio   # S3 suite; needs docker
```

## Code style

- Prefer longer straight-line functions to many small helpers. Inline a helper
  that has only one caller unless it names a real concept or backs a test.
- Add an interface only if there are two implementations or a test needs a fake.
- Delete dead code and the tests that only cover it. Don't keep it in case it
  becomes useful later.
- Add a test only for behaviour a user could notice. Don't add tests for wiring
  or exact wording.

## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>: <imperative summary, ≤72 chars>

<optional body: why, not what>
```

| Type | Use for |
|---|---|
| `feat` | New user-visible behaviour |
| `fix` | Bug fix |
| `perf` | Faster or smaller, with no change in behaviour |
| `refactor` | Restructuring with no change in behaviour |
| `chore` | Cleanup, dependencies and tooling |
| `docs` | Documentation only |
| `test` | Tests only |
| `ci` | Workflows and release scripts |

Add `!` after the type for breaking changes (`feat!: …`) and explain the break
in the body.

## Pull requests

- Keep each PR to one concern. The PR title follows the same format as a
  commit message.
- For `feat`, `fix` and `perf`, add an entry under `[Unreleased]` in
  `CHANGELOG.md`.
- Report security issues privately, as described in [SECURITY.md](SECURITY.md).
