---
title: CI automation
linkTitle: CI & releases
weight: 40
type: docs
prev: guides/performance
---

Gate on the exit code and stdout JSON ([exit codes](../../concepts/results/#exit-codes)).

| Exit | `valet validate` | `valet schema` ([schema-only checks](../schema/)) |
|---|---|---|
| `0` | No error-level failures (warnings allowed) | Compatible |
| `1` | Failed checks | Error-level schema mismatch |
| `2` | Operational/presentation failure; no manifest | Operational/configuration failure |
| `3` | Artifacts committed; stdout failed | — |

Without `-o`/`output.uri`, `validate` writes only the stdout summary. `schema` never publishes a validation manifest.

## GitHub Actions example

The bundled fixture exits `1` (missing record ID). Omit `output.uri` for this aggregate-only path.

```yaml
name: Validate data

on:
  pull_request:
  workflow_dispatch:

jobs:
  validate:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache-dependency-path: go.sum

      - name: Build Valet
        run: go build -trimpath -o dist/valet ./cmd/valet

      - name: Validate
        id: validate
        shell: bash
        run: |
          set +e
          dist/valet validate \
            -p examples/pipeline.yml \
            -i examples/sample.csv \
            --log-format json \
            > validation-summary.json \
            2> validation.log.ndjson
          code=$?
          set -e

          echo "exit_code=$code" >> "$GITHUB_OUTPUT"
          if test "$code" -gt 1; then
            exit "$code"
          fi

      - name: Upload validation report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: validation-result
          path: |
            validation-summary.json
            validation.log.ndjson
          if-no-files-found: warn

      - name: Enforce data quality
        if: steps.validate.outputs.exit_code == '1'
        run: exit 1
```

## Consume the summary

```bash
jq '{validation, error, rule_counts}' validation-summary.json
```

- Parse only after exit `0`/`1`; otherwise read stderr.
- Run lakehouse ingestion after **Enforce data quality**, on the same immutable input.
- Mandatory checks need `level: error`; warnings still exit `0`.
- Artifacts: add `-o validation-output`, upload `validation-output/`; the summary gains `manifest_uri`. Read `kind: issues`/`kind: rejects` entries from `manifest.json`, never by listing files ([results](../../concepts/results/#manifest)).
- `--output-mode report` publishes issues and reports without rewriting row data.

## JSON logs

```bash
valet validate ... --log-format json 2>valet.log.ndjson
```

Framing, configuration, I/O and resource failures exit `2` with no manifest.

## S3 jobs

AWS SDK credential chain (workload identity, OIDC, secret provider); no static credentials in the pipeline. Source and output may differ in endpoint, region, profile.

## Run checks locally

```bash
go test ./...
go test -tags=integration ./integration -timeout 20m
```

Object-store cases skip without endpoint and credentials. Real S3-compatible acceptance:

```bash
bash .github/scripts/test-s3-compatible.sh minio
bash .github/scripts/test-s3-compatible.sh rustfs
```

- Needs Go, a C compiler, Docker, curl, jq. Rootless Podman: prefix `CONTAINER_RUNTIME=podman`.
- Loopback bindings, local-only credentials; cleans up containers and data; any selected skip fails. `VALET_S3_LOG_DIR` keeps test events and server logs.

## Repository CI and release publishing

Workflows in `.github/workflows/` use Go from `go.mod` (1.27.3); don't lower it for a runner. Test-gate rationale: [Engineering spec](https://github.com/richardrh/valet/blob/main/docs/engineering/spec.md).

### CI contract

`go.yml` runs on PRs, pushes to `main`, manual dispatch and release calls (not other branch or tag pushes). Docs-only PRs (`docs/content`, layouts, Hugo config, engineering docs, `*.md`, `docs.yml`) skip every Go job.

| Lane | When | Jobs |
|---|---|---|
| PR | Every code PR | `lint`: modules, gofmt, vet, staticcheck, `govulncheck` v1.9.0, `actionlint` v1.7.12 · `test`: `go test -race ./...` · `cli`: release executable + whole integration suite, S3 included, against MinIO `07c3a429bfed` |
| Full | `main`, releases, manual runs, PRs labelled `full-ci` | adds `native` (Linux arm64, Windows amd64), `darwin` (amd64/arm64 cross-built and tests compiled; not run, signed or notarized), `s3` (MinIO + RustFS 1.0.3, release and race executables; logs `s3-compatibility-*`), `go-install` (isolated `GOBIN`; tags also `go install …@<tag>`) |
| Manual | Input `run_scale` | 10 GiB and 100 GiB Arrow streams |

- **`CI result`** is the single required check: it passes when every job that had to run passed.
- **Caching:** each job restores its own Go build, test and staticcheck caches, saved on every `main` commit; the MinIO build is cached by its pinned commit and Go version. Integration runs against a built executable always use `-count=1`.
- **AWS:** optional opt-in; not a release gate. **`docs.yml`:** Hugo Extended 0.156.0 (checksum-verified) for docs PRs, `main`, manual.

Release publication runs the full lane first. `docs.yml` PR builds have `contents: read` only, use the repository's default Pages URL as base URL and never touch Pages (they work before Pages exists; separate concurrency groups can't cancel deploys). Publishing runs only on `main` push/manual and use the Pages base URL (custom domain included).

### One-time GitHub setup

Repository settings; the workflows don't change them for you.

| # | Where | Do |
|---|---|---|
| 1 | Settings → Actions → General | Enable Actions; read-only default token; no PAT/secret needed |
| 1a | Org policy | Allow `contents: write`, `pages: write`, `id-token: write`; fork PRs may need approval |
| 2 | Ruleset on `main` | Require review and the **CI result** check |
| 2a | — | Don't require individual jobs, scale or the path-filtered docs check (skipped jobs would leave PRs pending) |
| 3 | Settings → Pages | Source **GitHub Actions**; don't commit `docs/public/`. Not enabled automatically: without it the first publishing run fails (PR builds don't) |
| 4 | Environments → **github-pages** | **Selected branches and tags**: `main` only; optional reviewers |
| 5 | Actions → **Documentation** | Run on `main`, check the URL; other branches only build |
| 6 | Tag ruleset | Only trusted maintainers create release tags; review workflow changes. The release job's `GITHUB_TOKEN` gets `contents: write`; Pages gets `pages: write` + `id-token: write`; PR builds get neither |

### Publish a version

Update `CHANGELOG.md`, review `main`, then:

```bash
git switch main
git pull --ff-only
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

Tags: `vMAJOR.MINOR.PATCH` (`v0.1.0`), optional hyphenated prerelease (`v0.1.0-rc.1`); no `+build.1` metadata.

`release.yml` validates the tag, runs full Go CI, creates a draft, uploads assets, then publishes via `GITHUB_TOKEN` (no PAT):

- `valet_<tag>_linux_amd64.tar.gz`, `valet_<tag>_linux_arm64.tar.gz`
- `valet_<tag>_darwin_amd64.tar.gz`, `valet_<tag>_darwin_arm64.tar.gz`
- `valet_<tag>_windows_amd64.zip`
- `pipeline.schema.json`: the pipeline JSON Schema for that version
- `checksums.txt`: SHA-256 per asset (not a signature or attestation)

Archives hold the executable, `README.md`, `CHANGELOG.md`, `SECURITY.md`, MIT `LICENSE`; dependencies keep their own licenses. Verify downloads: [installation guide](../../getting-started/installation/).

- Prerelease tags never become latest; stable ones do.
- Existing release or draft: refused. Failed draft: inspect, delete if safe, rerun the same tag.
- Never move or reuse a published tag; release a new version.
