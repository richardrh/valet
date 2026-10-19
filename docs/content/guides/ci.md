---
title: CI automation
weight: 40
type: docs
prev: guides/performance
---

For **full validation** (`valet validate`), exit `0` means data validation passed
and exit `1` means it completed with error-level issues. Both publish a terminal
manifest and structured artifacts. Exit `2` means no result committed.

For a **schema-only gate**, run `valet schema` and consume its stdout JSON
instead: exit `0` means schema compatibility, `1` an error-level schema mismatch,
and `2` an operational/configuration failure. Neither schema outcome publishes
a normal validation manifest. See [schema-only checks](../schema/) for the
supported checks and report contract.

## GitHub Actions example

This workflow builds Valet, validates a repository fixture, uploads the terminal
result and fails the job when data validation fails.

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
            -p validation/pipeline.yml \
            -i data/input.csv \
            -o validation-output \
            --json-output validation-summary.json
          code=$?
          set -e

          echo "exit_code=$code" >> "$GITHUB_OUTPUT"
          if test "$code" -gt 1; then
            exit "$code"
          fi

      - name: Upload validation result
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: validation-result
          path: |
            validation-output/
            validation-summary.json
          if-no-files-found: warn

      - name: Enforce data quality
        if: steps.validate.outputs.exit_code == '1'
        run: exit 1
```

## Consume the summary

`--json-output` writes the stable summary contract. Read `manifest_uri` from the
summary, then follow the manifest's checksummed `artifacts` reference.

```bash
python3 - <<'PY'
import json
with open("validation-summary.json") as stream:
    summary = json.load(stream)
print(summary["validation"])
print(summary["error"])
print(summary["manifest_uri"])
PY
```

For row-level details, read the artifact entries with `kind: issues` or
`kind: rejects`. Do not discover a completed run by listing output files; only
a durable `manifest.json` marks a consumable result.

## JSON logs

Use structured stderr when an orchestrator needs machine-readable pre-commit
failures:

```bash
valet validate ... --log-format json 2>valet.log.ndjson
```

Fatal framing, configuration, I/O and resource failures return exit `2` and do
not publish a terminal manifest.

## S3 jobs

Use the standard AWS SDK credential chain through your runner's workload
identity, OIDC role or secret provider. Do not put static credentials in the
pipeline document. Source and output can use separate endpoint, region and
profile settings when required.

## Repository CI and release publishing

The repository's automation lives in `.github/workflows/`. It uses the Go
version from `go.mod` (currently Go 1.27.2); do not lower the version merely to
fit a runner's preinstalled toolchain.

### CI contract

- `go.yml` runs on branch pushes, pull requests and manual dispatch, and can
  be called by the release workflow. Tag pushes do not start a duplicate
  standalone CI run.
- Quality gates verify modules, formatting, vet, race tests, vulnerability
  scanning with pinned `govulncheck` v1.9.0, and workflow validation with pinned
  `actionlint` v1.7.12.
- Linux amd64, Linux arm64 and Windows amd64 run native unit tests and CLI
  acceptance against the version-stamped binaries that become release assets.
- Darwin amd64 and arm64 are cross-built and their tests compiled on Linux.
  They are not executed on macOS, code-signed or notarized.
- S3-compatible acceptance runs against an isolated, pinned MinIO instance
  with local-only credentials. It runs the `TestVM` feature gates against both
  a release binary and a separately race-instrumented child CLI. The dedicated
  backend permits creating owned versioned test buckets through
  `VALET_S3_CREATE_VERSIONED_BUCKET=1`; shared user buckets are never reconfigured.
  This is not real AWS verification. Actual AWS tests require explicit opt-in,
  a bucket and scoped credentials; unavailable services are not verified.
- The generated scale workload is opt-in through the CI manual dispatch
  `run_scale` input. It generates separate 10 GiB and 100 GiB Arrow streams;
  normal CI and releases do not run this workload.
- `docs.yml` separately builds production Hugo documentation for
  documentation-related pull requests and `main` pushes, or manual dispatch.
  It pins Hugo Extended 0.156.0 and verifies the download against the official
  Hugo release checksums. PR builds have only `contents: read` and work before
  Pages is configured; they never invoke Pages configuration, upload or
  deployment. They use the repository's default Pages URL for the base URL.
  Publishing runs use the base URL returned by GitHub Pages, including a
  configured custom domain.

### One-time GitHub setup

These are repository settings to apply after creating the GitHub repository;
the workflows do not change them for you.

1. In **Settings → Actions → General**, enable GitHub Actions and allow the
   actions used by these workflows. Choose the read-only default workflow
   token permission; jobs request their own minimum additional permissions.
   No personal access token or repository secret is needed for CI, release
   publishing or Pages. Organizational policy must permit the workflows'
   explicit `contents: write`, `pages: write` and `id-token: write` grants.
   Fork PR workflows may require a maintainer's approval under the repository's
   Actions policy.
2. Set the default branch to `main`. Push the repository and let branch CI
   report its actual check names. Configure a branch-protection rule or
   ruleset for `main` requiring pull-request review and the non-optional Go CI
   quality, native acceptance, Darwin compilation and S3-compatible checks.
   Select the reported matrix check names, rather than guessing them before
   the first run. Do not require the opt-in scale job. The documentation
   workflow is path-filtered; requiring its check on every PR can leave
   unrelated changes pending forever unless those path filters are removed.
3. In **Settings → Pages → Build and deployment**, choose **GitHub Actions**
   as the source. Do not select a publishing branch or commit generated
   `docs/public/` output. The workflow reads the site configuration but does
   not enable Pages automatically.
4. Create or review the **github-pages** environment under **Settings →
   Environments**. Set deployment branches/tags to **Selected branches and
   tags** and allow the `main` branch only, not tags or wildcard branches.
   Add required reviewers if desired and supported by the repository plan.
   The workflow also guards publishing to `main` push/manual runs. PR refs use
   separate concurrency groups, so PR builds cannot block or cancel a
   deployment.
5. Run **Documentation** manually with `main` selected, or push a documentation
   change to `main`, and inspect the deployment URL. A manual run on another
   branch builds only. Missing Pages setup may fail a trusted publishing run,
   but never a PR production build.
6. Restrict creation of release tags to trusted maintainers with a tag ruleset
   where available. Review workflow changes carefully: a release job uses the
   built-in `GITHUB_TOKEN` with `contents: write` to publish assets. Pages alone
   uses `pages: write` and `id-token: write`; PR builds do not receive these
   grants.

### Publish a version

Update `CHANGELOG.md` and review the intended release commit on `main` before
creating a tag. Tags must be `vMAJOR.MINOR.PATCH`, optionally followed by a
hyphenated prerelease, for example `v0.1.0` or `v0.1.0-rc.1`. Build metadata
suffixes such as `+build.1` are not supported. For example:

```bash
git switch main
git pull --ff-only
git tag -a v0.1.0 -m "Release v0.1.0"
git push origin v0.1.0
```

`release.yml` validates the tag and calls the full reusable Go CI at that
tagged revision before packaging. It produces:

- `valet_<tag>_linux_amd64.tar.gz`
- `valet_<tag>_linux_arm64.tar.gz`
- `valet_<tag>_windows_amd64.zip`
- `valet_<tag>_darwin_amd64.tar.gz`
- `valet_<tag>_darwin_arm64.tar.gz`
- `checksums.txt`, containing the SHA-256 of each archive.

Each archive contains the executable, `README.md`, `CHANGELOG.md` and the
project's MIT `LICENSE`. Third-party dependencies retain their respective
licenses. See the [installation guide](../../getting-started/installation/)
for download and checksum verification instructions. Checksums are not
signatures or attestations.

The workflow creates a draft release, uploads all assets and only then
publishes it. A hyphenated version is marked as a prerelease and never marked
as latest; stable versions are published as latest. Publishing uses only the
workflow's `GITHUB_TOKEN`, not a PAT.

An existing release, including a draft, causes the workflow to refuse to
overwrite it. If a run fails after draft creation, inspect the draft and its
assets, delete that failed draft if it is safe to do so, then rerun the
workflow for the same unchanged tag. Never move or reuse an already published
version tag; publish a new version for corrections.
