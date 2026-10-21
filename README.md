# valet

Valet checks local and S3-hosted CSV, Parquet, NDJSON and JSON data using YAML
or JSON pipelines. It ships as a single self-contained CLI, built on Apache
Arrow Go, without Python, cgo or an Arrow C++ runtime.

## Choose what to check

| Goal | Command |
|---|---|
| Check Parquet column names and types without scanning values | `valet schema -p pipeline.yml -i input.parquet` |
| Check values and return only an aggregate JSON summary | `valet validate -p pipeline.yml -i input.parquet` |
| Check values, conversions, references and dataset rules, and write results | `valet validate -p pipeline.yml -i input.parquet -o results/` |
| Preview the schema, passes and state needed for a full run | `valet plan -p pipeline.yml -i input.parquet` |

**Schema-only is not full validation.** It returns a JSON compatibility report,
lists checks that were not evaluated, and creates no row outputs or completion
manifest. It cannot prove non-null values, ranges, uniqueness or payload
integrity. Use the [schema-only guide](docs/content/guides/schema.md) for the
supported-check table, report fields and exit codes.

Without an output URI, full validation writes one JSON summary to stdout and
creates no files, detailed per-row issue list or completion manifest. With
`-o` or `output.uri`, it publishes artifacts: `all` writes all row buckets,
`failures` writes warning/error rows, and `report` keeps issues and reports
without row files or raw rejects. Artifact runs publish their manifest last.
The default mode is `report` without a destination and `all` with one;
explicit `all` or `failures` requires a destination. `schema --format text`
gives readable typed differences; JSON remains its default.

Parquet value checks remain conservative by default. Explicit
`--parquet-stats trusted` can accept eligible producer claims in report/failures
modes, with separate metadata coverage and persisted evidence. It does not prove
the payload is intact. See [output modes and metadata trust](docs/content/guides/validation-modes.md)
and [`examples/parquet-statistics.yml`](examples/parquet-statistics.yml) before
enabling it. Detailed phase/byte/coverage diagnostics are stderr NDJSON with
`--log-format json`; stdout remains the requested result.

## Try the bundled sample

```bash
CGO_ENABLED=0 go build -trimpath -o valet ./cmd/valet
./valet plan -p examples/pipeline.yml -i examples/sample.csv
./valet validate -p examples/pipeline.yml -i examples/sample.csv
```

The sample validation exits `1` because one record ID is missing. That is a
completed validation result, not an operational failure. The command returns
one JSON summary on stdout and creates no files. To also write Parquet buckets,
detailed issues and a completion manifest, explicitly add `-o output/`.

For your Parquet file, the bundled schema example checks the expected names and
types while explicitly leaving its amount-value rule unevaluated:

```bash
./valet schema -p examples/parquet-schema.yml -i input.parquet
```

The [quickstart](docs/content/getting-started/quickstart.md) shows how to create
sample Parquet data with Valet and try that command without installing another
data-processing library. Both YAML and strict JSON pipeline documents are
supported; see [`examples/pipeline.json`](examples/pipeline.json).

Valet reimplements [validnator](https://github.com/richardrh/validnator)'s
declarative validation model; compatibility differences are documented in
[`CHANGELOG.md`](CHANGELOG.md).

The default managed-memory budget is 512 MiB, configurable with
`--memory-budget`; it is not a hard process-RSS limit. Exact uniqueness and
large reference indexes fail if their state exceeds the budget—there is no
disk spill or approximate fallback. `--workers` accepts 1–32 (default 1) and
enables bounded, ordered execution overlap and parallel Parquet row-group
decoding. Queued batches and decoder workspaces share the managed budget.

Logs are structured `log/slog` records on stderr. `--log-format json` emits one
JSON object per line, including argument diagnostics; `--log-level` sets the
level, and `-v` turns on debug output. Every record from a run carries its
`run_id`. Stdout defaults to one JSON summary, even when artifacts are selected.
`--plain` selects validnator-compatible `key=value` lines; `--stats` implies
plain output with per-rule counts unless JSON is explicitly requested.
`--json-output -` explicitly selects stdout JSON (and conflicts with `--plain`);
`--json-output PATH` writes an additional summary file while stdout stays JSON.
When `input.type` is omitted, it is inferred from the decoded input key suffix.
Exit `0` allows warnings; `1` means failed checks; `2` means an operational or
presentation failure without a committed manifest. Exit `3` means an artifact
manifest committed but stdout presentation failed: consume it rather than
rerunning validation. Building requires Go 1.27.3 or later.

See the [rules cookbook](docs/content/rules.md) for copy-paste checks covering
types, positive values, nulls, column comparisons, exact schemas, references,
conditions, uniqueness and dataset assertions. The
[getting-started guide](docs/content/getting-started/_index.md) covers the first
run; [configuration](docs/content/guides/configuration.md) covers sources,
schemas and limits; [structured output](docs/content/guides/output.md) covers
issues, rejects and manifests; and
[performance](docs/content/guides/performance.md) contains measured Parquet
throughput and schema-only inspection timings.

## Installation and releases

Build from source with Go 1.27.3 or later using the command above, or download
a versioned archive from [GitHub Releases](https://github.com/richardrh/valet/releases).
Releases provide Linux amd64/arm64, Windows amd64 and macOS (Darwin)
amd64/arm64 binaries. Verify the archive against the
release's `checksums.txt` before extracting it; each archive includes the
executable, this README, `CHANGELOG.md`, `LICENSE` and `SECURITY.md`. See the
[installation guide](docs/content/getting-started/installation.md) for commands.

Version tags such as `v0.1.0` or `v0.1.0-rc.1` run the full Go CI before
publishing assets. Prereleases do not become the latest stable release.
Linux and Windows receive native acceptance testing; macOS artifacts are
cross-built and their tests compiled, not run on macOS.

Valet is licensed under the [MIT License](LICENSE).
Third-party dependencies retain their respective licenses.
Report vulnerabilities through the private channel in [SECURITY.md](SECURITY.md).

## Documentation and GitHub setup

The documentation is also a Hugo/Hextra site. Preview it locally with:

```bash
hugo server --source docs --buildDrafts --disableFastRender
```

GitHub Pages deployment is defined in `.github/workflows/docs.yml`. Pull
requests build the production site with read-only permissions, without needing
Pages to be enabled. Only a `main` push or a manual run on `main` can publish;
the deployment URL comes from GitHub Pages configuration. For this repository,
the default site URL is <https://richardrh.github.io/valet/>.

Before the first deployment or release, follow the
[one-time GitHub setup and release guide](docs/content/guides/ci.md#repository-ci-and-release-publishing)
to enable Actions, select the Pages source, restrict the deployment environment
and configure branch protection. CI uses the Go version in `go.mod`; the site
build pins Hugo Extended 0.156.0 and verifies its official release checksum.


## Consuming validation errors
For an aggregate-only pipeline, omit `output.uri` from the pipeline and gate on
the exit code before parsing stdout:

```bash
set +e
./valet validate -p checks.yml -i immutable-input.csv > summary.json
code=$?
set -e
case "$code" in
  0|1) python3 -m json.tool summary.json ;;
  *) exit "$code" ;;
esac
exit "$code"
```

The redirected file is created by the shell, not Valet. The summary retains
counts, coverage, trust and provenance, but has no `manifest_uri` or per-row
issue stream. Keep inputs immutable through validation; identity checks do not
turn mutable local files into snapshots. Do not rerun against changing inputs
as a substitute for retaining requested artifacts.


With an explicit output destination, a committed run is machine-readable from `manifest.json`. Follow its
`artifacts` reference to `artifacts.ndjson`; records with `kind: issues` point
to `issues/part-*.ndjson`, and `kind: rejects` records point to
`rejects/part-*.ndjson`. Verify each referenced size and SHA-256 before
consuming it.

Each issue line contains `report_version`, `run_id`, source/row provenance when
applicable, stable `code`, `severity`, `origin`, `scope`, and optional `column`,
`reason`, and `count`. Rule messages and definitions are stored once in
`rules.json`, keyed by rule code.

Reject lines are emitted for recoverable ingestion/conversion failures. They
contain the recoverable raw record, per-column `valid`, `missing`, `null`, or
`invalid` state, and structured ingestion diagnostics. They contain source,
source ordinal, row, global row, and input line provenance.

Fatal framing failures such as malformed CSV/JSON, invalid UTF-8 framing, or an
oversized record are pre-commit input failures: no terminal manifest is
published. Use `--log-format json` to receive their structured command
diagnostic on stderr. They are not represented as reject rows because the
source record could not be framed safely.
## Tests

```bash
go test ./...
go test -tags=integration ./integration -timeout 20m
go test -tags=scale ./benchmarks -run '^TestScale100GiB$' -timeout 0 -v
```

The last command explicitly runs separate generated 10 GiB and 100 GiB Arrow
streams; it does not download or retain giant input files. It measures the
engine, not CSV parsing or S3 throughput.

CI requires the real CLI acceptance suite against two independent disposable
containers: MinIO (pinned source commit) and RustFS 1.0.3 (pinned image digest).
Both run release and race-instrumented binaries, including owned versioned
buckets; a skipped selected test fails the gate. Reproduce locally with Docker:

```bash
bash .github/scripts/test-s3-compatible.sh minio
bash .github/scripts/test-s3-compatible.sh rustfs
```

For rootless Podman, prefix either command with `CONTAINER_RUNTIME=podman`.
The runner needs Go, a C compiler for the race build, curl and jq. It binds only
loopback, uses local-only credentials and cleans up its containers and temporary
data. Set `VALET_S3_LOG_DIR` to retain JSON test events and server logs.

Real S3-compatible integration tests require `VALET_S3_ENDPOINT`,
`VALET_S3_BUCKET` and SDK credentials. `VALET_S3_CREATE_BUCKET=1`
allows test-bucket creation only for the explicitly configured custom endpoint.
AWS tests remain optional through `VALET_AWS_INTEGRATION=1` and
`VALET_AWS_BUCKET`. AWS service behavior, IAM and KMS are unverified; they are
not prerequisites for the S3-compatible release scope.
