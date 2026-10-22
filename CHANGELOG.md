# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed

- Clarified what Valet is and why to use it across the README, docs homepage
  and getting-started guide, leading with file-based ingestion gates and
  stdout reports rather than schema inspection or optional artifact output.
- Shortened the README to a product overview and runnable examples; replaced
  Python report-extraction snippets in onboarding docs with simple `jq` queries.
- Reduced the root README to purpose, capabilities, motivation, installation and
  usage; added Go installation instructions and a release-gating `go-install`
  job that exercises both checkout and published-tag installations.
- Added a concise README comparison with Great Expectations (GX Core) and
  Soda Core, linking their official documentation and distinguishing workflows
  without implying that either requires a hosted service or cannot read files.
- Added concise workflow/runtime and measured deployment-footprint tables for
  Valet, GX Core/pandas and Soda Core/DuckDB, including version/extras caveats
  and clarification that Valet links the Go AWS SDK rather than Python.

## [0.1.0-rc.1] - 2026-10-21

### Added

- Stdout-first `validate`: no output URI returns one aggregate JSON summary
  without artifact files, per-row issue lists or a manifest. Destination-aware
  defaults select `report` without a URI and `all` with one; `all`/`failures`
  require a destination. Explicit artifact publication remains manifest-last.
- JSON stdout is the validation default; `--plain` preserves key=value output,
  `--stats` implies plain/statistics unless JSON is explicit, and
  `--json-output PATH` adds a summary file without replacing JSON stdout.
  Presentation failure returns exit `2` without a committed manifest and
  exit `3` only after an artifact manifest commits.
- Added a security reporting and supported-version policy with private GitHub
  vulnerability reporting.
- Required release and race CLI acceptance against independent MinIO and RustFS
  containers, with pinned source/image, retained backend evidence and no skipped
  selected tests; AWS-specific verification remains outside the release scope.

- Go implementation of validnator's YAML/JSON pipeline validation using Apache Arrow Go, with a pure-Go CLI and internal reusable packages.
- Local/S3 CSV, Parquet, streaming JSON and NDJSON ingestion; explicit schemas, exact integer/decimal conversion and recoverable ingestion rejects.
- Budgeted batch execution, exact replay-based uniqueness, bounded reference indexes, conditional rules, `not_blank`, `is_present`, timestamp windows and dataset assertions.
- Streamed CSV/Parquet results, issues and reject diagnostics with manifest-last publication and cancellation cleanup.
- Structured `log/slog` logging on stderr (`-v/--verbose`, `--log-level`, `--log-format text|json`) with run-scoped `run_id`, validnator-compatible `--plain` key=value summaries, `--stats` per-rule failure counts, `input.type` inference from the source suffix, and signal-specific exit codes (130 SIGINT, 143 SIGTERM).
- Colocated tests, CLI/S3 acceptance scenarios, independent Parquet verification hooks and opt-in generated 100 GiB scale testing.
- Linux amd64/arm64 and Windows amd64 build/test workflow, darwin cross-builds, formatting/vet/race quality gates, independent MinIO/RustFS container integration, and optional 100 GiB scale acceptance.
- First-class strict JSON pipeline documents alongside YAML, using one normalized validation path and equivalent examples/tests.
- Exclusive numeric range bounds for direct `>`, `<`, and open-interval assertions, plus a runnable rules cookbook and orders fixture.
- Added gated version-tag GitHub releases with five platform archives and SHA-256 checksums, reusable CI with pinned vulnerability/workflow checks, and read-only PR documentation builds with main-only GitHub Pages deployment.
- Licensed Valet under MIT and required the license text in every release archive.
- Added `valet schema` for explicit footer-only Parquet schema compatibility reports, with zero checked rows and unevaluated data-dependent checks disclosed.
- Report-version-2 output modes (`all`, `failures`, `report`), canonical JSON/readable schema differences, and structured phase/byte/coverage diagnostics.
- Explicit trusted Parquet-statistics execution with selective range reads, final pinned-identity verification, separate metadata-claimed coverage and a persisted evidence ledger.
- Bounded fixed-width writer statistics and 62 black-box feature acceptance cases, including real MinIO range/write denial, version replacement, cancellation and broken-pipe scenarios.

### Changed

- Fused guarded Parquet page verification/decompression with ordered row-group decoding, reused budgeted metadata and codec workspaces, and added bounded execution overlap with 1–32 workers.
- Added independently owned check results, lazy/reusable issue storage, batch-bound typed operands, shared predicates, exact membership indexes, and shared reference-source scans.
- Switched to compatible fast Snappy encoding; buffered Parquet groups across batches, sliced contiguous selections, and selectively dictionary-encoded bounded low-cardinality columns and provenance.
- Buffered local writes and coalesced pinned S3 ranges; reused authoritative S3 publication digests and added adaptive, budgeted multipart concurrency.
- Reworked public onboarding and check/output documentation around schema-only versus full validation; added a dedicated supported-check guide and a runnable Parquet schema pipeline.
- Documented producer-statistics trust and stale/tampered metadata risks; conservative value scans remain the default, and metadata-accepted rows are never labelled value-checked.
- Added a dedicated implementation contract and black-box E2E acceptance catalogue for validation modes, schema diffs, structured diagnostics and trusted statistics.

### Fixed

- Size CSV decoder reservations from admitted record bytes and read-ahead rather
  than the configured maximum record size, retaining pre-allocation budget checks
  and oversized-record rejection.
- Preserved unread Parquet rows when mixed physical schemas switch between lexical conversion and native decoding; added an end-to-end regression comparing independently counted input rows with checked and published rows, including values and provenance.
- Enabled cgo specifically for CI race tests while retaining cgo-free distribution builds; corrected the Hugo cache context and documented first-push, Pages, and release setup.
- Made `plan` stop reporting validation/prepasses after ingestion schema rejection, consistently with execution.
- Preserved inference/prepass/final decoded-row visit counts and deferred baseline pass counts; corrected diagnostic metadata/payload and accepted-manifest byte attribution.
- Made publication fault injection portable across path separators and special-file rejection tests independent of diagnostic wording on Windows.
- Compared native provenance paths using the selected CSV backslash encoding in cross-platform end-to-end checks.
- Made documentation navigation site-wide instead of section-scoped, with expanded groups, concise sidebar labels and the full guide tree available on mobile.
- Bounded context expansion and CSV/JSON decoder growth before allocation; pipeline special files are rejected without blocking cancellation.
- Unified schema type aliases and date directives across validation, source conversion and checks; corrected civil dates, timezone abbreviations, Parquet temporals, binary references and issue-column attribution.
- Canonicalized local/S3 overlap and suffix handling, pinned inference replay identity, counted all Parquet scans, and included ignored Parquet columns in record limits.
- Preserved every independent schema diagnostic and included completion buffers in the reported managed-memory peak.
- Made local manifest publication directory-durable and reconciled ambiguous S3 PUT/multipart outcomes without orphaning uploads.
- Staged `--json-output PATH` before terminal commit and added exit `3` for post-commit stdout presentation failure.
- Closed follow-up gaps in decoder preallocation, raw-header replay identity, date-layout accounting, multi-check attribution, physical Parquet widths/pass counts, canonical plan types, Windows/local durability rollback, cancellation-safe S3 reconciliation, multipart cleanup capacity, durable staged summaries, and completion-lifetime memory accounting.
- Corrected `equals`, `not_equals`, and `all_equal` to intuitive assertion semantics: equality checks now pass when their names say they should.

### Security

- Require Go 1.27.3 to fix [GO-2026-6809](https://pkg.go.dev/vuln/GO-2026-6809) (compromised intermediate-certificate verification); retain `golang.org/x/net` v0.61.0 and the earlier standard-library security fixes.

### Differences from Python validnator

- Separate executable; does not preserve validnator's in-process DataFrame API or legacy detailed JSON/stdout report format. See `docs/engineering/spec.md` for the full contract.
- Equality checks intentionally differ from Python validnator: `equals` and `all_equal` are assertions that pass on equality, while `not_equals` passes on inequality. To preserve an existing Python rule, swap `equals` with `not_equals`; rewrite an old `all_equal` violation as an `all`-composed `when` condition plus a failing `not_equals` check on one operand.
- Composite reference keys preserve field order and distinguish null from empty strings. Reference CSV identifiers remain lexical strings rather than undergoing incidental numeric inference.
- Warning counts include warnings on error-bucket rows; typed conversion failures are not ordinary nulls. Explicit schemas use exact compatible typed comparisons.
- CSV output uses the documented reversible null/backslash encoding recorded in `rules.json`; Parquet retains native typed/null values. Read results only after their completion manifest exists.
- A numeric YAML literal compared with a string column (for example raw-string CSV) parses the cell as a number. A non-numeric cell makes the rule unevaluable instead of silently not matching. This applies to `when`, `equals`, `not_equals`, `all_equal` and `valid_values`. `valid_range` still treats a non-numeric cell as a violation.
- CSV and JSON use `encoding/csv` and `encoding/json/jsontext`. JSON errors report a byte offset without quoting values, and JSON record line numbers point at each record's opening `{`. The default output part size is 128 MiB.
