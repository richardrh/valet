# Review remediation specification

Status: implemented remediation contract. The requirements below refine `spec.md`; the main specification and implementation now incorporate these guarantees.

## 1. Scope and completion rule

This work corrects configuration amplification, decoder accounting, location identity, type and temporal semantics, replay identity, Parquet limits, check diagnostics, result reporting, and local/S3 publication.

All requirements are release-blocking. A remediation is complete only when:

1. every requirement in Sections 3–11 is implemented;
2. every acceptance case in Section 12 passes at its stated test layer;
3. package, integration, race, and release-target checks in Section 13 pass; and
4. obsolete branches, duplicated type/date mappings, boolean schema-issue suppression, and unsafe storage paths are removed rather than retained as compatibility shims.

This specification does not add connectors, parallel workers, spill-to-disk, automatic retries of complete validation runs, or new input/output formats.

## 2. Cross-cutting invariants

The implementation MUST preserve these invariants:

- **Bound before allocation.** Every allocation controlled by user input MUST have either an active managed-budget reservation covering its conservative peak or a fixed process-level bound established before allocation.
- **One canonical interpretation.** URI identity, schema type names, and date directives MUST each have one shared parser/normalizer used by validation and execution.
- **Pinned input.** Data used after discovery or inference MUST be the same data that established the schema and plan. Metadata aliases or same-size/same-mtime replacement MUST NOT bypass replay checks.
- **Whole-record limits.** `max_record_bytes` applies to the complete logical source record, including ignored fields.
- **Manifest authority.** Before `manifest.json` is durably published, failures are operational and no terminal result exists. After it is durably published, the run result is committed and MUST NOT be reported as an unpublished operational failure.
- **Exact diagnostics.** Every schema/dataset issue is emitted exactly once in the issue stream, and row issues identify the failing operand when a single operand is known.
- **No hidden passes.** Full source scans, digest scans, Parquet safety scans, and decoding passes MUST be represented by plan/summary pass accounting.

## 3. Configuration loading and CLI parsing

### 3.1 Bounded context substitution

Affected code: `internal/config/load.go`.

Configuration loading MUST retain the existing 4 MiB encoded YAML limit and add these post-decode limits:

- one expanded rule message: at most 1 MiB UTF-8 bytes;
- all expanded rule messages combined: at most `MaxConfigBytes`;
- replacement keys and rendered replacement values combined: at most `MaxConfigBytes`.

The substitution implementation MUST:

1. render and sort the context replacement table once per configuration;
2. preserve current single-pass replacement semantics—replacement output is not recursively expanded;
3. stream replacement output through a counting bounded builder;
4. check integer overflow before additions or capacity growth; and
5. return a configuration error naming the rule code when either message or aggregate expansion exceeds its bound.

It MUST NOT call `strings.Replacer.Replace` directly on an unbounded destination for each rule.

### 3.2 Pipeline files are regular, cancellable inputs

`config.Load` MUST accept a `context.Context`. All callers MUST pass the command context.

Pipeline paths MUST be opened through platform-specific regular-file helpers:

- Unix: open nonblocking, then `fstat`; reject FIFO, socket, block device, character device, and directory.
- Windows: reject named pipes and device namespaces and require a disk-backed regular file.

Cancellation MUST be checked before open, after open, and after the bounded read. Unsupported file types are configuration errors. The 4 MiB limit remains authoritative.

### 3.3 Logging format on parse failures

Affected code: `internal/cli/cli.go`.

CLI parsing MUST have two phases:

1. recognize syntactically valid presentation flags (`--log-format`, `--log-level`, `-v`/`--verbose`) without initializing storage or loading configuration;
2. perform full command and argument validation.

If a valid `--log-format json` is present, every subsequent diagnostic—including missing required arguments and unknown command options—MUST be emitted as one JSON log object on stderr. An invalid logging-format value itself MAY use the default text diagnostic because no valid formatter was selected.

## 4. Resource accounting

### 4.1 CSV and JSON decoder working sets

Affected code: `internal/source/csv_input.go`, `json_input.go`, and `lexical.go`.

A decoder MUST reserve its conservative active working set before a library decoder can read record bytes.
The maximum configured working set—not the bytes observed so far—MUST be reserved before constructing `encoding/csv.Reader` or `jsontext.Decoder`. CSV accounting uses the record-limit-implied maximum field count; a discovered header does not weaken that bound.

For CSV, the reservation MUST cover at least:

- the maximum delivered record bytes;
- `encoding/csv` line/record copies;
- field strings and field-index state at the maximum field count implied by the record limit; and
- the reusable converted-cell state.

For JSON/NDJSON, the reservation MUST cover at least:

- the limiter tape;
- `jsontext.Decoder` input/token storage;
- selected name/value strings and walker state;
- raw reject-record retention; and
- transform-name copies when transforms are enabled.

Reservations MAY be conservative and held for the decoder lifetime. Actual buffers SHOULD still grow lazily. A configuration whose decoder working set cannot fit the managed budget MUST fail with `ResourceLimitError` before reading record payload bytes; it MUST NOT allocate the record and reject it afterward.

The limiter MUST participate in accounting when its tape grows. Gzip decompression does not relax these requirements.

### 4.2 Compiled date-format state

Affected code: `internal/checks/compile.go` and `scalar.go`.

The compiled-state estimate MUST include:

- every source format byte;
- every translated Go layout byte;
- slice/string headers and per-format fixed state; and
- duplicate retained layouts used by derived column contexts.
Derived contexts sharing one check MUST share one immutable translated layout slice. The estimate still covers the separately retained compiled-check slice.

Reservation MUST occur before layouts are allocated. Compilation MUST fail with `ResourceLimitError` if the formats do not fit.

### 4.3 Final managed peak

Affected code: `internal/sink/writer.go` and `report.go`.

Before assigning `summary.ManagedPeak`, completion MUST reserve a conservative upper bound covering:

- `summary.json` encoding and storage-writer state;
- `manifest.json` encoding and storage-writer state; and
- references and digest state retained until commit.

`ManagedPeak` MUST be captured after a proof reservation covering the retained hold plus the largest later publication operation. The value returned to the CLI and encoded in `summary.json` MUST be identical and MUST include completion work. A concrete reservation covering retained completion state and the pre-commit callback remains held until terminal publication succeeds or abort cleanup finishes; later metadata/storage operations retain their own ordinary reservations.

## 5. Canonical locations and overlap prevention

### 5.1 Shared location parser

Affected code: `internal/config/validate.go` and `internal/storage` URI helpers.

Add one internal location parser returning either:

- local: canonical absolute path plus resolved existing-ancestor identity; or
- S3: decoded bucket and exact decoded key bytes.

S3 keys MUST NOT be passed through `path.Clean`; repeated slashes, dots, and trailing slashes are valid key bytes. Percent-encoded aliases MUST resolve to the same identity used by storage operations. Input-type inference MUST examine the decoded key path, after removing a decoded `.gz` suffix.

Local canonicalization MUST resolve symlinks/junctions in every existing ancestor. A nonexistent output suffix is appended to the resolved nearest existing ancestor. Windows comparison MUST use platform-appropriate case-insensitive path semantics.

### 5.2 Prefix overlap semantics

For local directory discovery, overlap uses path-component containment after canonicalization.

For S3 discovery, overlap uses exact S3 byte-prefix semantics. If source key prefix `p` would select output key `o` through `ListObjectsV2(Prefix=p)`, the locations overlap even when the next byte is not `/`. Therefore `data` overlaps `database/results`.

Validation MUST reject overlap before creating storage clients. Regression coverage MUST include percent-encoded `/` and `.`, S3 raw-prefix containment, local symlinked output ancestors, Windows case aliases, and non-overlapping sibling locations.

## 6. Canonical type and date vocabulary

### 6.1 Type normalization

Add one canonical type-normalization function in `internal/config` and use it for explicit schemas, `type_check`, `schema_matches`, derived check contexts, source builders, and report metadata.

The required aliases are:

| Accepted name | Canonical name |
|---|---|
| `bool`, `boolean` | `bool` |
| `int`, `integer` | `int64` |
| `float`, `double` | `float64` |
| `str`, `string`, `utf8` | `string` |
| `date`, `date32` | `date` |
| `timestamp`, `datetime` | `timestamp` |
| `decimal`, `decimal128` | `decimal` |

Width-specific integer/float names and `binary` remain unchanged. Unknown names MUST fail during `Config.Validate`; a configuration accepted by validation MUST NOT later fail because a consumer uses a narrower type vocabulary.

### 6.2 Date-directive translation

Source conversion and `date_format` checks MUST use one translator. It MUST support the complete set accepted by configuration validation, including `%D`, `%F`, `%R`, `%T`, `%h`, and `%:z`. Multi-character directives such as `%:z` MUST be parsed atomically. Unsupported or incomplete directives MUST fail during `Config.Validate`, not source/check compilation.

## 7. Temporal and binary semantics

### 7.1 Calendar dates

Date conversion MUST derive Date32 from the parsed civil year/month/day, independent of time-of-day or UTC offset. It MUST use an overflow-checked civil-date-to-epoch-day calculation and verify the result fits Arrow Date32. Integer division of `time.Unix()` is forbidden for this conversion.

Examples that MUST preserve their calendar day:

- `1969-12-31 12:00` with a date format containing time;
- a date string containing a positive or negative UTC offset; and
- leap day `2024-02-29`.

### 7.2 Timestamp zones and `%Z`

A parsed numeric offset (`%z` or `%:z`) defines the instant and takes precedence over `column.timezone`.

For `%Z`:

- `UTC` and `GMT` are accepted as zero offset;
- another abbreviation is accepted only when `column.timezone` names an IANA location and exactly one instant at that local wall time has that abbreviation;
- unknown, mismatched, nonexistent, or ambiguous abbreviations are conversion failures.

The implementation MUST NOT parse an abbreviation and then silently discard it in favor of `column.timezone`.

### 7.3 Parquet temporal values

Parquet Date32/Date64/timestamp values MUST NOT round-trip through RFC3339 text. Matching inferred/native types preserve the Arrow integer value and time unit. Explicit conversions use checked integer scaling and civil-date conversion.

A value representable by its source Arrow/Parquet temporal type remains valid when no narrowing target was requested. A value outside an explicitly requested target range follows the configured conversion-error policy; it is not mislabeled as malformed RFC3339.

### 7.4 Binary reference keys

The canonical textual form of a valid binary operand is standard padded base64. `referenceKey` MUST encode binary operands with that form; string reference values remain lexical strings. Thus binary bytes `A` match reference text `QQ==`. Null and empty remain distinct.

## 8. Source identity, Parquet bounds, and pass accounting

### 8.1 Complete replay identity

During validation setup, every local text object used for header discovery or inference MUST establish a complete digest baseline before the schema is frozen. When the inference row limit is reached, the reader MUST drain the remaining bytes through the identity-verifying stream without continuing type inference or retaining rows.
Header-only `raw_strings` discovery follows the same identity rule during validation: buffered header bytes remain part of the digest and the remaining decompressed stream is drained without row decoding. Plan mode may remain header-only.

A later pass MUST compare its complete digest with that baseline. Rewriting a file in place with the same size, inode, and restored modification time MUST be detected before terminal publication.

Plan-only operations MAY omit the content digest because they publish no result, but their output MUST describe only the observed metadata/sample and MUST NOT seed a later validation run.

### 8.2 Parquet plan and execution modes

Range readers MUST expose explicit metadata-only and identity-verifying modes.

- `plan`: read and validate only the compact footer, embedded Arrow schema, and logical/schema metadata. Closing the handle MUST NOT trigger a full-file digest.
- `schema`: read the same guarded metadata-only ranges, publish only a schema-scope JSON report, enumerate data-dependent checks not evaluated, and report zero checked rows. Declared footer counts and metadata identity do not imply payload validation.
- `validate`: every full digest or page-safety scan MUST be intentional, cancellable, and counted. Digesting SHOULD be combined with an already required complete scan when possible.

`source.Passes`, `DecoderPassesPerOpen`, plan JSON, and summary JSON MUST include every complete object scan. A hidden close-time scan is forbidden.
For local Parquet execution, the setup baseline digest and close-time replay digest remain counted scans. Page verification/decompression is now fused with row-group decoding rather than a separate complete-object safety scan; it must retain the same preallocation/encoding/count guards. `DecoderPassesPerOpen` therefore adds one local digest scan and zero S3 digest scans. S3 version/ETag identity verification that reads no complete payload adds no scan.

### 8.3 Parquet whole-record size

The native Parquet path MUST account for every source column when enforcing `max_record_bytes`, including columns ignored by an explicit output schema. Variable-width values use actual byte length; fixed-width values use their physical width plus bounded per-cell/name overhead.

The lexical and native paths MUST accept and reject the same row at the configured limit. Projection MAY avoid materializing ignored values only if equivalent size enforcement is derived safely before projection.

## 9. Check and diagnostic semantics

### 9.1 Failing operand attribution

A compiled check MUST retain its resolved operand names. When a rule fails:

- if exactly one unique operand among the failing checks is known, the row issue uses that column;
- if multiple distinct operands contributed and the check cannot identify one failing operand, the issue column is empty;
- a top-level rule column is treated only as the default operand, not as an unconditional diagnostic label.
Evaluation MUST continue after the first failed component check for attribution purposes, collecting all distinct failing operands while still emitting only one rule issue.

This preserves one row issue per violated rule while avoiding false column attribution.

### 9.2 Dataset tolerance

`tolerance` is accepted only for `equals` on floating `sum`, `min`, `max`, or `avg` metrics. It MUST be rejected for row counts, count/fraction metrics, integer aggregates, and decimal aggregates because those observations compare exactly. A supplied tolerance MUST never be silently ignored.

### 9.3 Plan-time schema gates

`checks.Describe` MUST resolve all gates whose outcome is known from the discovered schema before copying `SchemaIssues`. This includes warning-level `required_columns` and strict `schema_matches` outcomes. Castability outcomes that require a row scan remain described as requiring a prepass rather than fabricated as known failures.

### 9.4 Exactly-once schema issue emission

Replace the sink-wide `streamedSchema` boolean with identity tracking. Schema issue identity is the stable tuple of scope, code, column, and reason, so a streamed row-located castability detail suppresses its matching aggregate while an independent diagnostic remains distinct. `WriteIssue` records emitted identities; `Complete` emits only summary issues whose identities have not already been streamed.

Two different diagnostics MUST both be emitted even when one castability issue was streamed earlier. The issue stream, summary counts, and `summary.SchemaIssues` MUST agree.

## 10. Local publication durability

Affected code: `internal/storage/local.go` and platform publication helpers.

Local publication MUST use this order for every file:

1. write the temporary file;
2. sync file contents and metadata;
3. close the file;
4. publish without replacement in the target directory;
5. sync the containing directory before reporting success.

Directory creation MUST sync each newly modified parent directory. Before publishing `manifest.json`, every artifact/metadata file and every directory entry it references MUST already be durable. After publishing the manifest, its containing directory MUST be synced before `Complete` returns.

On POSIX, directory `fsync` failures are publication failures. On Windows, publication MUST retain no-replace semantics and use write-through/flush primitives appropriate to disk files and directories. Unsupported durability primitives MUST produce an explicit platform error rather than silently claiming durability.
Windows directory handles used with `FlushFileBuffers` MUST be opened with write access. If a containing-directory sync fails after no-replace publication, the newly published target MUST be removed and that removal sync attempted before returning the pre-commit error.

A failure before durable manifest commit invokes abort cleanup and leaves no terminal manifest. A failure after the manifest directory sync is a cleanup/logging failure and does not invalidate the committed result.

## 11. S3 publication and terminal CLI behavior

### 11.1 Retry-safe bounded object PUT

Every create-if-absent S3 PUT MUST carry a cryptographically random operation token and a SHA-256 body digest in object metadata. After an error that could follow request transmission—including a retry ending in `PreconditionFailed`—the client MUST reconcile with `HeadObject`:

- matching operation token, length, and digest: treat the PUT as successful;
- absent object: return the original operational error;
- existing object with different identity: return a collision error.

This applies to the terminal manifest and bounded metadata objects. Reconciliation calls remain bounded and cancellable.
Reconciliation after an ambiguous request MUST use a fresh bounded context derived with `context.WithoutCancel`; cancellation of the original request cannot prevent classifying a terminal object that already committed.

### 11.2 Multipart creation and completion

`CreateMultipartUpload` MUST NOT use transparent retries because it has no idempotency token. The create request MUST attach the run/write identity as object metadata.

If creation has an ambiguous transport result, the client MUST list multipart uploads for the exact run-owned key, abort every matching upload, and return an operational error. It MUST NOT start another upload until cleanup succeeds.
Cleanup capacity MUST be reserved before `CreateMultipartUpload` is transmitted. Listing and aborting an unknown upload reuse that reservation rather than requesting additional post-ambiguity capacity.

`CompleteMultipartUpload` ambiguity MUST be reconciled with `HeadObject` using the write identity, expected length, and digest. A matching completed object is success; otherwise the known upload is aborted when still present. `UploadPart` MAY retain retries because upload ID plus part number identifies an idempotent replacement.

Integration environments SHOULD also configure an abort-incomplete-multipart lifecycle rule, but lifecycle cleanup is defense in depth and not a correctness substitute.

### 11.3 Report publication and exit codes

`--json-output PATH` MUST be staged, successfully closed, atomically renamed, and have its containing directory synced before terminal manifest publication. Failure returns exit 2, removes the newly published report when necessary, syncs that removal best-effort, and leaves no terminal manifest.

Human stdout and `--json-output -` are emitted after manifest commit because the manifest is authoritative. If those writes fail after commit, the CLI MUST return a distinct exit code `3`, meaning **terminal result committed; presentation failed**. It MUST include the manifest URI in a best-effort stderr diagnostic and MUST NOT describe the validation run as unpublished or aborted. Exit code 3 is not a signal to rerun validation; consumers read the manifest.

Normal committed results retain exit 0/1 semantics. Pre-commit configuration, input, I/O, resource, and publication failures retain exit 2. Signal exits remain `128+N` only before commit.

## 12. Acceptance matrix

Each ID is a required regression. Tests MUST assert externally meaningful behavior, not source text or mock forwarding.

| ID | Layer | Scenario | Required observation |
|---|---|---|---|
| REM-01 | config unit | 4 MiB-bounded YAML with a repeated large context placeholder | Deterministic configuration error before large allocation; rule code named |
| REM-02 | CLI subprocess | FIFO/device passed as `--pipeline`, then SIGINT | Prompt termination; no blocked open/read; signal exit where supported |
| REM-03 | CLI subprocess | Missing `--pipeline` with `--log-format json` | Exactly one valid JSON diagnostic on stderr; exit 2 |
| REM-04 | source/resource | Large legal CSV record with budget below conservative decoder peak | `ResourceLimitError` before payload is delivered to `encoding/csv`; bounded RSS |
| REM-05 | source/resource | Gzip JSON/NDJSON record with the same mismatch | `ResourceLimitError`, bounded RSS, no process OOM |
| REM-06 | config/source/checks | Every accepted type alias in schema, `type_check`, and `schema_matches` | Validation and compilation agree; canonical type appears in plan/report |
| REM-07 | config/checks | Every accepted date directive, including `%F` and `%:z` | Validation and compilation agree; valid values pass |
| REM-08 | source | Pre-epoch Date32 with time, offset-bearing date, and leap day | Exact intended civil days |
| REM-09 | source | `%Z` values `UTC`, valid IANA abbreviation, mismatched and ambiguous abbreviations | Correct instant for valid cases; deterministic conversion failures otherwise |
| REM-10 | source/Parquet | Millisecond/microsecond timestamps and dates outside RFC3339 four-digit-year text range | Native values preserved when representable; checked target-range failure only when narrowing |
| REM-11 | checks | Binary main value `QQ==` against lexical reference `QQ==` | Referential-integrity pass; null/empty cases remain distinct |
| REM-12 | config/storage | S3 `data` vs `database`, percent aliases, local symlink output, Windows case alias | Every effective overlap rejected before storage initialization |
| REM-13 | config | `s3://bucket/data%2Eparquet` without explicit input type | Parquet inferred from decoded key |
| REM-14 | source/integration | Local input rewritten between inference and validation with same size/inode/restored mtime | Input-change failure; no terminal manifest |
| REM-15 | plan/source | Large local Parquet plan with byte-counting range handle | Only footer/schema ranges read; no close-time full scan |
| REM-16 | plan/summary | Validation with Parquet safety scan and local digest work | Every complete scan represented in pass counts |
| REM-17 | source/Parquet | Oversized ignored column with small selected columns | Native and lexical paths both reject at `max_record_bytes` |
| REM-18 | checks | Check-level column overrides top-level column | Issue identifies the actual single failing operand |
| REM-19 | config/checks | Tolerance on row count, fraction, integer, decimal, and floating aggregate | First four rejected during validation; floating equality applies tolerance |
| REM-20 | checks/resource | Many near-limit date formats under a constrained budget | Compilation fails at the managed budget before retained layout allocation |
| REM-21 | plan/checks | Warning `required_columns` and strict `schema_matches` mismatch | Plan contains the known schema diagnostics |
| REM-22 | sink/integration | Streamed castability issue plus independent summary-only schema issue | Both appear exactly once in issues and summary accounting |
| REM-23 | sink/resource | Completion encoding establishes the run high-water mark | Returned and persisted `ManagedPeak` are equal and include completion reservation |
| REM-24 | local storage | Injected file-sync, publish, directory-sync, and manifest-directory-sync failures | Pre-commit failures return error and no terminal manifest; operation order is durable-data before manifest |
| REM-25 | S3 fault integration | Server commits conditional PUT then drops response | Reconciliation recognizes the matching object; no false operational failure |
| REM-26 | S3 fault integration | Server creates multipart upload then drops response | All ambiguous uploads for the run-owned key are aborted; no orphan remains |
| REM-27 | S3 fault integration | Server completes multipart upload then drops response | Matching object is accepted as success; mismatched object is rejected |
| REM-28 | CLI integration | Unwritable `--json-output PATH` | Exit 2 and no terminal manifest |
| REM-29 | CLI integration | Stdout fails after terminal commit | Exit 3; manifest exists and is consumable; stderr identifies committed presentation failure |

Fault-injection storage tests MUST use protocol-observable behavior: recorded requests, persisted object/upload state, and final manifests. They MUST NOT pass merely because a mock method was called.

### 12.1 Required permanent test inventory

The implementation MUST add or extend these named tests. Equivalent table-driven
subtests MAY be combined, but the test output MUST retain the listed REM IDs so a
failure identifies the violated contract.

| Test file | Required test or subtest | Acceptance IDs |
|---|---|---|
| `internal/config/config_test.go` | `TestContextSubstitutionIsBounded`, `TestCanonicalTypeAliases`, `TestPortableDateDirectivesParse`, `TestCanonicalLocationOverlap`, `TestInferInputTypeUsesDecodedS3Key`, `TestDatasetToleranceApplicability` | REM-01, REM-06, REM-07, REM-12, REM-13, REM-19 |
| `internal/config/load_unix_test.go` | `TestLoadRejectsFIFOAndObservesCancellation` | REM-02 |
| `internal/config/load_windows_test.go` | `TestLoadRejectsNamedPipeAndDevice` | REM-02 |
| `internal/cli/cli_test.go` | `TestArgumentFailureHonorsJSONLogFormat`, `TestJSONOutputFailurePreventsCommit`, `TestPostCommitPresentationFailureHasDistinctExit` | REM-03, REM-28, REM-29 |
| `internal/source/convert_test.go` | `TestDate32UsesCivilDay`, `TestTimestampZoneAbbreviationPolicy`, `TestDatetimeAliasBuildsTimestamp` | REM-06, REM-08, REM-09 |
| `internal/source/source_test.go` | `TestCSVDecoderReservesBeforeRead`, `TestJSONDecoderReservesBeforeRead`, `TestGzipDecoderBudgetFailureIsBounded`, `TestInferencePinsCompleteLocalIdentity` | REM-04, REM-05, REM-14 |
| `internal/source/parquet_test.go` | `TestParquetTemporalValuesAvoidTextRoundTrip`, `TestParquetPlanReadsMetadataOnly` | REM-10, REM-15 |
| `internal/source/parquet_native_test.go` | `TestNativeParquetCountsIgnoredColumnsAgainstRecordLimit` | REM-17 |
| `internal/checks/compile_test.go` | `TestAcceptedAliasesCompile`, `TestAcceptedDateDirectivesCompile`, `TestDescribeResolvesKnownWarningGates`, `TestDateFormatCompilationIsBudgeted` | REM-06, REM-07, REM-20, REM-21 |
| `internal/checks/rows_test.go` | `TestBinaryReferenceUsesBase64CanonicalText`, `TestIssueColumnUsesResolvedFailingOperand` | REM-11, REM-18 |
| `internal/checks/aggregate_test.go` | `TestFloatingEqualityAppliesTolerance`, `TestExactMetricsRejectTolerance` | REM-19 |
| `internal/storage/local_test.go` | `TestLocalPublicationSyncOrder`, `TestLocalPublicationSyncFailurePreventsCommit`, `TestMetadataRangeCloseDoesNotDigestWholeFile` | REM-15, REM-24 |
| `internal/storage/upload_test.go` | `TestConditionalPutReconcilesLostSuccess`, `TestAmbiguousMultipartCreateAbortsEveryUpload`, `TestMultipartCompleteReconcilesLostSuccess` | REM-25, REM-26, REM-27 |
| `internal/sink/writer_test.go` | `TestCompleteEmitsEverySchemaIssueExactlyOnce`, `TestCompletionAllocationIncludedInManagedPeak`, `TestManifestFollowsDurableMetadata` | REM-22, REM-23, REM-24 |
| `internal/app/app_test.go` | `TestSummaryCountsEveryPhysicalPass`, `TestChangedInputCannotPublishManifest` | REM-14, REM-16 |
| `integration/decoder_limits_test.go` | `TestCompressedRecordBudgetFailsWithoutOOM` | REM-04, REM-05 |
| `integration/parquet_test.go` | `TestPlanDoesNotScanParquetData`, `TestParquetPassAccounting`, `TestParquetIgnoredFieldRecordLimit` | REM-15, REM-16, REM-17 |
| `integration/io_test.go` | `TestSameMetadataReplayMutationFails`, `TestDurabilityFailureLeavesNoManifest` | REM-14, REM-24 |
| `integration/s3_test.go` | `TestS3LostPutResponseReconciles`, `TestS3AmbiguousMultipartHasNoOrphans`, `TestS3LostCompleteResponseReconciles` | REM-25, REM-26, REM-27 |
| `integration/cli_test.go` | `TestJSONArgumentDiagnostic`, `TestJSONOutputFailureLeavesNoManifest`, `TestCommittedPresentationFailureExit` | REM-03, REM-28, REM-29 |

Unit tests for REM-04 and REM-05 MUST prove reservation precedes delivery to
the decoder, while the subprocess integration test proves the consumer-visible
failure mode and bounded process behavior. Storage unit tests MAY use injected
filesystem/S3 faults, but integration tests MUST inspect the resulting
objects, multipart-upload inventory, manifest visibility, and exit status.

Every REM ID MUST appear in at least one required permanent test. REM-02,
REM-12, REM-24, and REM-29 are platform-sensitive and MUST run on every
applicable release runner rather than being treated as compile-only coverage.


## 13. Verification gates

The completed change MUST pass:

```bash
go test ./...
go test -race ./internal/config ./internal/source ./internal/checks ./internal/storage ./internal/sink ./internal/app ./internal/cli
go test -tags=integration ./integration -timeout 20m
```

Additionally:

- MinIO fault-proxy tests MUST exercise REM-25 through REM-27.
- Linux amd64/arm64 and Windows amd64 workflows MUST run the local/CLI cases relevant to each platform.
- A subprocess memory test MUST exercise compressed CSV and JSON with `max_record_bytes` larger than the managed budget and demonstrate a controlled resource exit rather than OOM termination.
- Documentation MUST update the main specification, README exit-code text, architecture publication notes, and changelog to match implemented behavior.

No release claim may mark external AWS reconciliation verified unless the named AWS integration job ran; unavailable credentials produce a skip, not a pass.

## 14. Implementation order

Dependencies require this order:

1. shared canonical type/date/location helpers and contextual `config.Load`;
2. decoder and compiled-state accounting;
3. source temporal, Parquet, and replay corrections;
4. check semantics and schema-issue identity;
5. local durability and S3 reconciliation;
6. sink completion accounting and CLI commit/presentation behavior;
7. integration/fault tests and documentation synchronization.

A phase may merge only when its package-level regressions pass. The complete remediation is not releasable until every Section 12 acceptance case and Section 13 gate is satisfied.
