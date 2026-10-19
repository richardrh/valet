# Validation modes, schema diffs, diagnostics and Parquet statistics

Status: **implemented report-version-2 contract**. The accompanying real-CLI
acceptance tests are in `integration/vm_*_test.go`; execution evidence and
environment limitations are recorded in the companion E2E document.

The acceptance gate is the companion [black-box E2E specification](validation-modes-e2e.md).
Package/unit tests may supplement it, but do not satisfy its acceptance cases.
The [current implementation contract](spec.md) continues to govern behavior not
explicitly changed here.

## 1. Scope and decisions

Implement four user-visible features:

1. Full, failures-only and report-only row materialization.
2. Readable schema differences without losing the canonical JSON report.
3. Structured performance diagnostics on stderr, separate from result stdout.
4. Explicitly trusted Parquet-statistics shortcuts, with value-scan fallback,
   evidence and honest coverage accounting.

A companion bounded-statistics writer option makes suitable output reusable by
other tools. It does not enable statistics trust when reading that output.

Non-goals: a server/UI, checkpoint/resume, approximate validation, arbitrary
predicate pushdown, automatic trust based on `created_by`, signing/key management,
new rule semantics, or a guarantee of sub-10-second validation. No default input
integrity or value-validation guarantee may be weakened for performance.

## 2. Interfaces

```yaml
input:
  type: parquet
  parquet:
    statistics: off             # off | trusted; default off

output:
  uri: results/
  mode: all                     # all | failures | report; default all
  format: parquet
  parquet:
    statistics: none            # none | bounded; default none
```

```bash
valet validate -p pipeline.yml --output-mode failures
valet validate -p pipeline.yml --output-mode report --parquet-stats trusted
valet schema -p pipeline.yml --format text
valet schema -p pipeline.yml --format json
valet validate -p pipeline.yml --log-format json --log-level debug
```

CLI overrides pipeline configuration, which overrides defaults. Enum values are
case-sensitive. Explicit empty/unknown values fail with exit 2; they do not fall
back to defaults. Existing size, worker and input/output-overlap validation stays
in force in every output mode. Report-only still publishes a result destination;
it is not a stdout-only alternative to `schema`.

- `--output-mode` and `--parquet-stats` apply to `validate`. `plan` also accepts
  them and describes the selected materialization/trust policy without executing
  row validation, accepting statistics as outcomes, or publishing artifacts.
- `--format json|text` applies only to `schema`, defaults to JSON, and controls
  presentation rather than validation. No colors/ANSI sequences are emitted.
- Reject explicitly supplied command-inapplicable flags instead of silently
  ignoring them. In particular, `schema --parquet-stats`, `schema --output-mode`,
  `schema --plain`, and `schema --json-output` fail with exit 2.
- Shared pipeline output/statistics settings may be present when invoking
  `schema`; they do not cause row evaluation, output initialization or publishing.
  The report remains schema-scoped and discloses unevaluated value checks.
- Explicit `trusted` reading requires Parquet input. `bounded` writing requires
  Parquet row-output format. Neither creates a reader trust decision implicitly.
- Help/version require no input, reference, credential or output access.

## 3. Versioned result contract and invariants

### 3.1 Version boundary

The implementation of these features advances top-level summary, manifest,
schema and plan JSON to `report_version: 2`. All consumers, docs and acceptance
fixtures must migrate together; do not conditionally emit version 1 for default
mode or keep a second legacy behavior path. This is necessary because omitted
row artifacts and metadata-assisted coverage break version-1 assumptions.
Individual issue/input/rule records retain their documented semantics.

Data reports have `scope: data`; schema reports retain `scope: schema` and
`validation: not_evaluated`; plans have `scope: plan` and are not validation
results. Schema/plan runs gain a correlation `run_id` but never a normal result
manifest. Presentation format cannot change scope, outcome or exit code.

### 3.2 Counts versus evidence

In every completed data report:

- Existing `total`, `pass`, `warn` and `error` mean unique rows actually decoded
  and evaluated in the final validation pass. `total = pass + warn + error`.
- Prepass/replay work must not inflate those counts. `rows_scanned` counts
  decoded row visits across inference/prepasses/final evaluation, not unique
  coverage, footer claims or raw-byte hash passes.
- `verification` is `values` if no rows were accepted solely from statistics,
  `metadata_assisted` if any were, and `not_evaluated` for schema rejection
  before row/statistics evaluation.
- `trust_policy` records `none` or `producer_statistics`, including when an
  explicitly selected trusted policy eventually falls back to value scans.
- Add `coverage.value_rows` (equal to `total`), `coverage.metadata_claimed_rows`,
  and `coverage.accounted_rows` (their sum). Metadata counts are explicitly
  footer-declared, not a second name for checked rows.
- Add `rule_coverage` for each row-outcome-producing rule, separating final-pass
  `value_rows` from `metadata_claimed_rows`. `value_rows` counts rows presented
  to the rule, including not-applicable and unevaluable verdicts, not just
  successful operand evaluations. Existing mutually exclusive per-rule
  not-applicable/unevaluable row counters stay value-derived. Thus evaluated
  rule rows equal value_rows minus those two counters; a failing rule verdict
  follows existing OR precedence even if another check was unevaluable.
  Metadata-accepted passes do not invent failure counts.
- Add `coverage.row_groups_scanned`, `coverage.row_groups_statistics_only` and
  `coverage.row_groups_total` for Parquet. On successful complete processing the
  first two partition the total; a normal-path empty group counts as scanned
  even if it yields zero rows. Non-Parquet reports use null for these fields.
- Add `ingestion_verification: full|partial|not_evaluated`. Skipped payloads have
  not undergone value/encoding/record-size guards; accepting their statistics
  does not mark those guarantees verified. Full/partial refers to selected rows,
  not whether optional statistics were present.
- Record per-input identity method and payload coverage in the input index.
  Never put an ETag, footer hash or stat identity in a content-SHA-256 field.

For an entirely metadata-accepted file, `total` and `coverage.value_rows` are
zero, while `coverage.metadata_claimed_rows` may be nonzero. Human/plain output
must show both populations and the trust policy, not say "all rows checked".

All counter and provenance-prefix additions must be overflow-checked, including
cross-file metadata totals and per-rule coverage. Overflow is exit 2 with no
terminal manifest, never wrapping, saturation or conversion through Float64.

If decoded rows follow skipped groups/files, their source/global row numbers
include footer-declared prefix lengths. Preserve those logical ordering numbers
but label the report/manifest `provenance_position_basis: metadata_prefix`;
otherwise use `values`. Parquet row issues additionally include zero-based
`row_group`, one-based `row_in_group` and `position_basis`. These observed
group-local coordinates distinguish decoded locations from unverified prefix
lengths. Do not call the resulting absolute positions entirely value-verified.


`validation: pass|fail` remains the outcome under the selected policy. Under
`metadata_assisted`, pass is explicitly conditional on accepted producer claims.
Any normal row/schema/dataset error still causes validation failure. No fabricated
row issues, rule-failure counts or individual locations may be derived from a
row-group statistic. Metadata shortcuts in this contract accept passes only;
non-pass claims are scanned to establish real failures.

### 3.3 Publication

All data modes publish inputs, rules, summary, an artifact index, applicable
issues/evidence, and one manifest last. Exit 0 or 1 requires a committed manifest;
exit 2 or pre-commit signal interruption must not leave a consumable manifest.
Exit 3 still means manifest committed but presentation failed. Incomplete parts
are never a completed result. Existing no-replace, sync, S3 identity, multipart
cleanup, digest and cancellation guarantees remain in force.

The manifest records `output_mode`, `verification`, `trust_policy`, `coverage`
and explicit `row_output` policies/emitted counts for pass, warn, error and raw
reject records. The summary and manifest must agree. Artifact references retain
URI, format, record count, byte size and SHA-256. No omitted bucket gets a fake
empty file or reference.

## 4. Output modes

| Mode | Pass rows | Warning rows | Error rows | Raw rejects | Issues and reports |
|---|---|---|---|---|---|
| `all` | Emit | Emit | Emit | Emit | Emit |
| `failures` | Omit by policy | Emit | Emit | Emit | Emit |
| `report` | Omit by policy | Omit by policy | Omit by policy | Omit by policy | Emit |

Warnings are included in failures-only output. The name does not mean
error-level rows only. Preserve bucket precedence and all distinct rule issues
on error rows, including their warning issues.

Changing mode alone must not change parsing, conversion, rule/dataset execution,
row outcomes, issue ordering, failure counts, unevaluable/not-applicable counts,
exit status or source provenance. With statistics off, those results must match
`all` exactly. Dataset-only failures and schema gates still publish their
ordinary structured diagnostics even when no row bucket can represent them.

- Omitted rows must not enter a row encoder, open a row part/upload, construct
  provenance arrays for writing, or appear as complete rows in another report.
- Report-only omits original-record rejects but retains ingestion issues/counts
  and declares the number of raw reject records omitted by policy.
- Empty buckets may omit files; policy must distinguish an empty evaluated
  bucket from a nonempty bucket whose records were intentionally not written.
- `sum(artifact.count for bucket)` equals its emitted count, not its evaluated
  count when mode/policy omits rows. Issues/evidence use record counts, not rows.
- Rows emitted within each bucket retain input order, values and provenance
  across workers, files and part boundaries.
- Report-only is not anonymization: reports still contain schema/column names,
  input identities, rule configuration and potentially sensitive custom messages.
  Existing value-redaction requirements for ordinary issues/logs remain.
- No new row-limit or fail-fast behavior is implied. A completed report means
  the selected policy processed the entire dataset, not just its first failure.

## 5. Readable schema differences

`schema --format json` remains the machine default. `--format text` presents the
same canonical result, without changing checks or initiating data reads.

Version-2 JSON adds `differences`, with one item per policy-relevant mismatch:
source ordinal/URI, renamed column, original source name where different,
`kind` (`missing`, `unexpected`, `type_mismatch`), expected and actual typed
schema attributes, severity, rule code if applicable, and `expectation_source`
(`input_schema`, `rule`, `baseline_object`). Absent fields/types use null rather
than a fabricated type. Existing `schema_issues`, `checks`, `not_evaluated`,
objects/fields and compatibility remain available.

Text output must:

1. Start with compatible/incompatible **schema**, never data-validation success.
2. Group differences by source in source order; sort within a group by column,
   difference kind and rule declaration order for stable output.
3. Show missing/extra columns and expected versus actual type attributes,
   including integer width/signedness, decimal precision/scale and timestamp
   unit/timezone where relevant. Canonical aliases must not become differences.
4. Identify when the first object supplies the baseline, not pretend a user
   declared that schema. Allowed extra columns are not failures.
5. Preserve renamed/original names and warning severity without turning a
   warning-only mismatch into exit 1.
6. State zero checked rows and list skipped value/castability checks. Required
   column presence is not proof of non-null values.
7. Quote/escape control characters in user-supplied names and redact credentials
   in source identities. Never emit terminal control sequences from data.

Operational failures still use stderr and exit 2. No output client, reference
load, row decoder, content hash or data manifest may be introduced by formatting.
A text success and JSON success have the same exit; likewise for mismatches.

## 6. Structured performance diagnostics

### 6.1 Transport and envelope

JSON logging is NDJSON on stderr only. Stdout remains exactly the selected
summary/schema/plan presentation. Performance data must not be interleaved into
that result or appended to it. Existing summary outcome/coverage counters remain;
new detailed phase timing is log data, not another summary payload.

Each program-owned JSON log record carries `log_version: 1`, `event`, `level`,
`time`, and a readable `msg`. Once a run is allocated, include `run_id`, `command`
and a strictly increasing run-local `seq`. Serialize concurrent emissions; no
fragmented/interleaved JSON. Early CLI errors may omit run fields. Route SDK
logging through the same redacted adapter or suppress its unstructured notices.

At info/debug levels with writable stderr, one terminal `run.finished` record
appears after joins, cleanup/commit determination and presentation. It includes
`exit_code`, `scope`,
`manifest_committed`, outcome, output mode/trust policy where applicable,
`elapsed_ns`, `managed_peak_bytes`, source metadata/payload byte counters,
logical output-byte counters, value/statistics coverage and `counters_complete`.
Pre-commit failure/cancellation has incomplete counters and no committed result.
Exit 3 after a completed validation retains complete validation counters and
`manifest_committed: true`, with a distinct presentation-failed outcome.
Schema/plan success has `manifest_committed: false` by design.
Log-level filtering remains effective: successful info records need not appear
at warn/error levels; a terminal operational failure is error-level.

`source_metadata_bytes_read` and `source_payload_bytes_read` count bytes obtained
from the source backend, including repeated/hash reads and consumed retry bytes,
not cache hits or HTTP headers. For Parquet, magic/footer bytes are metadata and
page/chunk bytes are payload; non-Parquet input bytes are payload. Metadata-only
claims cannot conceal raw digest reads in another category. Proxy counter
comparisons use successful fully consumed requests; cancellation need not equal
bytes the server attempted to send.

`output_bytes_written` counts logical bytes accepted by storage writers,
including ultimately aborted parts, not retransmitted wire bytes.
`result_bytes_committed` is zero without a terminal manifest; after commit it is
the byte size of the manifest plus its owned referenced artifacts, counted once.
These counters do not count source objects merely mentioned in input indexes.


### 6.2 Phases and decisions

Emit aggregated `run.phase` records for work actually performed: discover,
metadata, reference, prepass, input_hash, decode, evaluate, encode, publish and
present. Each has phase, status, monotonic `work_ns`, applicable row/byte counts,
and separately accounted queue `wait_ns` where relevant. Work includes I/O wait
inside the operation; excludes queue admission wait. With concurrency, accumulated
worker times and nested phase spans may exceed wall time. They are not CPU time
and must not be presented or tested as additive wall-time percentages.

- Info: bounded phase/run summaries and `parquet.stats_summary` counts/reasons.
- Debug: `parquet.stats_decision` with source/group/check identity, claim
  `pass|fail|unknown`, action `skip|scan`, evidence-field names and reason code.
- Warn: once per run when statistics actually replace value checks, emit
  `validation.trust` explaining producer trust and unverified payload coverage.
- Never log raw cells, min/max values, rule literal values, credentials, secrets
  or complete input records by default. Decision types/field presence/coverage
  are enough for logs; persisted evidence preserves the audit basis separately.
- Stream events; do not retain one record per row/group in memory. Diagnostic
  accounting is budgeted O(phases + configured reasons), not O(input size).
- Logging failures alone are best-effort and must not abort a valid run or
  become exit 3. Broken stderr must not terminate the process via SIGPIPE.
  A failed stdout result write after commit still yields exit 3, not SIGPIPE.

No wall-time target is asserted by the logging feature. It must not add extra
payload reads or duplicate decoding just to compute diagnostic counters.

## 7. Trusted Parquet-statistics execution

### 7.1 Explicit trust and threat model

`off` is the default and continues value/guard execution with existing local
baseline/replay hashes and S3 pinning. It must not accept a value rule from
statistics. `trusted` means the operator accepts suitable producer statistics as
claims for eligible groups; it is not authentication or proof of an intact file.
Never infer trust from writer names, extensions, existing ETags or self-generated
checksums. There is no trusted publisher registry in this feature.

A stale or malicious but plausible footer can cause a conditional pass despite
bad values. This is an intentional, documented consequence of the explicit
policy, not a tamper-detection claim. CRCs are not authentication. A trusted
signature/hash authenticates bytes/provenance, not the correctness of statistics.
Verifying a trusted full-content hash requires reading the content.

A file with skipped groups cannot also claim full payload guards or a completed
full-content digest unless those bytes really were read. For selectively read
local files use pinned stat identity and disclose the weaker coverage. Revalidate
selectively read local identities before commit. Retain S3 version/ETag checks
on every request and perform a final conditional/version-specific metadata
identity check before publishing a selectively read result. A pinned immutable
old version remains valid if a newer version becomes current; never mix versions.
If no groups are eligible for skipping, use the ordinary complete-value path
and its normal integrity guarantees. Schema inspection stays schema-only even
when the pipeline selects this policy.

### 7.2 Eligible evidence

Only guarded footer column-chunk statistics are used initially, not page indexes,
Bloom filters or dictionary guesses. Check statistics against the exact pinned
object/schema/group/column. Presence must be tested explicitly: generated getters
returning zero for absent `null_count`/`nan_count` are not evidence.

Supported proofs:

| Check | Sufficient pass evidence, under trust |
|---|---|
| `not_null` | Explicit valid `null_count = 0` for every selected scalar column |
| `valid_range` | Every selected numeric column's safe lower/upper bounds satisfy all configured inclusive/exclusive bounds, with normal null semantics |
| Declared `nullable: false` | Explicit zero null count; otherwise scan for real ingestion diagnostics |

All operands of a multi-column assertion require proof; proving its first column
is insufficient. Severity does not change proof eligibility: a warning-level
failure must be scanned and materialized according to the output mode.
Already-resolved passing strict metadata schema gates do not themselves force
value reads. A failing warning-level schema rule needs actual rows to preserve
its row outcomes; castability/prepass-dependent gates still require value work.

Numeric range shortcuts require an explicit compatible input type, no conversion
or lexical/null-sentinel transformation, and the same exact predicate semantics
as decoded evaluation. Do not upgrade legacy Float64 coercion into different
exact semantics. Pure unambiguous column renames are permitted.

Support signed/unsigned integers and decimals without Float64 intermediaries.
For floating groups with non-null values, require explicit `nan_count = 0`,
finite min/max, a supported sort order and the same signed-zero comparison as
the value checker. Otherwise scan. The explicit exception is a truthful
all-null group (`null_count = declared_rows > 0`): it may pass `valid_range`
when nulls are allowed and no independent null constraint fails. There are no
non-null values to range-check, so no min/max or NaN count is required for that
case. Empty groups still take the ordinary path.

Prefer modern `min_value`/`max_value` with a supported type-defined order. Bounds
need not be exact observed extrema to establish a conservative pass; they must
be valid lower/upper bounds with the correct ordering. Do not infer failure from
an inexact bound crossing a threshold. Legacy-only/unknown-order evidence falls
back. Exactness flags never repair missing evidence or unsupported ordering.

Missing, malformed optional statistics, impossible null/NaN counts, reversed
bounds, invalid bound widths or unsupported encodings mean unknown and a value
scan. Structural footer errors, overflow, unsupported schema and allocation-limit
violations remain operational errors; the trust flag does not bypass guards.

Conditions, reference/uniqueness checks, statistics/aggregates/dataset assertions,
string predicates, arbitrary custom checks or conversions require value scans
initially. If any applicable check needs values, scan the entire group and run
all its normal checks; do not let a candidate metadata pass hide a value failure
once those values are decoded. All columns still receive ordinary guards on the
value path, including columns omitted from output.

### 7.3 Whole-group decision and materialization

Skip a positive-row-count group only when every configured applicable row check
has a supported pass proof, required schema/null policies permit it, no global
pass requires its values, and no output mode needs its passing rows. Unsupported
semantic ingestion/content checks remain explicitly not verified for skipped
payloads. The resource/encoding guards continue on any data actually decoded.

- `all` requires passing-row output, so it forces ordinary reads/evaluation.
- `failures` and `report` can skip fully proven-pass groups.
- Fail/unknown claims, absent statistics, empty groups and unsupported checks
  take the ordinary path. Positive null counts/range failures are scanned so
  exact issues, counts, severity and locations come from values.
- A skipped group is processed once: no row IDs, issues or reject records are
  fabricated. Footer row counts enter only metadata-claimed coverage.
- Do not implement count inference for global assertions in this feature;
  a configured global/prepass-dependent check forces full value processing.

Selective range caches/read-ahead must not fetch skipped payload. Metadata reads
may touch only the magic/header and actual footer/required structural ranges;
no full-file digest or whole-object GET may be disguised as metadata access.
Decoded-group ranges must be clipped so coalescing cannot cross into skipped
chunks. These requirements are tested by a real S3 forwarding proxy that denies
those bytes, not by trusting internal counters.

### 7.4 Persisted audit evidence

For `trusted` runs, stream a manifest-referenced `evidence/part-*.ndjson` ledger
with one final decision per selected row group in source/group order. It records
pinned source identity, group ordinal, footer-declared rows, footer SHA-256
(explicitly not a full-file hash), action/reason, and configured check identifiers
with evidence-field names/claim. Skipped groups disclose unverified ingestion
and payload coverage. The ledger need not contain raw min/max values; rechecking
those values requires the pinned source/footer. It is an audit trail, not a
standalone cryptographic proof.
The footer digest is lowercase hex SHA-256 over the serialized footer metadata
bytes only, excluding the final four-byte length and four-byte magic trailer.
The footer range/length is recorded so an independent reader can reproduce it.

All skipped groups require complete check-proof coverage; scanned groups record
the reason for fallback. Evidence record counts are records, not logical rows.
The ledger is budgeted, streamed, hashed and published before the terminal
manifest like other artifacts. It remains available when info/debug logs are
suppressed. No ledger is required when policy is `off`.

## 8. Bounded statistics in emitted Parquet

`output.parquet.statistics: bounded` emits per-column min/max/null counts only
for supported fixed-width boolean/integer/decimal columns under explicit group,
encoder and footer charges. Variable-width string/binary min/max remain disabled;
no unbounded extreme value may be retained. Float statistics remain disabled
until the writer can emit correct NaN counts and has its own E2E coverage.
`none` preserves today's no-column-statistics policy. Row counts are structural
metadata and are unaffected by this choice.

Verify statistics using an independent public Parquet metadata reader and known
fixture values. Re-reading one's own output still defaults to `off`; writer
configuration must never grant reader trust. Report mode must not initialize
an encoder merely because this writer option is configured.

## 9. Implementation and release gates

Recommended dependency order: versioned reports/materialization; canonical schema
differences and text presentation; diagnostic envelope/counters; bounded writer
statistics; selective planning/evidence; trusted short-circuit execution.

Every stage preserves the default value path. Migrate current acceptance consumers
and docs cleanly to version 2. Do not mark proposed tests as passing, add skipped
placeholder tests, or gate on units while the CLI cases are absent. Required E2E
runs and evidence are defined in the companion document. A release requires the
mandatory local/MinIO suites, race-instrumented CLI subset, supported native gates,
production documentation examples and cleanup checks. AWS and generated-scale
runs must be reported as exercised or explicitly not run, never inferred.
