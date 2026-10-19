# E2E acceptance: validation modes and trusted metadata

Status: **implemented black-box acceptance contract**.
The cases run through `integration/vm_*_test.go` against `VALET_BINARY`.
On 2026-10-19, Linux amd64 release and race-instrumented CLI gates each passed
all 62 case IDs (318 test nodes, zero skips) against real local MinIO, including
owned versioned buckets. Native arm64/Windows, real AWS and optional generated
large-scale gates are separate environments and are not implied by that result.

## 1. What counts as E2E

Every acceptance case launches the actual `valet` executable with pipeline files,
real input files/objects, OS pipes/signals and actual output destinations. Observe
exit status, stdout/stderr, committed artifacts, independently decoded values,
backend requests and cleanup. Do not call CLI/engine/source/sink functions in the
test process to exercise the feature. Importing public Arrow/Parquet APIs for
fixture production and independent reading is allowed; importing Valet internals
as an oracle is not.

Reuse `integration/` and `VALET_BINARY` from its current harness. Add named
`TestVM...` functions/subtests carrying the IDs below. Update the acceptance
catalogue only when those tests exist; do not register nonexistent tests or
install skipped placeholders. Units are optional supplements, not acceptance.

Assertions must target behavior, boundaries and actual effects. Do not snapshot
all human wording, inspect source text, test copied constants or treat a mocked
forwarding call/log echo as proof of I/O avoidance. Numeric/time/run-ID fields
must be parsed, not pinned to incidental output. A green command with "no tests
to run" is not a passing feature gate.

## 2. Shared black-box infrastructure

### 2.1 CLI and artifact oracle

- Run the supplied release artifact, with a per-command deadline and a margin
  before the outer test deadline. A harness timeout is failure, not exit 2/130.
- Decode stdout as exactly one JSON value when JSON is selected; reject trailing
  log records. Parse every stderr NDJSON record independently in JSON-log cases.
- Discover the terminal manifest first and follow its references. Do not accept
  orphan files as a completed result or assume a particular part count/filename.
- Verify every referenced byte size and SHA-256. Independently decode each row
  artifact and check literal expected values, validity, types, order and source
  provenance. Artifact counts must equal decoded records; issue/evidence counts
  must equal ledger records, not represented rows.
- Check summary/manifest agreement, output policies, coverage populations and
  report version. Only run IDs, timestamps, durations and run-root portions of
  artifact URIs may be normalized for logical comparisons.
- Verify all omitted artifacts are actually absent, not just absent from an
  index. For S3, list the isolated run prefix and observe attempted writes too.
- An independent public Parquet reader inspects footer statistics and decodes
  fixture/output data. Expectations come from literal fixture descriptors, not
  by copying the CLI's own summary into an expected object.

### 2.2 Real storage and controlled faults

Local files and real MinIO are mandatory. A forwarding HTTP proxy may pause,
count, deny or alter responses around real MinIO; it must not emulate Valet's
result or replace the object store with a canned success response. Existing
`integration/s3_test.go` provides the environment/proxy conventions.

The proxy must track HEAD, Range GET, unconditional GET, conditional/version
headers, upload initiation/parts/completion/abort, and published object paths.
Define allowable byte intervals from an independently inspected fixture:
header/magic, footer and selected decoded column chunks. For metadata-only or
skipped groups, reject payload-intersecting GETs, including read-ahead/tail-cache
overreads and unconditional whole-object GETs. A 403 in that experiment must
fail if the implementation secretly scans; a correct no-read path must succeed.
The evidence ledger's claimed zero decoded/read bytes is not sufficient alone.

Use barriers/channels at known HTTP operations, not arbitrary sleeps, to trigger
mutation, cancellation, backpressure and late publication failures. Observe the
actual child process and backend state after release/termination. Local mutation
can be synchronized by placing a later remote input/reference behind a proxy
barrier, so an earlier local input is already pinned before it is changed.

Use unique owned local directories/S3 prefixes; create an isolated versioned
bucket only with explicit test permission. Never enable versioning on a shared
user bucket. Every success/failure/timeout path removes its generated data,
versions and outstanding multipart uploads. Cleanup failure fails the test.

### 2.3 Fixtures

Generate fixtures during the run; no giant checked-in files. Preserve descriptors
with literal rows, schemas, null locations, source/group positions, footer and
payload intervals, expected statistics and independent decoded results.

**F-OUT — mixed outcomes, eight logical rows across two files.** Explicit schema:
`id: string`, `amount: int64`, `status: string`; row-error conversion policy.
`POSITIVE`: error, amount `> 0`. `STATUS_OK`: warn, status equals `ok`.

| Source/data row | ID | Original amount | Status | Outcome/diagnostics |
|---|---|---|---|---|
| A/1 | A | `10` | ok | pass |
| A/2 | B | `-1` | ok | error: POSITIVE |
| A/3 | C | `5` | pending | warn: STATUS_OK |
| A/4 | D | `-2` | pending | error: POSITIVE and STATUS_OK |
| B/1 | E | `0` | ok | error: POSITIVE |
| B/2 | F | `oops` | ok | error: INGEST_TYPE; POSITIVE unevaluable; one raw reject |
| B/3 | G | `7` | ok | pass |
| B/4 | H | `2` | pending | warn: STATUS_OK |

Expected decoded counts: total 8, pass 2, warn 2, error 4; seven ordinary issues
(POSITIVE 3, STATUS_OK 3, INGEST_TYPE 1), POSITIVE unevaluable 1, one reject.
Converted output amount for F is null, not the fabricated number zero or the
original string. Raw reject output, where enabled, retains `oops` and its state.
Provenance source rows restart at B; global rows remain 1–8.

For each row-outcome-producing rule, `rule_coverage.value_rows` counts final-pass
rows presented to that rule, including rows whose condition is not applicable
or whose verdict is unevaluable. It is not a success count. For F-OUT both rules
have value_rows 8 and metadata_claimed_rows 0; POSITIVE has not_applicable 0,
unevaluable 1 and seven evaluated rule rows; STATUS_OK has eight evaluated rule
rows and zero excluded/unevaluable rows. Counts are not multiplied by checks.

Generate equivalent CSV/NDJSON/JSON-array fixtures. The Parquet variant uses a
string amount column so the explicit conversion and bad lexeme are genuinely
exercised; it is not a trusted-statistics shortcut fixture.

**F-CLEAN/F-EMPTY.** Two clean rows for dataset-only failures; header/schema-only
zero-row inputs; all-warning and all-error small variants for empty-bucket cases.

**F-STAT — one nullable int64 amount column, four groups of four rows.** Explicit
compatible input type; rules POSITIVE (`> 0`, null passes) and NOT_NULL (error).

| Group, zero-based | Literal values | Footer statistics | Correct action under trusted report/failures |
|---|---|---|---|
| 0 | 1, 2, 3, 4 | min 1, max 4, null_count 0 | metadata-pass/skip |
| 1 | 5, null, 6, 7 | min 5, max 7, null_count 1 | scan: establish the null violation |
| 2 | -1, 0, 8, 9 | min -1, max 9, null_count 0 | scan: establish both range violations |
| 3 | 10, 11, 12, 13 | optional statistics absent | scan: cannot prove |

With stats off/all: total 16, pass 13, error 3, NOT_NULL 1, POSITIVE 2.
With trusted report/failures: decoded total 12, pass 9, error 3; metadata-claimed
rows 4; accounted rows 16; one skipped and three scanned groups. Errors occur
at logical source/global rows 6, 9 and 10. Those positions after group 0 have a
metadata-derived prefix and must be labelled accordingly (see contract).

Both F-STAT rules have `rule_coverage.value_rows = 12` and
`metadata_claimed_rows = 4`; both have zero not-applicable/unevaluable verdicts.

**F-FAST.** Two four-row int64 groups containing 1–8; truthful supported bounds
and explicit zero null counts. It must succeed with the S3 proxy denying every
payload range. Trusted report coverage: zero value rows, eight metadata-claimed
rows, two statistics-only groups; no row issues or fabricated pass-row records.

**F-SCHEMA.** A family of independently described files covering missing/extra
columns, logical type/precision/scale/unit/timezone differences, canonical aliases,
renames, warning-level gates and first-object baselines. Include payload markers
that must never appear in schema output/logs and corrupt-payload variants with
unchanged valid footers.

**F-LIE.** Payload `[null, -1, 11, 12]`; rewrite only its footer to claim min 10,
max 20, null_count 0. Keep schema, row/chunk counts, offsets and encoding valid.
Assert the payload prefix is byte-identical before/after footer rewriting, and
independently decode it to establish the two real bad rows. Also create a
corrupt-payload/valid-footer variant. Do not use a fixture whose corruption
accidentally makes footer parsing fail before the trust decision.

Public Parquet metadata/Thrift interfaces may rewrite optional footer fields;
no production-only fault flags or test branches may be added to Valet. Include
Snappy/Zstd/uncompressed, V1/V2 and dictionary variations where indicated.

## 3. Output-mode acceptance

| ID | Scenario | Required observable result |
|---|---|---|
| VM-OUT-01 | F-OUT, default/full output, CSV and Parquet destinations | Exit 1; exact eight converted rows, bucket order/provenance, seven issues and one raw reject; version-2 committed references/hash/counts correct. Existing default behavior is preserved apart from versioned additions. |
| VM-OUT-02 | F-OUT, failures-only | Exit 1; only C/H warning rows and B/D/E/F error rows, in their respective source order; no pass records/uploads; all seven issues and the raw reject remain; evaluated counts unchanged. |
| VM-OUT-03 | F-OUT, report-only | Exit 1; no pass/warn/error or raw reject records anywhere; all seven redacted issues, counts and one explicitly omitted reject; committed manifest declares omission rather than empty buckets. |
| VM-OUT-04 | Equivalent four input formats, all output modes | Same literal outcomes, invalid-state/unevaluable counts and source positions as F-OUT; no input-format shortcut changes conversion. Compare independently decoded logical results, not Parquet byte layout. |
| VM-OUT-05 | F-CLEAN plus an intentionally failing dataset row-count assertion | Exit 1 in every mode despite zero row errors; dataset result/issue present; failures/report do not invent an error row to represent the failure. |
| VM-OUT-06 | Missing required/schema-gated column | Completed schema rejection, exit 1 and manifest; zero value rows and no bucket artifacts in every mode; schema diagnostics retained. |
| VM-OUT-07 | Duplicates spanning files/groups plus ordered reference tuples | Every duplicate occurrence, including the first, and reference failure is still found in all modes; prepasses/reference reads occur as required; counts/order match literal expected positions. |
| VM-OUT-08 | Mode CLI/config precedence, empty/unknown values and wrong command scope | Prove precedence through actual row writes or denied writes, not echoed config. Invalid explicit inputs exit 2 before source/output access; no silent defaulting. |
| VM-OUT-09 | Real MinIO proxy forbids omitted bucket/reject paths | Failures-only succeeds without any pass upload attempt; report-only succeeds without any row/reject upload attempt. The same denied required write in all mode yields exit 2 and no manifest. |
| VM-OUT-10 | Metadata/index/manifest publication denied, then stdout pipe closed after successful commit | Publication failure: exit 2, no manifest. Closed stdout: exit 3 with a valid fully referenced manifest in every mode, not SIGPIPE exit. |
| VM-OUT-11 | SIGINT/SIGTERM during blocked input and multipart output | Correct signal exit, bounded termination, no terminal manifest, no live child/upload left after cleanup; partial artifacts are never collected as results. |
| VM-OUT-12 | Empty, all-warning and all-error inputs | Correct zero/nonzero counts and exit; no placeholder empty row files; warning rows are included by failures-only; report-only remains free of raw row artifacts. |

## 4. Schema-diff acceptance

These are executable `schema` invocations, not formatter-unit snapshots.

| ID | Scenario | Required observable result |
|---|---|---|
| VM-DIFF-01 | Required missing field, disallowed extra field and type mismatch | JSON and text both exit 1; canonical differences map each source/column to correct expected/actual attributes and reason; no fabricated absent type. |
| VM-DIFF-02 | Integer signedness/width, decimal precision/scale, timestamp unit/timezone and canonical aliases | Real mismatches remain distinguishable in text and JSON. Aliases the current checker considers equivalent do not become differences. No conversion is claimed. |
| VM-DIFF-03 | Multiple objects with no declared schema | First input is identified as baseline; later differences stay attached to their actual object; explicit list/discovery order remains deterministic. |
| VM-DIFF-04 | Pure renames and ambiguous rename collision | Correct renamed/original names shown; collision exits 2 rather than producing a misleading compatible diff. |
| VM-DIFF-05 | Warning-only mismatch and allowed extra fields | Warning-only exits 0 in both presentations and retains severity. Allowed extras are not printed as failures. |
| VM-DIFF-06 | Proxy forbids data payload, reference reads and all output writes | Schema JSON/text still complete using metadata only; references/value checks are not evaluated, no clients/writes/manifest are introduced by readable output. |
| VM-DIFF-07 | Corrupt payload, valid unchanged footer | Schema may succeed with zero checked rows and explicit unverified payload; ordinary full validation rejects the corruption. Text must not say the data passed. |
| VM-DIFF-08 | Invalid format enum, non-Parquet input, malformed/oversized footer | Exit 2, stderr diagnostic, no data outputs. Invalid presentation configuration fails before storage access. |
| VM-DIFF-09 | Repeated runs, workers 1/4, Unicode and control characters in names | Same ordered canonical differences; text preserves source/type associations and escapes injected controls/ANSI. Assert semantic associations, not a whole wording snapshot. |
| VM-DIFF-10 | Broken stdout and failure reading a later object | Exit 2, never 3 because no manifest committed; no data result directory. Logs identify the command/scope and do not leak payload markers. |

## 5. Structured-diagnostic acceptance

| ID | Scenario | Required observable result |
|---|---|---|
| VM-LOG-01 | Successful value run with summary JSON stdout and JSON logs | Stdout is one version-2 result only. Every stderr record is valid NDJSON with envelope fields; one permitted terminal record agrees on run ID, exit, counts and commitment. |
| VM-LOG-02 | Debug logging with workers 4/8, tiny batches and multiple objects | Strictly increasing unique sequence values; complete noninterleaved JSON; counters agree with independent rows/artifacts/backend reads, not worker count multiplied totals. |
| VM-LOG-03 | A proxy barrier holds input or output service | Reported phase work covers the independently observed stall. Nonnegative monotonic durations and queue waits; no test requires phase sums to equal wall time or a machine-specific elapsed threshold. |
| VM-LOG-04 | Validation failure, pre-commit operational failure and post-commit stdout failure | Terminal event distinguishes exits 1/2/3 and commitment truthfully. Failure does not log successful publication or complete counters when incomplete. |
| VM-LOG-05 | Schema JSON/text and plan commands | Correct scope/run correlation; no encode/publish work invented; no normal manifest; command result remains isolated from stderr logs. |
| VM-LOG-06 | Info/debug/warn/error filtering | Filtering works without changing results. Per-group proof events are debug-only; trust warning appears when allowed and statistics are used; persisted evidence survives suppressed logs. |
| VM-LOG-07 | Sensitive row values, custom messages and synthetic credentials; successful and failed I/O | Default/debug logs do not contain raw row markers, credentials or raw min/max values. Legitimate private configuration in result artifacts is not falsely advertised as anonymized. |
| VM-LOG-08 | Broken stderr pipe | Process still completes correct validation/publication and normal result stdout; no SIGPIPE termination or conversion of logging failure into exit 3. |
| VM-LOG-09 | Input/resource/publication failure while stages are active | One terminal record when the configured level permits, correct error classification, joins complete, counters explicitly incomplete and no consumable manifest. |
| VM-LOG-10 | SIGINT/SIGTERM through a controlled I/O barrier | Correct signal exit, no hanging stage, no false completion, final cancellation diagnostics where the log level permits. |
| VM-LOG-11 | Real MinIO responses that exercise SDK diagnostics | No unstructured SDK text corrupts JSON stderr; errors remain useful without bodies/secrets; stdout/report semantics unchanged. |
| VM-LOG-12 | Many small groups with logging on/off | Same exact results and backend data-access decisions; no extra payload reads for timing. Diagnostic memory stays within the managed limit; record RSS as observation, not a false RSS cap or brittle throughput SLA. |

## 6. Statistics and trust acceptance

| ID | Scenario | Required observable result |
|---|---|---|
| VM-STAT-01 | F-FAST, trusted report and failures modes; payload GETs denied | Exit 0 and committed report, zero value rows, eight metadata-claimed rows, two skips; no payload request/full digest, no fake pass rows; complete evidence/trust policy. Exercise JSON, default human and plain output with logs suppressed. |
| VM-STAT-02 | F-STAT mixed groups | Exact 12 decoded/4 claimed/16 accounted rows and three real failures; one skipped/three scanned groups. Deny group-0 payload only; read the rest; verify per-rule coverage, issue coordinates and human/plain disclosure. |
| VM-STAT-03 | Positive bounds but absent null_count; payload contains a null | Unknown rather than implicit zero; scans reveal NOT_NULL failure; denied fallback reads give exit 2. Include a multi-column rule whose first column proves pass but second has missing/contrary evidence. |
| VM-STAT-04 | All-null group with absent bounds; null allowed versus nullable:false/NOT_NULL | Range-only/null-allowed group may be metadata-accepted. A null constraint forces decoding and exact ingestion/rule issues, not a guessed issue count or location. |
| VM-STAT-05 | Inclusive/exclusive thresholds, >0, negatives, multi-column proofs and warning-only numeric/null violations | Match literal rule semantics. Every selected column must prove pass; warning violations force scanning and warning-row materialization in failures mode. Required subcases are below. |
| VM-STAT-06 | Exact int64 above 2^53, uint64 above 2^63, signed decimals around zero/scale boundaries | No Float64 or byte-lexicographic shortcut. A provable high-precision pass succeeds with payload denied; equality/negative boundary cases scan and find exact failures. |
| VM-STAT-07 | Float NaN count absent/positive/zero, infinities, signed zero | Absent NaN count is not zero. Unsupported/non-finite cases scan; explicit-schema row-error runs retain INGEST_TYPE and dependent unevaluable outcomes. Supported finite proofs preserve signed-zero semantics. |
| VM-STAT-08 | Missing modern bounds, legacy-only bounds, unknown order, invalid widths, reversed bounds, impossible optional null/NaN counts | Treat optional evidence as unusable and scan without bypassing guards. Valid value data can still pass. Do not confuse invalid optional evidence with a structurally invalid footer. |
| VM-STAT-09 | Conditions, conversions, lexical null mappings, no explicit numeric schema, custom/global/reference/uniqueness checks | Required value/prepass/reference work still occurs. Results match exact ordinary evaluation; no trusted shortcut bypasses an obligation. A pure rename can retain eligible typed proof. |
| VM-STAT-10 | F-FAST and F-LIE with all-output materialization | Read values and emit all real rows even if a candidate proof exists; verification is values, statistics-only rows are zero. F-LIE failures are caught. |
| VM-STAT-11 | A lying positive numeric candidate plus a string predicate that requires decoding | Scan the whole group and reevaluate all checks; the false numeric claim cannot hide its actual violation just because another check caused the read. |
| VM-STAT-12 | Off/default and conflicting CLI/config trust settings | No implicit trust from report mode, writer identity or self-produced output. YAML trusted + CLI off must scan F-LIE; YAML off + CLI trusted must skip F-FAST with payload denied; assert policy and coverage. |
| VM-STAT-13 | F-LIE before identity pinning | Off detects the null and negative values. Explicit trusted report may return conditional pass from those false claims; it must say metadata-assisted, zero value rows and unverified payload, emit trust evidence, and never claim tamper detection or value verification. |
| VM-STAT-14 | Corrupt payload with plausible pass statistics and valid footer | Ordinary path rejects corruption; trusted no-read path can only return a disclosed metadata-based result. No page/CRC/integrity claim is allowed for unread data. |
| VM-STAT-15 | Negative/inconsistent structural row counts, offset/length overflow, malformed/oversized metadata | Exit 2 before unsafe allocations; no manifest, no wrapping to plausible counts. Trust must not bypass footer/resource guards. |
| VM-STAT-16 | Object replacement during reads and after the last read, before final revalidation | ETag-only identity fails with exit 2/no manifest. An explicitly pinned still-accessible old version may succeed, never mixing versions. Observe the final identity request; versioned cases use an owned bucket. |
| VM-STAT-17 | Earlier local file pinned, later remote metadata read blocked, then local size/mtime/inode changes | Trusted selective execution revalidates local identity before commit and fails on detected change. It does not claim that stat pinning detects all same-size/restored-time attacks. |
| VM-STAT-18 | Log suppression and evidence publication failure | Evidence still exposes trust/coverage with logs at error level. Denying evidence/index publication gives exit 2 and no terminal manifest; failed partial ledgers are not consumable. |
| VM-STAT-19 | Default, explicit-none and bounded writer statistics roundtrip | Independent metadata inspection checks absence under default/none, numeric/boolean/decimal evidence under bounded, and disabled float statistics. Report-mode rereading defaults to value checks; explicit trust is separate. |
| VM-STAT-20 | Long/high-cardinality string/binary columns and changing groups during bounded writing | Variable-width extrema stay absent; values/order remain exact and budgeted. No unbounded retained extreme values; unavailable stats cause normal fallback on later reads. |
| VM-STAT-21 | Footer/proof/log resource exhaustion and denied fallback read | Typed operational failure/no manifest, no OOM or hidden unbudgeted evidence buffer. Insufficient proof must not become pass to avoid an I/O/resource error. |
| VM-STAT-22 | Empty groups/files and multiple sources with skipped prefixes | Correct zero-row handling, source/group order and counts. Later decoded issue positions carry metadata-prefix basis and observed group coordinates; no invented verification of skipped rows. |

### 6.1 Why the lying-statistics test must permit conditional acceptance

An unchanged false footer is not detectable without reading its data or relying
on external truth. Testing that trusted no-read mode always rejects F-LIE would
specify an impossible guarantee. The required protection is an explicit opt-in,
conservative default, honest evidence/counts, and no silent value/integrity claim.
The same fixture with stats off proves real violations are discoverable.

### 6.2 Optional statistics versus invalid structure

Test both classes independently. An absent `null_count`, invalid optional min
bound or unknown sort order should cause scan fallback. A negative row count,
out-of-object chunk or metadata allocation bomb is a structural/resource error.
Do not "fix" such tests by disabling all footer guards or by treating an absent
statistic getter's zero return as an observed zero.

### 6.3 Required boundary subcases

These are mandatory subcases of the existing IDs, not extra optional test IDs.

- **VM-STAT-01/02, human and plain consumers:** with `--log-level error`, run
  both default human stdout and `--plain`. F-FAST must visibly distinguish
  zero value-checked rows from eight metadata-claimed rows and disclose the
  producer-trust policy. F-STAT must distinguish 12 value rows from four
  metadata-claimed rows. Do not let truthful JSON hide misleading ordinary
  CLI output. Assert semantic labeled counts, not complete wording snapshots.
- **VM-STAT-03/05, proof completeness:** use two declared numeric columns.
  The first has truthful passing statistics; the second separately has a
  positive null count, failing range bounds, or missing evidence. Multi-column
  `not_null`/`valid_range` must scan the whole group and find literal failures.
  Also fail both operands on one row to require only one issue per rule/row.
  Check per-rule populations; proving the first operand is not enough.
- **VM-STAT-05/08, inexact bounds:** values `[10, 11]` with truthful conservative
  bounds `[1, 20]`, exactness flags false and null_count zero. A `> 0` rule
  can still skip with payload denied. A `> 5` rule cannot be proven from those
  bounds: it must scan and pass the actual values, with an unknown/inconclusive
  metadata claim rather than a fabricated failure because the lower bound is 1.
- **VM-STAT-05, warnings:** values `[1, null, -1, 2]`, truthful statistics,
  nullable input, and warning-level POSITIVE/NOT_NULL rules. Trusted failures
  and report modes both decode four rows, produce two warning issues/rows,
  zero errors and exit 0. Failures emits the null and negative rows; report
  omits them. No rows may be skipped merely because all failures are warnings.
- **VM-STAT-15, aggregate overflow:** malicious footer-declared counts across
  multiple small objects exceed the report/provenance counter range. Reject
  structurally earlier if applicable; otherwise reject at checked accumulation.
  Never emit wrapped positive coverage or a consumable manifest.
- **VM-STAT-09, conditional coverage:** values `[0, 2, "oops", 3]`, explicit
  integer conversion, a condition `amount > 0` and a `not_null` check. The
  rule has value_rows 4, not_applicable 1, unevaluable 1 and two evaluated
  rule rows; the bad conversion remains a real ingestion error. Metadata
  shortcuts are not used for the conditional rule.
- **VM-STAT-12, trust override:** YAML trusted plus CLI off on F-LIE must return
  exit 1 with four decoded rows, two real error rows, zero metadata rows and
  policy none. YAML off plus CLI trusted on F-FAST, with data reads denied,
  must return the conditional metadata result. Explicit empty/unknown trust
  overrides fail before access, rather than retaining the YAML setting.
- **VM-STAT-16, final S3 check:** let real MinIO obtain the last selected source
  response, pause before its body/completion is released to the CLI, replace
  the object, then release the old response. Permit no subsequent payload
  reads. Require a final conditional identity request, exit 2 and no manifest
  for ETag-only identity. Repeat with an explicitly pinned immutable old
  version still accessible: final version-specific validation must succeed
  and report that old version, not silently switch to the new one.
- **VM-STAT-19, independent writer policies:** write the same fixed-width,
  float and variable-width fixture using default, none and bounded policies.
  Inspect optional footer statistics: all absent under default/none; correct
  min/max/null counts only for supported fixed-width types under bounded;
  float and variable-width extrema remain absent. Structural row counts and
  mandatory page header counts are not optional column statistics. Re-read
  in report mode without a trust option to prove that writing statistics does
  not grant reader trust.

## 7. Cross-feature gates

| ID | Combination | Required result |
|---|---|---|
| VM-CROSS-01 | Modes × workers 1/4/8 on F-OUT | Same evaluated outcomes/issues and emitted row values/order/provenance per policy; no races or stale result storage. |
| VM-CROSS-02 | Modes × off/trusted on F-STAT | Correct policy-dependent scans/materialization/coverage. All mode forces values; failures/report can omit only metadata-proven-pass groups under explicit trust. |
| VM-CROSS-03 | Local→local, local→S3, S3→local, S3→S3 | Logical results/evidence identical; observed prohibited reads/writes remain absent; all object identities/digests/ref counts valid. |
| VM-CROSS-04 | Source-row failures plus dataset/schema failures and logging/presentation errors | Existing error precedence and commit boundary survive. A log write cannot override the real outcome; stdout failure after commit cannot hide the manifest. |
| VM-CROSS-05 | Repeated jobs in the same destination and timeout/signal cleanup | Unique runs; no overwrite/stale manifest collection; existing unrelated user objects untouched; owned multipart/uploads/fixture data cleaned. |
| VM-CROSS-06 | Native release binaries, CLI race build and small/large generated workloads | Run required native/local/MinIO gates; compare literal/independent outputs and observed IO. No new Python/cgo runtime required by the release CLI. Large-file benchmark data is deleted even on failure. |

## 8. Required CI execution and reporting

Run the implemented feature gate against an explicit child executable:

```bash
CGO_ENABLED=0 go build -trimpath -o /tmp/valet-release ./cmd/valet
VALET_BINARY=/tmp/valet-release go test -count=1 -tags=integration ./integration -run '^TestVM' -timeout 20m

CGO_ENABLED=1 go build -race -o /tmp/valet-race ./cmd/valet
VALET_BINARY=/tmp/valet-race GORACE='halt_on_error=1 atexit_sleep_ms=0' \
  go test -count=1 -tags=integration ./integration -run '^TestVM' -timeout 20m
```

The race command instruments the child CLI. Running `go test -race` around an
uninstrumented child is not equivalent. Windows paths and signal expectations
must be native equivalents; Unix-only signal cases are explicitly labeled,
not silently reported as executed on Windows.

Required release gates:

- Local cases on Linux amd64/arm64 and Windows amd64, using the native artifact.
- Real MinIO cases on Linux, with pinned backend, scoped credentials and owned
  namespaces. Keep local and object-store gates separately visible.
- A race-instrumented CLI run of concurrency, lifecycle, range/digest, evidence
  and structured-log cases, not only package-level race tests.
- Current acceptance suite remains passing with migrated version-2 consumers.
- Production docs/examples cover real new flags and clarify metadata trust;
  example commands must be exercised rather than only building Markdown.

AWS is explicit opt-in and reported separately. Darwin cross-compilation is not
native execution. If a mandatory environment is unavailable, the gate is not
verified; do not convert an all-skipped/zero-test run into release success.
Archive CLI version, target/toolchain, test IDs/results, source/fixture descriptor,
exit status, parsed reports/logs and backend request traces needed for diagnosis.
Never archive secret credentials or raw sensitive production data.

## 9. Performance evidence, not flaky test assertions

Use a separate generated-scale job for the existing 5 GB workload and a numeric
stats-eligible workload. Measure complete CLI time for all/report/failures,
off/trusted and useful worker counts. Keep generation and independent comparison
outside the timed interval. Report storage/cache conditions, output size, peak
RSS, managed peak, observed payload access, exact values/order/counts and trust
coverage. Delete input/output/generators under guaranteed cleanup.

Do not hard-code a sub-10-second wall-time assertion into shared-runner tests.
Use denied byte/write ranges and artifact absence as deterministic work-avoidance
proof. Hardware performance regressions need a controlled runner and recorded
baselines, not a timing threshold that can pass by omitting real checks.

## 10. Completion criteria

All four feature groups and all 62 IDs above have real CLI acceptance coverage,
with explicit required environments. No fake fallbacks, swallowed errors,
skipped placeholders, fabricated counts or "proof" based solely on diagnostic
echoes. The exact fixtures independently establish the expected outcomes.
The implementation is complete only when feature contracts, mandatory E2E gates,
documentation examples and cleanup obligations are satisfied together.
