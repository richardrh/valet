# Go and Apache Arrow implementation specification

Status: acceptance contract for the Go implementation in this repository (valet). Availability and verification are separate: external AWS and native target-platform gates require their named environments.

This specification defines a Go implementation of Validnator using Apache Arrow Go directly, replacing Polars for the new executable. The existing Python implementation remains available during development. The goal is bounded execution, a self-contained CLI distribution, and first-class local/S3 input and output—not a claim that changing languages alone improves performance.

Report-version-2 output modes, schema differences, structured diagnostics and
trusted metadata execution are governed by
[validation modes and metadata execution](validation-modes-spec.md), with
[black-box E2E acceptance cases](validation-modes-e2e.md). That contract
supersedes older output/counting descriptions below where they differ.


## 1. Decisions and scope

### Agreed requirements

| Area | Decision |
|---|---|
| Implementation | Go and Apache Arrow Go; no Python, Polars, Arrow C++, Acero, cgo or C/C++ runtime dependencies in the shipped application |
| Product | CLI now; reusable internal packages suitable for a later web application; no server or GUI now |
| Repository | Standalone Go module `github.com/richardrh/valet` in its own repository; the Python implementation stays in [validnator](https://github.com/richardrh/validnator) |
| Package visibility | `internal/` for implementation packages; no supported external Go API initially |
| Extensions | Checks compiled into a custom Go build; no dynamic plugin or embedded scripting runtime |
| Compatibility | Preserve existing declarative rules and intended behaviour; explicitly document corrected defects and changed output contracts |
| Sources and destinations | Local files, AWS S3, and verified S3-compatible storage first |
| Raw ingestion | CSV plus streaming NDJSON and JSON arrays of records; explicit parsing/conversion contracts, not silent coercion |
| Execution | Local Arrow batch execution; no warehouse SQL pushdown |
| Scale | Support 100 GB–1 TB and larger inputs when the configured rules and state fit the resource budget |
| Resource exhaustion | Fail explicitly at the managed memory budget; no disk spilling or approximate answers |
| Outputs | CSV/Parquet row buckets, summary JSON, and streamed issue records |
| Platforms | Linux amd64, Linux arm64, Windows amd64 |
| Large-scale testing | Generate bounded Arrow batches totalling at least 100 GiB; never require a giant checked-in fixture or upfront allocation |
| Test organisation | Colocate package tests, fuzz tests and ordinary benchmarks; separate genuinely end-to-end suites |

### Design defaults selected for unanswered operational questions

These are specification choices, not claims of additional user approval or observed performance:

- The default managed execution budget is 512 MiB. It is configurable and is **not a hard process-RSS limit**. Deployments must leave runtime/SDK headroom.
- MinIO (pinned source commit) and RustFS (pinned container digest) are required independent S3-compatible acceptance targets. AWS service behavior, IAM and KMS remain explicitly unverified and are outside the release prerequisite scope. Other services are not advertised as verified until their integration suite passes.
- Inputs are replayable files/objects. Standard-input streaming is not supported in this release because global checks may require another pass.
- Explicit input lists and prefix/directory discovery form one logical dataset. Input order is deterministic; bucket output preserves that order.
- No checkpoint/resume. An interrupted job is restarted as a new run.
- With an output URI, artifacts are published through a completion manifest in a unique run location. Without one, validation returns only an aggregate stdout summary; no artifact files or manifest exist. Incomplete artifacts are not valid completed artifact results.
- Standard AWS SDK credential discovery is supported. Explicit role-assumption configuration beyond SDK profiles, SSE-C, and special endpoint authentication are not separate product features in this release.
- New outputs count every warning occurrence, including warnings on rows assigned to the error bucket. This is a documented correction to current reporting.

### Explicitly outside this release

ClickHouse, Databricks, Snowflake, Fabric SQL and OneLake connectors; SQL pushdown; database writeback; a web application; the existing Python DataFrame API; Python callbacks; arbitrary user code in YAML; dynamic Go plugins; disk spill; recovery checkpoints; distributed execution; automatic schema evolution; automatic repair/imputation; and byte-for-byte legacy JSON/CSV report compatibility.

Source/sink boundaries must permit later connectors without making the engine aware of their authentication or query protocols. Do not create empty connector packages or advertise unimplemented providers.

## 2. Evidence and success criteria

The earlier investigation exercised the Python CLI with four existing rules and a synthetic three-column CSV. A million rows occupied approximately 16.4 MiB on disk. CSV-output validation took 5.32 seconds and peaked at approximately 3,328 MiB RSS; read-only processing peaked at approximately 121 MiB. CLI help had a median of approximately 0.10 seconds and 46 MiB peak RSS over five warm-cache fresh-process runs. These are diagnostic observations on one machine, not portable performance guarantees.

The relevant existing contracts are in the Python [validnator](https://github.com/richardrh/validnator) repository:

- [`pipeline.py`](https://github.com/richardrh/validnator/blob/HEAD/validnator/pipeline.py): orchestration, schema gates, bucket precedence and materialised results.
- [`check_types.py`](https://github.com/richardrh/validnator/blob/HEAD/validnator/validators/check_types.py): built-in check semantics.
- [`rule_loader.py`](https://github.com/richardrh/validnator/blob/HEAD/validnator/validators/rule_loader.py): rule composition and context substitution.
- [`pipeline_config.py`](https://github.com/richardrh/validnator/blob/HEAD/validnator/pipeline_config.py): input modes and schema-gate extraction.
- [`transforms.py`](https://github.com/richardrh/validnator/blob/HEAD/validnator/transforms.py): rename transforms.
- [`results.py`](https://github.com/richardrh/validnator/blob/HEAD/validnator/results.py): existing report behaviour.

Success means the complete scoped CLI works on all release targets, existing rule families have specified migration behaviour, supported row-local workloads do not accumulate memory with total input size, global state fails predictably at its budget, and local/S3 results are only published when complete.

Release evaluation must measure startup, peak RSS, throughput and **total distribution size**. Do not infer a win from a small launcher, an Arrow dependency, or `CGO_ENABLED=0` alone. No absolute throughput or binary-size guarantee is asserted before representative release builds exist.

### Observed Go execution

The generated scale acceptance job was executed on Linux amd64, Go 1.27.0,
Arrow Go 18.8.0, one validation worker, 4,096-row batches and a 128 MiB managed
budget. It generated fresh approximately 1 KiB string payloads plus typed
numeric columns, ran a nonnegative-value rule, and consumed results with the
bounded counting/digest sink. Both independent expected-count oracles and the
controlled sustained-RSS comparison passed.

| Logical Arrow volume | Rows | Elapsed | Peak managed reservations | Peak process RSS |
|---|---:|---:|---:|---:|
| 10.59 GiB | 11,658,435 | 22.53 s | 7.68 MiB | 57.79 MiB |
| 105.92 GiB | 116,584,346 | 226.80 s | 7.68 MiB | 63.90 MiB |

Managed residual allocations were zero after cleanup. These measurements do
not include real file decoding, network transport or full bucket/report
encoding. They establish bounded engine behaviour for this workload, not a
universal throughput claim or a claim that high-cardinality global state fits.

On the earlier million-row, 16.4 MiB CSV with four rules, the Go CLI took a
median of 1.82 s with 182 MiB peak RSS over three runs. The Python CLI took a
median of 5.89 s with about 3.3 GB on the same input and machine. Both produced
800,000 passes, 100,000 warnings and 100,000 errors. Their output formats
differ, so the comparison covers the whole command, not identical output. The
first Go build took 7.27 s on this workload. Removing a per-row CSV reader,
eager reject JSON and per-row map lookups cut the time by three quarters.
Batch size had little effect: 256 KiB to 64 MiB targets moved elapsed time by
about 1 s.

Stripped `CGO_ENABLED=0` release executables measured 41.9 MiB (linux/amd64),
36.6 MiB (linux/arm64) and 42.9 MiB (windows/amd64); gzip-compressed 12.5,
10.8 and 12.8 MiB. Most of that is the AWS SDK and Arrow/Parquet code, so the
executable is self-contained rather than small. On linux/amd64, `--help`
completed in under 10 ms with about 21 MiB peak RSS, against roughly 100 ms
and 46 MiB for the Python CLI. The arm64 and Windows binaries were
cross-compiled here; the CI matrix runs them on native runners.


## 3. Repository and package structure

```text
valet/
├── go.mod
├── go.sum
├── cmd/
│   └── valet/
│       └── main.go
├── internal/
│   ├── app/
│   ├── cli/
│   ├── config/
│   ├── rules/
│   ├── checks/
│   ├── engine/
│   ├── resource/
│   ├── source/
│   ├── storage/
│   ├── sink/
│   ├── report/
│   └── testutil/
│       ├── batchgen/
│       └── checksink/
├── integration/
│   └── testdata/
└── benchmarks/
```

This is a responsibility map, not a requirement to create empty packages. Keep tightly coupled implementation in one package until a concrete boundary warrants separation. There is no `pkg/`, generic `common` package, mirrored top-level unit-test tree, or separate Go module per connector.

| Package | Responsibility |
|---|---|
| `cmd/valet` | Process entry, exit status and CLI invocation only |
| `cli` | Arguments, terminal rendering, stderr diagnostics; no validation logic |
| `app` | One run's lifecycle, dependency assembly, cancellation and publication |
| `config` | YAML decoding, precedence, URI resolution and configuration diagnostics |
| `rules` | Typed rule definitions, schema binding, execution scope and compiled check contracts |
| `checks` | Built-in implementations and explicit registration into a per-run compiler |
| `engine` | Batch scheduling, evaluation, bucket routing and incremental aggregation |
| `resource` | Managed reservations, limits and resource-limit errors |
| `source` | Dataset discovery, CSV/Parquet/JSON decoding, schema-directed conversion with presence/invalid masks, raw-value provenance and replay |
| `storage` | Local/S3 object metadata, streaming/range access and output object operations |
| `sink` | CSV/Parquet encoding, bucket parts and bounded issue writing |
| `report` | Versioned summary/issue/manifest data contracts and encoding |

Dependencies flow from CLI/application assembly toward these components. `engine` must not import `cli`, parse YAML, instantiate AWS clients, or write directly to terminal output. `rules` defines contracts without importing the concrete built-in `checks` package. `app` supplies the registered implementations explicitly; no mutable global registry or registration through hidden `init()` side effects.

Use small interfaces at the consuming boundary. Do not require every component to implement an interface merely to make it mockable. Tests should exercise real implementations where practical.

The current compact implementation combines rule compilation and evaluation
in `internal/checks` and the bounded ordered execution pipeline in `internal/app`; report
contracts live in `internal/data` and encoders in `internal/sink`. The table
describes responsibility boundaries, not a requirement for empty `rules`,
`engine` or `report` packages. `internal/data` owns batch/provenance/field-state
contracts shared across these boundaries.


### Internal packages and compiled extensions

Custom checks live in this module or a custom fork and are explicitly included at the application composition point. They use the same typed compilation/evaluation contracts as built-ins and must declare execution scope and reserve their retained state. No public external extension ABI is promised. A future separately distributed extension SDK requires a deliberate public contract; it cannot import these internal packages from an unrelated repository.

## 4. Technology and build contract

- Use the official `github.com/apache/arrow-go/v18` module, pinning an evaluated release in `go.mod`. A future major-version move requires ordinary dependency migration, not a compatibility shim.
- Use Arrow arrays/record batches and the Arrow Go CSV/Parquet facilities where their resource behaviour meets this specification. Arrow is the data representation, not an assumption of a complete query engine.
- Use typed Go loops or available Arrow kernels for checks. Missing kernels must not lead to a C++ dependency or reflection-based per-cell evaluation.
- Use AWS SDK for Go v2 for S3 and standard credential/configuration loading.
- Use a maintained pure-Go YAML decoder with duplicate-key detection and bounded document parsing.
- Pin a supported Go toolchain, dependencies, object-store test images and fixture semantics. Record versions with benchmark artifacts.
- Release builds must succeed with `CGO_ENABLED=0` for all three targets. Run binaries on actual target OS/architecture runners; cross-compilation alone is not execution evidence.
- Release archives contain the executable, license notices and usage/configuration documentation, not Python environments or shared native libraries. Record archive size and executable size separately.
- Compression codecs advertised for CSV/Parquet must work in these builds. Initial required support: plain/gzip CSV; uncompressed, Snappy and Zstandard Parquet. Unsupported codecs produce an explicit input error.

## 5. Configuration and CLI

### Existing rule documents

Preserve the existing `name`, pipeline `version`, `context`, `input`, `transforms`, `rules` and `stats` vocabulary. Add `dataset_rules` for assertions over aggregate metrics. The existing pipeline `version` is user metadata, not the configuration schema version. The optional `config_version` reserves room for future vocabulary changes; `1` is the only version and omitting it is equivalent. There is no legacy vocabulary mode: the check meanings below apply to every document, including rule files copied from Python validnator (see Deliberate migration changes).

The new execution fields are `source`, `output` and `execution`. The existing `input` field continues to describe file format, raw-string/inference mode and null handling. Unknown fields, duplicate YAML keys, duplicate rule codes, unknown checks and malformed arguments are configuration errors. Empty-check rules remain valid no-op rules for compatibility and are identified in the plan.

A normative example of the new execution fields:

```yaml
config_version: 1
name: Exposure validation
version: "1.0"

source:
  uris:
    - s3://exposure-input/2026/data.csv
  s3:
    region: eu-west-1
    # endpoint and path_style are optional for S3-compatible services.

input:
  type: csv
  mode: raw_strings
  null_values: ["NULL", "null", "N/A", "n/a", ""]

execution:
  memory_budget: 512MiB
  batch_target: 8MiB
  max_record_bytes: 8MiB
  workers: 1

output:
  uri: s3://validation-results/exposures/
  format: parquet
  issues: ndjson

rules:
  - code: RECORD_ID_REQUIRED
    column: record_id
    level: error
    message: record_id is required
    checks:
      - type: not_null
```

`source.uris` is an ordered explicit list. Alternatively, `source.prefix` selects a local directory or S3 prefix, with `source.recursive` and a required format filter. The forms are mutually exclusive. Prefix discovery is sorted by canonical relative path/object key. `output` may have its own `s3` connection options; input and output need not use the same endpoint or credentials profile.

Relative local `reference_file`, `mapping_file` and `schema_file` paths resolve relative to the local pipeline document, not the process working directory. Absolute local paths and `s3://` references are preserved. Pipeline configuration accepts strict local `.yaml`, `.yml`, and `.json` documents; cloud support applies to datasets, references, mappings and outputs, not implicit remote configuration execution. JSON uses the same bounded node, strict-shape, exact-number, normalization, and semantic-validation path as YAML, while `.json` files must also be valid JSON syntax.

### Commands and precedence

- `valet validate -p pipeline.yml [-i URI] [-o URI]`: run validation. `-i` overrides the configured dataset with one explicit source; `-o` overrides output location.
- Without `-o`/`output.uri`, `validate` emits one aggregate JSON summary on stdout, with no sink/output client, files, per-row issue stream or manifest. Counts, coverage, trust, provenance and existing bounded schema/dataset diagnostics remain. Resolve an unspecified mode after CLI overrides: no URI means `report`, URI means `all`. Reject explicit `all`/`failures` without a URI at validation run entry, not config inspection by plan/schema.
- `valet validate ... --memory-budget 512MiB --batch-target 8MiB --workers 1`: explicit execution overrides.
- `valet plan -p pipeline.yml [-i URI]`: validate configuration, discover schemas and report required passes, global state and source capabilities. It may read metadata/inference samples; it does not execute the full validation or promise that future distinct-key state will fit.
- `valet schema -p pipeline.yml [-i URI]`: inspect Parquet footer/schema metadata only. Report schema compatibility separately with `scope: schema`, `metadata_only: true`, `validation: not_evaluated` and `rows_checked: 0`; footer counts are declared, not validated. Do not initialize output clients, load rule references, decode payloads, compute full-content digests or publish ordinary row-bucket manifests. Explicitly enumerate data-dependent checks not evaluated. Exit 0 compatible, 1 schema mismatch, 2 operational failure.
- `--help` and `--version` must not initialise storage clients, discover credentials or start worker pools.
- Preserve `--plain`, `--verbose` and `--stats` presentation intent. `--plain` prints the Python-compatible `key=value` summary lines (`title`, `total`, `pass`, `warn`, `error`, `output_dir`) followed by `status`, `validation`, `run_id` and `manifest`. `--stats` adds per-rule failure counts (rule and ingestion codes) and the configured `stats` aggregates. `-v`/`--verbose` equals `--log-level debug`; `--log-level debug|info|warn|error` (default `info`) and `--log-format text|json` (default `text`) select structured `log/slog` output on stderr. Log records within a run carry `run_id`. `--json-output PATH` writes the new versioned summary, not the legacy all-row-index JSON tree.
- JSON stdout is the default independently of artifact selection. `--stats` implies existing plain/statistics output unless JSON is explicit. `--json-output -` explicitly selects stdout JSON and conflicts with `--plain`; `--json-output PATH` writes an additional summary file while stdout remains JSON. Presentation failure without a committed artifact manifest returns exit `2`; exit `3` is reserved for committed-artifact presentation failure. Successful stdout-only terminal logging has `manifest_committed: false`.
- Legacy `--stdout` unified bucket CSV is explicitly unsupported in this release: reject it with migration guidance to bucket outputs and a manifest. Do not silently change its grouped-order semantics.

Precedence is explicit CLI override, then YAML, then documented defaults. Credentials use the SDK chain rather than ad hoc YAML/environment interpolation. stdout carries the requested summary; logs and diagnostics go to stderr only. Exit codes: `0` completed with no error-level row or dataset violations; `1` completed with error-level violations or a schema-gate rejection; `2` pre-commit configuration/input/I/O/resource/operational failure; `3` terminal result committed but stdout presentation failed; `128+N` cancellation by signal `N` before commit where the OS supports the conventional code (`130` SIGINT, `143` SIGTERM). Exit `3` identifies a consumable manifest and must not trigger a validation rerun. Warnings alone do not produce exit `1`. Dataset assertion failure does not retroactively reclassify every row.

## 6. Source, schema and snapshot contract

### Dataset and provenance

All selected files form one logical dataset for uniqueness, schema checks and statistics. Every row has provenance independent of user columns: source ordinal, source URI, 1-based data-row number within the source, and a 64-bit global row ordinal. CSV header lines do not count as rows; quoted multiline fields are one data row.

Source metadata and schema must be bounded by configured limits too. A prefix containing more objects than the discovery budget permits fails; discovery must not accumulate an unlimited list outside resource accounting. Record the discovered source identities in a streamed input manifest and its digest. A final result manifest references that object instead of embedding an unbounded array of source names.

The input manifest is run metadata, not a dataset/state spill. Replay must use the same discovered objects, not relist a prefix. Reject source/output prefix overlap that would make a run ingest its own output.

### Schema and physical decoding

- `input.type` accepts `csv`, `parquet`, `ndjson`, or `json`. When omitted it is inferred from the first effective source URI (after `-i`) with any `.gz` suffix ignored: `.parquet` → parquet, `.ndjson`/`.jsonl` → ndjson, `.json` → json, otherwise csv; a prefix source uses its format filter. S3 suffix inference uses the decoded object key, matching storage identity. An explicit type always wins. JSON means a streamed top-level array of record objects; NDJSON means one record object per nonblank line. Neither reads the whole file into a Go object tree.
- `raw_strings` preserves non-null CSV values as UTF-8 strings; configured null markers remain distinct from non-null strings not listed as markers. Do not infer numeric types for identifiers in this mode.
- Legacy `infer_types` retains the configured inference-row limit, defaulting to 1,000,000. Infer incrementally without retaining the entire sample, then reopen the source. Freeze the schema; later incompatible values produce an explicit input error rather than changing types between batches. This convenience mode is not the recommended dirty-data ingestion path.
- `input.schema` defines an explicit target schema and avoids inference. With an explicit schema, decode lexical values first and classify conversion errors as row-quality failures by default. This differs from treating a malformed CSV record or invalid JSON document as a value-conversion failure.
- Files in a dataset must have compatible declared schemas. Resolve by column name with deterministic order. Required CSV headers missing from any file cause schema rejection; optional declared fields may be absent and carry a presence mask. Extra fields are governed explicitly, never silently unioned.
- Initial supported logical types are null, boolean, signed/unsigned integers, float32/64, UTF-8, binary, date, timestamp with explicit timezone metadata, and fixed-precision decimal up to Decimal128. Accepted aliases are normalized once (`boolean`→`bool`, `int`/`integer`→`int64`, `float`/`double`→`float64`, `str`/`utf8`→`string`, `date32`→`date`, `datetime`→`timestamp`, `decimal128`→`decimal`) and every validator/consumer uses that canonical vocabulary. Preserve supported types in Parquet output. Nested Parquet schemas are rejected; JSON nested scalar selection is described below.
- Validate UTF-8, duplicate CSV headers, malformed records, field counts and declared size limits. No silent row skipping. Reserve the maximum configured conservative CSV/JSON decoder working set before constructing the library decoder or delivering record bytes; compressed input follows the same rule. CSV field-state accounting uses the record-limit-implied maximum field count and is not reduced after reading the header. `max_record_bytes` covers ignored fields and every native/lexical source column; fixed-width Parquet values use their Arrow physical width in both paths. Default CSV is comma-delimited, header-bearing UTF-8 with doubled-quote escaping; support configured delimiter and header presence/names. Accept CRLF/LF and an initial UTF-8 BOM, preserving newlines inside quoted fields. Other encodings require conversion outside this release.
- For Parquet, bound metadata, decoder, page and output buffers, not merely the requested batch row count. Inspect declared sizes but do not trust them as the sole allocation guard. Oversized row groups/pages or decoder paths that cannot honour the resource contract must fail with a documented resource error.
- Implementation: planning validates only the compact footer and schema semantics, including the embedded `ARROW:schema` and logical-type annotations, and reports canonical logical type names with timezone/decimal attributes separately. Each actual Parquet open adds one full scan of the compressed column chunks before Arrow decodes anything. Local opens add a separately counted close-time digest scan. Those scans reject type/encoding mismatches and bind identity while dictionary, delta and page expansion remain bounded. Compressed and uncompressed pages are limited to `max(max_record_bytes, batch_target)`, page headers to the smaller of that value and 16 MiB, and Zstandard windows to 8 MiB. Only flat required/optional leaves are accepted. Summary and plan pass counts include every scan.
- When a declared column's Arrow type already matches the file, and it has no trim, format, or date/timestamp conversion, the decoded Arrow buffers pass through without a text round trip. They are still checked for nullability, non-finite floats, invalid UTF-8 and decimal precision, with the same invalid/reject semantics as the lexical converter. Any other case uses the lexical converter.
- CSV is read with one long-lived `encoding/csv.Reader` per object and JSON/NDJSON with one `encoding/json/jsontext.Decoder` per object; there are no hand-written framers or tokenisers. A thin counting reader enforces `max_record_bytes` and keeps JSON record bytes and line numbers; record line numbers come from `csv.Reader.FieldPos` and the counting reader. Reject records are encoded only for rows that have an ingestion issue. The module requires Go 1.27, where `encoding/json/jsontext` is available without `GOEXPERIMENT`.

### Raw values, parsing and conversion

The stages are lexical decoding, presence/null classification, explicit conversion, then quality checks. They are distinct operations. CSV tokenisation is not schema inference; schema inference is not validation; validation does not silently repair data.

An explicit schema uses these fields:

```yaml
input:
  type: csv
  mode: raw_strings
  null_values: ["NULL", ""]  # whitespace is not silently trimmed
  schema:
    extra_fields: error
    columns:
      account_id: {type: string, required: true, nullable: false}
      amount: {type: decimal, precision: 18, scale: 2, required: true}
      units: {type: int64, required: true}
      active: {type: bool, true_values: ["true"], false_values: ["false"]}
      effective_date: {type: date, formats: ["%Y-%m-%d"]}
      observed_at: {type: timestamp, timezone: UTC}
```

`required` means the field must exist; `nullable` controls explicit/mapped null values. Defaults are `required: false`, `nullable: true`. With no schema, retain existing raw/inferred modes; JSON without a schema supports only flat scalar objects, freezing inferred fields/types from a bounded discovery sample. Production JSON should declare a schema.

Every selected field carries a bounded typed value plus explicit state: valid, missing, null, or invalid conversion. Missing is not null and invalid is not null, even if Arrow uses a null slot for all three; separate masks retain that distinction.

- Required missing values emit `INGEST_MISSING_FIELD`; forbidden nulls emit `INGEST_NULL`; failed conversions emit `INGEST_TYPE`. These reserved, error-level ingestion codes cannot be user rule codes.
- Input schema conversion runs once per field per batch and shares typed buffers with checks. Preserve decoded original CSV fields/JSON tokens long enough to emit any necessary reject record; do not keep them for the entire run.
- Invalid conversion rows enter the error bucket; valid cells still undergo independent checks. A check depending on an invalid operand is marked unevaluable, not evaluated against a fabricated null/zero. Count such skipped evaluations by rule. Do not issue a cascade of range/date/null errors for one parse failure.
- Presence checks can inspect missing/null/invalid state directly. A required-field/null ingestion failure and an explicitly configured rule are separate contracts and can each report an issue; do not deduplicate distinct user rules by message text.
- `input.on_type_error` defaults to `row_error`; an explicit `abort` mode ends the job with exit `2` and no result manifest. A legacy error-level `schema_matches` castability gate deliberately retains whole-dataset rejection semantics and therefore takes precedence over row quarantine.
- Conversion failures produce `rejects/part-*.ndjson` records containing source identity, original decoded record, field states and conversion reasons. This is bounded output, not a retained in-memory reject list. Original CSV quoting/line endings need not be byte-identical; field content must be recoverable. JSON numeric lexemes must be retained exactly in the original record. These reject artifacts contain source data and require the same access controls as bucket output. Logs and ordinary issue records remain value-redacted.
- Schema types apply after declared renames; source-field selection occurs before renaming. Reject ambiguous mapping/renaming collisions.

Conversion policy:

| Target | Contract |
|---|---|
| String/identifier | Preserve content and leading zeros. No trimming, case folding or Unicode normalisation unless explicitly configured |
| Integer | Parse exact integer syntax with bounds. No rounding/truncation, boolean-to-number coercion or Float64 intermediary; reject `1.5`, overflow and string `1.0` |
| Decimal | Parse base-10 exactly to declared precision/scale; permit excess fractional digits only when all discarded digits are zero; otherwise fail rather than round. No locale/currency-symbol guessing |
| Float | Parse finite values with target-width overflow checking; document representational rounding. Reject NaN/Infinity as valid values |
| Boolean | Accept actual JSON/Parquet booleans; CSV strings or JSON strings only through explicit/default token sets. No generic truthiness or case folding |
| Date | Validate calendar dates against explicit formats; ISO date is the default. Reject ambiguous day/month interpretation |
| Timestamp | Require an offset/Z or an explicitly configured source timezone. Reject timezone-less ambiguous/nonexistent DST local times. Normalize instants to UTC while preserving original reject data; do not use the machine's local timezone |

Per-field `trim: true` and explicit `true_values`/`false_values` are supported parsing options, not hidden defaults. Configured CSV null markers apply after explicitly requested trimming. Missing and null remain independent. Empty JSON strings and strings such as `"NULL"` are not JSON null merely because CSV uses those markers.

### JSON and NDJSON contract

Stream tokens into schema-directed batch builders. Use exact numeric tokens, not default `float64` decoding into `map[string]any`. Large JSON integers such as `9007199254740993` must survive exactly when assigned to int64/decimal or retained as raw data.

- JSON arrays contain objects only; nested arrays are not additional rows. A top-level singleton object is not implicitly treated as a dataset.
- NDJSON ignores blank lines but records physical line location separately from logical row ordinal. Nonblank lines contain one complete object.
- Duplicate object keys, invalid UTF-8, malformed syntax, non-object records and excessive nesting/record length are fatal input errors. Do not guess record boundaries after malformed JSON/CSV. No partial valid result is published.
- `input.schema.columns.<name>.path` optionally selects a scalar using JSON Pointer syntax, for example `/address/postcode`; default is the top-level field of that name. No JSONPath predicates, automatic flattening or array explosion.
- Missing paths, explicit `null` and invalid selected non-scalar values stay distinct. A selected object/array where a scalar is required is a row-level `INGEST_TYPE` failure.
- `extra_fields` is `error` by default or explicitly `ignore`. Undeclared CSV headers reject the schema; unexpected JSON fields produce row-level `INGEST_EXTRA_FIELD`. For nested projection, declared pointer ancestors are allowed and unexpected sibling fields obey the same policy. Ignored values are still tokenised under depth/size limits, not materialised.
- Mixed record types must not trigger per-batch schema evolution. Default maximum nesting depth is 64 and maximum record size is `max_record_bytes`, including skipped fields.
- Support plain/gzip JSON and NDJSON with streaming decompression and bounded decoder buffers.

### Replay and immutability

Global checks and inference can require reopening input. For S3, pin version IDs where available; otherwise use conditional requests with the captured ETag and metadata. ETags are identity validators, not universal content hashes. All range reads use the same identity conditions. Missing conditional-read capability is an explicit limitation for a target requiring replay.

For local files, capture metadata and verify a streamed content digest across complete replay passes. Bounded inference drains the remaining object through the digest reader after its sample is complete, without retaining or inferring additional rows. Validation also drains a raw-string CSV after header discovery without decoding its rows, so the header-derived schema and later validation share one pinned identity. A changed source—including same-size, same-inode content with a restored modification time—prevents publication. Plan-only Parquet metadata handles and raw-string CSV discovery do not perform a full digest; validation digest/safety/close scans are explicit in pass counts. Concurrent in-place mutation during a single read is outside snapshot guarantees; operators must provide immutable inputs. Reference/mapping inputs are captured once per run and share the same identity policy.

## 7. Execution and ownership

### Pipeline

1. Parse configuration, validate output location, and build immutable rule definitions.
2. Discover source schemas; apply ordered rename transforms to bind the effective schema.
3. Compile checks once, resolving columns, types, regular expressions and date formats.
4. Load shared projected reference indexes under budget.
5. Perform required preflight/global accumulation passes, sharing a pass when safe.
6. Replay if necessary; decode/convert and evaluate bounded batches, accumulate dataset metrics, and route rows/issues/rejects to bounded sinks.
7. Finalise dataset assertions, close output parts, write summary and publish the completion manifest last.

Row-local plans need one validation pass after any inference. Repeated source reads are an explicit plan property and must be reported; they are not hidden retries.

### Batch contract

A source provides schema, replay capability, provenance and a pull-based stream of Arrow record batches. A successful read transfers one owned reference to the consumer. The consumer releases it after processing; any component retaining it beyond that scope must explicitly retain/release its own reference. Errors, cancellation and early termination obey the same ownership rule.

The compiled row-check contract binds once to a schema, declares `row`, `reference`, `schema` or `global` scope, and produces a non-null violation bitmap plus an unevaluable bitmap where conversion/condition operands are invalid. Global checks additionally have bounded accumulation and finalisation state. Dataset assertions use a separate aggregate contract and emit dataset-scope issues, not fabricated row bitmaps. Result masks and temporary casts are batch-scoped; metadata/messages are shared per rule.

Evaluate rules sequentially within a batch, reusing bounded preparation and scratch where safe. Each returned result owns its budgeted buckets/issues until an explicit idempotent `Release`; the next evaluation or program close must not invalidate an outstanding result. Cache at most one released result under budget. Do not retain per-rule bitmaps for the entire dataset. Shared scalar/state/condition preparation is batch-scoped and cleared before the input batch can be released.

Use immutable Arrow buffers where possible. Copies needed for decoding, filtering or output encoding are legitimate but must be bounded and accounted for; do not advertise blanket zero-copy execution. No per-row `map[string]any`, reflection dispatch, or repeated human-readable message structs in the hot path.

### Concurrency

Execution accepts 1–32 workers and defaults to one. Parquet row groups may be verified/decompressed/decoded concurrently through bounded worker-owned state and emitted in source/group order. The application may overlap input, ordered single-owner rule evaluation and ordered single-owner writing, with a bounded global in-flight window and independently owned batch/result reservations. Tight budgets reduce admission rather than creating unbounded queues. Every stage must join and drain ownership before source close, abort or final publication.

Run-scoped cancellation propagates to readers, SDK calls, workers and writers. On the first fatal failure stop accepting work, cancel outstanding operations, release retained batches and close/abort sinks. Do not invoke `os.Exit` inside reusable packages.

## 8. Memory budget and no-spill contract

`memory_budget` covers managed live/reserved memory: Arrow buffers, decoder staging, reference/global indexes, rule scratch space, queues, reorder buffers, writer row groups, multipart upload buffers and discovery metadata. Defaults: 512 MiB budget, 8 MiB target decoded batch, 8 MiB maximum CSV record, one worker. These are configurable starting policies, not measured optimal values.

A target batch size is not a hard ceiling: variable-width data, a single large row and decoder staging require separate limits. Before an operation begins, reserve its conservative peak working set, including simultaneous old/new buffers during growth. If the reservation cannot fit, return a typed `ResourceLimitError` describing phase, limit, current reservation and attempted allocation without exposing sensitive row data.

Arrow allocation hooks alone are insufficient. SDKs, Go maps, strings and codecs can allocate outside Arrow. Use explicitly bounded structures and accounted conservative capacities for retained keys/state; include map growth/copy overhead or use a budgetable hash-table implementation. A dependency allocation path that cannot be bounded must be constrained at its boundary or rejected for the advertised resource-limited mode. Do not recover arbitrary panics and relabel them as ordinary resource errors.

The budget is **not** an RSS cap. Go runtime stacks, GC bookkeeping, allocator retention and libraries create additional process memory. `GOMEMLIMIT` is a soft runtime control, not enforcement of this contract or a guarantee of graceful termination. Report managed peak and observed process RSS separately. A container/OS hard limit can terminate the process; no successful manifest may result.

There is no input spooling, external sort, disk-backed hash index or temporary state spill. Ordinary outputs and small run metadata are not spill mechanisms. If a global index cannot fit, the run fails rather than dropping a rule, sampling, silently increasing the limit or providing approximate answers.

## 9. Validation semantics and compatibility

### Shared invariants

- Checks are assertions: a failed assertion is a violation. Violations from checks within a rule combine with OR; emit at most one issue per rule per row even if several assertions fail.
- Multiple-column checks fail when any selected column fails the assertion unless the named check specifies a different composite operation.
- Bucket precedence is error, then warning, then pass. Preserve all issue severities on an error row.
- Resolve context placeholders in messages as the existing unified pipeline does. Do not evaluate arbitrary expressions or substitute secrets into reports.
- Missing check operands are a compilation/schema error, not a silently skipped column. Header/schema absence has its own schema-gate handling.
- No nullable violation masks: each check defines null handling explicitly. Invalid conversions remain separate from null and make dependent value checks unevaluable rather than silently passing.
- User data is never overwritten by provenance/diagnostic field names. A collision with reserved output fields is a configuration error with a remediation message.

### Required built-in coverage

| Check | Required behaviour |
|---|---|
| `not_null` | Null is a violation |
| `valid_length`, `min_length`, `max_length` | Unicode code-point length, not byte length; null fails; preserve documented string conversion for min/max |
| `valid_range` | Inclusive bounds by default; `exclusive_min`/`exclusive_max` make the corresponding bound strict; null passes; boolean/non-numeric/non-finite values fail numeric validation |
| `equals` | Value must equal the specified non-null value; a mismatch or null is a violation |
| `not_equals` | Value must differ from the specified non-null value; equality is a violation and null passes unless separately forbidden |
| `all_equal` | Every declared column must equal the supplied value; any mismatch or null is a violation |
| `has_null_keys` | Any null component is a violation |
| `valid_values` | Non-null value outside the allowed set fails; null passes and should be combined with `not_null` when forbidden |
| `matches_pattern` | Regex search semantics, not implicit full-string match; null fails |
| `is_unique` | All occurrences of a duplicated value fail, including the first; null fails |
| `is_unique_composite` | Ordered tuple uniqueness; any null component fails; all duplicate occurrences fail |
| `compare_columns` | Fail when the configured comparison is false or either operand is null/invalid; preserve configured numeric/date type context |
| `referential_integrity` | Membership in the declared reference column using documented string normalisation; null and empty remain distinct |
| `referential_integrity_composite` | Membership in an ordered tuple of reference values; null and empty remain distinct; never sort components |
| `required_columns` | Error severity is a schema gate; warning severity applies the resolved dataset-wide outcome |
| `schema_matches` | Strict metadata checking or castability checking as configured, including required/extra columns and bounds |
| `valid_schema_columns` | Check effective user-column names against the reference list; provenance is not part of user schema |
| `type_check` | Preserve explicit integer width/fraction bounds, boolean restrictions, date rules and null policy by target type |
| `date_format` | A non-null value must parse with at least one configured format; null passes |

Preserve `rename_columns` and `schema_rename`, including ordered application, inline/file mappings, excluded keys and source-key selection. Compile mappings once per run. Reject duplicate output column names. Support the existing `sum`, `count`, `avg` statistics over all validated rows: count is non-null count; empty sum/average are zero for compatibility; invalid numeric casts are excluded consistently. Document floating-point reduction tolerance; do not require bit-identical sums across different parallel reduction orders.

### Types, expressions and portability

A typed semantic layer must separate Arrow physical storage from check conversion policy. Without an explicit input schema, preserve existing Float64 numeric comparison semantics for legacy numeric rules, including their precision limitations. With an explicit schema, range and column comparisons use declared types: exact integers/decimals, timestamps as instants, strings by specified lexical order. Reject incompatible or potentially lossy mixed-type comparisons at compilation unless the configuration declares the conversion. Exact decimal/int keys for uniqueness must never be routed through Float64. Integer type checks reject fractions, overflow and booleans. Numeric/date type checks permit null where current semantics do; string and boolean type checks reject null. Boolean type checks accept actual booleans, not arbitrary truthy strings; schema conversion can explicitly produce those booleans first.

A numeric YAML literal compared with a string-typed column (raw-string CSV, for example) parses the cell as an exact decimal at evaluation. This applies to `when` comparisons and `in`, and to `equals`, `not_equals`, `all_equal` and `valid_values`. A cell that is not a number makes that rule unevaluable and is counted; it is never silently treated as not equal or not applicable. A quoted string literal still compares as exact text. `valid_range` keeps its own contract: a non-numeric cell is a violation.

Regex uses Go's supported regular-expression syntax. Reject unsupported constructs at plan time with the offending rule code; do not reinterpret them. Date format translation preserves supported existing percent directives and order of format attempts; reject unsupported directives rather than applying Go layouts literally. The migration compatibility corpus must enumerate formats/patterns used by production rule files and prove them on both implementations.

### Global checks without spilling

For exact uniqueness, accumulate exact keys with occurrence state saturated at two, then replay the dataset to mark every duplicate, including its first occurrence. Hash collisions must be resolved by exact key equality; hash-only answers are forbidden. Fail when the key state exceeds budget. Nulls can be marked without storing them as unique keys.

Large reference indexes have the same budget constraint. Project only required columns, share compatible indexes between rules, and build each once per run. No unbounded `to_list` equivalent.

Metadata schema gates run before row evaluation. Value-level castability gates require a preflight pass before row outputs. Stream detailed schema diagnostics while counting failures; do not retain every offending value. Warning-level whole-dataset checks also precompute their dataset-wide result so chunk boundaries cannot change which rows warn.

All preflight/replay plans require stable replayable sources. A source that cannot satisfy this is rejected rather than producing batch-local approximations.

### Deliberate migration changes

| Existing behaviour | New contract |
|---|---|
| Composite reference keys sort field values | Ordered tuple membership; swapped components no longer match |
| Reference normalisation conflates null and empty string | Distinct keys; reference null matches input null only under the explicit membership semantics |
| Some multi-column checks skip missing columns | Missing operands fail compilation/schema validation |
| Warning counts omit warnings on error-bucket rows | Counts and streamed issues include every violated warning rule |
| Delimited code/message strings are the internal result model | Structured issues and numeric rule IDs internally |
| Full detailed JSON stores every row index in memory | Versioned summary plus streamed issue records |
| Range handling can accept non-finite numeric values | Non-finite non-null numeric values fail range validation |
| Implicit backend regex/date acceptance | Unsupported syntax is diagnosed explicitly |
| Legacy coercion-based numeric comparison | Preserved without a declared schema; explicitly typed input uses exact compatible typed comparisons |
| `equals`/`all_equal` flag rows that match the value; `not_equals` flags rows that differ | Every check is an assertion. `equals` flags rows that differ from the value or are null; `not_equals` flags rows equal to it, and null passes; `all_equal` flags rows where any listed column differs or is null. Existing rules invert: swap `equals` for `not_equals`. An old `all_equal` (all columns equal the value) becomes an `all`-composed `when` plus a failing check |

These differences must have behavioural migration fixtures and release notes. Do not add a permanent legacy engine mode or compatibility aliases to preserve defects. Other differences discovered during implementation require a documented decision before release, not silent changes to expected outputs.

### Data-engineering assessment and minimum additions

The current checks cover row completeness, validity, uniqueness, reference membership and some cross-column consistency. They are a useful foundation, but not sufficient alone for reliable ingestion of untrusted raw records. `type_check` asks whether values are castable; it does not establish a persistent typed schema, preserve failed-conversion provenance, or distinguish missing/null/invalid states.

| Dimension | Existing capability | Decision |
|---|---|---|
| Structural integrity | CSV loading and schema gates | Explicit CSV/JSON framing, duplicate fields, size limits and stable schemas |
| Completeness | `not_null`, required columns | Add `not_blank`, `is_present` and independent schema `required`/`nullable` |
| Type validity | Castability | Schema-directed exact conversion and recoverable rejects; never silently null bad input |
| Domain validity | Sets, regex, range, lengths | Retain with typed/null/Unicode semantics |
| Key integrity | Single/composite uniqueness | Exact cross-file results; budgeted two-pass processing |
| Reference integrity | Membership | Fix ordered keys/null distinction; share bounded indexes |
| Conditional consistency | No general applicability condition | Add bounded declarative `when` conditions |
| Dataset completeness/quality | Display statistics, no assertions | Add counts, fractions and numeric aggregate thresholds |
| Temporal validity | Date parsing, column comparisons | Typed timestamps and explicit-bound `timestamp_window` |
| History/reconciliation | No cross-run baseline | Not in this release; do not claim drift/anomaly detection or cross-dataset reconciliation |

Required additions:

1. `not_blank`: fail for missing/null strings or strings empty after Unicode whitespace trimming for the check. Do not modify output values. Invalid conversion remains unevaluable and already fails ingestion.
2. `is_present`: fail only for an absent field/path; explicit null is present.
3. `timestamp_window`: inclusive configured `min` and/or `max` timestamp bounds with explicit offset/timezone. Null passes unless separately forbidden. Compile bounds once; do not read the wall clock per row. Freshness policies can supply a fixed cutoff through generated configuration. `compare_columns` covers start-before-end.
4. `when` on row rules: typed leaf `{column, operator, value}` or composition `{all: [...]}`, `{any: [...]}`, `{not: ...}`. Operators: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `is_null`, `is_present`; unary operators omit `value`. Bound expression depth/node count. Conditions select applicable rows; checks remain assertions evaluated only for applicable rows. Null/missing condition comparisons are false; explicit null/presence operators inspect state. Invalid operands propagate unevaluable state unless the Boolean result is already determined. Count false conditions as not applicable, not successful evaluation.
5. `dataset_rules`: assertions over bounded aggregate state, independent of row bucketing.

Conditional global/schema checks are rejected initially. Conditional uniqueness changes which rows enter the key set and needs a separately specified feature, not accidental batch-local behaviour.

#### Dataset assertions

Each has a unique `code`, `level`, `metric`, optional `column` or `rule_code`, and inclusive `min`/`max` bounds or `equals`. Row and dataset codes share one unique namespace. Require at least one bound; reject incompatible metric/operand combinations.

- `row_count`: all successfully framed records, including error rows.
- `missing_count`, `null_count`, `blank_count`, `conversion_error_count`: explicit field-state counts. Blank means present non-null empty/whitespace string, not missing/null.
- `missing_fraction`, `null_fraction`, `blank_fraction`, `conversion_error_fraction`: corresponding count divided by `row_count`.
- `rule_failure_count`, `rule_failure_fraction`: distinct rows violating a configured row rule; fraction denominator is applicable and evaluable rows for that rule, not all rows. Expose numerator, denominator, not-applicable and unevaluable counts.
- `sum`, `min`, `max`, `avg`: valid non-null declared numeric values over all rows, including rows failing unrelated rules. Expose contributing/excluded counts. Use checked sufficiently wide integer/decimal accumulation; fail on overflow rather than wrap or silently convert to float. Float `equals` assertions require an explicit absolute tolerance.

For nonintegral integer/decimal averages, keep an exact sum/count pair internally and compare thresholds by checked cross-multiplication; report the pair as the exact value rather than inventing a rounding policy. Decimal sums carry declared scale; checked wider accumulators may be used internally. Overflow remains an operational error. Legacy display-only stats retain their previously specified float behaviour.

An empty denominator or aggregate with no contributing values is `undefined`; the assertion fails with `NO_OBSERVATIONS`. `row_count` and count metrics remain zero. This deliberately differs from legacy display-only empty-average zero. A zero-row file is not healthy merely because no row check fired.

```yaml
rules:
  - code: ACTIVE_ACCOUNT_REQUIRED
    level: error
    column: account_id
    when: {column: active, operator: eq, value: true}
    checks:
      - type: not_blank

dataset_rules:
  - code: DATASET_NOT_EMPTY
    level: error
    metric: row_count
    min: 1
  - code: AMOUNT_PARSE_RATE
    level: error
    metric: conversion_error_fraction
    column: amount
    max: 0.001
```

Dataset failures emit dataset-scope issues, cause overall validation failure at error severity, and leave row buckets unchanged. Nothing is published until assertions finish. If a schema gate rejects the input, assertions requiring a complete scan are `not_evaluated`, not fabricated as zero.

#### Deferred quality features

Historical volume/distribution drift, anomaly detection, percentiles, approximate distinct metrics, cross-dataset control totals, automatic repair, fuzzy matching, locale guessing, array expansion and arbitrary arithmetic expression languages are not required. They need additional semantics/state/product decisions; do not substitute authoritative-looking heuristics.

No business rule is silently installed. Document an explicit recommended pipeline for nonempty input, required keys, conversion-error thresholds and timestamp bounds. An unconfigured dimension was not checked.


## 10. Output contracts and publication

### Run layout

With an explicit output URI, each run uses a unique child location under the configured destination:

```text
<output>/<run-id>/
  inputs.ndjson
  rules.json
  pass/part-000001.parquet
  warn/part-000001.parquet
  error/part-000001.parquet
  issues/part-000001.ndjson
  artifacts.ndjson
  summary.json
  manifest.json
```

CSV replaces the bucket extension when selected. Empty buckets have zero parts and explicit zero counts. `rules.json` stores rule codes, messages, severities and definitions once; rule metadata is not repeated on every row. Output parts roll at a configurable bounded target (default 128 MiB encoded bytes, with bounded writer-buffer overshoot). Parquet row groups have their own managed-memory bound.

CSV encoding is reversible: `\N` denotes null; non-null string backslashes
are doubled before ordinary CSV quoting. Thus an empty string remains empty,
and literal `\N` becomes `\\N`. Consumers first parse CSV, distinguish the
null sentinel, then unescape string backslashes using `rules.json`'s
`csv_null` and `csv_string_escape` metadata. Binary values use base64;
decimals retain declared scale; timestamps use UTC RFC3339Nano.

Reserved provenance columns use the `__valet_` prefix: `source`,
`source_ordinal`, `row` and `global_row`. Parquet keeps their typed values.
Parquet output uses Snappy, disables optional column statistics/dictionaries
to bound retained state, and caps row groups/part metadata separately from
the byte target.


- Bucket rows contain transformed user columns and reserved provenance fields; they do not duplicate all diagnostic messages.
- Issue records contain run/source/row identity, rule code, severity, origin (`rule`, `ingest`, or `dataset`) and check location where available. Each record represents one violated rule for one row, or one ingestion failure per field/code. Schema/dataset issues have no fabricated row number. Do not include raw values.
- The stdout summary and artifact `summary.json` use the current report contract (`report_version: 2`) with terminal status, total/bucket counts, per-rule and ingestion counts, unevaluable counts, bounded dataset/schema results, statistics, duration, passes, managed-memory peak, coverage/trust/provenance and output policy. `manifest_uri` is omitted without artifact publication.
- `artifacts.ndjson` lists written parts, sizes and integrity metadata incrementally, including reject records when conversions fail. `manifest.json` references it with count/digest rather than holding an unbounded part list.
- For a completed row-validation run, `pass + warn + error = total`; when publishing issues, per-rule counts equal corresponding streamed row issue counts. Dataset issues are counted separately and may cause exit `1` even if every row passes. A schema-gate rejection reports rows scanned and schema failures separately; it must not claim those rows passed row validation.

Version report/configuration contracts independently of executable versions. Preserve full 64-bit row identities in JSON numbers with a documented consumer precision warning; consumers unable to represent them exactly must use a lossless JSON decoder.

### Commit and failure behaviour

For artifact runs only, `manifest.json` marks the terminal, consumable artifact result. Publish it after every output has closed successfully, requested file summary output has been atomically published and its containing directory synced, and the summary/artifact metadata is durable. It records `completed` or `schema_rejected`; completed validation with error rows is still a valid completed result, not an operational failure. Stdout-only runs instead return the aggregate summary and never claim artifact commitment.

Local publication syncs each temporary file, publishes without replacement, and syncs each changed containing directory; the manifest directory sync is the commit boundary. If directory sync fails after publication, remove the newly published target and sync that removal before returning failure. Windows directory flush handles require write access. S3 create-if-absent PUTs carry a random write identity and content digest and reconcile ambiguous responses with a fresh bounded context independent of request cancellation. Multipart creation disables transparent retry, pre-reserves cleanup capacity, and cleans unknown uploads for the run-owned key; ambiguous completion verifies identity, length, and a streamed content digest. This is an application commit protocol, not atomic multi-object storage. Consumers must start from the manifest and verify referenced metadata, not assume a listing implies completeness.

On pre-commit operational failure/cancellation, abort outstanding multipart uploads, close files and leave no success/terminal-result manifest. Best-effort failure metadata may be written, but must not resemble a completed result. Partial artifacts can remain in the unique run location; document lifecycle/cleanup procedures and never delete pre-existing user output. Completion retains a managed-memory reservation through publication or abort, while each metadata/storage operation also keeps its normal bounded reservation. After commit, stdout failure returns exit `3` without invalidating or deleting the manifest. No unbounded buffering is permitted merely to avoid partial artifacts.

Bucket rows preserve deterministic source order. Row issues follow source order, then ingestion field declaration order, then configured rule order; dataset issues follow at finalisation in configured order. This ordering must not require retaining the complete output. All encoders are bounded, including slow output, compression and multipart queues.

## 11. S3 and security

- Support explicit objects and deterministic prefix discovery, paginated listing, streaming GET, conditional range GET for Parquet, and bounded multipart output.
- Configure endpoint, region, path-style addressing and profile independently for source/reference/output storage where needed. Default to AWS endpoints and certificate verification. Do not automatically disable TLS validation for a custom endpoint.
- Use the standard AWS SDK credential chain, including temporary credentials and provider refresh. Do not store static keys in pipeline documents, manifests or logs.
- Custom CA trust uses documented platform/SDK configuration. An explicit HTTP endpoint is allowed for local integration environments, not selected automatically for remote services.
- Support default bucket encryption and AWS SSE-S3/SSE-KMS output options through explicit sink settings. Never promise a service supports AWS encryption/authentication features merely because it accepts S3 requests.
- Bound SDK concurrency, multipart part sizes and retry buffers. Use finite SDK retries within the run context only for idempotent operations. Conditional object PUT and multipart completion reconcile ambiguous success; multipart creation uses one attempt and explicit cleanup. Do not add automatic whole-job retries.
- Protect logs from credential/header disclosure; redact query strings on diagnostic URLs. Report rule codes and provenance without dumping sensitive rows.
- Run the real release and race CLI suites against independent MinIO and RustFS containers, including owned versioned buckets. Selected skips or missing executions fail the compatibility gate. Optional AWS tests use isolated prefixes and scoped credentials; they are not release prerequisites or substitute evidence. Compatibility claims list tested service versions/features and limitations.

## 12. Testing and benchmark specification

### Placement

Package tests, fuzz tests and ordinary benchmarks live beside production code as `*_test.go`. Prefer `package name_test` for behaviour observable through exported package APIs; use `package name` when access to internal details is genuinely needed. Do not export implementation details just for tests. Both arrangements are compatible with `internal/`.

Small package-specific fixtures live in that package's `testdata/`. Cross-package CLI/file/S3 scenarios live in `integration/`. Shared batch generators and verification sinks are narrowly scoped test-support packages, not a generic utility framework. Benchmarks/soaks that exercise the full application belong under `benchmarks/`.

### Deterministic batch generator

The generator implements the same batch-source boundary as real adapters. It allocates only a bounded current batch and any explicitly allowed in-flight batches. Values depend on a fixed seed and global row ordinal, not a single repeated batch.

Required scenarios include narrow numeric data; wide strings with varying lengths; null/empty/Unicode values; sparse and dense failures; late failures; cross-batch/file duplicates; bounded and growing cardinality; reference membership; and oversized records that trigger limits. Known outcomes are derived independently from the generator definition, not from the validator implementation.

The 100 GiB acceptance workload measures **logical Arrow data**, not physical CSV or compressed Parquet size. Define volume as the sum of data, offset and validity buffer lengths for fresh generated input batches, counting shared buffers once within each batch, excluding allocator capacity and rule/output scratch. Record row count, schema and average width too. Repeated constant/dictionary payload must not be presented as equivalent to generating fresh high-volume values.

Use the actual compiler and engine with a bounded verification sink that accumulates bucket/per-rule counts and an order-sensitive digest over provenance, bucket and issue identities. Small fixtures independently establish exact records; scale runs establish counts and boundary sentinels. A second identical run can check determinism but is not an independent correctness oracle.

### Test tiers

| Tier | Workload | Execution policy |
|---|---|---|
| Unit/fuzz | Small batches and small fixtures; semantic boundaries and ownership | Ordinary `go test ./...` |
| Resource | Generated volume several times the budget; constrained queues; slow sinks; growing global state | Regular CI with bounded duration |
| Integration | Real CLI, CSV/Parquet encoders/decoders, local storage, pinned MinIO and RustFS containers | Required two-backend integration matrix; optional AWS runs reported separately |
| Scale | At least 100 GiB generated Arrow data through the real engine | Explicit scheduled/manual acceptance job |
| Adapter soak | Generated CSV stream and rolling bounded Parquet objects through actual readers/writers | Explicit acceptance job; no giant retained fixture |

The scale/adapter suites must be opt-in, using explicit test selection/build tags and configured volume. They must not execute as part of ordinary `go test ./...`. Large generated input remains ephemeral; do not commit or download 100 GB test artifacts.

### Resource and correctness assertions

- Managed reservations never exceed the configured budget. Assert graceful `ResourceLimitError` at a deliberately insufficient budget and exact cleanup on error/cancellation.
- Assert release of batches on successful, failed, cancelled and slow-sink paths; use Arrow checked allocation facilities where applicable plus accounting for non-Arrow state.
- Prove memory does not accumulate with total volume for fixed-cardinality row-local workloads. Compare sustained windows and runs at increasing volume, not RSS returning to zero after each batch.
- Record process peak RSS separately using OS-specific measurement. On a controlled runner, compare otherwise identical 10 GiB and 100 GiB fixed-state runs; the scale gate allows no more than 64 MiB sustained-RSS increase after warm-up. Record sampling interval and allocator/runtime behaviour. This is an acceptance target, not a measured result; a failure requires investigation, not automatic tolerance widening.
- Global high-cardinality no-spill cases are expected to fail at the budget; low-cardinality global cases must complete and flag first and later duplicates across batches/files.
- A late parse error, changed replay source or failed final upload must not publish a terminal-result manifest.
- Verify malformed records, missing columns, nulls, decimals, fractional integers, non-finite values, date formats, regex incompatibilities, schema-gate timing, warning/error overlap and structured reports.
- Verify local/S3 cross-format round trips and semantic parity independently of the synthetic source. Direct Arrow injection does not test parsing, compression, network buffering or actual output encoding.
- Include bounded read/decoder limits for malicious size declarations and large Parquet pages; a small requested batch does not prove bounded decoding.

### Comparative measurements

Run the existing four-rule workload and representative production rules against the Python release and Go release using equivalent input, output and semantics. Separate documented semantic corrections. Measure fresh-process help, empty/tiny validation, sustained validation, allocations/managed peak, RSS, output time and binary/archive size. Record hardware, OS, versions, compression, worker count, cache conditions and repetitions. No language-performance claims based only on toy arithmetic or a mocked sink.

### Precision of tests specified before implementation

Specify behavioural contracts now: fixtures, exact typed values, row identities, rule outcomes, issue counts, exit categories, ordering and publication invariants. These expectations are independent of package internals and must not be generated by the implementation under test.

| Certainty | Specify now | Do not freeze prematurely |
|---|---|---|
| Exact, normative | Input/output examples, missing/null/invalid states, precedence, key equality, error categories, commit protocol | Log sentences, private helper names, allocation count, exact part boundaries |
| Exact invariants | Equivalent format results, ownership, bounded state and ordering | Arrow buffer layout, hash algorithm, goroutine count |
| Provisional until measured | RSS allowance, throughput, startup and package-size targets | A universal hard RSS cap or assumed speedup over Python |
| Dependency-sensitive | Codec support, regex/date syntax, conditional reads and platform cancellation | Assuming library documentation proves the application path |

The 64 MiB comparative-RSS allowance above is a candidate threshold for the initial controlled runner, not a portable release promise. Calibrate it once with recorded evidence before making it a required gate. Managed-budget enforcement, exact outcomes and no accumulation with total volume remain requirements.

A developer must not update expected results merely because the new engine produces a different answer. Differences require an implementation fix or an explicit semantic change in this document and migration notes.

### End-to-end harness

Build the real CLI once per integration job and launch it as a child process. Each scenario gets a fresh temporary run root or unique object prefix, real configuration and small fixtures. Do not mock the compiler, Arrow executor or encoders.

Assert exit status, manifest presence/status, decoded bucket rows in order, exact issue identities/counts, summary invariants and provenance. Parse outputs with independent CSV/JSON readers. Read Parquet output back with Arrow Go's `pqarrow`, not the CLI's own source package, and assert exact typed values: decimal coefficient, precision and scale, nulls, identifiers and provenance. The test suite needs only the Go toolchain.

Normalise only run IDs, temporary destinations, durations and wall-clock timestamps. Never normalise away row order, schema, numeric precision or issue identity. Assert stable machine error categories and source/field location, not incidental prose.

Use real fault paths where feasible: truncated input, a local fault proxy before MinIO, permissions and process cancellation. Package-level injected allocation failures supplement this suite but are not relabelled as end-to-end storage proof. Linux signals and Windows process control have platform-specific mechanisms with common cleanup/publication assertions.

### Canonical acceptance fixtures

#### Fixture A: raw row rules and overlapping severities

Input is raw CSV with the empty-string null marker:

```csv
id,amount,country
a,10,GB
b,-1,GB
c,20,
d,-1,
,5,GB
```

Rules in order: `ID_REQUIRED` error/not_null on `id`; `AMOUNT_RANGE` error/valid_range min 0 on `amount`; `COUNTRY_REQUIRED` warn/not_null on `country`.

| Source row | Bucket | Issue codes in order |
|---|---|---|
| 1 | pass | none |
| 2 | error | AMOUNT_RANGE |
| 3 | warn | COUNTRY_REQUIRED |
| 4 | error | AMOUNT_RANGE, COUNTRY_REQUIRED |
| 5 | error | ID_REQUIRED |

Exact summary: total 5, pass 1, warn 1, error 3; rule counts `ID_REQUIRED=1`, `AMOUNT_RANGE=2`, `COUNTRY_REQUIRED=2`; five issues; exit `1`; completed manifest. Tiny-batch and delayed-worker variants must preserve these results and ordering.

#### Fixture B: exact conversion and recovery

```csv
id,amount,units
0007,12.30,2
0008,oops,2
0009,1.001,3
0010,5.00,1.5
0011,,4
```

Schema: `id` string; `amount` decimal(10,2), nullable; `units` int64. Empty maps to null. `AMOUNT_NONNEGATIVE` applies typed valid_range min 0.

Exact result: pass rows 1 and 5; error rows 2, 3 and 4; no warnings. `INGEST_TYPE` issues are `(2, amount)`, `(3, amount)`, `(4, units)`. `AMOUNT_NONNEGATIVE` has zero violations and two unevaluable rows. Preserve `0007` and decimal 12.30 exactly; row 5 has a true null amount, not an invalid amount. Three reject records retain `oops`, `1.001` and `1.5`. Exit `1`, completed manifest. With `input.on_type_error: abort`, exit `2`, no terminal-result manifest.

#### Fixture C: JSON presence and precision

NDJSON input, with an equivalent JSON-array variant:

```json
{"id":9007199254740993,"note":null}
{"id":9007199254740994}
{"id":9007199254740995,"note":""}
```

Schema: required non-null int64 `id`, optional nullable string `note`. Rules in order: `NOTE_PRESENT` error/is_present and `NOTE_NONBLANK` warn/not_blank on `note`.

Exact result: row 1 warn (`NOTE_NONBLANK`); row 2 error (`NOTE_PRESENT`, `NOTE_NONBLANK`); row 3 warn (`NOTE_NONBLANK`). Total 3, pass 0, warn 2, error 1. IDs retain exact values. Explicit null, missing and empty remain distinct field states. Rule counts are 1 and 3 respectively; no ingestion failure; exit `1`.

#### Fixture D: global keys and corrected reference semantics

Two sources contain keys `[A, B]` and `[C, A]`. With `is_unique`, global rows 1 and 4 fail; rows 2 and 3 pass. Batch/file boundaries must not change that result.

Reference tuples are `[("US", "CA"), (null, "x")]`; input tuples are `[("US", "CA"), ("CA", "US"), ("", "x"), (null, "x")]`, with empty preserved by configuration. Composite membership passes rows 1 and 4, fails rows 2 and 3. Neither sorted components nor null/empty equivalence is allowed.

#### Fixture E: dataset failure without row reclassification

Header-only CSV with an explicit schema and `DATASET_NOT_EMPTY` (`row_count min 1`) completes with all row counts zero, one dataset error, exit `1`, and a completed manifest with overall validation failure. Without that assertion it completes with exit `0`. Zero-byte CSV without configured headerless column names is an input error, not a header-only dataset.

For Fixture B, amount `conversion_error_fraction` is 2/5. A maximum of 0.3 fails once at dataset scope without changing buckets. An aggregate over no contributing values is `undefined` and fails with `NO_OBSERVATIONS`, not numeric zero.

### End-to-end acceptance catalogue

Identifiers are stable acceptance references, not claims that tests already exist.

| ID | Scenario | Required observation |
|---|---|---|
| E2E-01 | Fixture A through CLI to CSV and Parquet | Exact oracle, summary/issue parity and row provenance |
| E2E-02 | Fixture B, including abort mode | Exact decimals/identifiers, invalid-versus-null, recoverable rejects, no cascade of failures |
| E2E-03 | Fixture C in JSON and NDJSON | Exact large integers and missing/null/empty semantics |
| E2E-04 | Quoted CSV delimiter/newline/quote, CRLF and BOM | Exact decoded values and logical row numbers; malformed closing quote aborts |
| E2E-05 | Duplicate JSON keys, malformed trailing input, size/depth limit, selected non-scalar | Structural failures abort; a valid non-scalar conversion is a row reject; bounded decoder |
| E2E-06 | Required CSV header absent, unexpected JSON key, optional absent field | Schema rejection versus row failure versus permitted missing state |
| E2E-07 | Late castability-gate failure; warning whole-dataset check | No row output published after gate rejection; warning applies globally |
| E2E-08 | Fixture D uniqueness across files/batches | First and later duplicate fail; null fails; growing state fails at budget without spill |
| E2E-09 | Fixture D composite membership | Ordered keys and distinct null/empty; exact foreign-key outcomes |
| E2E-10 | Conditional rule with true/false/null/invalid operands | Applicable, not-applicable and unevaluable are distinct; invalid condition already fails ingestion |
| E2E-11 | Fixture E and threshold boundaries | Dataset failure independent of buckets; inclusive bounds; undefined metric handling |
| E2E-12 | Leap dates, timezone offsets, explicit windows, ambiguous local time | Calendar/instant correctness; no host-timezone or per-row clock dependence |
| E2E-13 | Inline/local/S3 renames and relative paths | Effective-schema binding; collisions/missing operands diagnosed; correct mapped values |
| E2E-14 | Local→MinIO, MinIO→local, MinIO→MinIO; file formats | Local-fixture parity through real signing, listing, GET/range GET and uploads |
| E2E-15 | Explicit/prefix ordering, paginated discovery, output overlap | Deterministic order; selected objects once; overlap rejected |
| E2E-16 | Changed input between passes/range requests | Input-change failure; no mixed-version result or terminal manifest |
| E2E-17 | Slow sink and delayed worker | Same ordered rows, no omissions/duplicates, bounded backpressure |
| E2E-18 | Final upload/write failure and interruption | Operational/interruption exit; no result manifest; abort uploads; preserve unrelated data |
| E2E-19 | Unknown check, duplicate rule/YAML key, unsupported pattern/date syntax | Plan-time diagnosis; no silent fallback or success output |
| E2E-20 | Budget failure in reference load, decoder, writers and uniqueness | Resource-error category, bounded cleanup, no terminal manifest |
| E2E-21 | Gzip CSV/JSON and required Parquet codecs | Actual codec paths, semantic parity, truncated/corrupt stream detection |
| E2E-22 | Every built-in against the compatibility corpus | Preserved cases match independent expected behaviour; corrections match the new contract |
| E2E-23 | Actual release targets | Core local scenarios without Python/native shared-library prerequisites |
| E2E-24 | Small isolated AWS runs | Credentials, conditional reads, upload and supported encryption; MinIO alone is insufficient |

E2E-20 is supplemented by deterministic package-level reservation tests; byte-exact failure thresholds need not match across machines. E2E-24 explicitly reports not run when credentials are unavailable, never passed.

The 100 GiB synthetic-source job is an engine scale test. It is not a substitute for real parsing/encoding, S3 integration or adapter soaks.

### Coding against this contract

For each implementation slice, select acceptance IDs, add small fixtures and independently specified expected semantic records, then observe the narrow missing-behaviour test fail. Implement the real path and pass its CLI/integration scenario. Keep unit tests focused on boundaries E2E cannot isolate cheaply.

Do not merge fake implementations or permanently skipped acceptance tests and call a feature complete. Track unimplemented IDs in delivery status until their real path exists. Final release requires every applicable acceptance ID and resource/scale gate, with explicit platform and AWS evidence.


## 13. Migration and delivery gates

1. **Freeze behavioural contracts:** inventory all built-in checks, transforms, stats and production YAML usage; create language-neutral small input/expected-output fixtures with explicit corrected-defect cases. Python is a reference, not the oracle for known defects.
2. **Prove the dependency/runtime constraints:** real pure-Go CSV/Parquet/JSON/NDJSON read paths, CSV/Parquet/issue output, Arrow ownership and AWS/MinIO object access on release targets. Establish decoder/codec memory limits before promising bounded execution.
3. **Deliver the complete local executor:** all scoped rule families, global replay, resource errors, cancellation, ordered sinks and versioned reports. No unsupported built-in silently skipped.
4. **Deliver S3 equivalence:** main/reference/mapping inputs and result outputs, bounded uploads, conditional replay, authentication and failure publication behaviour.
5. **Pass semantic, integration and scale acceptance:** generated 100 GiB stream, actual adapter soaks, no-spill global failure cases and target-OS execution. Publish measurements and known limits.
6. **Release and document:** binaries, checksums, dependency/license information, configuration reference, output schemas, custom-build check instructions and migration notes. Leave the Python release usable during evaluation; no automatic replacement of users' Python installation.

Release readiness requires the applicable gates above, not merely successful compilation or a generated-stream benchmark. Required object-store proof is the independent MinIO/RustFS container matrix; AWS-specific verification is outside scope and remains unverified. Native Linux arm64/Windows execution must be recorded separately from local compatibility tests and cross-compilation.

## 14. References

- [Apache Arrow Go](https://github.com/apache/arrow-go): native Go implementation and buffer ownership.
- [Arrow Go Parquet integration](https://pkg.go.dev/github.com/apache/arrow-go/v18/parquet/pqarrow).
- [AWS SDK for Go v2 configuration](https://docs.aws.amazon.com/sdk-for-go/v2/developer-guide/configure-gosdk.html).
- [Go runtime memory-limit semantics](https://pkg.go.dev/runtime).
- [Google Go style decisions](https://google.github.io/styleguide/go/decisions.html): package and test naming.
- [Go testing package](https://pkg.go.dev/testing): colocated tests, external test packages, benchmarks and fuzzing.
