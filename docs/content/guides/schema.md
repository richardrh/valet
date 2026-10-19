---
title: Schema-only Parquet checks
weight: 15
type: docs
prev: guides/configuration
next: guides/validation-modes
---

Use `valet schema` to check **Parquet column names and data types without
reading the row values**. It works with local files and S3 objects and does not
rewrite the data.

Only Parquet supports this metadata-only command. CSV and JSON do not carry a
reliable typed schema in a footer; use full validation for those formats.

Use `valet validate` when you also need to check values, nulls, formats,
uniqueness, references or dataset totals. A compatible schema does not mean
those checks passed.

## Run a schema check

Use a pipeline that declares the schema you expect. The repository includes
[`examples/parquet-schema.yml`](https://github.com/richardrh/valet/blob/main/examples/parquet-schema.yml):

```bash
valet schema -p examples/parquet-schema.yml -i input.parquet
```

The same command accepts an S3 object:

```bash
valet schema -p examples/parquet-schema.yml -i s3://my-bucket/data/input.parquet
```

S3 uses the normal AWS SDK credential chain and the pipeline's `source.s3`
settings. Output settings and output credentials are not needed for this command.
Configuration can be YAML or JSON. Save stdout if you want to keep the report:

```bash
valet schema -p examples/parquet-schema.yml -i input.parquet > schema-report.json
```

There is no `-o` requirement, output bucket, `summary.json` or completion
manifest for a schema-only check.

For readable missing-column and type differences instead of JSON:

```bash
valet schema -p examples/parquet-schema.yml -i input.parquet --format text
```

Both presentations check the same metadata and use the same exit codes. JSON
includes canonical `differences` with expected/actual type attributes; text
groups those differences by source and lists checks that were not evaluated.

## Declare the expected columns and types

This rule requires three named columns with the listed logical types:

```yaml
input:
  type: parquet

rules:
  - code: EXPECTED_SCHEMA
    level: error
    checks:
      - type: schema_matches
        mode: strict
        columns:
          record_id: string
          location: string
          amount: int64
        require_columns: true
        allow_extra_columns: true
```

- Use `mode: strict` for a metadata-only type comparison. The default
  `schema_matches` mode is `castable`, which needs row data to prove conversion.
- `require_columns: true` rejects a missing listed column.
- Set `allow_extra_columns: false` if the listed columns must be the complete
  column set.
- Use `level: error` to make a schema mismatch return exit `1`. Warning-level
  mismatches appear in `schema_issues` but do not make the command fail.

For a check that only requires names, without asserting types:

```yaml
rules:
  - code: COLUMNS_PRESENT
    level: error
    checks:
      - type: required_columns
        columns: [record_id, location, amount]
```

Alternatively, declare types under `input.schema.columns`. In schema-only mode,
Valet compares them with the file's actual types rather than attempting any
conversion. Mark each required column with `required: true`, and use
`input.schema.extra_fields: error` to reject undeclared fields. Declared decimal
precision/scale and timestamp parameters are also compared. Configured column
renames are applied before name matching.

The bundled pipeline uses `input.schema` with `extra_fields: ignore`: schema
inspection permits additional fields, while full validation selects only the
three declared user columns. This lets the quickstart revalidate generated
Parquet data without copying its existing `__valet_*` provenance columns; the
new run writes fresh provenance.

If you provide neither an expected input schema nor schema rules, a single-file
check only establishes that its supported footer/schema metadata can be read.
For multiple files without `input.schema`, Valet also checks their schemas for
consistency against the first file. Declare your expected schema when you need
a specific data contract.

## Which checks run?

| Configuration or check | What `valet schema` establishes |
|---|---|
| `input.schema.columns` | Declared types match actual file types; required columns are present |
| `input.schema.extra_fields: error` | No undeclared input columns are present |
| `required_columns` | The named columns are present |
| `schema_matches`, `mode: strict` | The configured name/type schema and required/extra-column policies match |
| `schema_matches`, `mode: castable` | Presence/extra-column policies only; value castability remains not evaluated |
| `type_check`, `not_null`, `not_blank`, ranges, patterns and dates | Not evaluated: these are value checks |
| Uniqueness, references, `valid_schema_columns`, statistics and dataset assertions | Not evaluated: these need row or reference data |

> [!IMPORTANT]
> A **required column** must exist. That does not mean every value is non-null.
> `nullable: false`, `not_null` and other value constraints require full
> validation. A string column's type also does not prove that every string
> contains a valid date or matches a pattern.

Schema-only inspection reads guarded footer/schema metadata for every selected
object. It does not decode pages, check their contents, verify full-file content
digests or prove that the payload is uncorrupted. A file can pass schema
inspection and still fail full validation.

## Can Parquet statistics check values without a scan?

Parquet producers can store per-row-group column statistics, such as null
counts and minimum/maximum bounds. Under an explicit trust policy, suitable
statistics could establish some conditions without decoding the values:

| Condition | Metadata evidence needed in every relevant row group |
|---|---|
| No null values | An exact null count of zero |
| Every non-null value is greater than `x` (including `> 0`) | A valid minimum/lower bound greater than the threshold |
| Every non-null value lies within an inclusive range | Valid lower and upper bounds inside that range |

Null handling is separate from min/max: a positive minimum does not establish
that there are no nulls. Missing or insufficient statistics mean **unknown**,
not pass. Logical types and ordering must be supported. If NaNs would fail a
floating-point rule, min/max alone cannot establish its success because those
statistics do not establish the absence of NaNs.

**Current behavior:** `valet schema` does not execute value checks; they remain
listed as not evaluated. `valet validate` reads values by default. Its explicit
`--parquet-stats trusted` policy can use suitable footer evidence in report or
failures-only modes, with separate metadata-claimed coverage and an evidence
ledger. See [output modes and metadata trust](../validation-modes/).

Parquet output omits column statistics by default. The optional bounded writer
policy emits supported fixed-width statistics without enabling reader trust.

### Trust and tampering

> [!WARNING]
> Footer statistics are claims made by the producer, not independent proof of
> the row values. A structurally valid footer can disagree with the payload.

A normal producer should write matching data and statistics. A producer bug,
an incorrectly rewritten file or deliberate tampering can leave stale or
false statistics. Bounds that look sensible and a readable footer are not
sufficient to detect that disagreement.

File identity and content integrity provide different guarantees:

- A pinned S3 version/ETag or local replay digest helps detect changes relative
  to the identity established for the run. It does not prove that the original
  statistics were truthful.
- A content hash verified against an independently trusted expected hash can
  detect changes to those bytes, but requires reading the content. A hash
  calculated from an already-bad file simply identifies that bad file.
- A trusted signature can authenticate the signed bytes or producer. It does
  not, by itself, prove that the producer calculated correct statistics.
- Page CRCs can detect covered accidental corruption. They are not
  cryptographic authentication and an attacker can recompute them.

Metadata-assisted runs explicitly record their producer-trust policy and
distinguish metadata-claimed outcomes from outcomes verified from decoded
values. Keep the default `off` policy for conservative value validation.
Inconclusive proofs cause a value scan; schema-only inspection instead reports
value checks as not evaluated and never silently starts a scan.

Footer-declared rows are not counted as value-checked rows. Group statistics do
not supply individual failing-row locations: groups that cannot be proven pass
are decoded for real diagnostics. A referenced evidence ledger survives log
suppression. Timing/resource breakdowns go to stderr diagnostics, not interleaved
with stdout JSON.

## Read the JSON report

These are selected fields from a compatible one-file report; the full report
also includes each object's fields, schema diagnostics and check outcomes:

```json
{
  "report_version": 2,
  "scope": "schema",
  "metadata_only": true,
  "validation": "not_evaluated",
  "compatible": true,
  "rows_checked": 0,
  "objects_checked": 1,
  "declared_rows": 3
}
```

- **`compatible`** is the schema outcome: no error-level schema mismatches were
  found. It is not the outcome of the row rules.
- **`checks`** lists metadata-check outcomes. **`schema_issues`** explains schema
  mismatches, including their source, column, severity and reason.
- **`not_evaluated`** lists checks and guarantees that require data access.
  Row rules in a shared pipeline are disclosed here, not silently passed.
- **`declared_rows`** is the count claimed by the footer. No rows were decoded
  or validated; **`rows_checked` stays zero**.

| Exit | Meaning |
|---:|---|
| `0` | Schema is compatible; inspect the report for warnings and skipped checks |
| `1` | An error-level schema mismatch was found; inspect `schema_issues` |
| `2` | Configuration, unsupported format, metadata, storage or resource failure; inspect stderr |

Exit `0` or `1` does **not** imply that a normal validation manifest exists.
Logs and operational diagnostics go to stderr, not into the JSON report.

## Check every file, not just a sample

Configure an ordered list under `source.uris`, or use prefix discovery:

```yaml
source:
  prefix: s3://my-bucket/data/
  filter: parquet
  recursive: true

input:
  type: parquet
```

Add this source to your schema pipeline, then run `valet schema -p pipeline.yml`
without `-i`. Supplying `-i` replaces the configured source with that one file
or object; it does not check the rest of the prefix.

## Run the value checks too

The bundled pipeline also contains `AMOUNT_NONNEGATIVE`, a row rule. The
schema command lists it as not evaluated. To execute it and publish row outputs:

```bash
valet validate -p examples/parquet-schema.yml -i input.parquet -o results/
```

Full validation scans the rows, performs any configured conversion and data
checks, and publishes a terminal manifest. See [structured output](../output/)
for consuming those results, or [quickstart](../../getting-started/quickstart/#try-schema-only-checks-on-parquet)
to try both commands using generated sample Parquet data.
