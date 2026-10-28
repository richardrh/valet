---
title: Schema-only Parquet checks
linkTitle: Parquet schema checks
weight: 15
type: docs
prev: guides/configuration
next: guides/validation-modes
---

`valet schema` checks **Parquet column names and types without reading rows**. CSV/JSON, and any value checks, need `valet validate`.

## Run a schema check

```bash
valet schema -p examples/parquet-schema.yml -i input.parquet
valet schema -p examples/parquet-schema.yml -i s3://my-bucket/data/input.parquet
valet schema -p examples/parquet-schema.yml -i input.parquet > schema-report.json
valet schema -p examples/parquet-schema.yml -i input.parquet --format text
```

- Uses [`examples/parquet-schema.yml`](https://github.com/richardrh/valet/blob/main/examples/parquet-schema.yml); S3 uses the AWS SDK chain and `source.s3`.
- No `-o`, `summary.json` or manifest. `--format text` groups differences by source.

## Declare the expected columns and types

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

| Setting | Effect |
|---|---|
| `mode: strict` | Compares types from metadata; default `castable` needs row data |
| `require_columns: true` | Missing listed column fails |
| `allow_extra_columns: false` | Listed columns must be the full set |
| `level: error` | Mismatch → exit `1`; `warn` only reports |

Names only:

```yaml
rules:
  - code: COLUMNS_PRESENT
    level: error
    checks:
      - type: required_columns
        columns: [record_id, location, amount]
```

`input.schema.columns` also works: types, decimal/timestamp parameters, `required: true` and `extra_fields: error` are compared after renames. The bundled pipeline uses `extra_fields: ignore` so `__valet_*` provenance columns pass.

With no declared schema, Valet only checks that metadata is readable and, for several files, consistent with the first.

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
> A **required column** must exist; its values may still be null. Pages are not decoded, so a passing file can still fail full validation.

## Can Parquet statistics check values without a scan?

Not with `valet schema`: value checks are listed as not evaluated. `valet validate --parquet-stats trusted` can use footer statistics in report or failures-only modes; see [output modes and metadata trust](../validation-modes/). Missing statistics mean unknown, not pass.

### Trust and tampering

> [!WARNING]
> Footer statistics are producer claims, not proof. Stale or tampered statistics look valid.

ETags, digests, CRCs and signatures do not prove statistics are correct. Keep the default `off` unless you trust the producer. [Validation modes spec](https://github.com/richardrh/valet/blob/main/docs/engineering/validation-modes-spec.md)

## Read the JSON report

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

| Field | Meaning |
|---|---|
| `compatible` | No error-level schema mismatch; not row-rule results |
| `checks` | Metadata-check outcomes |
| `schema_issues` | Mismatches: source, column, severity, reason |
| `differences` | Expected/actual type attributes |
| `not_evaluated` | Checks needing data, incl. row rules |
| `declared_rows` | Footer row count; `rows_checked` stays `0` |

| Exit | Meaning |
|---:|---|
| `0` | Schema is compatible; inspect the report for warnings and skipped checks |
| `1` | An error-level schema mismatch was found; inspect `schema_issues` |
| `2` | Configuration, unsupported format, metadata, storage or resource failure; inspect stderr |

No validation manifest is written. Logs go to stderr.

## Check every file, not just a sample

```yaml
source:
  prefix: s3://my-bucket/data/
  filter: parquet
  recursive: true

input:
  type: parquet
```

Run `valet schema -p pipeline.yml` **without** `-i`; `-i` checks only that one object.

## Run the value checks too

```bash
valet validate -p examples/parquet-schema.yml -i input.parquet -o results/
```

This runs row rules such as `AMOUNT_NONNEGATIVE` and publishes outputs and a manifest. See [structured output](../output/) or the [quickstart](../../getting-started/quickstart/#try-schema-only-checks-on-parquet).
