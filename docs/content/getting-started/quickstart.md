---
title: Quickstart
weight: 20
type: docs
prev: getting-started/installation
next: guides/configuration
---

## Inspect the input

```bash
cat examples/sample.csv
```

```csv
record_id,location,amount
REC001,North,5000
REC002,South,500
,East,2000
REC004,West,1500000
```

Row 3 has no `record_id`.

## Plan the run

```bash
dist/valet plan \
  -p examples/pipeline.yml \
  -i examples/sample.csv
```

Expect: CSV source, `string`, `string`, `int64` columns, four rules, two [passes](../../concepts/execution/#passes). `pipeline.json` gives the same plan.

## Validate

```bash
dist/valet validate \
  -p examples/pipeline.yml \
  -i examples/sample.csv \
  > demo-summary.json

echo "exit=$?"
```

Expect `exit=1` (one error-level issue). Stdout is the JSON [report](../../concepts/results/#report); without `-o` no files are written.

```json
{
  "status": "completed",
  "validation": "fail",
  "total": 4,
  "pass": 3,
  "warn": 0,
  "error": 1,
  "rows_scanned": 8,
  "passes": 2
}
```

## Read the result

```bash
jq '{validation, total, pass, warn, error, rule_counts}' demo-summary.json
```

Gate on Valet's [exit code](../../concepts/results/#exit-codes), not `jq`'s; see [CI automation](../../guides/ci/).

## Optional: write Parquet and detailed issues

```bash
dist/valet validate \
  -p examples/pipeline.yml \
  -i examples/sample.csv \
  -o demo-output \
  > artifact-summary.json

echo "exit=$?"
```

Still `exit=1`; also writes Parquet pass/error buckets, issues and a [manifest](../../concepts/results/#manifest).

`--output-mode failures|report` trims row files; see [output modes](../../concepts/results/#output-modes).

```bash
MANIFEST="$(jq -r '.manifest_uri' artifact-summary.json)"
jq . "$MANIFEST"
```

The issues NDJSON (listed in `artifacts.ndjson`) contains:

```json
{
  "row": 3,
  "global_row": 3,
  "line": 4,
  "code": "RECORD_ID_REQUIRED",
  "severity": "error",
  "scope": "row",
  "column": "record_id"
}
```

## Try schema-only checks on Parquet

Check the **pass** Parquet part from the run above:

```bash
ARTIFACTS="$(jq -r '.artifacts.uri' "$MANIFEST")"
PARQUET="$(jq -r 'select(.kind == "pass" and .format == "parquet") | .uri' "$ARTIFACTS")"

dist/valet schema -p examples/parquet-schema.yml -i "$PARQUET"
```

Expect exit `0`: one file's schema matches (`record_id`, `location`, `amount`); the `AMOUNT_NONNEGATIVE` value rule is **not evaluated**.

```json
{
  "scope": "schema",
  "metadata_only": true,
  "validation": "not_evaluated",
  "compatible": true,
  "rows_checked": 0,
  "objects_checked": 1,
  "declared_rows": 3
}
```

No row values are checked and nothing is written. To check values too (exit `0`, three passing rows):

```bash
dist/valet validate -p examples/parquet-schema.yml -i "$PARQUET"
```

Next: [schema checks](../../guides/schema/), [rules cookbook](../../rules/), [runnable example](../../rules/#runnable-example).
