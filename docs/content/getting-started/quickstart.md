---
title: Quickstart
weight: 20
type: docs
prev: getting-started/installation
next: guides/configuration
---

The repository includes equivalent YAML and JSON pipelines plus a four-row CSV
fixture.

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

The third data row has a missing `record_id`.

## Plan the run

```bash
dist/valet plan \
  -p examples/pipeline.yml \
  -i examples/sample.csv
```

Use `examples/pipeline.json` to exercise the equivalent strict JSON pipeline.
The plans are identical.

The plan reports a CSV source, inferred `string`, `string`, `int64` columns,
four rules and two required passes.

## Validate

```bash
rm -rf demo-output demo-summary.json

dist/valet validate \
  -p examples/pipeline.yml \
  -i examples/sample.csv \
  -o demo-output \
  --json-output demo-summary.json

echo "exit=$?"
```

Expected exit code: `1`. Validation completed successfully but found one
error-level issue.

```json
{
  "status": "completed",
  "validation": "fail",
  "total": 4,
  "pass": 3,
  "warn": 0,
  "error": 1,
  "rows_scanned": 4,
  "passes": 2
}
```

## Read the result

Extract the terminal manifest without requiring `jq`:

```bash
MANIFEST="$(python3 -c '
import json
with open("demo-summary.json") as f:
    print(json.load(f)["manifest_uri"])
')"

python3 -m json.tool "$MANIFEST"
```

Follow the manifest's `artifacts` reference to `artifacts.ndjson`; the `issues`
entry points to an NDJSON part containing:

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

The run above writes Parquet bucket files. Select one committed **pass** part
through the manifest, using Python only as a helper to read the JSON index:

```bash
PARQUET="$(python3 - "$MANIFEST" <<'PY'
import json
import sys

with open(sys.argv[1]) as stream:
    manifest = json.load(stream)
with open(manifest["artifacts"]["uri"]) as stream:
    for line in stream:
        artifact = json.loads(line)
        if artifact["kind"] == "pass" and artifact["format"] == "parquet":
            print(artifact["uri"])
            break
    else:
        raise SystemExit("No committed pass Parquet part found")
PY
)"

dist/valet schema -p examples/parquet-schema.yml -i "$PARQUET"
```

Expected exit code: `0`. This checks **one generated file's schema**, not the
original CSV or all files in the validation run.

The example pipeline requires `record_id`, `location` and `amount` with types
`string`, `string` and `int64`. It allows additional provenance columns. Its
`AMOUNT_NONNEGATIVE` value rule is listed as **not evaluated**. During full
validation, it selects these three user columns and writes fresh provenance
rather than copying the old run's `__valet_*` fields.

Selected fields from the JSON report:

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

`compatible: true` means the expected schema matched. It does not mean any row
value was validated. No new bucket files or manifest are created.

To check the values in that Parquet part as well:

```bash
dist/valet validate -p examples/parquet-schema.yml -i "$PARQUET" -o parquet-results/
```

Expected exit code: `0`, with three passing rows. See
[schema-only Parquet checks](../../guides/schema/) for missing-column/type
diagnostics, skipped checks, S3 and multi-file usage.


Continue with the [rules cookbook](../../rules/) or the complete
[runnable orders example](../../rules/#runnable-example).
