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
if dist/valet validate \
    -p examples/pipeline.yml \
    -i examples/sample.csv \
    > demo-summary.json
then
  code=0
else
  code=$?
fi
echo "exit=$code"
```

Expected exit code: `1`. Validation completed successfully but found one
error-level issue. The command returns one JSON summary on stdout. No output
destination is configured, so Valet creates no files or manifest; the shell
redirection saves the report as `demo-summary.json`.

Selected report fields:

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
python3 -m json.tool demo-summary.json
```

For a pipeline gate, check the exit code: `0` permits the next step, `1` blocks
it because checks failed, and `2` means validation could not complete or its
stdout report could not be delivered. Exit `0` allows warnings; use
`level: error` for checks that must block ingestion. Keep the input immutable
between validation and your separate lakehouse upload.

The report includes per-rule counts and coverage, not every row-level issue.
See [CI automation](../../guides/ci/) for an exit-gated workflow.

## Optional: write Parquet and detailed issues

Add an output destination when you want rewritten data and row-level diagnostics:

```bash
if dist/valet validate \
    -p examples/pipeline.yml \
    -i examples/sample.csv \
    -o demo-output \
    > artifact-summary.json
then
  code=0
else
  code=$?
fi
echo "exit=$code"
```

This also exits `1`, but publishes Parquet pass/error buckets, detailed issues
and a completion manifest. Stdout remains JSON. The default artifact mode is
`all`; `--output-mode failures` omits passing rows, while `--output-mode report`
keeps reports/issues but omits row files. Neither mode changes which checks run.

Extract the terminal manifest without requiring `jq`:

```bash
MANIFEST="$(python3 -c '
import json
with open("artifact-summary.json") as f:
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

The optional artifact run above writes Parquet bucket files. Run that example
first, then select one committed **pass** part through its manifest:

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
dist/valet validate -p examples/parquet-schema.yml -i "$PARQUET"
```

Expected exit code: `0`, with three passing rows. See
[schema-only Parquet checks](../../guides/schema/) for missing-column/type
diagnostics, skipped checks, S3 and multi-file usage.


Continue with the [rules cookbook](../../rules/) or the complete
[runnable orders example](../../rules/#runnable-example).
