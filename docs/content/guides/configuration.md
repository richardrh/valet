---
title: Pipeline configuration
weight: 10
type: docs
prev: getting-started/quickstart
next: guides/schema
---

Valet accepts local `.yaml`, `.yml` and `.json` pipeline documents. JSON uses
strict RFC syntax; YAML and JSON share duplicate-key detection, shape
validation, exact-number preservation, defaults and semantic validation.

## Top-level structure

```yaml
name: Orders validation
version: "1.0"

source:
  uris: [orders.csv]

input:
  type: csv
  schema:
    extra_fields: error
    columns:
      order_id: {type: string}
      quantity: {type: int64}

output:
  uri: output/
  format: parquet

execution:
  memory_budget: 512MiB
  batch_target: 8MiB
  max_record_bytes: 1MiB
  workers: 1

rules: []
dataset_rules: []
```

CLI `-i` and `-o` replace the configured input and output. Explicit execution
flags override the document. Relative references, mappings and schema files
resolve from the pipeline document's directory.

## Sources

Use one ordered list:

```yaml
source:
  uris:
    - data/part-001.csv
    - s3://bucket/data/part-002.csv
```

Or deterministic prefix discovery:

```yaml
source:
  prefix: s3://bucket/data/
  filter: csv
  recursive: true
```

Explicit lists and prefixes are mutually exclusive. Local and S3 discovery is
sorted deterministically.

## Input formats

Supported values are `csv`, `json`, `ndjson` and `parquet`. If omitted, the
format is inferred from the decoded source suffix; an explicit type always
wins. A trailing `.gz` is ignored during inference for text formats.

CSV supports headers, configured delimiters and raw-string or inferred modes.
JSON is a streamed top-level array of objects; NDJSON is one object per
nonblank line. Parquet accepts flat required/optional schemas and validates
metadata and pages before decoding.

## Explicit schemas

```yaml
input:
  schema:
    extra_fields: error
    columns:
      id: {type: string, required: true, nullable: false}
      amount: {type: decimal, precision: 18, scale: 2}
      effective_date: {type: date, formats: ["%F"]}
      observed_at:
        type: timestamp
        timezone: UTC
        formats: ["%Y-%m-%dT%H:%M:%S%:z"]
```

Explicit schemas preserve exact identifiers and numeric values. Recoverable
conversion failures produce ingestion issues and reject records; they do not
silently become ordinary nulls.

### Choose schema inspection or value conversion

During `valet validate`, `input.schema` controls conversion and output types.
During `valet schema`, the same declaration is compared with the actual
Parquet types without conversion; a convertible string column does not count
as an already-integer column.

```bash
valet schema -p pipeline.yml -i input.parquet
```

This command checks supported footer metadata only, reports zero checked rows
and creates no row outputs or completion manifest. `nullable: false`, date
formats, `type_check`, row rules and references still require full validation.
See [schema-only Parquet checks](../schema/) for a complete example, the
supported-check table, JSON report and exit codes.

### Output policy and optional Parquet statistics

```yaml
input:
  parquet:
    statistics: off       # off (default) or explicitly trusted
output:
  mode: all               # all, failures, report
  format: parquet
  parquet:
    statistics: none      # none (default), bounded
```

Use `--output-mode` and `--parquet-stats` to override these policies. Writer
statistics do not grant reader trust. Report-only still validates the data by
default and publishes a terminal result, but does not encode row buckets or raw
rejects. See [output modes and metadata trust](../validation-modes/) before
enabling producer-statistics shortcuts.

## Execution limits

`memory_budget` limits managed decoder, batch, rule-state and publication
reservations; it is not a process RSS limit. `batch_target` controls target
Arrow batch size. `max_record_bytes` bounds one decompressed CSV row, one JSON
record, or one Parquet row/page. It does **not** bound the total input file.
Multi-gigabyte files stream through the same reusable decoder and batch state.

`workers` accepts 1–32 and defaults to 1. Larger values enable bounded,
ordered execution overlap and parallel Parquet row-group verification/decoding;
stateful rule evaluation and output publication retain a single ordered owner.
Worker admission is constrained by the available managed budget, row-group
sizes and decoder workspaces. Increasing workers does not increase the budget.

Recommended starting point for a 1 GiB pod:

```yaml
execution:
  memory_budget: 512MiB
  batch_target: 8MiB
  max_record_bytes: 1MiB # maximum one CSV row, not the CSV file
  workers: 1
```

Set `GOMEMLIMIT=768MiB` to retain process headroom. Row-local checks remain
bounded as file size grows. Exact uniqueness and reference indexes grow with
distinct-key cardinality and fail explicitly if their state exceeds the
managed budget; there is no hidden disk spill or approximate fallback.

## Next

- Use the [rules cookbook](../../rules/) for row, schema and dataset assertions.
- See [operations](../operations/) for pod sizing and S3 behavior.
- See [performance](../performance/) for measured Parquet throughput and schema-only inspection.
