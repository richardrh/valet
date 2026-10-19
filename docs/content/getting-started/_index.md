---
title: Getting started
weight: 10
type: docs
next: getting-started/installation
---

Valet is a self-contained Go CLI for validating local or S3-hosted CSV,
Parquet, NDJSON and JSON data. Pipeline documents can be strict YAML or strict
JSON; both formats normalize to the same configuration and behavior.

{{< cards >}}
  {{< card link="installation" title="Installation" subtitle="Download a release or build the CLI." >}}
  {{< card link="quickstart" title="Quickstart" subtitle="Validate the sample, then check its Parquet schema." >}}
  {{< card link="../guides/schema" title="Schema-only Parquet checks" subtitle="Check columns and types without scanning rows." >}}
  {{< card link="../rules" title="Rules cookbook" subtitle="Choose schema, row and dataset checks." >}}
{{< /cards >}}

## Choose the right command

| You want to… | Command | Result |
|---|---|---|
| Check Parquet columns and types without reading values | `valet schema -p pipeline.yml -i input.parquet` | Schema-only JSON report; no data outputs |
| Check row values, conversions, references and dataset rules | `valet validate -p pipeline.yml -i input.parquet -o results/` | Checked rows, issues and a terminal manifest |
| Preview configuration, schemas, passes and required state | `valet plan -p pipeline.yml -i input.parquet` | Execution-plan JSON; not a validation result |

Only `schema` is Parquet-only. Full validation supports all four input formats.
Both schema inspection and planning may succeed even when a row value would
fail full validation. See [schema-only checks](../guides/schema/) for the exact
checks supported without reading data.

## Full validation lifecycle

1. Optionally run `valet plan` to understand the work and managed state.
2. Run `valet validate` to read, convert and check the data.
3. Read the published `manifest.json`, then follow its artifact references.
   Partial output without a terminal manifest is not a completed result.

`valet schema` is separate: read its JSON report from stdout. It never publishes
the normal row buckets or manifest.

## Exit codes

| Exit | `valet schema` | `valet validate` |
|---:|---|---|
| `0` | Schema compatible; row checks were not evaluated | Completed without error-level violations |
| `1` | Error-level schema mismatch; inspect the JSON report | Completed with row, dataset or schema violations; read the manifest |
| `2` | Configuration, format, metadata, storage or resource failure | Pre-commit configuration, input, I/O, resource or publication failure |
| `3` | Not used: schema inspection commits no terminal manifest | Manifest committed, but stdout presentation failed |
| `128+N` | Interrupted by signal `N` | Interrupted by signal `N` before commit |
