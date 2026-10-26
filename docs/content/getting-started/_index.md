---
title: Getting started
weight: 10
type: docs
sidebar:
  open: true
next: getting-started/installation
---

## What is Valet?

Valet is a **command-line data quality gate for file-based pipelines**. Use it
before ingesting a file into a lakehouse or warehouse, or before passing data
to another job.

You describe checks in YAML or JSON and run them against local or
S3-compatible CSV, Parquet, NDJSON or JSON files. By default, Valet returns one
aggregate JSON report on stdout and an exit code, without rewriting the data
or creating output files.

## Why Valet?

Reading a file successfully does not establish that its contents meet your
requirements. Valet checks expected schemas, missing values, ranges, duplicate
keys, references and relationships between columns before those problems
propagate downstream. Keep the checks in version control and run the same
rules wherever the file enters your pipeline.

Your caller owns the next step: continue after exit `0`, block ingestion after
failed checks (`1`), and handle execution failures separately (`2`). Warnings
do not block by default; set mandatory rules to `level: error`.

Valet is a self-contained CLI, not another service to deploy. It does not
schedule jobs or upload files into a lakehouse. Keep your existing uploader
and validate the same immutable input before invoking it.

## Start here

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
| Check values without publishing files | `valet validate -p pipeline.yml -i input.parquet` | One aggregate JSON summary on stdout; no manifest |
| Check row values, conversions, references and dataset rules | `valet validate -p pipeline.yml -i input.parquet -o results/` | Checked rows, issues and a terminal manifest |
| Preview configuration, schemas, passes and required state | `valet plan -p pipeline.yml -i input.parquet` | Execution-plan JSON; not a validation result |

Only `schema` is Parquet-only. Full validation supports all four input formats.
Both schema inspection and planning may succeed even when a row value would
fail full validation. See [schema-only checks](../guides/schema/) for the exact
checks supported without reading data.

## Full validation lifecycle

1. Optionally run `valet plan` to understand the work and managed state.
2. Run `valet validate` to read, convert and check the data.
3. Without an output URI, read the aggregate stdout JSON; no files or manifest
   are created. With `-o`/`output.uri`, read the published `manifest.json` and
   follow its artifact references. Partial artifacts without it are incomplete.

`valet schema` is separate: read its JSON report from stdout. It never publishes
the normal row buckets or manifest.

## Exit codes

| Exit | `valet schema` | `valet validate` |
|---:|---|---|
| `0` | Schema compatible; row checks were not evaluated | Completed without error-level violations; warnings allowed |
| `1` | Error-level schema mismatch; inspect the JSON report | Completed with row, dataset or schema violations; inspect the summary |
| `2` | Configuration, format, metadata, storage or resource failure | Operational or presentation failure without a committed manifest |
| `3` | Not used: schema inspection commits no terminal manifest | Manifest committed, but stdout presentation failed |
| `128+N` | Interrupted by signal `N` | Interrupted by signal `N` before commit |
