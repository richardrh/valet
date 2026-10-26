---
title: Getting started
weight: 10
type: docs
sidebar:
  open: true
next: getting-started/installation
---

## What is Valet?

A command-line validation gate for CSV, Parquet, NDJSON or JSON files (local or S3-compatible) before a lakehouse, warehouse or next job.
Checks live in YAML or JSON; by default you get one aggregate JSON report on stdout and an exit code, with no rewritten data or output files.

## Why Valet?

- **Readable ≠ correct:** checks schemas, missing values, ranges, duplicate keys, references and column relationships before problems propagate. Same versioned rules wherever the file enters.
- **Caller owns the next step:** continue on exit `0`, block on failed checks (`1`), handle execution failures separately (`2`). Warnings do not block by default; set mandatory rules to `level: error`.
- **Self-contained CLI, not a service:** no scheduling or lakehouse upload. Keep your uploader; validate the same immutable input before invoking it.

## Start here

{{< cards >}}
  {{< card link="installation" title="Installation" subtitle="Download a release or build the CLI." >}}
  {{< card link="quickstart" title="Quickstart" subtitle="Validate the sample, then check its Parquet schema." >}}
  {{< card link="../concepts" title="Concepts" subtitle="Pipelines, rules, results and execution." >}}
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

Only `schema` is Parquet-only; full validation supports all four formats. `schema` and `plan` may succeed even when a row value would fail full validation — see [schema-only checks](../guides/schema/).

## Full validation lifecycle

1. Optional: `valet plan` to see the work and managed state.
2. `valet validate` reads, converts and checks the data.
3. Read results:
   - no output URI → aggregate stdout JSON; no files or manifest.
   - `-o`/`output.uri` → published `manifest.json` and its artifact references; partial artifacts without it are incomplete.

`valet schema` is separate: JSON report on stdout only; never publishes row buckets or a manifest.

## Exit codes

| Exit | `valet schema` | `valet validate` |
|---:|---|---|
| `0` | Schema compatible; row checks were not evaluated | Completed without error-level violations; warnings allowed |
| `1` | Error-level schema mismatch; inspect the JSON report | Completed with row, dataset or schema violations; inspect the summary |
| `2` | Configuration, format, metadata, storage or resource failure | Operational or presentation failure without a committed manifest |
| `3` | Not used: schema inspection commits no terminal manifest | Manifest committed, but stdout presentation failed |
| `128+N` | Interrupted by signal `N` | Interrupted by signal `N` before commit |
