---
title: Structured output
linkTitle: Results & reports
weight: 20
type: docs
prev: guides/validation-modes
next: guides/operations
---

What `valet validate` writes. Terms: [report](../../concepts/results/#report), [manifest](../../concepts/results/#manifest); modes: [output modes](../validation-modes/).

| Stdout flag | Effect |
|---|---|
| (default) | JSON summary |
| `--plain` | `key=value` |
| `--stats` | Plain statistics unless JSON requested |
| `--json-output -` | JSON; conflicts with `--plain` |
| `--json-output PATH` | Also write summary to a file |

## Schema-only output is different

`valet schema` writes JSON to stdout only; no artifacts or manifest.

```bash
valet schema -p pipeline.yml -i input.parquet > schema-report.json
```

Use `compatible` for the outcome; see [schema report](../schema/#read-the-json-report).

## Publication tree

Written only with `-o`/`output.uri`. Read `manifest.json` first; ignore runs without one.

```text
<output>/<run-id>/
├── manifest.json
├── summary.json
├── inputs.ndjson
├── rules.json
├── artifacts.ndjson
├── pass/part-*.csv|parquet
├── warn/part-*.csv|parquet
├── error/part-*.csv|parquet
├── issues/part-*.ndjson
├── rejects/part-*.ndjson
└── evidence/part-*.ndjson
```

| Path | Holds |
|---|---|
| `manifest.json` | URI, size, SHA-256 of every artifact; `row_output` |
| `rules.json` | Rule messages and definitions |
| `pass/` `warn/` `error/` `rejects/` | Only if the mode writes them and they're nonempty |
| `evidence/` | Trusted-statistics runs only |

CSV/Parquet buckets follow `output.format`; everything else is JSON/NDJSON. Integrity details: [Engineering spec](https://github.com/richardrh/valet/blob/main/docs/engineering/validation-modes-spec.md).

## Summary

Status, outcome, row and per-rule counts, coverage, memory peak and the manifest URI.

- `status: completed` + `validation: fail` means error-level issues, not a crash.
- A metadata-assisted pass can have `total: 0`; read `verification` and `coverage`.

## Issues

```json
{
  "report_version": 2,
  "run_id": "...",
  "source": "orders.csv",
  "source_ordinal": 1,
  "row": 2,
  "global_row": 2,
  "line": 3,
  "code": "QUANTITY_POSITIVE",
  "severity": "error",
  "origin": "rule",
  "scope": "row",
  "column": "quantity"
}
```

Rows and lines are 1-based. Parquet adds `row_group` (0-based) and `row_in_group` (1-based).

## Rejects

| Failure | Result |
|---|---|
| Recoverable | Raw record, provenance, per-column `valid`/`missing`/`null`/`invalid` |
| Fatal framing | Exit `2`, no manifest; diagnostics on stderr |

## Full-validation exit codes

| Exit | Do |
|---:|---|
| `0` | Pass; warnings allowed |
| `1` | Error-level row, dataset or schema failures |
| `2` | No manifest; read stderr |
| `3` | Manifest committed, stdout failed; use it |
| `128+N` | Interrupted; no result |
