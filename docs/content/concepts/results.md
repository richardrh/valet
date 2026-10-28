---
title: Results
weight: 30
type: docs
---

Every run prints a report and returns an exit code; files appear only with an output URI. Details: [structured output](../../guides/output/).

## Report

One JSON summary on stdout (`--plain` for `key=value`; `--json-output PATH` adds a file).

| Key | Meaning |
|---|---|
| `report_version`, `run_id` | Report format (`2`) and this run's ID |
| `status`, `validation` | Terminal status; `pass` / `fail` (`not_evaluated` for schema-only). `completed` can pair with `fail` |
| `total`, `pass`, `warn`, `error` | Unique final-pass value-evaluated rows per bucket |
| `rows_scanned`, `passes` | Decoded row visits across all passes; passes run |
| `rule_counts`, `ingest_counts` | Failures per rule code and per `INGEST_*` code |
| `unevaluable`, `not_applicable` | Rows a rule could not evaluate, or skipped by `when` |
| `dataset_results`, `dataset_issues`, `schema_issues`, `stats` | Dataset rules, schema checks and configured aggregates |
| `verification`, `coverage`, `rule_coverage`, `trust_policy` | `values` or `metadata_assisted`; value vs footer-claimed rows |
| `scope` | `data` for `validate`, `plan` for `plan` |
| `output_mode`, `row_output` | Output mode and, per bucket, what was written or omitted |
| `duration_seconds`, `managed_peak_bytes` | Run time and peak managed memory |
| `manifest_uri` | Manifest URI, only when committed with `-o` (`manifest=` in `--plain`) |

## Exit codes

| Exit | Meaning |
|---:|---|
| `0` | Completed; no error-level failures (warnings allowed) |
| `1` | Completed with error-level row, dataset or schema failures |
| `2` | Operational or presentation failure; no committed manifest; see stderr |
| `3` | Manifest committed but stdout failed; consume the manifest |
| `130` / `143` | SIGINT / SIGTERM before commit (`128+N`); no terminal result |

## Output modes

| Mode | Default when | Writes |
|---|---|---|
| `report` | No output URI | Stdout only; with a URI also issues, indexes, summary, manifest |
| `all` | Output URI set | All row buckets + rejects + reports |
| `failures` | — | `warn`/`error` rows + rejects + reports |

Set with `output.mode` or `--output-mode`. `all`/`failures` without a URI is an error. See [validation modes](../../guides/validation-modes/).

## Issues and rejects

| Artifact | Contents |
|---|---|
| `issues/part-*.ndjson` | One line per issue: `code`, `severity`, `origin`, `scope`, `column`, `source` URI, 1-based `source_ordinal`/`row`/`global_row`/`line` |
| `rejects/part-*.ndjson` | Raw record, provenance, per-column `valid`/`missing`/`null`/`invalid`, ingestion diagnostics |

Fatal framing failures produce no row: exit `2`, diagnostics on stderr (`--log-format json`).

## Manifest

`manifest.json` is the commit point.

- Written last; references inputs, rules, artifacts and summary by URI, size, SHA-256 and record count.
- No manifest → ignore partial artifacts.
- Before commit, failures are operational (exit `2`); after, the result stands (exit `3` if stdout fails).
