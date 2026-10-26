---
title: Structured output
linkTitle: Results & reports
weight: 20
type: docs
prev: guides/validation-modes
next: guides/operations
---

This page describes **full validation** (`valet validate`). Without `-o` or
`output.uri`, it writes one aggregate JSON summary to stdout and creates no
artifact files, metadata, evidence ledger, detailed per-row issue list or
manifest. Validation still runs normally, retaining counts, coverage, trust,
provenance and bounded schema/dataset summary diagnostics.

With an output URI, consume results through `manifest.json`; ignore partial
artifacts without a terminal manifest. `--output-mode all` writes all row
buckets, `failures` writes warning/error rows, and `report` omits row files and
raw rejects while keeping issues, indexes, summary and manifest. The default
mode resolves after overrides: no URI means `report`; a URI means `all`.
Explicit `all`/`failures` without a URI is rejected by `validate`.
See [output modes and metadata trust](../validation-modes/).

Stdout defaults to JSON regardless of artifact selection. `--plain` selects
`key=value`; `--stats` implies plain/statistics output unless JSON is explicitly
requested. `--json-output -` explicitly selects JSON stdout and conflicts with
`--plain`. `--json-output PATH` requests an additional summary file; stdout
remains JSON. Shell redirection can also save the stdout summary.

## Schema-only output is different

`valet schema` writes its JSON report directly to stdout. It does not create
the publication tree below, row buckets, `summary.json` or a completion
manifest. Save it with shell redirection if needed:

```bash
valet schema -p pipeline.yml -i input.parquet > schema-report.json
```

Use `compatible` for the schema outcome. `validation: not_evaluated` and
`rows_checked: 0` make clear that row data was not checked; `declared_rows`
comes from the footer. The [schema-only guide](../schema/#read-the-json-report)
explains `checks`, `schema_issues`, `not_evaluated` and schema-command exit codes.

## Publication tree
This tree exists only with an explicit output URI:


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

The manifest references inputs, rules, artifacts and summary by URI, byte size,
SHA-256 and record count where applicable. Verify these fields before consuming
parts.

Row and reject directories are conditional on output mode and nonempty content.
The evidence ledger is produced for trusted-statistics execution. Omitted
buckets have no dummy files; the manifest's `row_output` records the policy and
emitted/omitted decoded-row counts.

## Summary

The stdout summary (and `summary.json` when publishing artifacts) reports:

- terminal status and validation outcome;
- unique final-pass value-evaluated total/pass/warning/error counts;
- rows scanned and physical pass count;
- per-rule and ingestion counts;
- not-applicable and unevaluable counts;
- dataset/schema outcomes;
- managed-memory peak;
- terminal manifest URI, only when artifacts are committed;
- report version `2`, output mode, trust policy and verification scope;
- separate value-derived and footer-claimed coverage, including per-rule coverage;
- row materialization policy and emitted/omitted counts.

`status: completed` can have `validation: fail`; this means validation ran to
completion and found error-level issues. It is not an operational failure.

For metadata-assisted runs, `validation: pass` is conditional on trusted producer
statistics. Read `verification`, `coverage` and the evidence references rather
than interpreting `total` as all footer-declared rows. A metadata-only accepted
run can have zero value-evaluated rows and nonzero metadata-claimed rows.

## Issues

Issue NDJSON is compact and stable for downstream parsing:

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

Messages and definitions live once in `rules.json`, keyed by stable rule code.
Issue provenance is 1-based and identifies source, source row, global row and
physical text line where available.

For Parquet, issues also carry observed zero-based `row_group` and one-based
`row_in_group` coordinates. If preceding groups/files were skipped, absolute
source/global row positions include footer-declared prefixes and are labelled
`position_basis: metadata_prefix`; the manifest discloses that basis.

## Rejects

Recoverable framing/conversion failures retain:

- the recoverable raw record;
- source and row provenance;
- per-column `valid`, `missing`, `null` or `invalid` states;
- structured ingestion diagnostics.

Fatal framing failures cannot safely produce a logical row. They return exit
`2`, publish no terminal manifest and can be consumed as JSON diagnostics from
stderr with `--log-format json`.

## Full-validation exit codes

| Exit | Consumer action |
|---:|---|
| `0` | Completed without error-level failures; warnings are allowed |
| `1` | Completed with error-level row, dataset or schema failures |
| `2` | Operational/presentation failure with no committed manifest; inspect stderr |
| `3` | Artifact manifest committed but stdout presentation failed; consume it |
| `128+N` | Interrupted before commit; no terminal artifact result |

## Output formats

CSV uses a documented reversible null/backslash encoding. Parquet preserves
native typed values and nulls. Issue, reject, input and artifact indexes remain
NDJSON regardless of the selected bucket format.
