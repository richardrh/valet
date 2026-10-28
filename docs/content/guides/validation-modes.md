---
title: Output modes and metadata trust
linkTitle: Output modes & stats
weight: 17
type: docs
prev: guides/schema
next: guides/output
---

Choose **which rows to write** separately from **what to validate**. Terms: [output modes](../../concepts/results/#output-modes).

## Choose which rows to write

| Mode | Row files | Also written |
|---|---|---|
| `all` (default with URI) | pass, warn, error | issues, raw rejects, indexes, summary, manifest |
| `failures` | warn, error | issues, raw rejects, indexes, summary, manifest |
| `report` (default without URI) | none, no raw rejects | issues, indexes, summary, evidence, manifest |

```bash
valet validate -p pipeline.yml -i input.parquet -o results/ --output-mode failures
valet validate -p pipeline.yml -i input.parquet -o reports/ --output-mode report
```

- Without a URI, only the stdout summary is produced; `all`/`failures` are rejected.
- Every mode runs full validation. `row_output` reports omitted buckets and row counts.
- `report` is not anonymization: names, schemas and messages may be sensitive.

```yaml
output:
  uri: results/
  format: parquet
  mode: failures
```

## Use Parquet statistics only when you trust the producer

> [!WARNING]
> Statistics can be wrong or tampered with. Keep the default `off` for untrusted data.

`--parquet-stats trusted` lets a row group pass from footer null counts and bounds without decoding.

```bash
valet validate -p numeric-checks.yml -i input.parquet -o reports/ \
  --output-mode report --parquet-stats trusted --log-format json
```

```yaml
input:
  type: parquet
  parquet:
    statistics: off
  schema:
    columns:
      amount: {type: int64, required: true, nullable: false}

output:
  mode: report

rules:
  - code: AMOUNT_POSITIVE
    column: amount
    level: error
    checks:
      - type: valid_range
        min: 0
        exclusive_min: true
```

| Situation | Result |
|---|---|
| Null count 0, minimum > 0 | Group passes from statistics |
| Missing stats, nulls, unprovable bound, warn-level violation | Group is scanned |
| `--output-mode all` | Always scans |
| Conversions, conditions, references, uniqueness, global checks | Always scans |
| `valet schema --parquet-stats` | Rejected |

Valet-written files are never trusted automatically. See [trust and tampering](../schema/#trust-and-tampering); proof rules: [Engineering spec](https://github.com/richardrh/valet/blob/main/docs/engineering/validation-modes-spec.md).

## Read coverage, not just the outcome

| Field | Meaning |
|---|---|
| `total`, `pass`, `warn`, `error`, `coverage.value_rows` | Rows decoded and checked |
| `coverage.metadata_claimed_rows` | Rows passed from statistics |
| `coverage.accounted_rows` | Sum of both |
| `verification` | `values`, `metadata_assisted`, `not_evaluated` |
| `trust_policy`, `ingestion_verification` | Accepted assumptions; ingestion coverage |
| `rule_coverage` | Both counts per rule |

A `metadata_assisted` pass depends on trusting the producer, and `total` can be 0. Trusted runs also write an evidence ledger ([Engineering spec](https://github.com/richardrh/valet/blob/main/docs/engineering/validation-modes-spec.md)).

## Write bounded statistics for downstream consumers

Off by default. Writes integer, decimal and boolean statistics only:

```yaml
output:
  format: parquet
  parquet:
    statistics: bounded
```

## Inspect performance without mixing it into result JSON

```bash
valet validate -p pipeline.yml -i input.parquet -o results/ \
  --json-output - --log-format json --log-level debug
```

| Stream/event | Content |
|---|---|
| stdout | Result JSON |
| stderr | NDJSON logs |
| `run.phase` | Per-phase work (phases overlap; don't sum) |
| `run.finished` | Outcome, time, memory, bytes, coverage |
| `parquet.stats_decision` (debug) | Skip/scan reason |

Exit `3`: use the committed manifest; don't rerun.
