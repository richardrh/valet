---
title: Output modes and metadata trust
weight: 17
type: docs
prev: guides/schema
next: guides/output
---

Choose **what to validate** separately from **which rows to write**. Ordinary
validation reads values by default, even when you ask for a smaller report.

## Choose which rows to write

```bash
valet validate -p pipeline.yml -i input.parquet -o results/ --output-mode failures
valet validate -p pipeline.yml -i input.parquet -o reports/ --output-mode report
```

| Mode | Row files written | Other results |
|---|---|---|
| `all` (default) | Pass, warning and error rows | Issues, raw rejects, indexes, summary and manifest |
| `failures` | Warning and error rows, not passing rows | Issues, raw rejects, indexes, summary and manifest |
| `report` | No row files or raw reject records | Issues, indexes, summary, applicable evidence and manifest |

All modes still execute the selected validation policy and publish a terminal
manifest. Report-only does not mean stop at the first failure or write only to
stdout. Warning/error/unevaluable counts and dataset failures are retained.
Report-only retains ingestion diagnostics but omits the original rejected record.

Configure the default in the pipeline, or override it with `--output-mode`:

```yaml
output:
  uri: results/
  format: parquet
  mode: failures
```

`row_output` in the version-2 result declares which buckets were omitted by
policy and how many decoded rows were emitted or omitted. No dummy empty bucket
is created. Report-only is not anonymization: input identities, schemas, rule
configuration and custom messages can still contain sensitive information.

## Use Parquet statistics only when you trust the producer

> [!WARNING]
> A readable footer and plausible statistics do not prove that the payload
> matches them. Keep the default `off` policy for untrusted data.

Parquet footers can supply null counts and numeric bounds. Explicit trusted
execution may accept an eligible row group without decoding it when every
applicable assertion has sufficient pass evidence and its passing rows do not
need to be written:

```bash
valet validate -p numeric-checks.yml -i input.parquet -o reports/ \
  --output-mode report --parquet-stats trusted --log-format json
```

A compatible pipeline declares exact input types and the rules to check:

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

The example keeps the pipeline conservative; the command explicitly overrides
it to `trusted`. A zero null count can support the non-null requirement, and a
safe minimum greater than zero can support the range. Missing statistics mean
**scan**, not pass. A range that cannot be proven, a positive null count or a
warning-level violation also causes scanning so real issues and row locations
come from values.

Important limits:

- `all` needs passing-row output and therefore reads the data.
- Conversions, conditions, references, uniqueness, global assertions and other
  unsupported checks require the value path. If a group is decoded, all its
  normal checks run; a candidate metadata pass cannot hide a real violation.
- Numeric proof preserves integer/decimal precision. Floating-point shortcuts
  need explicit NaN information and suitable finite bounds; ordinary min/max
  alone is not enough.
- `schema` remains a footer/schema command. It does not run these value checks
  and rejects an explicitly supplied `--parquet-stats` option.
- Writer identity or statistics in Valet-generated output never enable reader
  trust automatically. `off` remains the default.

A buggy producer or deliberate edit can make a trusted run accept a false
claim. See [trust and tampering](../schema/#trust-and-tampering) before enabling
this policy. Identity checks detect changes relative to the pinned object;
they do not establish that its original statistics were truthful.

## Read coverage, not just the outcome

Version-2 summaries distinguish:

- `total`, `pass`, `warn`, `error` and `coverage.value_rows`: unique rows actually
  decoded and evaluated in the final pass.
- `coverage.metadata_claimed_rows`: rows accepted only from producer statistics.
- `coverage.accounted_rows`: the sum of those two populations.
- `verification`: `values`, `metadata_assisted`, or `not_evaluated` after an
  early schema rejection.
- `trust_policy` and `ingestion_verification`: which assumptions were accepted
  and whether ingestion/content checks covered the selected rows.
- `rule_coverage`: each rule's value-derived and metadata-claimed populations.
  Presented value rows can include not-applicable or unevaluable verdicts.

An entirely statistics-accepted result can legitimately have `total: 0` and
nonzero metadata-claimed coverage. It must not be described as all rows checked.
A `validation: pass` result with `verification: metadata_assisted` is conditional
on the selected producer-trust policy, not independent value verification.

Trusted runs publish a referenced evidence ledger with source/group identities,
footer hashes, decisions, reasons and proof-field names. It survives suppressed
logs. A footer hash is not a content hash; rechecking the evidence requires the
pinned source. Later observed row positions can include unverified skipped-prefix
lengths, disclosed by `provenance_position_basis: metadata_prefix` and group-local
issue coordinates.

## Write bounded statistics for downstream consumers

Valet omits column statistics by default. To emit bounded fixed-width evidence:

```yaml
output:
  format: parquet
  parquet:
    statistics: bounded
```

This enables supported boolean/integer/decimal statistics. Float statistics and
variable-width string/binary extrema remain disabled. Structural row counts are
present independently of this option. Choosing `bounded` is not an instruction
to trust those statistics on subsequent reads.

## Inspect performance without mixing it into result JSON

```bash
valet validate -p pipeline.yml -i input.parquet -o results/ \
  --json-output - --log-format json --log-level debug
```

Stdout is the result JSON; stderr is NDJSON diagnostics. `run.phase` records
separate work such as metadata, hashing, decoding, checks and publishing.
`run.finished` records the exit/commit outcome, elapsed time, managed peak,
bytes and coverage. Debug `parquet.stats_decision` records explain skip/scan
choices; a trust warning is emitted when statistics replace values. Raw cells
and raw min/max values are not logged by default.

Concurrent phase work can overlap and exceed elapsed wall time; it is not CPU
time and must not be added into wall-time percentages. Failed/incomplete runs
mark counters accordingly. Exit `3` retains a committed result even when stdout
presentation fails; consume the manifest rather than rerunning it.
