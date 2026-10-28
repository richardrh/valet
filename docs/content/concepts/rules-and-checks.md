---
title: Rules and checks
weight: 20
type: docs
---

A rule groups one or more checks under a stable code. Examples: [rules cookbook](../../rules/).

## Rules

| Field | Meaning |
|---|---|
| `code` | Stable, unique across `rules` and `dataset_rules`; `INGEST_*` is reserved |
| `level` | `error` or `warn` |
| `column` | Default column; a check's `column`/`columns` overrides it |
| `checks` | Mapping of check name to arguments, e.g. `not_null: true`, `valid_values: [A, B]`; see [syntax](../../rules/) |
| `when` | Optional condition |
| `message`, `name` | Shown for failures; stored once in `rules.json` |

## Levels

| Level | Row bucket | Blocks (exit `1`) |
|---|---|---|
| `error` | `error` | Yes |
| `warn` | `warn` | No; exit `0` allows warnings |

## Check scopes

| Scope | Checks | Notes |
|---|---|---|
| row | `not_null`, `valid_range`, `matches_pattern`, `type_check`, … | Each row independently |
| global | `is_unique`, `is_unique_composite` | Prepass + replay; every duplicate, including the first, fails |
| reference | `referential_integrity`, `referential_integrity_composite` | Probe an index built from a reference file |
| schema | `required_columns`, `schema_matches`, `valid_schema_columns` | One dataset-wide outcome |
| dataset | `dataset_rules` | Aggregate metrics, see below |

## Conditions

`when` applies to row and reference checks.

```yaml
when:
  all:
    - {column: status, operator: eq, value: PAID}
    - {column: refunded, operator: eq, value: false}
```

- Leaf: `column`, `operator` (`eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `is_null`, `is_present`), `value`.
- Compose with `all`, `any`, `not`.
- False condition → rule not applicable; invalid operand → unevaluable.

## Row buckets

| Bucket | Row has |
|---|---|
| `pass` | No failures |
| `warn` | Only `warn`-level failures |
| `error` | Any `error`-level failure or ingestion issue |

`total = pass + warn + error`.

## Ingestion issues

Raised while reading and converting; always `error` severity.

| Code | Cause |
|---|---|
| `INGEST_TYPE` | Value cannot convert to the declared type |
| `INGEST_NULL` | Null in a `nullable: false` column |
| `INGEST_MISSING_FIELD` | `required: true` field missing |
| `INGEST_EXTRA_FIELD` | Undeclared JSON field with `extra_fields: error` |
| `INGEST_MALFORMED` | CSV row with wrong field count, `on_malformed: reject` |
| `INGEST_SCHEMA` | Schema scope: required column absent, type mismatch, undeclared or inconsistent column |

Checks on an invalid cell count as unevaluable rather than null.

## Dataset rules

```yaml
dataset_rules:
  - code: DATASET_NOT_EMPTY
    level: error
    metric: row_count
    min: 1
```

| Metric family | Examples |
|---|---|
| Row count | `row_count` |
| Missing/null/blank/conversion | counts and fractions, e.g. `null_fraction` |
| Rule failures | counts and fractions |
| Numeric | `sum`, `min`, `max`, `avg` |
