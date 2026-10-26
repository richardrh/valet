---
title: Rules cookbook
weight: 40
type: docs
toc: true
---

Check schemas, individual rows and whole datasets. A row violates a rule when any of its checks fails; at most one issue is emitted per rule and row.

```yaml
rules:
  - code: STABLE_RULE_CODE      # stable, unique across rules and dataset_rules
    name: Human-readable name
    level: error                # error: fails validation; warn: row stays a warning
    column: default_column
    message: Explanation shown for failures
    checks:
      - type: check_name
        parameter: value        # inline beside type; no nested params field
```

- Check-level `column`/`columns` overrides the rule's `column`; the two are mutually exclusive. Unknown types and parameters are rejected.

## Check reference

Scopes: see [Check scopes](../concepts/rules-and-checks/#check-scopes). `column*` = `column` or `columns` (or the rule's `column`).

| Check | Scope | Parameters | Null | Notes |
|---|---|---|---|---|
| `not_null` | row | `column*` | fails | null or missing fails |
| `not_blank` | row | `column*` | fails | also empty / Unicode-whitespace-only strings |
| `is_present` | row | `column*` | explicit null passes | only whether the JSON field/path exists |
| `valid_length` | row | `column*`, `length` | fails | Unicode code points |
| `min_length` | row | `column*`, `min_length` | fails | code points |
| `max_length` | row | `column*`, `max_length` | fails | code points |
| `valid_range` | row | `column*`, `min`, `max`, `exclusive_min`, `exclusive_max` | passes | inclusive by default; `exclusive_min` needs `min`, `exclusive_max` needs `max` |
| `equals` | row | `column*`, `value` | fails | fails on mismatch |
| `not_equals` | row | `column*`, `value` | passes | fails only on equality |
| `all_equal` | row | `columns` (required), `value` | fails | every selected column equals `value` |
| `has_null_keys` | row | `columns` (required) | fails | |
| `valid_values` | row | `column*`, `values` | passes | allowed set |
| `matches_pattern` | row | `column*`, `pattern` | fails | Go regexp; searches unless anchored |
| `is_unique` | global | `column*` | fails | every duplicate occurrence fails, incl. the first |
| `is_unique_composite` | global | `columns` (required) | fails | as above |
| `compare_columns` | row | `column`, `other_column`, `operator` | fails | `==` `!=` `<` `<=` `>` `>=` |
| `referential_integrity` | reference | `column`, `reference_file`, `reference_column` | distinct from empty | |
| `referential_integrity_composite` | reference | `columns` (required), `reference_file`, `reference_columns` | distinct from empty | ordered column lists |
| `required_columns` | schema | `columns` (required) | — | schema gate at `error`; extra columns allowed |
| `schema_matches` | schema | `columns` (name → type map), `mode`, `require_columns`, `allow_extra_columns` | — | schema gate at `error` |
| `valid_schema_columns` | schema | `reference_file`, `reference_column` | — | |
| `type_check` | row | `column*`, `expected_type` | target-dependent | not run by `valet schema` |
| `date_format` | row | `column*`, `format` or `formats` | passes | |
| `timestamp_window` | row | `column*`, `min`, `max`, `timezone` | passes | inclusive bounds; offset-less wall times need IANA `timezone` |

- Invalid conversion makes a check unevaluable, not null. Uniqueness state counts against the [memory budget](../concepts/execution/#memory-budget).
- Relative `reference_file` paths resolve from the pipeline file.
- `compare_columns`: declare an input schema so numbers, decimals and dates compare by type, not as strings.

## Conditions

`when` limits a rule to matching rows. Allowed only on `row` and `reference` scope checks (conditional global/schema checks are rejected).

- Operators: `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `is_null`, `is_present`; compose with `all: [...]`.
- False condition: rule not applicable. Invalid operand: counted as unevaluable.

```yaml
- code: CLOSED_AT_REQUIRED
  level: error
  column: closed_at
  when: {column: status, operator: eq, value: CLOSED}
  checks: [{type: not_null}]

# composed
when:
  all:
    - {column: status, operator: eq, value: PAID}
    - {column: refunded, operator: eq, value: false}
```

## Dataset metrics

`dataset_rules` are separate from row rules: `code`, `level`, `metric`, then `min` / `max` / `equals` (`equals` excludes `min`/`max`; `min` ≤ `max`).

| Metric | Needs | Bounds |
|---|---|---|
| `row_count` | no `column`, no `rule_code` | nonnegative integer |
| `missing_count`, `null_count`, `blank_count`, `conversion_error_count` | `column` | nonnegative integer |
| `missing_fraction`, `null_fraction`, `blank_fraction`, `conversion_error_fraction` | `column` | 0–1 |
| `rule_failure_count` / `rule_failure_fraction` | `rule_code` of a known row rule; no `column` | integer / 0–1 |
| `sum`, `min`, `max`, `avg` | `column` declared numeric in an explicit input schema | any number; `tolerance` (finite, ≥ 0) only with `equals` on float columns |

```yaml
dataset_rules:
  - {code: DATASET_NOT_EMPTY, level: error, metric: row_count, min: 1}
  - {code: CUSTOMER_NULL_RATE, level: error, metric: null_fraction, column: customer_id, max: 0.001}
  - {code: ORDER_TOTAL, level: error, metric: sum, column: subtotal, min: 0}
```

## Recipes

One example per pattern; other checks follow the same shape.

```yaml
rules:
  - code: CUSTOMER_ID_REQUIRED
    level: error
    column: customer_id
    checks: [{type: not_null}]
  - code: QUANTITY_POSITIVE          # > 0; null passes, add not_null to forbid it
    level: error
    column: quantity
    checks: [{type: valid_range, min: 0, exclusive_min: true}]
  - code: SUBTOTAL_EXCEEDS_DISCOUNT  # rule-level column not needed
    level: error
    checks: [{type: compare_columns, column: subtotal, other_column: discount, operator: ">"}]
  - code: ORDER_LINE_UNIQUE
    level: error
    checks: [{type: is_unique_composite, columns: [order_id, line_number]}]
  - code: CUSTOMER_EXISTS
    level: error
    column: customer_id
    checks:
      - {type: referential_integrity, reference_file: references/customers.csv, reference_column: customer_id}
  - code: QUANTITY_IS_INTEGER        # row-level type check, e.g. with mode: raw_strings
    level: error
    column: quantity
    checks: [{type: type_check, expected_type: int64}]
```

### Columns and schema

```yaml
  - code: EXACT_SCHEMA               # exact names and types, none missing or extra
    level: error
    checks:
      - type: schema_matches
        mode: strict
        require_columns: true
        allow_extra_columns: false
        columns: {order_id: string, customer_id: string, quantity: int64, unit_price: decimal, created_at: timestamp}
```

With Parquet input, schema checks run without row reads: `valet schema -p pipeline.yml -i input.parquet`.

| `schema_matches` mode | Behaviour |
|---|---|
| `strict` | Compares logical types; does not prove non-null, valid-format or in-range values |
| `castable` (default) | Needs full validation; `valet schema` reports it not evaluated (presence/extra-column policies still checked) |

- `valet schema` compares actual file types without conversion; `valet validate` may apply `input.schema` first.
- `input.schema` `extra_fields: error` / `required: true` check columns, not values; they do not replace `not_null`.
- Type control: `valet schema` (Parquet types, no row scan) · `input.schema` (conversion; failures are ingestion rejects) · `type_check` (rule issue; not run by `valet schema`). Type names and schemas: [Pipelines](../concepts/pipelines/#type-names). Report fields and exit codes: [schema-only guide](../guides/schema/).

## Runnable example

The complete orders cookbook is in `examples/cookbook/`:

```bash
go build -o dist/valet ./cmd/valet

dist/valet plan \
  -p examples/cookbook/orders.yml \
  -i examples/cookbook/orders.csv

dist/valet validate \
  -p examples/cookbook/orders.yml \
  -i examples/cookbook/orders.csv \
  -o cookbook-output \
  --json-output cookbook-summary.json
```

Exit `1` is expected: the example contains invalid rows. Follow `manifest_uri` in `cookbook-summary.json`, then the manifest's artifact index, to the structured issues.
