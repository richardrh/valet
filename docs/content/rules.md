---
title: Rules cookbook
weight: 40
type: docs
toc: true
---

Valet can check schemas, individual rows and whole datasets. For row rules,
a row violates a rule when any check in it fails; at most one issue is emitted
for that rule and row, even when several checks fail.

**Only checking Parquet names and types?** Use `valet schema` with
`required_columns`, `schema_matches` in `strict` mode, or `input.schema`.
These are distinct from value checks. The [schema-only guide](../guides/schema/#which-checks-run)
lists exactly what runs and what is reported as not evaluated.

Use `level: error` when a failure must make validation fail and `level: warn`
when the row should remain a warning. Every code must be stable and unique
across `rules` and `dataset_rules`.

```yaml
rules:
  - code: STABLE_RULE_CODE
    name: Human-readable name
    level: error
    column: default_column
    message: Explanation shown for failures
    checks:
      - type: check_name
        parameter: value
```

Check parameters are inline beside `type`; there is no nested `params` field.
A check-level `column` or `columns` overrides the rule-level `column`.

## Required and nonblank values

Reject null or missing values:

```yaml
- code: CUSTOMER_ID_REQUIRED
  level: error
  column: customer_id
  checks:
    - type: not_null
```

Reject null, missing, empty, or Unicode-whitespace-only strings:

```yaml
- code: CUSTOMER_NAME_REQUIRED
  level: error
  column: customer_name
  checks:
    - type: not_blank
```

Check only whether a JSON field/path was present; explicit null remains present:

```yaml
- code: EXTERNAL_ID_PRESENT
  level: error
  column: external_id
  checks:
    - type: is_present
```

## Data types

Choose the type control that matches the question:

| Question | Use |
|---|---|
| Does this Parquet file have the expected column types? | `valet schema` with an expected schema; no row scan |
| Can each value be converted into a stable output type? | `input.schema` during `valet validate` |
| Can this cell be interpreted as a type, as a validation rule? | `type_check` during `valet validate` |

`type_check` is not executed by the schema-only command, even if its expected
type resembles a Parquet column type. Use the metadata schema controls for that
purpose.

### Convert the input into an explicit schema

Use `input.schema` when the output must have stable Arrow types. Failed
conversions become structured ingestion rejects rather than ordinary rule
issues.

```yaml
input:
  type: csv
  schema:
    extra_fields: error
    columns:
      order_id: {type: string}
      quantity: {type: int64}
      unit_price: {type: decimal, precision: 12, scale: 2}
      event_date: {type: date, formats: ["%F"]}
      observed_at:
        type: timestamp
        timezone: UTC
        formats: ["%Y-%m-%dT%H:%M:%S%:z"]
```

### Check whether a value can be interpreted as a type

Use `type_check` for a row-level issue, especially with `mode: raw_strings`:

```yaml
- code: QUANTITY_IS_INTEGER
  level: error
  column: quantity
  checks:
    - type: type_check
      expected_type: int64
```

Canonical types are `null`, `bool`, signed/unsigned integer widths,
`float32`, `float64`, `string`, `binary`, `date`, `timestamp`, and `decimal`.
Aliases including `integer`, `double`, `utf8`, `date32`, `datetime`, and
`decimal128` are normalized.

## Positive values and numeric ranges

Strictly greater than zero:

```yaml
- code: QUANTITY_POSITIVE
  level: error
  column: quantity
  checks:
    - type: valid_range
      min: 0
      exclusive_min: true
```

Inclusive range:

```yaml
- code: PERCENTAGE_RANGE
  level: error
  column: percentage
  checks:
    - type: valid_range
      min: 0
      max: 100
```

Open interval:

```yaml
- code: PROBABILITY_OPEN_INTERVAL
  level: error
  column: probability
  checks:
    - type: valid_range
      min: 0
      max: 1
      exclusive_min: true
      exclusive_max: true
```

Bounds are inclusive by default. `exclusive_min: true` requires `min`, and
`exclusive_max: true` requires `max`. Null passes `valid_range`; combine it
with `not_null` when null is forbidden.

## Compare two columns

Require `subtotal > discount`:

```yaml
- code: SUBTOTAL_EXCEEDS_DISCOUNT
  level: error
  checks:
    - type: compare_columns
      column: subtotal
      other_column: discount
      operator: ">"
```

Require two columns to match:

```yaml
- code: PAID_TOTAL_MATCHES_SUBTOTAL
  level: error
  checks:
    - type: compare_columns
      column: paid_total
      other_column: subtotal
      operator: "=="
```

Operators are `==`, `!=`, `<`, `<=`, `>`, and `>=`. Explicit input schemas are
recommended so numeric, decimal, date, and timestamp comparisons use their
declared types instead of lexical string ordering.

## Exact columns and names

Require specific columns but allow additional columns:

```yaml
- code: REQUIRED_COLUMNS
  level: error
  checks:
    - type: required_columns
      columns: [order_id, customer_id, quantity]
```

Require exactly the declared names and types, with no missing or additional
columns:

```yaml
- code: EXACT_SCHEMA
  level: error
  checks:
    - type: schema_matches
      mode: strict
      require_columns: true
      allow_extra_columns: false
      columns:
        order_id: string
        customer_id: string
        quantity: int64
        unit_price: decimal
        created_at: timestamp
```

With Parquet input, both examples above can run without row reads:

```bash
valet schema -p pipeline.yml -i input.parquet
```

Place the rules under the pipeline's `rules:` key. `mode: strict` compares
logical schema types; it does not prove non-null, valid-format or in-range
values. The default `mode: castable` needs full validation to prove value
conversion and is listed as not evaluated by `valet schema` (although its
presence/extra-column policies can still be checked).

At error severity, these are schema gates during full validation too. Normal
validation may apply an explicit input schema first; the schema-only command
compares the file's actual types without conversion.

`input.schema.extra_fields: error` rejects undeclared input fields, and
`required: true` requires a declared column to exist. Neither is a substitute
for `not_null` on the values. See the [schema-only guide](../guides/schema/)
for report fields, skipped checks and exit codes.

## Allowed and forbidden values

Require membership in an allowed set:

```yaml
- code: STATUS_ALLOWED
  level: error
  column: status
  checks:
    - type: valid_values
      values: [OPEN, PAID, CANCELLED]
```

Require one exact value:

```yaml
- code: CURRENCY_IS_USD
  level: error
  column: currency
  checks:
    - type: equals
      value: USD
```

Forbid one value:

```yaml
- code: TEST_ACCOUNT_FORBIDDEN
  level: error
  column: account_type
  checks:
    - type: not_equals
      value: TEST
```

`equals` fails on a mismatch or null. `not_equals` fails only on equality and
allows null; add `not_null` if null is forbidden. `all_equal` requires every
selected column to equal one supplied value.

## String lengths and patterns

```yaml
- code: COUNTRY_CODE_LENGTH
  level: error
  column: country_code
  checks:
    - type: valid_length
      length: 2
```

```yaml
- code: EMAIL_FORMAT
  level: error
  column: email
  checks:
    - type: matches_pattern
      pattern: '^[^@[:space:]]+@[^@[:space:]]+\.[^@[:space:]]+$'
```

Lengths count Unicode code points. Regexes use Go regular-expression syntax
and search unless explicitly anchored.

## Uniqueness

```yaml
- code: ORDER_ID_UNIQUE
  level: error
  column: order_id
  checks:
    - type: is_unique
```

```yaml
- code: ORDER_LINE_UNIQUE
  level: error
  checks:
    - type: is_unique_composite
      columns: [order_id, line_number]
```

Exact uniqueness is a replay-based global check. Every duplicate occurrence,
including the first, fails. Distinct-key state is charged to the managed-memory
budget.

## Conditional rules

Only require `closed_at` for closed orders:

```yaml
- code: CLOSED_AT_REQUIRED
  level: error
  column: closed_at
  when:
    column: status
    operator: eq
    value: CLOSED
  checks:
    - type: not_null
```

Composed condition:

```yaml
when:
  all:
    - {column: status, operator: eq, value: PAID}
    - {column: refunded, operator: eq, value: false}
```

Condition operators are `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `is_null`,
and `is_present`. A false condition makes the rule not applicable. An invalid
condition operand is counted as unevaluable.

## References

```yaml
- code: CUSTOMER_EXISTS
  level: error
  column: customer_id
  checks:
    - type: referential_integrity
      reference_file: references/customers.csv
      reference_column: customer_id
```

Composite keys use `referential_integrity_composite` with ordered `columns` and
`reference_columns`. Relative reference paths resolve from the pipeline file,
not the process working directory.

## Dates and timestamps

```yaml
- code: EVENT_DATE_FORMAT
  level: error
  column: event_date
  checks:
    - type: date_format
      formats: ["%F", "%d/%m/%Y"]
```

```yaml
- code: OBSERVED_WINDOW
  level: error
  column: observed_at
  checks:
    - type: timestamp_window
      min: "2026-01-01T00:00:00Z"
      max: "2026-12-31T23:59:59Z"
```

Timestamp bounds are inclusive. Wall times without an offset require an IANA
`timezone` parameter.

## Dataset assertions

Dataset rules are separate from row rules:

```yaml
dataset_rules:
  - code: DATASET_NOT_EMPTY
    level: error
    metric: row_count
    min: 1

  - code: CUSTOMER_NULL_RATE
    level: error
    metric: null_fraction
    column: customer_id
    max: 0.001

  - code: ORDER_TOTAL
    level: error
    metric: sum
    column: subtotal
    min: 0
```

Supported metric families are row count; missing/null/blank/conversion counts
and fractions; rule-failure counts and fractions; and numeric `sum`, `min`,
`max`, and `avg`.

## Built-in check reference

| Check | Main parameters | Null behavior |
|---|---|---|
| `not_null` | `column` or `columns` | fails |
| `not_blank` | `column` or `columns` | fails |
| `is_present` | `column` or `columns` | explicit null passes |
| `valid_length` | `length` | fails |
| `min_length` | `min_length` | fails |
| `max_length` | `max_length` | fails |
| `valid_range` | `min`, `max`, `exclusive_min`, `exclusive_max` | passes |
| `equals` | `value` | fails |
| `not_equals` | `value` | passes |
| `all_equal` | `columns`, `value` | fails |
| `has_null_keys` | `columns` | fails |
| `valid_values` | `values` | passes |
| `matches_pattern` | `pattern` | fails |
| `is_unique` | `column` or `columns` | fails |
| `is_unique_composite` | `columns` | fails |
| `compare_columns` | `column`, `other_column`, `operator` | fails |
| `referential_integrity` | `column`, `reference_file`, `reference_column` | distinct from empty |
| `referential_integrity_composite` | `columns`, `reference_file`, `reference_columns` | distinct from empty |
| `required_columns` | `columns` | schema check |
| `schema_matches` | `columns`, `mode`, `require_columns`, `allow_extra_columns` | schema check |
| `valid_schema_columns` | `reference_file`, `reference_column` | schema check |
| `type_check` | `expected_type` | target-dependent |
| `date_format` | `format` or `formats` | passes |
| `timestamp_window` | `min`, `max`, optional `timezone` | passes |

Missing values follow the check's null policy. Invalid conversion state is
unevaluable rather than silently treated as null.

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

A validation exit of `1` is expected because the example intentionally contains
invalid rows. Follow `manifest_uri` in `cookbook-summary.json`, then the
manifest's artifact index, to inspect the structured issues.
