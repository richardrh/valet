---
title: Pipelines
weight: 10
type: docs
---

A pipeline describes what to read, how to type it and which rules to run. Full reference: [pipeline configuration](../../guides/configuration/).

## Pipeline file

| Item | Detail |
|---|---|
| Formats | Local `.yaml`, `.yml` or `.json`; JSON is strict RFC syntax |
| Shared validation | Duplicate keys, shape, exact numbers, defaults, semantics |
| Top-level keys | `config_version`, `name`, `version`, `context`, `source`, `input`, `transforms`, `execution`, `output`, `rules`, `dataset_rules`, `stats` |
| `$schema` | Top-level key accepted and ignored in JSON and YAML; in YAML a `# yaml-language-server: $schema=…` modeline also works ([editor support](../../guides/configuration/#editor-support-and-ai-agents)) |
| CLI overrides | `-i`/`-o` replace input/output; explicit execution flags override the document |
| Relative paths | Sources, output, references, mappings and schema files resolve from the pipeline's directory |

## Inputs and formats

| Setting | Values |
|---|---|
| `source.uris` | Ordered list; local paths and `s3://` URIs can mix |
| `source.prefix` + `filter`, `recursive` | Deterministic, sorted discovery; exclusive with `uris` |
| `input.type` | `csv`, `json` (streamed top-level array of objects), `ndjson` (one object per nonblank line), `parquet` (flat schemas) |
| Type inference | From source suffix when omitted; trailing `.gz` ignored for text formats; explicit type wins |

All inputs form one logical dataset. Inputs must be replayable files or objects; stdin is not supported.

## Input modes

| `input.mode` | Behaviour |
|---|---|
| `infer_types` (default) | Infer types from up to `infer_schema_length` rows (default 1,000,000), then freeze; a later incompatible value is an input error |
| `raw_strings` | Keep non-null CSV values as UTF-8 strings; null markers (`null_values`, default `NULL`, `null`, `N/A`, `n/a`, `""`) stay distinct |

| Option | Values | Effect |
|---|---|---|
| `on_type_error` | `row_error` (default), `abort` | With a declared schema: conversion failure becomes an `INGEST_TYPE` row issue, or stops the run |
| `on_malformed` | `abort` (default), `reject` | See [malformed input](../execution/#malformed-input) |

## Declared schemas

`input.schema` sets exact target types instead of inference.

```yaml
input:
  schema:
    extra_fields: error   # or ignore
    columns:
      id: {type: string, required: true, nullable: false}
      amount: {type: decimal, precision: 18, scale: 2}
```

- Conversion failures become [ingestion issues](../rules-and-checks/#ingestion-issues) and rejects, never silent nulls.
- `extra_fields: error` rejects undeclared fields; `ignore` selects only declared columns.
- `valet plan --format pipeline` writes a schema block from a sample.

## Type names

Any letter case. C fixed-width names (without `_t`), Python and Arrow/Parquet names are accepted; reports and `valet plan` print the last column.

| Values | Write | Reported as |
|---|---|---|
| Signed integers | `int8`, `int16`, `int32`, `int64`; `int`, `integer` = `int64` | `int8` … `int64` |
| Unsigned integers | `uint8`, `uint16`, `uint32`, `uint64` | `uint8` … `uint64` |
| 32-bit floats | `float32` | `float32` |
| 64-bit floats | `float64`, `double`; `float` = `float64` | `float64` |
| Exact decimals | `decimal`, `decimal128` with `precision` (1–38) and `scale` | `decimal` |
| Booleans | `bool`, `boolean` | `bool` |
| Text | `str`, `string`, `utf8`, `large_string`, `large_utf8` | `string` |
| Bytes | `bytes`, `binary`, `large_binary` | `binary` |
| Dates | `date`, `date32`, `date64` | `date` |
| Timestamps | `datetime`, `timestamp` | `timestamp` |
| Nulls only | `null` | `null` |

- `int` and `float` follow Python (64-bit). In C, Arrow (pyarrow prints `float32` as `float`) and Parquet (`FLOAT`), `float` is 32-bit: write `float32` there.
- Names whose size or sign varies by platform (`long`, `char`, `short`, `unsigned`) are rejected with the spelling to use; `int32_t` → write `int32`:

```text
input.schema column "qty" type "long" is not a supported type: long is 32-bit on Windows and 64-bit on Linux and macOS; write int32 or int64
```
