---
title: Execution
weight: 40
type: docs
---

Valet streams input in bounded batches under a managed memory budget. Sizing: [operations](../../guides/operations/).

## Memory budget

| Setting | Default | Meaning |
|---|---|---|
| `memory_budget` | `512MiB` | Managed decoder, batch, rule-state, index and publication reservations; **not** a process RSS limit |
| `max_record_bytes` | `8MiB` | One CSV row or JSON record; Parquet pages are limited to the larger of this and `batch_target`; not the file size |

- Set `GOMEMLIMIT` (e.g. `768MiB` with a 512 MiB budget) for process headroom.
- Uniqueness and reference indexes grow with distinct keys; over budget they fail explicitly. No disk spill, no approximation.

## Batches and workers

| Setting | Default | Meaning |
|---|---|---|
| `batch_target` | `8MiB` | Target Arrow batch size, not a hard ceiling |
| `workers` | `1` (range 1–32) | Overlap stages and decode Parquet row groups in parallel |

- Rule evaluation and output stay single-owner and ordered.
- Workers share the budget; more workers do not raise it.

## Passes

| Pass | When |
|---|---|
| Inference | `infer_types` without a declared schema |
| Prepass | Global checks (`is_unique*`) collect exact keys; `schema_matches` castability gates are resolved |
| Reference load | Reference checks; each index built once, shared |
| Final pass | Evaluates rules; counts come from here only |

Inputs are re-read between passes; local files are digest-checked and S3 objects pinned by version/ETag.

## Malformed input

| Input | Default (`on_malformed: abort`) | `on_malformed: reject` |
|---|---|---|
| CSV row with wrong field count | Exit `2`, error names file and line | Row → `error` bucket, one `INGEST_MALFORMED`, raw fields in rejects |
| Unclosed/stray quotes, invalid UTF-8, blank lines, oversized records, malformed JSON/NDJSON | Exit `2` | Exit `2` |

Mapping files (`transforms`) always fail on malformed rows. Details: [malformed CSV rows](../../guides/configuration/#malformed-csv-rows).
