---
title: Operations
weight: 30
type: docs
prev: guides/output
next: guides/performance
---

Running Valet in pods and batch jobs: memory, storage, signals, logs.

## Kubernetes memory

`execution.memory_budget` is a managed limit, not an RSS cap; leave headroom for the Go runtime, SDK and buffers ([memory budget](../../concepts/execution/#memory-budget)).

> [!IMPORTANT]
> Limits are per **decompressed row or record**, not per file. A 5 GB CSV with ordinary rows fits the 1 GiB profile (slower, not 5 GB of memory).

| Pod limit | `GOMEMLIMIT` | Managed budget | Batch target | Max one CSV row | Max one JSON record | Max one Parquet row/page |
|---:|---:|---:|---:|---:|---:|---:|
| 512 MiB | 384 MiB | 256 MiB | 4 MiB | 512 KiB | 2 MiB | 4 MiB |
| 1 GiB | 768 MiB | 512 MiB | 8 MiB | 1 MiB | 4 MiB | 8 MiB |
| 2 GiB | 1.5 GiB | 1 GiB | 8–16 MiB | 2 MiB | 8 MiB | 8–16 MiB |

```yaml
resources:
  requests:
    memory: 768Mi
  limits:
    memory: 1Gi
env:
  - name: GOMEMLIMIT
    value: 768MiB
```

| What | Memory behaviour |
|---|---|
| `max_record_bytes` | A large ceiling does not by itself mean a large allocation; CSV reservations grow with actual record bytes. |
| Workers | Share the same managed budget. |
| Row-local checks | Stay bounded as file size grows. |
| Exact uniqueness, reference indexes | Grow with distinct keys; can exhaust the budget and fail explicitly (no spilling or approximation). |
| Parquet row groups | Keep uncompressed ≤ 32 MiB for the 1 GiB profile; large groups can exhaust the guard. |

- Profiles are starting points: measure RSS with your real compression, schemas, output, cardinality and storage before setting production limits.
- Small-fixture MB/s does not predict runtime; see [Performance](../performance/).

Internals (decoder buffers, queues, reorder state): [Engineering spec](https://github.com/richardrh/valet/blob/main/docs/engineering/spec.md).

## S3

Uses the standard AWS SDK credential chain; never put static credentials in pipeline documents.

| Topic | Behaviour |
|---|---|
| Settings | Endpoint, region, path-style addressing and profile, set separately for source and output. |
| Publication | Unique run prefixes; writes never overwrite. |
| Multipart | Ambiguous uploads are aborted; constrained budgets upload serially. |
| Reads | Adjacent range reads are coalesced; schema-only inspection reads just the footer. |
| Digests | Publication and artifact references share one completed digest. |

Configure an abort-incomplete-multipart lifecycle rule as a backstop. Write reconciliation and part sizing: [Engineering spec](https://github.com/richardrh/valet/blob/main/docs/engineering/spec.md).

## Local publication

Files are published without replacement and fsynced; `manifest.json` is written last and marks the commit ([Engineering spec](https://github.com/richardrh/valet/blob/main/docs/engineering/spec.md)).

## Metadata trust is not data integrity

- Schema-only inspection checks metadata, not payload; Parquet statistics can be false in a valid footer.
- Version pinning and ETags do not prove the statistics match the values.
- Valet reads values by default. `--parquet-stats trusted` accepts eligible producer claims with reduced coverage, disclosed in the result and evidence ledger. Read [trust and tampering](../schema/#trust-and-tampering) and [output modes](../validation-modes/) first.
- Keep trusted digests, signatures and producer permissions outside the data's write boundary; an attacker who can replace both file and checksum can fake the evidence.

## Signals and cancellation

| Signal | Effect | Exit code |
|---|---|---|
| SIGINT | Cancel, close readers/writers, abort known multipart uploads, no terminal manifest | `130` |
| SIGTERM | Same | `143` |

## Logging

Logs go to stderr; stdout carries only requested output.

```bash
valet validate ... --log-format json --log-level info
valet validate ... --log-level debug
```

| Item | Detail |
|---|---|
| All run records | `run_id`; URIs redacted; row values and credentials never logged. |
| JSON records | `log_version`, `event`, run-local `seq`, command identity. |
| `run.phase` | Work/queue-wait timings; phases overlap, so not additive CPU. |
| `decode` phase | CSV/JSON/NDJSON parsing and batch conversion, once per batch. |
| `run.finished` | Exit/commit outcome, byte counters, managed peak, coverage. |
| `parquet.stats_decision` | Per row group, debug level; raw cells and min/max never logged. |
| Logging failure | Does not abort validation; exit `3` still reports the committed manifest. |

Event schema: [validation modes spec](https://github.com/richardrh/valet/blob/main/docs/engineering/validation-modes-spec.md).

### Why a run failed

With text logs, a failed run (exit `2`) ends with an error and, where known, a hint:

```text
valet: error: malformed CSV in data/orders.csv at line 3: found 4 fields; the header has 3
valet: hint: quote any field that contains ",", and check this line for a missing or extra ","; set input.on_malformed: reject to reject such rows and check the rest of the file
```

With `--log-format json`, the same information is in the `command failed` record:

- `error_class`: `input_format`, `resource`, `not_found`, `access_denied`, `canceled`, `deadline` or `operation`.
- Pipeline load/validation errors show the full message (e.g. `unknown field config.rules[0].chekcs (line 5)`); other errors show class and typed fields only, to avoid leaking row values.

Malformed input (`error_class: input_format`) is reported by position and reason, never record contents:

| Field | Meaning |
|---|---|
| `input_format` | `csv`, `json` or `ndjson` |
| `input_source` | The file or object, without credentials or query string |
| `input_line`, `input_column` | Where the record starts, when known |
| `input_offset` | Byte offset, when known |
| `framing_reason` | `field_count`, `quote`, `bare_quote`, `csv_syntax`, `blank_line`, `missing_header`, `invalid_utf8`, `record_too_large`, `json_syntax`, `truncated`, `not_array`, `not_object`, `trailing_data`, `multiple_records`, `multiline_record`, `duplicate_key` or `too_deep` |
| `expected_fields`, `found_fields` | Header and record field counts, for `field_count` |
| `hint` | How to fix the input |

Malformed records stop the run; `input.on_malformed: reject` rejects only wrong-field-count CSV rows ([malformed CSV rows](../configuration/#malformed-csv-rows)).

## Health checks for batch jobs

See [exit codes](../../concepts/results/#exit-codes) for definitions.

| Exit | Treat as | Read |
|---|---|---|
| `0` | Completed, warnings allowed | stdout JSON or manifest |
| `1` | Completed, checks failed | stdout JSON or manifest |
| `2` | Failed, no committed manifest | stderr |
| `3` | Manifest committed, stdout failed | manifest |
| `130`, `143` | Interrupted by signal | — |
