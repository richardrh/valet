---
title: Performance
weight: 35
type: docs
prev: guides/operations
next: guides/ci
---

Valet streams with no file-size ceiling: volume, rules, compression and storage set speed; record limits, global-rule cardinality and the budget decide fit.

> [!NOTE]
> `max_record_bytes: 1MiB` allows a multi-gigabyte CSV if each row is at most 1 MiB.

## Measured 5 GB Parquet workload

| Setup | Value |
|---|---|
| Date, machine | 2026-10-19; Intel Core Ultra X7 358H, 16 logical CPUs, Linux amd64, Go 1.27.2 |
| Storage | Disk-backed local (not RAM-backed `/tmp`) |
| Runs | Single timed runs, not medians or cold-cache |
| Input | **5,003,192,038 bytes, 10,215,424 rows**; 512-byte string ID, `int64` amount, string status; Snappy |
| Pipeline | Explicit schema; `not_blank`, positive range, allowed status; 512 MiB budget, 8 MiB batches, 1 MiB record limit; all rows pass |

| Build | Configured workers | Full CLI elapsed | Peak process RSS |
|---|---:|---:|---:|
| Before optimizations | 1 | 65.91 s | 58.7 MiB |
| Optimized | 1 | 24.61 s | 89.4 MiB |
| Optimized | 4 | 17.57 s | 147.5 MiB |
| Optimized | 8 | 17.19 s | 224.9 MiB |

- Timed end to end: startup, decoding, every check, output writes, manifest. Fixture generation excluded.
- Output: 5,400,820,356 bytes in 41 parts (before: ~5.15 GB in 156); faster Snappy trades size for CPU. Outputs were verified value-for-value against input.
- The sub-10-second target was **not reached**; past four workers gains are small (evaluation and output stay ordered single-owner stages) and memory rises.

## Comparison with GX Core

Measured 2026-10-26, same workstation, with [`benchmarks/competitors`](https://github.com/richardrh/valet/tree/main/benchmarks/competitors):

- 10,000,000 rows: `id` int64 with nulls, `amount` decimal(18,2), `quantity` int64, `status` string.
- Checks: `id` not null, `amount` 0–10000, `quantity` 1–1000, `status` in a fixed set; ~1% of rows fail each.
- Median of three fresh processes, warm cache; both tools reported the expected failure counts.

| Input | Tool | Wall time | Peak RSS |
|---|---|---:|---:|
| CSV, 251 MiB | Valet, 1 worker | 5.37 s | 53 MiB |
| CSV | Valet, 4 workers | 3.60 s | 74 MiB |
| CSV | GX Core 1.24.1 + pandas 3.1.0 | 2.71 s | 1,467 MiB |
| Parquet, 93 MiB | Valet, 1 worker | 3.04 s | 112 MiB |
| Parquet | Valet, 4 workers | 2.53 s | 280 MiB |
| Parquet | GX Core + pandas | 17.22 s | 2,654 MiB |

| | GX Core | Valet |
|---|---|---|
| Memory | Whole file in a pandas DataFrame | Streams within the managed budget |
| Notes | Parquet decimals become Python `Decimal` (slow); CSV `amount` compared as float64 | No rules, 1 worker: 3.8 s CSV, 1.4 s Parquet (CSV reading ≈ 70%) |
| `abc` in decimal column | Range expectation errors instead of counting | Row fails, cell counted as `INGEST_TYPE`, run continues |
| Wrong CSV field count | `pandas.read_csv` raises | Exits `2` |

## Schema-only Parquet inspection

```bash
valet schema -p pipeline.yml -i input.parquet
```

Took **0.040 s** on the same file, startup included. It reads only the footer: `scope: schema`, `metadata_only: true`, `validation: not_evaluated`, zero checked rows, unverified row count.

## Parquet execution costs

- Full local validation keeps a baseline digest and a final replay digest; page verification is fused into row-group decoding, giving three source passes on the measured plan.
- S3 identity pinning adds no extra digest scan.
- MB/s depends on row width and compression; don't extrapolate from small fixtures. No service-level guarantees.

## What changes throughput

| Factor | Effect |
|---|---|
| Schema inference | Extra pass; explicit CSV schema avoids it |
| Uniqueness, global checks | Replay plus cardinality-dependent memory |
| Wide strings, decimals, timestamps, regexes, conversion failures | Higher per-row cost |
| Output format, compression, part size | Sink cost |
| Cold storage, S3 latency, TLS, range requests | Can dominate compute |
| Gzip | Lower input MB/s |
| Small files | Dominated by startup and publication |

## Capacity planning

Benchmark the exact pipeline in the target pod and storage with representative data. Keep `memory_budget` below the pod limit and `GOMEMLIMIT` with headroom ([operations](../operations/)).

Opt-in scale suite:

```bash
go test -tags=scale ./benchmarks \
  -run '^TestScale100GiB$' \
  -timeout 0 \
  -v
```
