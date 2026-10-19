---
title: Performance
weight: 35
type: docs
prev: guides/operations
next: guides/ci
---

Valet is streaming and does not impose a whole-file size ceiling. Input volume,
schema, rules, compression and storage determine elapsed time; record/page
limits, global-rule cardinality and the managed budget determine whether a run fits.


> [!NOTE]
> File size and record size are separate. `max_record_bytes: 1MiB` allows a
> multi-gigabyte CSV as long as each individual row is at most 1 MiB.

## Measured 5 GB Parquet workload

Measured 2026-10-19 on an Intel Core Ultra X7 358H workstation with 16 logical
CPUs, Linux amd64 and Go 1.27.2. Input and output were on disk-backed local
storage, not `/tmp` (which is RAM-backed on this workstation). These are single
timed runs, not medians or cold-cache guarantees.

The input was regenerated with the same deterministic generator as the baseline:
**5,003,192,038 bytes and 10,215,424 rows**. It contained a 512-byte string ID,
an `int64` amount and a string status, with Snappy compression. The pipeline used
an explicit schema, three row-local checks (`not_blank`, a positive range and
allowed status values), a 512 MiB managed budget, 8 MiB batch target and 1 MiB
record limit. Every row passed.

| Build | Configured workers | Full CLI elapsed | Peak process RSS |
|---|---:|---:|---:|
| Before optimizations | 1 | 65.91 s | 58.7 MiB |
| Optimized | 1 | 24.61 s | 89.4 MiB |
| Optimized | 4 | 17.57 s | 147.5 MiB |
| Optimized | 8 | 17.19 s | 224.9 MiB |

Timing includes process startup, input identity checks, guarded decoding, every
row check, output encoding/writes and terminal manifest publication. Fixture
generation and subsequent independent verification are excluded.

Optimized runs published 5,400,820,356 bytes across 41 Parquet row parts, versus
approximately 5.15 GB across 156 parts before optimization. Fast compatible
Snappy and the new layout trade output size for lower CPU and encoding overhead.
All three optimized runs were independently compared against the input for
every user-column value and row order, as well as checked/published row counts.
Generated input, outputs and the temporary benchmark workspace were deleted.

The sub-10-second full-validation/rewrite target was **not reached**. Increasing
workers beyond four provided little benefit on this workload; evaluation and
output retain ordered single-owner stages. More workers also require more live
decoder and in-flight memory.

## Schema-only Parquet inspection

```bash
valet schema -p pipeline.yml -i input.parquet
```

Footer-only schema inspection of that same file took **0.040 s**, including
process startup. This is not full data validation: the result reports
`scope: schema`, `metadata_only: true`, `validation: not_evaluated`, zero checked
rows and an unverified footer-declared row count. It neither scans row values
nor rewrites the input.

## Parquet execution costs

Full local validation retains the initial baseline digest and final replay
digest. Page safety verification/decompression is fused into bounded row-group
decoding rather than performing a separate complete compressed-page scan.
The measured row-local local plan therefore reported three source passes.
S3 identity pinning does not add a full-file digest scan.

Compressed MB/s is highly dependent on row width and compression ratio. Do not
extrapolate an arbitrary multi-gigabyte runtime from a tiny, narrow fixture.
These measurements are not service-level guarantees.

## What changes throughput

- Schema inference adds discovery work; an explicit CSV schema avoids that pass.
- Exact uniqueness and other global checks add replay plus cardinality-dependent
  managed state.
- Wider strings, decimals, timestamps, regexes and conversion failures cost
  more per row.
- CSV output, Parquet output, compression choice and part sizing change sink
  cost.
- Cold storage, S3 latency, TLS, range requests and object-store throughput can
  dominate local compute.
- Gzip decompression reduces input MB/s.
- Small files are dominated by process startup and publication metadata.

## Capacity planning

Benchmark the exact pipeline in the target pod and storage environment. Use
representative row widths, compression, output format, reference cardinality
and failure rates. Keep `memory_budget` below the pod limit and set
`GOMEMLIMIT` with additional headroom as described in [operations](../operations/).

For repeatable large-volume engine measurements, use the opt-in generated scale
suite:

```bash
go test -tags=scale ./benchmarks \
  -run '^TestScale100GiB$' \
  -timeout 0 \
  -v
```
