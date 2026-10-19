---
title: Operations
weight: 30
type: docs
prev: guides/output
next: guides/performance
---

## Kubernetes memory

`execution.memory_budget` is a managed reservation limit, not a hard RSS cap.
Leave headroom for the Go runtime, stacks, allocator retention, SDK state and
filesystem/network buffers.

> [!IMPORTANT]
> The limits below are for **one decompressed row or record**, not for the
> whole file. Valet streams input and has no configured whole-file size ceiling.
> A 5 GB CSV with ordinary rows is expected to fit in the 1 GiB pod profile;
> it takes longer, but it does not require 5 GB of memory.

Recommended initial profiles:

| Pod limit | `GOMEMLIMIT` | Managed budget | Batch target | Max one CSV row | Max one JSON record | Max one Parquet row/page |
|---:|---:|---:|---:|---:|---:|---:|
| 512 MiB | 384 MiB | 256 MiB | 4 MiB | 512 KiB | 2 MiB | 4 MiB |
| 1 GiB | 768 MiB | 512 MiB | 8 MiB | 1 MiB | 4 MiB | 8 MiB |
| 2 GiB | 1.5 GiB | 1 GiB | 8–16 MiB | 2 MiB | 8 MiB | 8–16 MiB |

Each active decoder reuses bounded working state. Batches and results have
independent ownership; bounded queues may overlap reading, checking and writing.
Their buffers, row-group workspaces and reorder state remain charged until the
last consumer releases them. More workers share the same managed budget.
Row-local execution remains bounded as total file size grows.

Exact uniqueness and reference indexes are different: their state grows with
distinct-key cardinality and can exhaust the managed budget even when every row
is small. They fail explicitly rather than spilling or becoming approximate.

These are conservative starting points. Measure representative RSS with your
compression, schemas, output format, uniqueness cardinality and reference
indexes before setting production requests and limits.

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

Parquet producers should generally keep uncompressed row groups at or below 32
MiB for the 1 GiB profile. A large row group can exhaust its guard even when
individual rows are small.

Benchmark the actual schema, rules, compression and output mode on the target
storage. A small narrow fixture's compressed MB/s is not a general file-size
runtime estimate. S3, gzip, wider rows, global checks and cold storage can change
elapsed time without changing the bounded-memory property.

## S3

Valet uses the standard AWS SDK credential chain. Configure endpoint, region,
path-style addressing and profile independently for source and output when
needed. Do not put static credentials in pipeline documents.

S3 publication uses unique run prefixes and create-if-absent writes. Ambiguous
PUT and multipart completion responses are reconciled by write identity, size
and digest. Multipart creation is not retried transparently; ambiguous uploads
are listed and aborted under pre-reserved cleanup capacity.

Pinned range reads coalesce adjacent payload access under bounded caches;
metadata-only schema inspection uses exact footer ranges. Multipart buffers grow
lazily, select a legal part size under available memory, and admit a bounded
number of concurrent uploads. Constrained budgets use serial uploads.
Publication and artifact references share one authoritative completed digest.

Configure an abort-incomplete-multipart lifecycle rule as defense in depth.

## Local publication

Temporary files are synced, published without replacement and followed by a
containing-directory sync. `manifest.json` is published and directory-synced
last. A pre-commit directory-sync failure removes the newly published target
before returning failure.

## Metadata trust is not data integrity

Schema-only inspection checks metadata, not payload contents. Parquet column
statistics can be stale or false even when a footer is structurally valid.
Object-version pinning, ETags and replay digests do not establish that the
producer's original statistics matched its values.

Valet reads values by default. Explicit `--parquet-stats trusted` can accept
eligible producer claims with reduced payload coverage; its result and evidence
ledger disclose that policy. See [statistics and tampering](../schema/#trust-and-tampering)
and [output modes and metadata trust](../validation-modes/) before enabling it.

An attacker who can replace both a file and its untrusted checksum can replace
the apparent evidence too. Keep independently trusted digests, signatures and
producer permissions outside the same unrestricted write boundary as the data.

## Signals and cancellation

SIGINT and SIGTERM cancel the run, close readers/writers, abort known multipart
uploads and avoid a terminal manifest. Conventional pre-commit exit codes are
`130` and `143`.

## Logging

Logs go to stderr; stdout is reserved for requested presentation output.

```bash
valet validate ... --log-format json --log-level info
valet validate ... --log-level debug
```

Every run-scoped record includes `run_id`. URIs are redacted before logging;
row values and credentials are not logged.

JSON diagnostics include `log_version`, `event`, run-local `seq` and command
identity. `run.phase` records monotonic work/queue-wait measurements;
`run.finished` reports the final exit/commit outcome, byte counters, managed peak
and coverage. Concurrent phase work can overlap and is not additive CPU time.

Per-group `parquet.stats_decision` events are debug-level; summaries and a
producer-trust warning explain actual skips. Raw cells and raw min/max values
are not logged. A logging failure alone does not abort validation; an exit-3
stdout failure still reports the already committed manifest.

## Health checks for batch jobs

Treat exits `0` and `1` as completed runs and read their manifests. Treat exit
`2` and signal exits as uncommitted failures. Exit `3` means the manifest is
already authoritative even though stdout presentation failed.
