# Architecture

The module `github.com/richardrh/valet` uses Arrow Go arrays and record
batches, not Polars or Arrow C++. Packages under `internal/` are reusable by
commands in this module but are not an external embedding API.

| Package | Responsibility |
|---|---|
| `cmd/valet`, `internal/cli` | Process signals, commands, presentation and exit codes |
| `internal/app` | Assemble and close a run; connect source, checks and sink |
| `internal/config` | Bounded strict YAML/JSON decoding, defaults and post-override validation |
| `internal/source` | Streaming file formats, schema conversion, renames and replay |
| `internal/checks` | Compiled rules, global passes, conditions and aggregate assertions |
| `internal/storage` | Local/S3 identities, reads, range access and bounded publication |
| `internal/sink` | CSV/Parquet parts, structured issues/rejects and completion manifests |
| `internal/resource` | Managed reservations, Arrow allocator and bounded Parquet codecs |
| `internal/data` | Shared Arrow ownership, field states, provenance and result structures |

Execution accepts 1–32 workers (default 1), with bounded, ordered overlap
between input, single-owner rule evaluation and single-owner output. Parquet
row-group verification/decoding can run concurrently; queued batches and
worker workspaces share the managed budget. Rule messages are run metadata
rather than repeated per-row structs.

Uniqueness uses an exact budgeted key index and replay so the first occurrence
of a duplicate fails too. Reference indexes are projected and shared. Neither
mechanism spills to disk; an oversized index fails explicitly. The allocator
tracks live managed reservations and growth overlap, not total process RSS.

CSV is read with `encoding/csv` and JSON/NDJSON with `encoding/json/jsontext`,
one reader per object; there are no hand-written parsers. Decoding preserves
missing, null and invalid conversion states. Declared schemas yield typed Arrow
columns; invalid cells do not silently become ordinary nulls. Reject artifacts
retain original records and diagnostics.

## Logging

`cmd/valet` builds a `Runtime` holding the `*slog.Logger`, stdout, stderr,
version and an optional pre-commit summary callback, and passes it to `cli.Run`
and `app.Run`. Nothing uses a package-level logger or carries one in
`context.Context`. The handler writes to stderr only; stdout is reserved for
the summary, plan and JSON output. `--log-format text|json` picks the handler
even for argument failures, and `--log-level` the level. `-v`/`--verbose` is
the same as `--log-level debug`.

`app.Run` adds `run_id` with `Logger.With` and passes the logger to `storage`,
`source`, `checks` and `sink` through their `WithLogger` options. Each defaults
to a discard logger, so tests need no setup. Each lifecycle event is logged
once, by the package that owns it: discovery, inference and the Parquet safety
scan, compilation, prepass, reference index loads, passes, part and manifest
publication, aborts, close failures and the budget peak. Per-batch detail is
logged at debug level. URIs are logged through `storage.LogURI`, which strips
credentials, queries and fragments. Row values and credentials are never
logged. AWS SDK messages are routed into the same logger at debug level.

## Publication
Without an output URI, validation uses aggregate-only `report` mode: no sink,
output AWS client, metadata, row issues, evidence files or manifest is created.
Counts, coverage, trust, provenance and bounded schema/dataset diagnostics
remain in the summary. Stdout defaults to JSON; a presentation failure without
a committed manifest returns exit `2`, not `3`. Successful stdout-only terminal
logs report `manifest_committed: false`.


With an explicit output URI, output parts and metadata use a unique run location. Local files
and containing directory entries are synced before the terminal manifest;
ambiguous S3 writes are reconciled by write identity, length and digest.
`manifest.json` is published last, and its absence means the artifact run is incomplete
regardless of which parts exist. Requested summary files participate in the
pre-commit callback. Stdout remains post-commit presentation; failure there
returns exit `3` without invalidating the manifest. This is an application
publication protocol, not an atomic multi-object S3 transaction.

## Tests

Tests live beside owning packages; cross-component subprocess scenarios live
under `integration/`. `benchmarks/` contains opt-in generated-data workloads
that exercise the real rule engine without retaining giant fixtures.

The [specification](spec.md) records schemas, quality semantics, compatibility
changes, resource policies and acceptance cases.
