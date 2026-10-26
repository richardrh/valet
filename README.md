# valet

Valet is a validation gate for data files before they reach your warehouse or
lakehouse. It checks CSV, Parquet, JSON and NDJSON files on local disk or
S3-compatible storage against a YAML or JSON pipeline, and its exit code and
JSON report decide whether a file moves on.

Valet is designed for short-lived batch jobs with an explicit managed-memory
budget. That budget is not a hard process-RSS limit.

The concept of Valet is that on a server memory usage is more important than runtime.
Since it is designed for batch data validation memory constraints often play a bigger role.

Valet outputs structured logs, structured json reports and writes data to s3 compatible storage.

Valet suits any workload that can batch ingest and run cli commands, e.g. Apache Airflow or similar.

You can pipe the output of Valet reports into any ingestion tool, but piping it into jq is a good
use case to quickly query multiple valet reports.

## Performance
Valet trades speed for bounded memory: it reads and checks every row itself, so
its memory is capped by the managed budget while pandas holds the whole file.

| 10M rows, 4 checks             | Valet, 1 worker | Valet, 4 workers | GX Core + pandas |
|--------------------------------|----------------:|-----------------:|-----------------:|
| CSV (251 MiB): wall time       |           5.4 s |            3.6 s |        **2.7 s** |
| CSV: peak memory               |      **53 MiB** |           74 MiB |        1,467 MiB |
| Parquet (93 MiB): wall time    |       **3.0 s** |        **2.5 s** |           17.2 s |
| Parquet: peak memory           |     **112 MiB** |          280 MiB |        2,654 MiB |

Both tools reported the same failure counts. Clean, generated data on an Intel
Core Ultra X7 358H (16 threads), warm page cache; medians of three fresh
processes. Versions: GX `1.24.1`/pandas `3.1.0`, Python `3.12`. Reproduce with
[`benchmarks/competitors`](benchmarks/competitors/run.py); method and
dirty-input behaviour are in the
[performance guide](https://richardrh.github.io/valet/guides/performance/#comparison-with-gx-core).

## Where does it fit?

Valet checks a file before anything loads it, while the original records still
exist:

- **Loading into staging.** Validate each file drop and load it only on exit
  `0`. With an output destination (`-o`), rejected rows keep their original
  records, so the producer can see exactly what to fix.
- **Accepting large uploads.** A service that accepts CSV, Parquet or JSON
  uploads can run Valet on the stored upload (local or S3) and accept or refuse
  it from the exit code and report. Memory is set by the managed budget, not by
  the size of the upload.
- **Pipeline and CI gates.** Any scheduler that runs commands, such as Airflow
  or a CI job, can block the next step when error-level checks fail.

Once data is in the warehouse, warehouse-side tools such as Soda or GX can
monitor it. Valet complements them; it does not replace them.

## What can it do?

- Read CSV, Parquet, JSON and NDJSON from local files or S3-compatible storage.
- Check schemas, missing values, ranges, duplicate keys, references and column relationships.
- Return a JSON report, or optionally write CSV/Parquet row outputs and detailed issues.
- Treat values that fail conversion (for example `abc` in a decimal column) as
  row results: the row fails, the cell is counted, and with an output
  destination the original record is written to rejects.
- Compare integers and decimals exactly, without rounding through floats.

Structurally malformed input (a CSV row with the wrong number of fields, broken
quoting or invalid UTF-8) stops the run with exit `2` and an error naming the
file, the line and how to fix it. With `input.on_malformed: reject`, CSV rows
with the wrong number of fields become rejects instead and the rest of the
file is still checked.

## Why use it?

Catch bad data before loading it downstream. Keep your checks in YAML or JSON
and use Valet's exit code to decide whether your pipeline continues—without
writing a validation script for each input or deploying another service.


## How does it compare?
Valet runs with no Python dependency.

| Aspect          | Valet                                    | [GX Core](https://docs.greatexpectations.io/docs/core/introduction/try_gx/) | [Soda Core](https://github.com/sodadata/soda-core) |
|-----------------|------------------------------------------|-----------------------------------------------------------------------------|----------------------------------------------------|
| Best fit        | Files before they are loaded             | Python, DataFrame, SQL and Spark workflows                                  | Checks on tables in a connected data source        |
| Who reads the data | Valet itself                          | pandas, Spark or the database                                               | The connected data source's SQL engine             |
| Unparseable values | Row fails, cell counted, original record kept as a reject | pandas reads the column as text; range expectations return an error instead of a count | Must be handled while loading; Soda's docs recommend staging values as text and adding format checks |
| Malformed rows  | Run stops with exit `2`                  | `pandas.read_csv` raises                                                    | Must be handled by the loader before Soda runs     |
| Checks          | YAML or JSON                             | Python Expectations and suites                                              | YAML data contracts                                |
| Deployment      | One static executable                    | Python environment                                                          | Python environment plus connector                  |
| Python required | **No**                                   | Yes                                                                         | Yes                                                |
| Results         | JSON and exit code; optional rows/issues | Structured validation results                                               | CLI/API verification results                       |

GX also reads files directly
([GX](https://docs.greatexpectations.io/docs/core/connect_to_data/filesystem_data/)).
Choose Valet for a standalone file-validation gate, not a hosted platform.

### Measured deployment footprint
Valet has a small deployment footprint and no external dependencies to run its core.

| Linux amd64 payload                                  |        Valet | GX Core + pandas |
|------------------------------------------------------|-------------:|-----------------:|
| Compressed application download, excluding Python    | **12.8 MiB** |         76.4 MiB |
| Installed application/dependencies, excluding Python | **42.5 MiB** |        248.1 MiB |
| Installed total including Python                     | **42.5 MiB** |        324.7 MiB |
| Python distributions                                 |     **None** |               36 |

Measured versions: Valet `v0.1.0-rc.1`; GX `1.24.0`/pandas `3.0.6`; Python
`3.12.15`.


## Install

[Download a binary](https://github.com/richardrh/valet/releases), or use Go 1.27.3+:

```sh
go install github.com/richardrh/valet/cmd/valet@latest
```

## Run
jq is not required but the below example shows how a user can filter or format the output
by piping into other cli tools.

```sh
valet validate -p pipeline.yml -i input.parquet > report.json
jq '{validation, total, pass, warn, error}' report.json
```

Exit `0`: passed (warnings allowed). Exit `1`: failed checks. Exit `2`: execution
failed. Use Valet's exit code—not `jq`'s—to gate the next step.

Editors can autocomplete and check pipelines against the published
[JSON Schema](https://richardrh.github.io/valet/schema/v1/pipeline.schema.json);
AI agents can load all docs from
[`llms-full.txt`](https://richardrh.github.io/valet/llms-full.txt). See
[editor support and AI agents](https://richardrh.github.io/valet/guides/configuration/#editor-support-and-ai-agents).

[Docs and examples](https://richardrh.github.io/valet/) · [MIT License](LICENSE)
