# valet

Valet is a command-line data validation tool for checking files before they
enter a lakehouse, warehouse or downstream pipeline.

Valet is designed for short-lived batch jobs with an explicit managed-memory
budget. That budget is not a hard process-RSS limit.

The concept of Valet is that on a server memory usage is more important than runtime.
Since it is designed for batch data validation memory constraints often play a bigger role.

Valet outputs structured logs, structured json reports and writes data to s3 compatible storage.

Valet suits any workload that can batch ingest and run cli commands, e.g. Apache Airflow or similar.

You can pipe the output of Valet reports into any ingestion tool, but piping it into jq is a good
use case to quickly query multiple valet reports.

## What can it do?

- Read CSV, Parquet, JSON and NDJSON from local files or S3-compatible storage.
- Check schemas, missing values, ranges, duplicate keys, references and column relationships.
- Return a JSON report, or optionally write CSV/Parquet row outputs and detailed issues.

## Why use it?

Catch bad data before loading it downstream. Keep your checks in YAML or JSON
and use Valet's exit code to decide whether your pipeline continues—without
writing a validation script for each input or deploying another service.


## How does it compare?
Valet runs with no Python dependency.

| Aspect          | Valet                                    | [GX Core](https://docs.greatexpectations.io/docs/core/introduction/try_gx/) | [Soda Core](https://github.com/sodadata/soda-core) |
|-----------------|------------------------------------------|-----------------------------------------------------------------------------|----------------------------------------------------|
| Best fit        | Files before ingestion                   | Python, DataFrame, SQL and Spark workflows                                  | Contracts across connected datasets                |
| Checks          | YAML or JSON                             | Python Expectations and suites                                              | YAML data contracts                                |
| Deployment      | One static executable                    | Python environment                                                          | Python environment plus connector                  |
| Python required | **No**                                   | Yes                                                                         | Yes                                                |
| DuckDB required | **No**                                   | No for pandas                                                               | Only for the DuckDB connector                      |
| Results         | JSON and exit code; optional rows/issues | Structured validation results                                               | CLI/API verification results                       |

GX and Soda also support files ([GX](https://docs.greatexpectations.io/docs/core/connect_to_data/filesystem_data/),
[Soda/DuckDB](https://docs.soda.io/soda-documentation/reference/data-source-reference-for-soda-core/duckdb/duckdb-advanced-usage)).
Choose Valet for a standalone file-validation gate, not a hosted platform.

### Measured deployment footprint
Valet has a small deployment footprint and no external dependencies to run its core.

| Linux amd64 payload                                  |        Valet | GX Core + pandas | Soda Core + DuckDB |
|------------------------------------------------------|-------------:|-----------------:|-------------------:|
| Compressed application download, excluding Python    | **12.8 MiB** |         76.4 MiB |           30.8 MiB |
| Installed application/dependencies, excluding Python | **42.5 MiB** |        248.1 MiB |           90.3 MiB |
| Installed total including Python                     | **42.5 MiB** |        324.7 MiB |          166.8 MiB |
| Python distributions                                 |     **None** |               36 |                 30 |

Measured versions: Valet `v0.1.0-rc.1`; GX `1.24.0`/pandas `3.0.6`;
Soda/connector `4.28.0`/DuckDB `2.0.0`; Python `3.12.15`.


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

[Docs and examples](https://richardrh.github.io/valet/) · [MIT License](LICENSE)
