---
title: CLI reference
weight: 10
type: docs
---

```text
valet validate -p PIPELINE [-i URI] [-o URI] [options]
valet plan     -p PIPELINE [-i URI] [options]
valet schema   -p PIPELINE [-i URI] [options]
valet --help | --version
```

| Command | Does |
|---|---|
| `validate` | Validates the data. Prints a JSON summary to stdout; with `-o`, also publishes artifacts and a manifest |
| `plan` | Describes schemas, passes and state without validating. `--format pipeline` prints an `input.schema` block inferred from the data |
| `schema` | Checks Parquet footer and schema compatibility without reading rows. See [schema-only checks](../../guides/schema/) |

CLI values override the pipeline file, which overrides defaults. Logs and diagnostics go to stderr.

## Flags

Each command accepts only its own flags.

| Flag | Value | `validate` | `plan` | `schema` |
|---|---|:-:|:-:|:-:|
| `-p`, `--pipeline` | Local `.yaml`, `.yml` or `.json` file (required) | ✓ | ✓ | ✓ |
| `-i`, `--input` | File or `s3://` object; replaces the configured source. Not stdin | ✓ | ✓ | ✓ |
| `-o`, `--output` | Directory or `s3://` prefix for artifacts | ✓ | ✓ | |
| `--memory-budget` | Size, e.g. `512MiB` (default `512MiB`) | ✓ | ✓ | ✓ |
| `--batch-target` | Size, e.g. `8MiB` | ✓ | ✓ | |
| `--workers` | Number of validation workers (default `1`) | ✓ | ✓ | |
| `--output-mode` | `all`, `failures` or `report`. Default `report` without `-o`, `all` with it | ✓ | ✓ | |
| `--parquet-stats` | `off` (default) or `trusted` | ✓ | ✓ | |
| `--format` | `plan`: `json` (default) or `pipeline`; `schema`: `json` (default) or `text` | | ✓ | ✓ |
| `--plain` | Print the summary as `key=value` lines | ✓ | | |
| `--stats` | Include per-rule failure counts and configured statistics | ✓ | | |
| `--json-output` | Also write the summary JSON to a path; `-` selects JSON on stdout | ✓ | | |
| `-v`, `--verbose` | Debug logging, same as `--log-level debug` | ✓ | ✓ | ✓ |
| `--log-level` | `debug`, `info` (default), `warn` or `error` | ✓ | ✓ | ✓ |
| `--log-format` | `text` (default) or `json` | ✓ | ✓ | ✓ |

`-h`/`--help` and `--version` work without reading any storage.

## Flag errors

Flag errors exit `2` before any input is read.

| Input | Message |
|---|---|
| `schema -p p.yml -o out` | `-o is not a schema option; schema accepts --pipeline, --input, --format, …` |
| `validate --bogus` | `unknown flag --bogus; see valet --help` |
| `--output-mode some` | `--output-mode must be all, failures or report, not "some"` |
| `--workers many` | `--workers must be a number, not "many"` |
| `-i -` | `--input - (stdin) is unsupported: inputs must be replayable files or s3:// objects` |
| `-v --log-level info` | `--verbose selects debug logging and conflicts with --log-level info` |
| `--plain --json-output -` | `--plain conflicts with --json-output -` |

## Exit codes

| Code | Meaning |
|---|---|
| `0` | Passed; warnings allowed |
| `1` | Validation or schema failure |
| `2` | Configuration, input, I/O or resource failure before anything was committed |
| `3` | The result was committed, but printing it failed |
| `130` | Interrupted with Ctrl-C (SIGINT); no result committed |
| `143` | Terminated (SIGTERM), e.g. by a CI timeout or `kill`; no result committed |

`130` and `143` follow the shell convention of 128 + signal number. See [signals and cancellation](../../guides/operations/#signals-and-cancellation) for what is cleaned up.
