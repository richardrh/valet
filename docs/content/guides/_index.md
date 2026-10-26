---
title: Guides
linkTitle: Validation guides
weight: 30
type: docs
sidebar:
  open: true
---

Configure a pipeline, choose schema-only or full validation, then pick outputs and run it in production or CI.

{{< cards >}}
  {{< card link="configuration" title="Pipeline configuration" subtitle="YAML and JSON structure, sources, schemas and resource controls." >}}
  {{< card link="schema" title="Schema-only Parquet checks" subtitle="Check names and types without reading values; understand skipped checks and JSON results." >}}
  {{< card link="validation-modes" title="Output modes and metadata trust" subtitle="Report-only/failures-only results, explicit statistics trust and structured diagnostics." >}}
  {{< card link="output" title="Structured output" subtitle="Summaries, issues, rejects, artifacts and manifests." >}}
  {{< card link="operations" title="Operations" subtitle="Memory sizing, S3, exit codes and production deployment." >}}
  {{< card link="performance" title="Performance" subtitle="Measured Parquet throughput, schema-only timing and capacity planning." >}}
  {{< card link="ci" title="CI automation" subtitle="Run Valet in GitHub Actions and consume structured results." >}}
{{< /cards >}}
