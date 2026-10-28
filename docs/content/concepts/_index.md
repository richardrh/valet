---
title: Concepts
weight: 15
type: docs
sidebar:
  open: true
---

How one `valet validate` run turns input files into a report and an exit code.

```mermaid
flowchart LR
  A[Read inputs<br/>local or S3] --> B[Convert to schema<br/>ingestion issues]
  B --> C[Run checks<br/>row, global, reference, schema]
  C --> D[Row buckets<br/>pass / warn / error]
  D --> E[Report on stdout<br/>outputs + manifest with -o]
  E --> F[Exit code<br/>0 / 1 / 2 / 3]
```

| Page | Covers |
|---|---|
| [Pipelines](pipelines/) | Pipeline file, inputs and formats, input modes, declared schemas, type names |
| [Rules and checks](rules-and-checks/) | Rules, levels, check scopes, conditions, row buckets, ingestion issues, dataset rules |
| [Results](results/) | Report, exit codes, output modes, issues and rejects, manifest |
| [Execution](execution/) | Memory budget, batches and workers, passes, malformed input |
