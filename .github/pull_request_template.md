## Summary

<!--
The smallest view that makes the change clear: a call tree, file tree,
pseudocode, a diff sketch or a Mermaid diagram. Keep prose brief.
-->

## Evidence

<!--
Show a before and after: the test that failed and now passes, command output,
benchmark numbers, or a screenshot of rendered docs.
-->

- **Before:**
  **After:**

## Merge Danger

**Door:** <!-- one-way (hard to reverse: output contracts, published artifacts, releases) or two-way -->

**Blast Radius:** <!-- one word, e.g. docs, ci, rules, ingestion, output, storage, release -->

<!-- Who or what could break: pipeline configs, exit codes, report/manifest fields, S3 behaviour. -->

## Checklist

- [ ] `gofmt -l .` is empty, and `go vet ./...` and `go test ./...` pass
- [ ] Behaviour changes have a test that failed before this change
- [ ] Rule, report, manifest or exit-code changes are documented in `docs/content/`
- [ ] `CHANGELOG.md` has an `[Unreleased]` entry for user-visible changes
