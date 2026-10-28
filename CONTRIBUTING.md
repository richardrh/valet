
## Commit messages

Use [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>: <imperative summary, ≤72 chars>

<optional body: why, not what>
```

| Type | Use for |
|---|---|
| `feat` | New user-visible behaviour |
| `fix` | Bug fix |
| `perf` | Faster or smaller, with no change in behaviour |
| `refactor` | Restructuring with no change in behaviour |
| `chore` | Cleanup, dependencies and tooling |
| `docs` | Documentation only |
| `test` | Tests only |
| `ci` | Workflows and release scripts |

Add `!` after the type for breaking changes (`feat!: …`) and explain the break
in the body.

## Pull requests

- Keep each PR to one concern. The PR title follows the same format as a
  commit message.
- For `feat`, `fix` and `perf`, add an entry under `[Unreleased]` in
  `CHANGELOG.md`.
- Report security issues privately, as described in [SECURITY.md](SECURITY.md).
