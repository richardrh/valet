# Valet documentation site

This directory is the Hugo site root. Public user documentation lives under
`content/`; internal engineering records live under `engineering/` and are not
rendered into the site.

## Prerequisites

- Hugo Extended 0.146.0 or later. CI pins Hugo Extended 0.156.0.
- Go 1.27.2 or later (the repository's `go.mod` version), used by Hugo Modules
  to fetch the pinned Hextra v0.13.0 theme.

## Preview

From the repository root:

```bash
hugo server \
  --source docs \
  --buildDrafts \
  --disableFastRender
```

Open <http://localhost:1313/>.

## Production build

```bash
hugo --source docs --gc --minify
```

Output is written to `docs/public/` and ignored by Git.

## GitHub Pages

`.github/workflows/docs.yml` runs a production build on documentation-related
pull requests, pushes to `main` and manual runs. Pull requests have only
`contents: read`, use the repository's default `https://<owner>.github.io/<repo>/`
base URL (or the root URL for an `<owner>.github.io` repository), and do not
configure Pages, upload a Pages artifact or deploy. These builds work before
Pages is enabled, including on forks.

For a push to `main` or a manual run explicitly selecting `main`, a separate
job reads the Pages configuration with `pages: read`. The build uses that
configuration's base URL, including a configured custom domain, and uploads
the site. The deployment job alone has `pages: write` and `id-token: write`.
The workflow does not enable Pages or change repository settings.

Enable Pages once in **Settings → Pages → Build and deployment → Source →
GitHub Actions**. Restrict the `github-pages` environment to the `main` branch.
Only trusted `main` runs deploy; manual runs on other branches build only.
Concurrency is scoped by ref, so a pull request cannot cancel or block a
`main` deployment.

CI downloads Hugo Extended 0.156.0 from the
[official Hugo release](https://github.com/gohugoio/hugo/releases/tag/v0.156.0)
and checks the package against that release's `hugo_0.156.0_checksums.txt`
before installing it. See the
[CI guide](content/guides/ci.md#repository-ci-and-release-publishing) for the
complete one-time GitHub setup and version-tag publishing procedure.

## Updating Hextra

Edit `docs/go.mod`, then run:

```bash
cd docs
hugo mod tidy
hugo mod graph
```

Commit both `go.mod` and `go.sum`. Run a production build before opening a pull
request.

## Content organization

- `content/_index.md` — homepage.
- `content/getting-started/` — installation and first run.
- `content/guides/` — configuration, schema-only checks, output modes/metadata trust, outputs, operations, performance and automation.
- `content/rules.md` — copy-paste cookbook and built-in check reference.
- `engineering/architecture.md` — internal package and lifecycle design.
- `engineering/spec.md` — internal implementation contract.
- `engineering/review-remediation-spec.md` — internal remediation history.
- `engineering/validation-modes-spec.md` — report-version-2 output, schema-diff, diagnostics and trusted-statistics implementation contract.
- `engineering/validation-modes-e2e.md` — real-CLI acceptance cases, independent oracles, fault scenarios and exercised gate evidence.

### Navigation

Keep `type: docs` on `content/_index.md` even though its layout is
`hextra-home`. Hextra uses the homepage type to choose a site-wide sidebar;
without it, desktop navigation is limited to the current first-level section.
Getting Started and Validation Guides stay expanded, while `linkTitle` provides
short sidebar labels without changing article titles or URLs. The navbar's
Guides entry exposes that same guide tree in the mobile menu. The Reference
landing page remains available but is excluded from the sidebar because its
cards duplicate the actual pages.

Use page-relative Markdown links or Hugo `relref` links. Do not duplicate
normative rule or output behavior into multiple public pages; guides should
summarize and link to the owning user-facing reference.
