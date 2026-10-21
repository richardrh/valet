# Security policy

## Supported versions

Security fixes target the latest published release, including the latest
prerelease until a stable release is available. Older releases are not
maintained; upgrade before checking whether a reported issue still applies.
The `main` branch is development code, not a supported release.

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/richardrh/valet/security/advisories/new).
Do not disclose an unpatched vulnerability in a public issue.

Include the Valet version, operating system, storage backend/version, a minimal
pipeline and synthetic input, reproduction steps, and the expected impact.
For resource-exhaustion reports, include the configured limits and distinguish
managed-memory accounting from measured process RSS. Remove credentials,
signed URLs, personal data and production records from all attachments.

Coordinate disclosure through the private advisory. Response and remediation
timing depend on impact and available maintainer capacity; there is no fixed SLA.

## Scope and deployment

Report crashes, unsafe resource growth, incorrect validation or publication
that could misrepresent untrusted input, credential disclosure, and relevant
dependency vulnerabilities. A passing validation report does not make data safe
for arbitrary downstream use. Explicitly trusted Parquet statistics are producer
claims, not payload verification.

Use least-privilege storage credentials and immutable input identities. Backend
compatibility tests do not establish AWS IAM/KMS behavior or the security of an
object-store deployment. See the [operations guide](docs/content/guides/operations.md).
