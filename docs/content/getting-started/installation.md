---
title: Installation
weight: 10
type: docs
prev: getting-started
next: getting-started/quickstart
---

## Download a release

Once a version has been published, download an archive and `checksums.txt`
from [GitHub Releases](https://github.com/richardrh/valet/releases).
Replace `<tag>` with the full version tag, such as `v0.1.0` or
`v0.1.0-rc.1`; these are examples, not a promise that those releases exist.

| Platform | Archive |
| --- | --- |
| Linux amd64 | `valet_<tag>_linux_amd64.tar.gz` |
| Linux arm64 | `valet_<tag>_linux_arm64.tar.gz` |
| Windows amd64 | `valet_<tag>_windows_amd64.zip` |
| macOS Intel | `valet_<tag>_darwin_amd64.tar.gz` |
| macOS Apple Silicon | `valet_<tag>_darwin_arm64.tar.gz` |

Every archive contains `valet` (`valet.exe` on Windows), `README.md`,
`CHANGELOG.md` and `LICENSE`. Valet uses the MIT License; third-party
dependencies retain their respective licenses.

### Linux

For Linux amd64, in an empty download directory:

```bash
tag=v0.1.0 # Replace with a published tag.
archive="valet_${tag}_linux_amd64.tar.gz" # Use linux_arm64 for ARM64.
release_url="https://github.com/richardrh/valet/releases/download/$tag"
curl --fail --location --remote-name "$release_url/$archive"
curl --fail --location --remote-name "$release_url/checksums.txt"
sha256sum --check --ignore-missing checksums.txt
tar -xzf "$archive"
mkdir -p "$HOME/.local/bin"
install -m 0755 valet "$HOME/.local/bin/valet"
"$HOME/.local/bin/valet" --version
```

Stop if checksum verification fails. Add `$HOME/.local/bin` to `PATH` if it is
not already present.

### macOS

Download the matching `darwin` archive and `checksums.txt`. Run
`shasum -a 256 valet_<tag>_darwin_<arch>.tar.gz` and compare the complete digest
with the matching filename in `checksums.txt` before extracting with `tar -xzf`.
Install the extracted `valet` in a directory on your `PATH`.

Darwin binaries are cross-built and their tests compile in CI, but they are
not executed on macOS runners. They are not code-signed or notarized; local
macOS security policy may require explicit approval.

### Windows

In PowerShell, in an empty download directory:

```powershell
$tag = "v0.1.0" # Replace with a published tag.
$archive = "valet_${tag}_windows_amd64.zip"
$releaseUrl = "https://github.com/richardrh/valet/releases/download/$tag"
Invoke-WebRequest "$releaseUrl/$archive" -OutFile $archive
Invoke-WebRequest "$releaseUrl/checksums.txt" -OutFile checksums.txt
$entry = @(Get-Content checksums.txt | Where-Object {
    ($_ -split '\s+')[-1] -eq $archive
})
if ($entry.Count -ne 1) { throw "Missing or duplicate archive checksum" }
$expected = ($entry[0] -split '\s+')[0]
if ((Get-FileHash $archive -Algorithm SHA256).Hash -ne $expected) {
    throw "Archive checksum mismatch"
}
Expand-Archive $archive -DestinationPath valet-release
.\valet-release\valet.exe --version
```

Move `valet.exe` to a directory on your `PATH`.

Checksums detect corrupted or mismatched downloads; they are not a detached
signature or an independent authenticity guarantee. Download both files from
the intended repository's release over HTTPS.

## Build from source

- Go 1.27.2 or later is required, matching the repository's `go.mod`.
- No Python, Polars, cgo, Arrow C++ or native compression library is required.


```bash
git clone https://github.com/richardrh/valet.git
cd valet
mkdir -p dist
CGO_ENABLED=0 go build -trimpath -o dist/valet ./cmd/valet
```

To build a specific release, check out its published version tag before
running `go build`. The default source build reports a development version;
use the version-stamping command below when a version string is needed.

Verify the binary:

```bash
dist/valet --version
dist/valet --help
```

## Cross-build

Linux arm64:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -trimpath -o dist/valet-linux-arm64 ./cmd/valet
```

Windows amd64:

```bash
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
  go build -trimpath -o dist/valet.exe ./cmd/valet
```

## Reproducible version stamping

Release builds can inject the version printed by `--version`:

```bash
CGO_ENABLED=0 go build \
  -trimpath \
  -ldflags="-s -w -X main.version=v0.1.0" \
  -o dist/valet \
  ./cmd/valet
```

The repository CI executes native Linux amd64/arm64 and Windows amd64 tests
and release-binary CLI acceptance, plus Darwin cross-builds and test
compilation. S3-compatible acceptance uses an isolated MinIO instance; it does
not verify real AWS. See the [CI guide](../../guides/ci/) for release publishing
and the one-time GitHub setup.
