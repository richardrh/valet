---
title: Installation
weight: 10
type: docs
prev: getting-started
next: getting-started/quickstart
---

Go 1.27.3+ is needed only for Go builds; no cgo or native libraries.

## Download a release

Get an archive and `checksums.txt` from [GitHub Releases](https://github.com/richardrh/valet/releases). `<tag>` is a full published tag, e.g. `v0.1.0-rc.2`; prereleases are not marked latest stable.

| Platform | Archive |
| --- | --- |
| Linux amd64 | `valet_<tag>_linux_amd64.tar.gz` |
| Linux arm64 | `valet_<tag>_linux_arm64.tar.gz` |
| Windows amd64 | `valet_<tag>_windows_amd64.zip` |
| macOS Intel | `valet_<tag>_darwin_amd64.tar.gz` |
| macOS Apple Silicon | `valet_<tag>_darwin_arm64.tar.gz` |

Archives contain `valet` (`valet.exe`), `README.md`, `CHANGELOG.md`, `LICENSE` (MIT; dependencies keep their licenses), `SECURITY.md`. Checksums catch corrupt downloads, not tampering; fetch both over HTTPS from this repo.

### Linux and macOS

In an empty directory (Linux amd64 shown):

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

Stop if the checksum fails; add `$HOME/.local/bin` to `PATH`. On macOS use the `darwin_amd64`/`darwin_arm64` archive and replace `sha256sum` with `shasum -a 256 "$archive"`, comparing against `checksums.txt`. Darwin builds are unsigned, not notarized, and not run on macOS in CI; macOS may ask for approval.

### Windows

PowerShell, in an empty directory:

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

Move `valet.exe` onto your `PATH`.

## Install with Go

```bash
go install github.com/richardrh/valet/cmd/valet@latest   # or @v0.1.0-rc.2 to pin
```

Installs to `GOBIN` (default `$(go env GOPATH)/bin`); add it to `PATH`. `--version` prints `dev`.

## Build from source

```bash
git clone https://github.com/richardrh/valet.git && cd valet
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=v0.1.0" -o dist/valet ./cmd/valet
```

Check out a tag first for a specific release; omit `-ldflags` for a development version. Cross-build with `GOOS`/`GOARCH` (e.g. `GOOS=windows GOARCH=amd64`, `-o dist/valet.exe`). Platform test coverage: [CI guide](../../guides/ci/).
