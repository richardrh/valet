#!/usr/bin/env bash
set -euo pipefail

: "${VERSION:?Set VERSION to the release tag}"
if [[ ! "$VERSION" =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$ ]]; then
  printf 'Unsupported release tag: %s\n' "$VERSION" >&2
  exit 1
fi

mkdir -p dist
output="$(realpath dist)"
staging="$(mktemp -d)"
trap 'rm -rf "$staging"' EXIT

metadata=(README.md CHANGELOG.md LICENSE SECURITY.md)

archives=()
for target in linux-amd64 linux-arm64 windows-amd64 darwin-amd64 darwin-arm64; do
  executable=valet
  if [[ "$target" == windows-* ]]; then
    executable=valet.exe
  fi
  source="artifacts/valet-$target/$executable"
  test -s "$source"
  directory="$staging/$target"
  mkdir -p "$directory"
  cp "$source" "$directory/$executable"
  cp "${metadata[@]}" "$directory/"
  name="valet_${VERSION}_${target//-/_}"
  if [[ "$target" == windows-* ]]; then
    archive="$output/$name.zip"
    (cd "$directory" && zip -q "$archive" "$executable" "${metadata[@]}")
  else
    chmod 755 "$directory/$executable"
    archive="$output/$name.tar.gz"
    tar -czf "$archive" -C "$directory" "$executable" "${metadata[@]}"
  fi
  archives+=("$(basename "$archive")")
done

(cd "$output" && sha256sum "${archives[@]}" > checksums.txt && sha256sum --check checksums.txt)
