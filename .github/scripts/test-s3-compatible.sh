#!/usr/bin/env bash
set -euo pipefail

backend="${1:-}"
case "$backend" in
  minio|rustfs) ;;
  *) printf 'Usage: bash .github/scripts/test-s3-compatible.sh minio|rustfs\n' >&2; exit 2 ;;
esac

runtime="${CONTAINER_RUNTIME:-docker}"
repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/valet-s3-${backend}.XXXXXX")"
logs="${VALET_S3_LOG_DIR:-$work/logs}"
container="valet-s3-${backend}-${GITHUB_RUN_ID:-$$}-${GITHUB_RUN_ATTEMPT:-1}"
port="${VALET_S3_PORT:-19000}"
mkdir -p "$logs"
cleanup() {
  status=$?
  trap - EXIT
  if [[ -s "$work/container-id" ]]; then
    cid="$(< "$work/container-id")"
    "$runtime" logs "$cid" > "$logs/server.log" 2>&1 || true
    "$runtime" rm --force --volumes "$cid" >/dev/null || true
  fi
  rm -rf "$work"
  exit "$status"
}
trap cleanup EXIT
cd "$repo"

# These credentials belong only to this disposable loopback-bound server.
export AWS_ACCESS_KEY_ID=valet-ci
export AWS_SECRET_ACCESS_KEY=valet-ci-local-only-password
export AWS_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
export AWS_PROFILE="" AWS_CONFIG_FILE="$work/aws-config" AWS_SHARED_CREDENTIALS_FILE="$work/aws-credentials"
export VALET_S3_ENDPOINT="http://127.0.0.1:$port" VALET_S3_BUCKET=valet-ci
export VALET_S3_CREATE_BUCKET=1 VALET_S3_CREATE_VERSIONED_BUCKET=1

if [[ "$backend" == minio ]]; then
  # Official registry images for this release are no longer publicly pullable.
  # Build the same immutable source used by the original MinIO gate, without
  # substituting an older third-party image. Scratch adds no moving base image.
  minio_version=v0.0.0-20250907161309-07c3a429bfed
  # MINIO_BUILD_DIR lets CI keep the build between runs; it is keyed on this
  # script, so changing minio_version rebuilds.
  minio_build="${MINIO_BUILD_DIR:-$work/minio-build}"
  if [[ ! -x "$minio_build/minio" || ! -s "$minio_build/LICENSE" ]]; then
    GOBIN="$minio_build" CGO_ENABLED=0 go install "github.com/minio/minio@$minio_version"
    cp "$(go env GOMODCACHE)/github.com/minio/minio@$minio_version/LICENSE" "$minio_build/LICENSE"
  fi
  image=localhost/valet-ci-minio:07c3a429bfed
  "$runtime" build --file .github/containers/minio.Dockerfile --tag "$image" "$minio_build"
  "$runtime" run --detach --name "$container" --cidfile "$work/container-id" \
    --publish "127.0.0.1:$port:9000" \
    --tmpfs /data:rw,mode=1777 --tmpfs /tmp:rw,mode=1777 \
    --env "MINIO_ROOT_USER=$AWS_ACCESS_KEY_ID" \
    --env "MINIO_ROOT_PASSWORD=$AWS_SECRET_ACCESS_KEY" "$image"
  health=/minio/health/ready
else
  image=docker.io/rustfs/rustfs:1.0.3@sha256:9ca9559533b537d10482fe923288b676ad08f92a7069d9a17327fbae3cab688b
  "$runtime" run --detach --name "$container" --cidfile "$work/container-id" \
    --publish "127.0.0.1:$port:9000" \
    --env "RUSTFS_ACCESS_KEY=$AWS_ACCESS_KEY_ID" \
    --env "RUSTFS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY" \
    --env RUSTFS_ADDRESS=:9000 --env RUSTFS_CONSOLE_ENABLE=false "$image" /data
  health=/health/ready
fi
curl --fail --silent --show-error --retry 30 --retry-delay 1 --retry-connrefused \
  --retry-all-errors --max-time 5 "$VALET_S3_ENDPOINT$health"
printf '\nTesting %s (%s) at %s\n' "$backend" "$image" "$VALET_S3_ENDPOINT"

CGO_ENABLED=0 go build -trimpath -o "$work/valet" ./cmd/valet
CGO_ENABLED=1 go build -race -trimpath -o "$work/valet-race" ./cmd/valet
# The AWS-only E2E-24 is deliberately excluded, not counted as a passing or
# skipped compatibility case. Every selected test must execute on both servers.
selection='^(TestE2E(14|15|16|17|18)S3|TestVM|TestStdout)'
for flavor in release race; do
  binary="$work/valet"
  if [[ "$flavor" == race ]]; then binary="$work/valet-race"; fi
  VALET_BINARY="$binary" GORACE='halt_on_error=1 atexit_sleep_ms=0' \
    go test -count=1 -json -tags integration ./integration -run "$selection" -timeout 20m \
    | tee "$logs/$flavor.jsonl"
  jq -es '
    any(.[]; .Action == "pass" and .Package != null and .Test == null)
    and any(.[]; .Action == "pass" and .Test != null)
    and all(.[]; .Action != "skip")
  ' "$logs/$flavor.jsonl" >/dev/null
done
