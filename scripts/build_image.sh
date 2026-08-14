#!/usr/bin/env bash
#
# Build the multi-arch runtime image.
#
#   scripts/build_image.sh --tag myorg/velox-spark:1.5.0.1 --push
#
# One tag, two variants (linux/amd64 + linux/arm64). Hosts and Docker Desktop
# on Macs pull the right one automatically.
#
# Note on where this runs: buildx will happily emulate arm64 on an x86 host,
# but every apt and pip step then runs under QEMU. It works and it is slow. If
# you have the DGX available, register it as a native builder instead:
#
#   docker buildx create --name multi --node local  --platform linux/amd64
#   docker buildx create --name multi --append --node dgx \
#          --platform linux/arm64 ssh://user@dgx
#   docker buildx use multi
#
# Requires ./dist to be populated by scripts/build_wheels.sh.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TAG=""
PLATFORMS="linux/amd64,linux/arm64"
PUSH=0
LOAD=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --tag)       TAG="$2"; shift 2 ;;
        --platforms) PLATFORMS="$2"; shift 2 ;;
        --push)      PUSH=1; shift ;;
        --load)      LOAD=1; shift ;;
        -h|--help)   sed -n '2,22p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

PKG_VERSION="$(grep -m1 '^version = ' "$ROOT/pyproject.toml" | cut -d'"' -f2)"
[[ -n "$TAG" ]] || TAG="velox-spark:${PKG_VERSION}"

shopt -s nullglob
WHEELS=("$ROOT"/dist/*.whl)
shopt -u nullglob
if [[ ${#WHEELS[@]} -eq 0 ]]; then
    echo "!! no wheels in ${ROOT}/dist. Run scripts/build_wheels.sh first." >&2
    exit 1
fi

# The image build gates on `velox-spark doctor`, which fails when no JAR is
# bundled. Catching a missing platform wheel here just turns a confusing
# mid-build failure into a clear one.
for platform in ${PLATFORMS//,/ }; do
    case "$platform" in
        */amd64) want="x86_64" ;;
        */arm64) want="aarch64" ;;
        *) continue ;;
    esac
    found=0
    for wheel in "${WHEELS[@]}"; do
        [[ "$(basename "$wheel")" == *"$want"* ]] && found=1
    done
    if [[ $found -eq 0 ]]; then
        echo "!! ${platform} requested but no ${want} wheel in dist/." >&2
        echo "   The image build would fall back to the pure-Python wheel and" >&2
        echo "   fail its doctor check. Build that wheel, or drop the platform:" >&2
        echo "     scripts/build_image.sh --platforms linux/amd64" >&2
        exit 1
    fi
done

# buildx cannot --load a multi-platform result into the local daemon; the image
# store holds one architecture per tag. Single-platform builds can.
OUTPUT="--output=type=image"
if [[ $PUSH -eq 1 ]]; then
    OUTPUT="--push"
elif [[ $LOAD -eq 1 ]]; then
    if [[ "$PLATFORMS" == *,* ]]; then
        echo "!! --load cannot take a multi-platform build." >&2
        echo "   Use --push, or build one platform: --platforms linux/arm64 --load" >&2
        exit 2
    fi
    OUTPUT="--load"
fi

echo "==> tag        ${TAG}"
echo "==> platforms  ${PLATFORMS}"
echo "==> wheels     ${#WHEELS[@]}"

docker buildx build \
    --platform "$PLATFORMS" \
    --file "$ROOT/docker/Dockerfile" \
    --build-arg "VELOX_SPARK_VERSION=${PKG_VERSION}" \
    --tag "$TAG" \
    $OUTPUT \
    "$ROOT"

echo
echo "Verify each variant actually got its native engine:"
for platform in ${PLATFORMS//,/ }; do
    echo "  docker run --rm --platform ${platform} ${TAG} velox-spark doctor"
done
