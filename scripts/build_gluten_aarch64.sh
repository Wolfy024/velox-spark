#!/usr/bin/env bash
#
# Build the aarch64 Gluten/Velox bundle JAR. Run this on the DGX Spark.
#
#   scripts/build_gluten_aarch64.sh                 # builds from main
#   scripts/build_gluten_aarch64.sh --gluten-ref v1.5.0
#
# Output lands in ./jars/ along with gluten-commit.txt recording the exact
# commit that produced it. When a build comes out green, pin that commit --
# aarch64 breakage is an open item upstream and main will drift back under you.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/jars"
GLUTEN_REF="main"
NUM_THREADS=4
NO_CACHE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --gluten-ref) GLUTEN_REF="$2"; shift 2 ;;
        --threads)    NUM_THREADS="$2"; shift 2 ;;
        --out)        OUT="$2"; shift 2 ;;
        --no-cache)   NO_CACHE="--no-cache"; shift ;;
        -h|--help)    sed -n '2,12p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

ARCH="$(uname -m)"
if [[ "$ARCH" != "aarch64" && "$ARCH" != "arm64" ]]; then
    echo "!! this host is ${ARCH}, not aarch64." >&2
    echo "   Building under QEMU emulation takes many hours for a tree this" >&2
    echo "   size. Run this on the DGX Spark instead." >&2
    echo "   To override anyway: docker buildx build --platform linux/arm64 ..." >&2
    exit 1
fi

# Velox's heavier translation units need several GB each. With too little RAM
# the build dies well in, after a long wait, with a bare 'Killed'.
if [[ -r /proc/meminfo ]]; then
    total_gb=$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1024 / 1024 ))
    if (( total_gb < 16 )); then
        echo "!! ${total_gb}GB RAM detected; the Velox build needs ~16GB at" >&2
        echo "   NUM_THREADS=${NUM_THREADS}. Lower --threads or use a bigger host." >&2
        exit 1
    fi
fi

mkdir -p "$OUT"

echo "==> building Gluten ${GLUTEN_REF} for aarch64 with ${NUM_THREADS} threads"
echo "    this takes 1-3 hours on a warm cache and longer cold"

docker buildx build \
    --platform linux/arm64 \
    --file "$ROOT/docker/Dockerfile.gluten-aarch64" \
    --build-arg "GLUTEN_REF=${GLUTEN_REF}" \
    --build-arg "NUM_THREADS=${NUM_THREADS}" \
    --target export \
    --output "type=local,dest=${OUT}" \
    ${NO_CACHE} \
    "$ROOT"

echo
if [[ -f "$OUT/gluten-commit.txt" ]]; then
    echo "==> built from commit $(cat "$OUT/gluten-commit.txt")"
    echo "    If this build is green, pin it:"
    echo "      scripts/build_gluten_aarch64.sh --gluten-ref $(cat "$OUT/gluten-commit.txt")"
fi
echo
ls -lh "$OUT"/*.jar
echo
echo "Next:"
echo "  scripts/build_wheels.sh --x86-jar <official centos_7 jar> \\"
echo "                          --arm-jar ${OUT}/<the jar above>"
