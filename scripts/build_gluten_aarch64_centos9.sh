#!/usr/bin/env bash
#
# Build the aarch64 Gluten/Velox bundle JAR with a glibc 2.34 floor.
#
#   scripts/build_gluten_aarch64_centos9.sh [--gluten-ref v1.6.0] [--threads 6]
#       [--high-mem-jobs 2] [--link-jobs 1] [--mem 30g] [--swap 40g] [--cpus 8]
#       [--image <snapshot>]     # resume from a `docker commit` of a failed run
#
# Two stages: `docker build` prepares the CentOS Stream 9 toolchain image
# (cheap), then `docker run` does the compile under a hard cgroup memory cap
# and CPU quota. The cap is the point: Velox translation units take several
# GB each and the libvelox.so link more, and on a shared host an uncapped
# build is the process the kernel OOM killer reaches for first, or worse,
# not the one it reaches for. Inside the cap only this container dies.
#
# Velox marks its function-registration libraries as a high-memory ninja job
# pool; Gluten's build-velox.sh sets that pool equal to NUM_THREADS, which is
# how 6 threads at 20g got OOM-killed on MaxByAggregate.cpp. --high-mem-jobs
# caps that pool separately (patched into build-velox.sh at run time).
#
# vcpkg binaries, ccache and the maven repo live in named volumes, so a retry
# after a failure resumes rather than restarts. For the Velox object files
# themselves, snapshot the dead container and pass it back in:
#   docker commit gluten-c9-build gluten-aarch64-centos9-partial:1
#   scripts/build_gluten_aarch64_centos9.sh --image gluten-aarch64-centos9-partial:1
#
# Output: ./jars-c9/gluten-velox-bundle-spark3.5_2.12-centos_9_aarch64-<ver>.jar

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/jars-c9"
IMG="gluten-aarch64-centos9-env"
NAME="gluten-c9-build"
GLUTEN_REF="v1.6.0"
NUM_THREADS=6
HIGH_MEM_JOBS=2
LINK_JOBS=1
MEM="30g"
SWAP="40g"
CPUS="8"
RESUME_IMAGE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --gluten-ref)    GLUTEN_REF="$2"; shift 2 ;;
        --threads)       NUM_THREADS="$2"; shift 2 ;;
        --high-mem-jobs) HIGH_MEM_JOBS="$2"; shift 2 ;;
        --link-jobs)     LINK_JOBS="$2"; shift 2 ;;
        --mem)           MEM="$2"; shift 2 ;;
        --swap)          SWAP="$2"; shift 2 ;;
        --cpus)          CPUS="$2"; shift 2 ;;
        --image)         RESUME_IMAGE="$2"; shift 2 ;;
        --out)           OUT="$2"; shift 2 ;;
        -h|--help)       sed -n '2,26p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

[[ "$(uname -m)" == "aarch64" ]] || { echo "!! run this on an aarch64 host" >&2; exit 1; }
mkdir -p "$OUT"

if [[ -n "$RESUME_IMAGE" ]]; then
    IMG="$RESUME_IMAGE"
    echo "==> [$(date -u +%FT%TZ)] stage 1 skipped: resuming from ${IMG}"
else
    echo "==> [$(date -u +%FT%TZ)] stage 1: toolchain image ${IMG} (Gluten ${GLUTEN_REF})"
    docker build \
        --file "$ROOT/docker/Dockerfile.gluten-aarch64-centos9" \
        --build-arg "GLUTEN_REF=${GLUTEN_REF}" \
        --tag "$IMG" \
        "$ROOT/docker"
fi

# --memory-swap is the memory+swap total, so this is MEM plus SWAP of swap.
MEM_BYTES=$(numfmt --from=iec "${MEM^^}")
SWAP_BYTES=$(numfmt --from=iec "${SWAP^^}")
MEMSWAP=$(( MEM_BYTES + SWAP_BYTES ))

echo "==> [$(date -u +%FT%TZ)] stage 2: compile, ${NUM_THREADS} threads (${HIGH_MEM_JOBS} high-mem, ${LINK_JOBS} link), ${MEM} cap + ${SWAP} swap, ${CPUS} cpus"
docker rm -f "$NAME" >/dev/null 2>&1 || true
docker run --name "$NAME" \
    --memory="$MEM" --memory-swap="$MEMSWAP" --cpus="$CPUS" \
    -e "NUM_THREADS=${NUM_THREADS}" \
    -e "MAX_HIGH_MEM_JOBS=${HIGH_MEM_JOBS}" \
    -e "MAX_LINK_JOBS=${LINK_JOBS}" \
    -e "VCPKG_MAX_CONCURRENCY=${NUM_THREADS}" \
    -e "MAKEFLAGS=-j${NUM_THREADS}" \
    -v gluten-c9-vcpkg:/root/.cache/vcpkg \
    -v gluten-c9-ccache:/root/.cache/ccache \
    -v gluten-c9-m2:/root/.m2 \
    -v "$OUT:/out" \
    "$IMG" bash -c '
set -euxo pipefail
source /opt/rh/gcc-toolset-12/enable
gcc --version | head -1
cd /src/gluten
# Let the high-memory and link pools be capped independently of NUM_THREADS.
sed -i "s/MAX_HIGH_MEM_JOBS=\$NUM_THREADS MAX_LINK_JOBS=\$NUM_THREADS/MAX_HIGH_MEM_JOBS=\${MAX_HIGH_MEM_JOBS:-\$NUM_THREADS} MAX_LINK_JOBS=\${MAX_LINK_JOBS:-\$NUM_THREADS}/" ep/build-velox/src/build-velox.sh
grep -n "MAX_HIGH_MEM_JOBS=" ep/build-velox/src/build-velox.sh
./dev/builddeps-veloxbe.sh \
    --enable_vcpkg=ON \
    --build_tests=OFF \
    --build_benchmarks=OFF \
    --enable_s3=ON \
    --enable_hdfs=ON \
    --enable_gcs=OFF \
    --enable_abfs=OFF
mvn clean package \
    -Pbackends-velox -Pspark-3.5 -Piceberg -Phudi -Pdelta -Ppaimon \
    -DskipTests -Dmaven.source.skip=true
jar="$(find package/target -name "gluten-velox-bundle-*.jar" ! -name "*-sources.jar" | head -1)"
test -n "$jar"
version="$(basename "$jar" | sed -nE "s/.*-([0-9]+\.[0-9]+\.[0-9]+(-SNAPSHOT)?)\.jar$/\1/p")"
test -n "$version" || version=unknown
cp "$jar" "/out/gluten-velox-bundle-spark3.5_2.12-centos_9_aarch64-${version}.jar"
cp /gluten-commit.txt /out/gluten-commit.txt
ls -lh /out
'
echo "==> [$(date -u +%FT%TZ)] done"
ls -lh "$OUT"
