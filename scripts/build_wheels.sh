#!/usr/bin/env bash
#
# Build all three velox-spark wheels from one source tree.
#
#   dist/velox_spark-<ver>-py3-none-manylinux_2_17_x86_64.whl   <- x86 JAR inside
#   dist/velox_spark-<ver>-py3-none-manylinux_2_35_aarch64.whl  <- ARM JAR inside
#   dist/velox_spark-<ver>-py3-none-any.whl                     <- no JAR
#
# pip automatically picks the most specific match for the machine it runs on,
# so every user types the same `pip install velox-spark` and Linux hosts get
# the native engine while macOS/Windows fall back to plain Spark.
#
# Usage:
#   scripts/build_wheels.sh --x86-jar path/to/bundle.jar \
#                           --arm-jar path/to/bundle.jar
#
# Either JAR may be omitted; the corresponding wheel is skipped. The pure
# Python wheel is always built.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
JAR_DIR="$ROOT/src/velox_spark/jars"
OUT="$ROOT/dist"

X86_JAR=""
ARM_JAR=""

# --- glibc floors -----------------------------------------------------------
# The platform tag encodes the oldest system a wheel will install on, and pip
# enforces it. Get it wrong in the permissive direction and the wheel installs
# happily on a host too old to load the native library, failing later with an
# unreadable link error.
#
# The floor is read out of the native libraries themselves -- see
# detect_platform_tag(). Filenames are not reliable for this: upstream renamed
# the bundles between 1.5.0 (`centos_7_x86_64`) and 1.6.0 (`linux_amd64`), and
# neither name states a glibc version.

# Used only when the JAR carries no readable ELF, or objdump is unavailable.
X86_PLAT=""
ARM_PLAT=""
X86_PLAT_FALLBACK="manylinux_2_17_x86_64"
ARM_PLAT_FALLBACK="manylinux_2_35_aarch64"

EXTRA_JARS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --x86-jar)   X86_JAR="$2"; shift 2 ;;
        --arm-jar)   ARM_JAR="$2"; shift 2 ;;
        --x86-plat)  X86_PLAT="$2"; shift 2 ;;
        --arm-plat)  ARM_PLAT="$2"; shift 2 ;;
        # Pure-Java companions (gluten-iceberg, iceberg-spark-runtime) carry
        # no native code, so the same file goes into every platform wheel.
        --extra-jar) EXTRA_JARS+=("$2"); shift 2 ;;
        --out)       OUT="$2"; shift 2 ;;
        -h|--help)   sed -n '2,25p' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

for extra in "${EXTRA_JARS[@]:-}"; do
    [[ -z "$extra" || -f "$extra" ]] || { echo "!! --extra-jar $extra not found" >&2; exit 1; }
done

PKG_VERSION="$(grep -m1 '^version = ' "$ROOT/pyproject.toml" | cut -d'"' -f2)"
echo "==> package version ${PKG_VERSION}"

# The package version's first three components must equal the Gluten release in
# the JAR filename. This is the check that stops a config wheel from ever being
# paired with a JAR built against a different Spark shim.
check_jar_version() {
    local jar="$1" arch="$2" base jar_version snapshot=""
    base="$(basename "$jar")"
    jar_version="$(echo "$base" | sed -nE 's/.*-([0-9]+\.[0-9]+\.[0-9]+)-SNAPSHOT\.jar$/\1/p')"
    if [[ -n "$jar_version" ]]; then
        snapshot=1
    else
        jar_version="$(echo "$base" | sed -nE 's/.*-([0-9]+\.[0-9]+\.[0-9]+)\.jar$/\1/p')"
    fi
    if [[ -z "$jar_version" ]]; then
        echo "!! cannot read a Gluten version from ${base}" >&2
        echo "   expected a name ending in -X.Y.Z.jar or -X.Y.Z-SNAPSHOT.jar" >&2
        exit 1
    fi

    # Releases pair with X.Y.Z.<rev> package versions; SNAPSHOT jars only with
    # X.Y.Z.devN. The asymmetry is deliberate: a snapshot must never be
    # installable under a version string that reads as a release.
    if [[ -n "$snapshot" ]]; then
        if [[ "$PKG_VERSION" != "$jar_version".dev* ]]; then
            echo "!! ${arch} JAR is a ${jar_version}-SNAPSHOT (unreleased main build)" >&2
            echo "   pyproject.toml : ${PKG_VERSION}" >&2
            echo "   Snapshot jars require a .devN package version, e.g." >&2
            echo "   ${jar_version}.dev1 -- so users can see it is a pre-release." >&2
            exit 1
        fi
        echo "    ${arch} JAR is Gluten ${jar_version}-SNAPSHOT (dev pairing ok)"
    else
        if [[ "$PKG_VERSION" != "$jar_version".* || "$PKG_VERSION" == "$jar_version".dev* ]]; then
            echo "!! version mismatch for ${arch}" >&2
            echo "   pyproject.toml : ${PKG_VERSION}" >&2
            echo "   ${arch} JAR    : ${jar_version} (${base})" >&2
            echo "   Set pyproject version to ${jar_version}.<revision> and retry." >&2
            exit 1
        fi
        echo "    ${arch} JAR matches Gluten ${jar_version}"
    fi
}

# Derive the platform tag by reading the native libraries inside the JAR.
#
# Filenames are not trustworthy for this. Upstream has already changed the
# convention once -- 1.5.0 shipped `centos_7_x86_64`, 1.6.0 ships `linux_amd64`
# -- and neither name states the actual glibc floor. The ELF headers do: the
# highest GLIBC_x.y symbol version any library references IS the floor, and the
# ELF machine type IS the architecture. Both are facts about the binary that
# survive any renaming upstream does next.
#
# This also catches the mistake that matters most: a bundle JAR is NOT
# architecture-neutral -- it wraps libvelox.so and libgluten.so -- so handing
# the x86_64 bundle to --arm-jar produces a wheel that installs on the DGX and
# then dies at native library load.
detect_platform_tag() {
    local jar="$1" expected_arch="$2" fallback="$3" override="$4"

    if [[ -n "$override" ]]; then
        echo "$override"
        return
    fi

    if ! command -v objdump >/dev/null; then
        echo "    objdump not found (install binutils), falling back to ${fallback}" >&2
        echo "$fallback"
        return
    fi

    python3 - "$jar" "$expected_arch" "$fallback" <<'PY'
import re, shutil, subprocess, sys, tempfile, zipfile
from pathlib import Path

jar, expected_arch, fallback = sys.argv[1], sys.argv[2], sys.argv[3]

def note(msg):
    print(msg, file=sys.stderr)

ELF_MACHINE = {"i386:x86-64": "x86_64", "aarch64": "aarch64"}

with zipfile.ZipFile(jar) as zf:
    # Endswith, not a glob: '*.so*' also matches names like
    # '...factory.set.sorted.MutableSortedSetFactory'.
    libs = [n for n in zf.namelist() if n.endswith(".so")]
    if not libs:
        note(f"    no .so found inside {Path(jar).name}, falling back to {fallback}")
        print(fallback)
        sys.exit(0)

    tmp = Path(tempfile.mkdtemp(prefix="velox-spark-"))
    try:
        floors, arches = set(), set()
        for name in libs:
            path = tmp / Path(name).name
            with zf.open(name) as src, open(path, "wb") as dst:
                shutil.copyfileobj(src, dst)

            syms = subprocess.run(["objdump", "-T", str(path)],
                                  capture_output=True, text=True).stdout
            for major, minor in re.findall(r"GLIBC_(\d+)\.(\d+)", syms):
                floors.add((int(major), int(minor)))

            hdr = subprocess.run(["objdump", "-f", str(path)],
                                 capture_output=True, text=True).stdout
            m = re.search(r"architecture:\s*([^,]+)", hdr)
            if m:
                arches.add(ELF_MACHINE.get(m.group(1).strip(), m.group(1).strip()))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

if len(arches) > 1:
    note(f"!! JAR contains libraries for multiple architectures: {sorted(arches)}")
    sys.exit(1)

arch = arches.pop() if arches else None
if arch and arch != expected_arch:
    note("!! architecture mismatch")
    note(f"   {Path(jar).name}")
    note(f"   contains {arch} libraries, but was passed as the {expected_arch} JAR.")
    note("   Bundle JARs contain compiled .so files and are not portable")
    note("   across architectures.")
    sys.exit(1)

if not floors:
    note(f"    no GLIBC symbol versions found, falling back to {fallback}")
    print(fallback)
    sys.exit(0)

major, minor = max(floors)
note(f"    {len(libs)} native libs, highest glibc requirement {major}.{minor}")
print(f"manylinux_{major}_{minor}_{arch or expected_arch}")
PY
}

clean_jar_dir() {
    find "$JAR_DIR" -type f ! -name 'README.md' -delete
}

# Pull the upstream attribution files out of the bundle so the wheel carries
# them. The bundle statically links Velox, Arrow, folly and a long dependency
# tail; redistributing it without NOTICE is a licence violation.
extract_notices() {
    local jar="$1"
    python3 - "$jar" "$JAR_DIR" <<'PY'
import sys, zipfile
from pathlib import Path

jar, out = sys.argv[1], Path(sys.argv[2])
copied = 0
with zipfile.ZipFile(jar) as zf:
    for name in zf.namelist():
        base = name.rsplit("/", 1)[-1]
        if name.startswith("META-INF/") and base.upper().startswith(("LICENSE", "NOTICE")):
            (out / base).write_bytes(zf.read(name))
            copied += 1
print(f"    extracted {copied} licence/notice file(s) from the bundle")
PY
}

build_wheel() {
    local plat="$1"
    rm -rf "$ROOT/build" "$ROOT"/src/*.egg-info
    python3 -m build --wheel --outdir "$OUT" "$ROOT" >/dev/null

    local wheel
    wheel="$(ls -t "$OUT"/velox_spark-"$PKG_VERSION"-py3-none-any.whl)"

    if [[ "$plat" == "any" ]]; then
        echo "    -> $(basename "$wheel")"
        return
    fi

    # setuptools tags this py3-none-any because there is no compiled Python
    # extension in it -- the architecture-specific part is a .so inside a JAR,
    # which the build backend cannot see. Retag so pip refuses to install it on
    # the wrong machine.
    python3 -m wheel tags \
        --python-tag py3 --abi-tag none --platform-tag "$plat" \
        --remove "$wheel" >/dev/null
    echo "    -> velox_spark-${PKG_VERSION}-py3-none-${plat}.whl"
}

mkdir -p "$OUT" "$JAR_DIR"
trap clean_jar_dir EXIT

build_arch() {
    local arch="$1" jar="$2" fallback="$3" override="$4" plat

    if [[ -z "$jar" ]]; then
        echo "==> ${arch}: no JAR given, skipping"
        echo "    (hosts of this architecture will get the pure-Python wheel"
        echo "     and run on unaccelerated Spark, with a warning)"
        return
    fi
    [[ -f "$jar" ]] || { echo "!! ${arch}: $jar not found" >&2; exit 1; }

    echo "==> ${arch} wheel"
    check_jar_version "$jar" "$arch"
    # Not `local plat=$(...)`: `local` would swallow a nonzero exit status and
    # let an architecture mismatch through.
    plat="$(detect_platform_tag "$jar" "$arch" "$fallback" "$override")"
    clean_jar_dir
    cp "$jar" "$JAR_DIR/"
    for extra in "${EXTRA_JARS[@]:-}"; do
        [[ -n "$extra" ]] && cp "$extra" "$JAR_DIR/"
    done
    extract_notices "$jar"
    build_wheel "$plat"
}

build_arch x86_64  "$X86_JAR" "$X86_PLAT_FALLBACK" "$X86_PLAT"
build_arch aarch64 "$ARM_JAR" "$ARM_PLAT_FALLBACK" "$ARM_PLAT"

echo "==> pure Python wheel (no JAR)"
clean_jar_dir
build_wheel "any"

echo
echo "Built into ${OUT}:"
ls -1sh "$OUT"/*.whl
echo
echo "Upload all of them to the SAME index. If the platform wheels are on an"
echo "index a user cannot reach, pip silently installs the pure one and they"
echo "get unaccelerated Spark with only a warning to explain it."
