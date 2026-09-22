"""Startup checks that turn a native crash on the first query into a sentence.

Every check here exists because a real deployment failed *after* the session
looked healthy. Velox's failure mode for a missing prerequisite is a JNI
exception three minutes into the first scan, with a stack trace that names
nothing a data engineer can act on. These checks run before the JVM starts,
where the fix is still cheap.

Order of appearance matches the order the failures were met:

1. **Architecture.** A bundle JAR wraps ``libgluten.so`` / ``libvelox.so`` for
   exactly one CPU. Pointing ``GLUTEN_JAR_PATH`` at an amd64 jar on an ARM
   node loads fine and dies at the first native call with
   ``FileNotFoundException: linux/aarch64/libgluten.so``.
2. **Time zone database.** Velox reads the IANA zoneinfo directory from the
   OS. Slim container images ship without ``tzdata`` and every native task
   fails with ``discover_tz_dir failed to find zoneinfo``. Gluten 1.7.0's
   Velox honours ``TZDIR``; this package depends on the ``tzdata`` wheel so
   it can point ``TZDIR`` at a bundled copy when the OS has none.
3. **Classloader split.** The Gluten bundle sits on the driver's application
   classpath. Jars delivered through ``--packages`` / ``spark.jars`` live in
   Spark's child classloader, which the application loader cannot see. Any
   class the bundle reaches for by name -- Iceberg's ``SparkBatchQueryScan``,
   then Iceberg's ``S3FileIO`` and its AWS SDK -- must therefore be on the
   application classpath too, or the first Iceberg scan fails with
   ``NoClassDefFoundError``.
"""

from __future__ import annotations

import glob
import os
import platform
import re
import shlex
import zipfile
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------
# 1. architecture
# ---------------------------------------------------------------------------

# platform.machine() -> the directory Gluten's JniLibLoader looks under.
_JNI_ARCH = {"x86_64": "amd64", "AMD64": "amd64", "aarch64": "aarch64", "arm64": "aarch64"}


def host_jni_arch() -> Optional[str]:
    """The ``linux/<arch>/`` directory name Gluten will look for on this host."""
    return _JNI_ARCH.get(platform.machine())


def jar_native_archs(jar: os.PathLike) -> List[str]:
    """Architectures whose ``libgluten.so`` the bundle carries (usually one)."""
    try:
        with zipfile.ZipFile(jar) as zf:
            names = zf.namelist()
    except (OSError, zipfile.BadZipFile):
        return []
    found = []
    for name in names:
        m = re.fullmatch(r"linux/([a-z0-9_]+)/libgluten\.so", name)
        if m:
            found.append(m.group(1))
    return sorted(found)


def check_jar_architecture(jar: os.PathLike) -> Optional[str]:
    """Return an explanation when the bundle cannot run on this CPU, else None.

    Cheap: reads the zip directory only, never the 100+ MB libraries.
    """
    want = host_jni_arch()
    have = jar_native_archs(jar)
    if want is None or not have:
        # Unknown host arch or a jar with no native libs: nothing to say here;
        # the JVM will report whatever happens next.
        return None
    if want in have:
        return None
    return (
        f"{Path(jar).name} carries native libraries for {', '.join(have)} "
        f"but this host is {platform.machine()} (needs linux/{want}/). "
        "A Gluten bundle is compiled for one CPU architecture; this one "
        "would load and then fail on the first native call with "
        f"'FileNotFoundException: linux/{want}/libgluten.so'. Install the "
        f"velox-spark wheel for {platform.machine()}, or point GLUTEN_JAR_PATH "
        "at a bundle built for it."
    )


# ---------------------------------------------------------------------------
# 2. time zone database
# ---------------------------------------------------------------------------

_OS_TZ_DIRS = ("/usr/share/zoneinfo/uclibc", "/usr/share/zoneinfo")
TZ_ENV = "TZDIR"


def os_timezone_database() -> Optional[str]:
    """The zoneinfo directory the OS provides, if any (what Velox looks for)."""
    for d in _OS_TZ_DIRS:
        if os.path.isdir(d):
            return d
    return None


def bundled_timezone_database() -> Optional[str]:
    """The zoneinfo directory from the ``tzdata`` wheel this package depends on."""
    try:
        import tzdata  # type: ignore
    except ImportError:
        return None
    path = os.path.join(os.path.dirname(tzdata.__file__), "zoneinfo")
    return path if os.path.isdir(path) else None


def ensure_timezone_database() -> Tuple[str, Optional[str]]:
    """Make sure the native engine will find a zoneinfo directory.

    Returns ``(status, note)``:

    * ``("os", None)``: the OS has one; nothing to do.
    * ``("env", None)``: ``TZDIR`` was already set by the operator.
    * ``("bundled", dir)``: no OS database, ``TZDIR`` now points at the
      ``tzdata`` wheel's copy. Honoured by Gluten >= 1.7.0.
    * ``("missing", warning)``: nothing available; the first native query
      will fail and the warning says how to fix it.

    Must run before the JVM starts: the native library reads ``TZDIR`` from
    the JVM's environment, which is a copy of this process's environment at
    launch time.
    """
    if os_timezone_database():
        return "os", None
    preset = os.environ.get(TZ_ENV)
    if preset and os.path.isdir(preset):
        return "env", None
    bundled = bundled_timezone_database()
    if bundled:
        os.environ[TZ_ENV] = bundled
        return "bundled", bundled
    return "missing", (
        "no IANA time zone database: /usr/share/zoneinfo does not exist and "
        "the tzdata Python package is not installed. Velox needs one and "
        "fails every native task with 'discover_tz_dir failed to find "
        "zoneinfo'. Fix: install the OS package (apt-get install tzdata / "
        "dnf install tzdata) in the image, or `pip install tzdata` so this "
        "package can point TZDIR at it."
    )


# ---------------------------------------------------------------------------
# 3. classloader split: jars from --packages that the bundle must see
# ---------------------------------------------------------------------------

# Artifacts that Gluten's own code reaches for by class name. If any of these
# arrive via --packages/spark.jars.packages they have to be promoted onto the
# application classpath, together with everything they load themselves
# (FileIO implementations, catalogs, cloud SDKs) -- hence "all of them", not
# just the runtime.
_SPLIT_SENSITIVE = ("iceberg-spark-runtime", "iceberg-", "hudi-spark", "delta-spark", "paimon-spark")


def _submit_args() -> List[str]:
    try:
        return shlex.split(os.environ.get("PYSPARK_SUBMIT_ARGS", ""))
    except ValueError:
        return []


def declared_packages(extra_conf: Optional[Dict[str, str]] = None) -> List[str]:
    """Maven coordinates the session will ask spark-submit to resolve."""
    coords: List[str] = []
    args = _submit_args()
    for i, a in enumerate(args):
        if a == "--packages" and i + 1 < len(args):
            coords += args[i + 1].split(",")
        elif a.startswith("--packages="):
            coords += a.split("=", 1)[1].split(",")
    if extra_conf and extra_conf.get("spark.jars.packages"):
        coords += str(extra_conf["spark.jars.packages"]).split(",")
    return [c.strip() for c in coords if c.strip()]


def declared_jars(extra_conf: Optional[Dict[str, str]] = None) -> List[str]:
    """Explicit jar paths from ``--jars`` and ``spark.jars`` (local files only)."""
    paths: List[str] = []
    args = _submit_args()
    for i, a in enumerate(args):
        if a == "--jars" and i + 1 < len(args):
            paths += args[i + 1].split(",")
        elif a.startswith("--jars="):
            paths += a.split("=", 1)[1].split(",")
    if extra_conf and extra_conf.get("spark.jars"):
        paths += str(extra_conf["spark.jars"]).split(",")
    out = []
    for x in (x.strip() for x in paths):
        if not x or "://" in x and not x.startswith("file:"):
            continue
        out.append(x[len("file:"):] if x.startswith("file:") else x)
    return out


def ivy_cache_dirs() -> List[str]:
    """Where spark-submit keeps resolved ``--packages`` jars."""
    dirs = []
    if os.environ.get("SPARK_JARS_IVY"):
        dirs.append(os.path.join(os.environ["SPARK_JARS_IVY"], "jars"))
    home = os.path.expanduser("~")
    dirs += [os.path.join(home, ".ivy2.5.2", "jars"), os.path.join(home, ".ivy2", "jars")]
    return [d for d in dirs if os.path.isdir(d)]


def resolve_packages(coords: Iterable[str]) -> Tuple[List[str], List[str]]:
    """Map ``group:artifact:version`` coordinates to cached jar files.

    Returns ``(found_paths, missing_coords)``. spark-submit names cached files
    ``<group>_<artifact>-<version>.jar``; only exact matches are accepted so a
    stale sibling version is never promoted by mistake.
    """
    cache: List[str] = []
    for d in ivy_cache_dirs():
        cache += glob.glob(os.path.join(d, "*.jar"))
    by_name = {os.path.basename(p): p for p in cache}
    found, missing = [], []
    for c in coords:
        parts = c.split(":")
        if len(parts) < 3:
            missing.append(c)
            continue
        group, artifact, version = parts[0], parts[1], parts[-1]
        hit = by_name.get(f"{group}_{artifact}-{version}.jar")
        (found.append(hit) if hit else missing.append(c))
    return found, missing


def split_sensitive(coords_or_paths: Iterable[str]) -> List[str]:
    """The subset of coordinates/paths the bundle will need to see directly."""
    return [c for c in coords_or_paths if any(k in c for k in _SPLIT_SENSITIVE)]


def classpath_promotion(
    extra_conf: Optional[Dict[str, str]] = None,
) -> Tuple[List[str], List[str], Optional[str]]:
    """Decide which user-supplied jars must join the application classpath.

    Returns ``(jars_to_promote, unresolved_coords, warning)``. When nothing
    split-sensitive is declared, all three are empty. When something is but
    cannot be located (first run before spark-submit has downloaded it, or a
    custom ivy dir), the warning explains what will break and how to fix it.
    """
    coords = declared_packages(extra_conf)
    jars = declared_jars(extra_conf)
    if not split_sensitive(coords) and not split_sensitive(jars):
        return [], [], None
    found, missing = resolve_packages(coords)
    # Only the split-sensitive local jars are promoted from spark.jars/--jars;
    # a user's unrelated jars stay where they put them.
    promote = found + [j for j in split_sensitive(jars) if os.path.isfile(j)]
    warning = None
    if missing:
        warning = (
            "Iceberg/Hudi/Delta/Paimon arrive via --packages, but these "
            f"coordinates are not in the local ivy cache yet: {', '.join(missing)}. "
            "The Gluten bundle sits on the driver's application classpath and "
            "cannot see jars in Spark's child classloader, so the first table "
            "scan will fail with NoClassDefFoundError (SparkBatchQueryScan, "
            "S3FileIO, ...). Run any Spark session once to populate the cache "
            "and start again, or pass the jar files via extra_jars=[...]."
        )
    return promote, missing, warning


# ---------------------------------------------------------------------------
# error explainer: what a native failure actually means
# ---------------------------------------------------------------------------

KNOWN_FAILURES: Sequence[Tuple[str, str]] = (
    (
        r"linux/(aarch64|amd64)/libgluten\.so",
        "Wrong-architecture Gluten bundle: the jar has no native library for "
        "this CPU. Install the velox-spark wheel for this architecture "
        "(pip picks it automatically when nothing overrides it) or fix "
        "GLUTEN_JAR_PATH.",
    ),
    (
        r"discover_tz_dir failed to find zoneinfo",
        "No IANA time zone database. Install tzdata in the image, or use "
        "velox-spark >= 1.7.0.1 with the tzdata Python package so TZDIR is "
        "pointed at a bundled copy (Gluten >= 1.7.0 honours TZDIR).",
    ),
    (
        # Gluten 1.7.0 does not surface discover_tz_dir on the driver: the
        # missing database turns into a *validateConfig* failure that blames
        # the session's time zone value instead. Same cause, misleading text.
        r"session 'session_timezone' set with invalid value|"
        r"tz::getTimeZoneID\(\*tz, false\) != -1",
        "Velox rejected the session time zone. Almost always this means the "
        "IANA time zone database is missing, not that the zone name is wrong "
        "(the JVM has its own copy, so Spark accepted the name). Check that "
        "/usr/share/zoneinfo exists; install tzdata in the image, or use "
        "velox-spark >= 1.7.0.1 with the tzdata Python package, which points "
        "TZDIR at a bundled copy. If the database IS present, then the zone "
        "name really is one Velox does not know.",
    ),
    (
        r"NoClassDefFoundError: org/apache/iceberg/spark/source/SparkBatchQueryScan|"
        r"ClassNotFoundException: org\.apache\.iceberg\.spark\.source\.SparkBatchQueryScan",
        "Classloader split: Iceberg's runtime is in Spark's child classloader "
        "(--packages / spark.jars) while the Gluten bundle, which contains "
        "the Iceberg scan transformer, is on the application classpath and "
        "cannot see it. Pass iceberg=True, or extra_jars=[<iceberg-spark-runtime "
        "and everything it needs>], or let velox-spark promote --packages "
        "from the ivy cache (resolve_packages=True).",
    ),
    (
        r"Cannot initialize FileIO implementation .*S3FileIO|"
        r"NoClassDefFoundError: software/amazon/awssdk",
        "Iceberg's S3FileIO is on the application classpath but the AWS SDK "
        "it needs (iceberg-aws-bundle) is not. Everything Iceberg loads by "
        "name must be on the same classpath: add the aws bundle (and the "
        "catalog jar, e.g. iceberg-nessie) via extra_jars=[...].",
    ),
    (
        r"sun\.misc\.Unsafe or java\.nio\.DirectByteBuffer\.<init>\(long, int\) not available",
        "JDK 17 module system blocks netty's direct buffers. Add "
        "--add-opens=java.base/java.nio=ALL-UNNAMED and "
        "-Dio.netty.tryReflectionSetAccessible=true to driver and executor "
        "extraJavaOptions (velox-spark sets these; this means a session "
        "was built without it).",
    ),
    (
        r"NoSuchMethodError: .*ColumnarShuffleManager",
        "Spark version mismatch: the Gluten bundle was compiled against a "
        "different Spark 3.5.x patch release than the one running. "
        "velox-spark pins pyspark==3.5.5; make SPARK_HOME match.",
    ),
    (
        r"Query memory capacity\[.*\] .*exceeded|Exceeded memory pool cap|"
        r"OutOfMemoryError: Direct buffer memory",
        "Off-heap arena too small for this query. Raise offheap= on "
        "get_session (spark.memory.offHeap.size) or lower the parallelism.",
    ),
)


def explain_error(error: object) -> Optional[str]:
    """Match a Spark/Gluten exception text against the failures seen in the wild."""
    text = str(error)
    for pattern, advice in KNOWN_FAILURES:
        if re.search(pattern, text):
            return advice
    return None


def summarize(spark_conf_extra: Optional[Dict[str, str]] = None) -> Dict[str, object]:
    """Static preflight snapshot used by ``velox-spark doctor``."""
    from . import jar

    found, source = jar.resolve()
    arch_problem = check_jar_architecture(found) if found else None
    tz_os = os_timezone_database()
    tz_bundled = bundled_timezone_database()
    promote, missing, warning = classpath_promotion(spark_conf_extra)
    libc = platform.libc_ver()[1] or "unknown"
    return {
        "host_arch": platform.machine(),
        "glibc": libc,
        "jar": str(found) if found else None,
        "jar_source": source,
        "jar_archs": jar_native_archs(found) if found else [],
        "arch_problem": arch_problem,
        "tz_os": tz_os,
        "tz_bundled": tz_bundled,
        "tz_env": os.environ.get(TZ_ENV),
        "packages": declared_packages(spark_conf_extra),
        "promote": promote,
        "unresolved_packages": missing,
        "classpath_warning": warning,
    }
