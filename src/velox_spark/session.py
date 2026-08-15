"""Session construction -- the one function most users will ever call."""

from __future__ import annotations

import os
import sys
import warnings
from typing import Dict, Mapping, Optional

from . import config, diagnostics, jar, jdk, memory

# Global off switch for operators who need to disable acceleration fleet-wide
# without editing anyone's code or redeploying a wheel.
_DISABLE_ENV = "VELOX_SPARK_DISABLE"


class NativeEngineUnavailable(RuntimeError):
    """Raised when ``require_native=True`` but no Gluten JAR could be loaded."""


def _jvm_already_running() -> bool:
    """Whether a SparkContext exists, meaning startup configs are already fixed."""
    try:
        from pyspark import SparkContext
    except ImportError:  # pragma: no cover - pyspark is a hard dependency
        return False
    return SparkContext._active_spark_context is not None


def _disabled_by_env() -> bool:
    return os.environ.get(_DISABLE_ENV, "").strip().lower() in ("1", "true", "yes")


def _ensure_worker_python() -> None:
    """Point Python workers at the interpreter running this code.

    pyspark 3.5 launches workers with bare ``python3`` from PATH unless
    PYSPARK_PYTHON says otherwise. Inside a venv that is the *system*
    interpreter -- often a different version with no pyspark installed -- and
    every worker-side operation (UDFs, RDDs, createDataFrame from local data)
    dies with a cryptic environment-variable error. An explicit PYSPARK_PYTHON
    is always respected.
    """
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def get_session(
    app_name: str = "velox-spark",
    master: Optional[str] = None,
    *,
    enabled: bool = True,
    require_native: bool = False,
    iceberg: bool = False,
    offheap: Optional[object] = None,
    driver_memory: Optional[object] = None,
    jar_path: Optional[str] = None,
    extra_conf: Optional[Mapping[str, str]] = None,
    quiet: bool = False,
):
    """Build a SparkSession with the Gluten/Velox native engine configured.

    Falls back to ordinary Spark -- loudly, never silently -- when the native
    engine is unavailable on this platform.

    Args:
        app_name: Spark application name.
        master: Spark master URL. Left unset by default so the value from
            ``spark-submit`` or the cluster environment wins; that resolves to
            ``local[*]`` on a workstation.
        enabled: Set False to build a plain Spark session with no plugin. Use
            this for A/B comparisons where you want an unaccelerated baseline.
        require_native: Raise instead of falling back when no JAR is available.
            Production jobs that exist *because* of the accelerator should set
            this so a degraded deploy fails at startup rather than at 3am.
        iceberg: Wire up Apache Iceberg: puts the bundled iceberg-spark-runtime
            on the classpath and registers IcebergSparkSessionExtensions.
            Catalog configuration is still yours to supply via ``extra_conf``.
            Works with or without the native engine; with it, Iceberg scans
            offload to Velox via the bundled gluten-iceberg module.
        offheap: Off-heap size for Velox, e.g. ``"24g"``. Defaults to a fraction
            of host memory, container limits respected.
        driver_memory: JVM heap for the driver, e.g. ``"16g"``. Defaults from
            host memory. Ignored if a SparkContext already exists.
        jar_path: Explicit bundle JAR, overriding both ``GLUTEN_JAR_PATH`` and
            the JAR bundled in this wheel.
        extra_conf: Additional Spark settings. These are applied last and win
            over everything this function sets.
        quiet: Suppress the startup summary.

    Returns:
        A configured ``pyspark.sql.SparkSession``.
    """
    from pyspark.sql import SparkSession

    def say(message: str) -> None:
        if not quiet:
            print(message)

    if _jvm_already_running():
        # spark.plugins, off-heap size and driver memory are all read when the
        # JVM launches. Once it is up they cannot be changed, and quietly
        # returning the existing session would look like success.
        warnings.warn(
            "velox_spark: a SparkContext already exists, so Gluten's startup "
            "settings (spark.plugins, off-heap memory, driver memory) cannot be "
            "applied. Returning the existing session unchanged. Call "
            "get_session() before creating any other Spark session.",
            RuntimeWarning,
            stacklevel=2,
        )
        return SparkSession.builder.getOrCreate()

    _ensure_worker_python()

    builder = SparkSession.builder.appName(app_name)
    if master:
        builder = builder.master(master)

    # Sensible defaults regardless of whether the native engine is available,
    # so the plugin-off baseline is a fair comparison rather than a strawman.
    builder = builder.config("spark.sql.adaptive.enabled", "true")

    want_native = enabled and not _disabled_by_env()
    if enabled and _disabled_by_env():
        say(f"velox_spark: disabled by {_DISABLE_ENV}; using unaccelerated Spark.")

    # Iceberg pieces are resolved up front so the native and non-native paths
    # compose the same way: the runtime jar rides along whichever classpath
    # gets built below.
    iceberg_runtime = jar.iceberg_runtime_jar() if iceberg else None
    if iceberg and iceberg_runtime is None:
        warnings.warn(
            "velox_spark: iceberg=True but no iceberg-spark-runtime jar is "
            "bundled in this installation. Assuming your environment provides "
            "Iceberg on the classpath; if not, the session will fail to start.",
            RuntimeWarning,
            stacklevel=2,
        )

    applied: Dict[str, str] = {}
    if want_native:
        found, source = jar.resolve(jar_path)

        if found is None:
            reason = jar.describe_missing()
            if require_native:
                raise NativeEngineUnavailable(f"velox_spark: {reason}")
            warnings.warn(f"velox_spark: {reason}", RuntimeWarning, stacklevel=2)
        else:
            java_major = jdk.check()
            offheap_bytes = (
                memory.parse_size(offheap) if offheap else memory.default_offheap()
            )
            heap_bytes = (
                memory.parse_size(driver_memory)
                if driver_memory
                else memory.default_heap()
            )
            extra_jars = jar.companion_jars()
            if iceberg_runtime is not None:
                extra_jars = extra_jars + [iceberg_runtime]
            applied = config.gluten_config(
                jar=found,
                offheap_bytes=offheap_bytes,
                driver_memory_bytes=heap_bytes,
                master=master,
                java_major=java_major,
                extra_jars=extra_jars,
            )
            for key, value in applied.items():
                builder = builder.config(key, value)

            say(
                f"velox_spark: Gluten enabled ({source} JAR: {found.name})\n"
                f"  off-heap {memory.format_size(offheap_bytes)}  "
                f"driver heap {memory.format_size(heap_bytes)}  "
                f"java {java_major or 'unknown'}"
                + (f"\n  companions: "
                   f"{', '.join(j.name for j in extra_jars)}" if extra_jars else "")
            )

    if not applied and iceberg_runtime is not None:
        # Unaccelerated session that still wants Iceberg: put the runtime jar
        # on the classpath ourselves, since the native block did not.
        builder = builder.config("spark.jars", str(iceberg_runtime))
        builder = builder.config(
            "spark.driver.extraClassPath", str(iceberg_runtime)
        )

    if iceberg:
        builder = builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

    # User overrides go on last so they can undo anything above.
    merged = dict(extra_conf or {})
    for key, value in merged.items():
        builder = builder.config(key, str(value))

    spark = builder.getOrCreate()

    if applied:
        effective = {**applied, **{k: str(v) for k, v in merged.items()}}
        for note in config.warnings_for(effective):
            warnings.warn(f"velox_spark: {note}", RuntimeWarning, stacklevel=2)

    return spark


def disable_gluten(spark) -> None:
    """Turn native execution off on a live session.

    This flips ``spark.gluten.enabled``, which is read per query. Off-heap
    memory stays allocated -- that is a startup setting and cannot be undone
    without a new session -- but no operator will be offloaded to Velox.

    For a genuinely clean baseline, build the session with
    ``get_session(enabled=False)`` instead.
    """
    spark.conf.set(config.GLUTEN_ENABLED_KEY, "false")


def enable_gluten(spark) -> None:
    """Re-enable native execution on a session that was disabled at runtime.

    Only works if the plugin was loaded at startup; if it was not, this is a
    no-op and ``status()`` will still report ``engaged=False``.
    """
    spark.conf.set(config.GLUTEN_ENABLED_KEY, "true")


def status(spark) -> Dict[str, object]:
    """Whether Gluten is engaged in this session, and the settings that decide it."""
    return diagnostics.status(spark)
