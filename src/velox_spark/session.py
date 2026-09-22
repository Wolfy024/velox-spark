"""Session construction -- the one function most users will ever call."""

from __future__ import annotations

import os
import sys
import warnings
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence

from . import config, diagnostics, jar, jdk, memory, preflight

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
    executor_memory: Optional[object] = None,
    jar_path: Optional[str] = None,
    extra_conf: Optional[Mapping[str, str]] = None,
    extra_jars: Optional[Sequence[str]] = None,
    resolve_packages: bool = True,
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
        executor_memory: JVM heap per executor, e.g. ``"8g"``. Only applied on
            cluster masters (local mode has no separate executor JVM), where
            it is set explicitly so the YARN/k8s container request covers
            heap + overhead + off-heap. Defaults to the driver heap size.
        jar_path: Explicit bundle JAR, overriding both ``GLUTEN_JAR_PATH`` and
            the JAR bundled in this wheel.
        extra_conf: Additional Spark settings. These are applied last and win
            over everything this function sets.
        extra_jars: Local jar files to place on ``spark.jars`` *and* the
            driver/executor ``extraClassPath``, next to the Gluten bundle.
            This is the fix for the classloader split: the bundle lives on
            the JVM's application classpath and cannot see jars that Spark
            loads in its child classloader (``--packages``, ``spark.jars``).
            Anything Gluten reaches for by class name -- an Iceberg runtime
            you supply yourself, its FileIO (``iceberg-aws-bundle``), its
            catalog (``iceberg-nessie``) -- goes here.
        resolve_packages: When Iceberg/Hudi/Delta/Paimon coordinates appear
            in ``--packages`` (``PYSPARK_SUBMIT_ARGS`` or
            ``spark.jars.packages``), look the resolved jars up in the local
            ivy cache and promote them as if passed via ``extra_jars``. On
            when the native engine is on; the startup summary lists what was
            promoted, and a coordinate that is not cached yet produces a
            warning that names the failure it will cause.
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
            # --- preflight: fail here, in words, not in a JNI stack trace ---
            arch_problem = preflight.check_jar_architecture(found)
            if arch_problem:
                if require_native:
                    raise NativeEngineUnavailable(f"velox_spark: {arch_problem}")
                warnings.warn(
                    f"velox_spark: {arch_problem} Falling back to unaccelerated Spark.",
                    RuntimeWarning, stacklevel=2,
                )
                found = None

        if found is not None:
            java_major = jdk.check()
            offheap_bytes = (
                memory.parse_size(offheap) if offheap else memory.default_offheap()
            )
            heap_bytes = (
                memory.parse_size(driver_memory)
                if driver_memory
                else memory.default_heap()
            )

            # Velox reads the IANA zoneinfo directory from the OS at the first
            # native task. On an image without tzdata, point TZDIR (honoured
            # by Gluten >= 1.7.0) at the copy the tzdata wheel carries. Must
            # happen before the JVM launches: it inherits this environment.
            tz_status, tz_note = preflight.ensure_timezone_database()
            if tz_status == "missing":
                message = f"velox_spark: {tz_note}"
                if require_native:
                    raise NativeEngineUnavailable(message)
                warnings.warn(message, RuntimeWarning, stacklevel=2)

            # Jars the bundle must be able to see: user-supplied first (so a
            # user's Iceberg version wins over the bundled runtime), then
            # Gluten's own companions.
            promoted: List[Path] = []
            if resolve_packages:
                promote, _missing, split_warning = preflight.classpath_promotion(
                    dict(extra_conf or {})
                )
                promoted = [Path(p) for p in promote]
                if split_warning:
                    warnings.warn(f"velox_spark: {split_warning}", RuntimeWarning, stacklevel=2)
            user_jars: List[Path] = []
            for item in extra_jars or []:
                path = Path(item).expanduser().resolve()
                if not path.is_file():
                    raise NativeEngineUnavailable(
                        f"velox_spark: extra_jars entry {item!r} does not exist"
                    )
                user_jars.append(path)
            all_extra: List[Path] = promoted + user_jars + jar.companion_jars()
            if iceberg_runtime is not None:
                all_extra = all_extra + [iceberg_runtime]

            applied = config.gluten_config(
                jar=found,
                offheap_bytes=offheap_bytes,
                driver_memory_bytes=heap_bytes,
                master=master,
                java_major=java_major,
                extra_jars=all_extra,
                executor_memory_bytes=(
                    memory.parse_size(executor_memory) if executor_memory else None
                ),
            )
            for key, value in applied.items():
                builder = builder.config(key, value)

            tz_line = {
                "os": "",
                "env": f"\n  tz database: TZDIR={os.environ.get(preflight.TZ_ENV)}",
                "bundled": f"\n  tz database: none in OS, TZDIR -> tzdata package ({tz_note})",
                "missing": "\n  tz database: MISSING -- native tasks will fail",
            }[tz_status]
            promoted_line = (
                f"\n  promoted onto the driver classpath (from --packages/spark.jars): "
                f"{', '.join(p.name for p in promoted)}" if promoted else ""
            )
            user_line = (
                f"\n  extra jars: {', '.join(p.name for p in user_jars)}" if user_jars else ""
            )
            companions = jar.companion_jars() + ([iceberg_runtime] if iceberg_runtime else [])
            say(
                f"velox_spark: Gluten enabled ({source} JAR: {found.name})\n"
                f"  off-heap {memory.format_size(offheap_bytes)}  "
                f"driver heap {memory.format_size(heap_bytes)}  "
                f"java {java_major or 'unknown'}"
                + (f"\n  companions: "
                   f"{', '.join(j.name for j in companions)}" if companions else "")
                + promoted_line + user_line + tz_line
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

    if not applied and driver_memory:
        # An unaccelerated session (enabled=False, or no JAR) still honours an
        # explicit driver heap -- the validation harness relies on this to
        # give the vanilla baseline arm the same total memory as the native
        # arm's heap + off-heap.
        builder = builder.config(
            "spark.driver.memory",
            memory.format_size(memory.parse_size(driver_memory)),
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
        # The master the context actually resolved, covering env/spark-submit.
        for note in config.distribution_notes(
            spark.sparkContext.master,
            executor_memory_applied="spark.executor.memory" in applied,
        ):
            warnings.warn(f"velox_spark: {note}", RuntimeWarning, stacklevel=2)
        _check_driver_heap(spark, applied.get("spark.driver.memory"))
        _silence_known_noise(spark)

    return spark


def _check_driver_heap(spark, requested: Optional[str]) -> None:
    """Warn when builder-time ``spark.driver.memory`` did not reach the JVM.

    The driver JVM is launched by the py4j gateway; whether builder config
    becomes a ``-Xmx`` depends on how the process was started (bare python vs
    spark-submit vs a notebook kernel with PYSPARK_SUBMIT_ARGS already set).
    Off-heap sizing assumes the requested heap, so a silently smaller JVM is
    worth a loud warning. Compares against Runtime.maxMemory() -- the actual
    -Xmx -- not the conf echo.
    """
    if not requested:
        return
    try:
        actual = int(spark._jvm.java.lang.Runtime.getRuntime().maxMemory())
        wanted = memory.parse_size(requested)
    except Exception:  # noqa: BLE001 - diagnostics must never break startup
        return
    # maxMemory() reports usable heap, a few % under -Xmx; 0.75 separates
    # "accounting noise" from "the setting never arrived".
    if actual < wanted * 0.75:
        warnings.warn(
            f"velox_spark: requested driver heap {requested} but the JVM is "
            f"running with ~{memory.format_size(actual)} "
            "(Runtime.maxMemory). The builder-time spark.driver.memory did "
            "not reach the JVM -- this happens under spark-submit or a "
            "notebook kernel with PYSPARK_SUBMIT_ARGS preset. Pass "
            "--driver-memory there instead.",
            RuntimeWarning,
            stacklevel=3,
        )


def _silence_known_noise(spark) -> None:
    """Quiet Gluten's known-cosmetic log noise on the driver.

    ``MetricsUtil: Updating native metrics failed due to null`` is emitted
    around mixed native/JVM plans and means nothing actionable. Scoped to
    that one logger via log4j2's Configurator so real WARNs still surface.
    Best-effort: any failure leaves logging exactly as it was.
    """
    try:
        jvm = spark._jvm
        jvm.org.apache.logging.log4j.core.config.Configurator.setLevel(
            "org.apache.gluten.metrics.MetricsUtil",
            jvm.org.apache.logging.log4j.Level.ERROR,
        )
    except Exception:  # noqa: BLE001 - cosmetic; never fail the session
        pass


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
