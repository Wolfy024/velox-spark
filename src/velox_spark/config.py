"""The Gluten configuration block.

Everything this package knows about how to turn Gluten on lives here, so there
is exactly one place to look when something needs to change.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Dict, List, Optional

from . import jdk, memory

GLUTEN_PLUGIN = "org.apache.gluten.GlutenPlugin"
COLUMNAR_SHUFFLE_MANAGER = "org.apache.spark.shuffle.sort.ColumnarShuffleManager"

# Runtime kill switch. Unlike spark.plugins, this one is read per query, so it
# can be flipped on a live session -- see session.disable_gluten().
GLUTEN_ENABLED_KEY = "spark.gluten.enabled"


def is_local_master(master: Optional[str]) -> bool:
    """Whether this session runs driver and executors in one JVM on one host.

    Matters because a JAR bundled inside a Python wheel exists at that path only
    where the wheel is installed. In local mode driver and executor are the same
    process, so pointing the executor classpath at it is always safe; on a real
    cluster it would be a path that does not resolve on the workers.
    """
    resolved = master or os.environ.get("MASTER") or os.environ.get("SPARK_MASTER")
    if not resolved:
        # Notebook kernels and the pyspark shell carry the master inside
        # PYSPARK_SUBMIT_ARGS rather than an argument or MASTER variable.
        submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
        match = re.search(r"--master[=\s]+(\S+)", submit_args)
        if match:
            resolved = match.group(1)
    if not resolved:
        # spark-submit defaults to local[*] when no master is given anywhere.
        # NOTE: under `spark-submit --master yarn job.py` the master lives in
        # the JVM only and is invisible here; session.py cross-checks the
        # resolved master after startup and warns when this guess was wrong.
        return True
    return resolved.startswith("local")


def gluten_config(
    jar: Path,
    offheap_bytes: int,
    driver_memory_bytes: Optional[int],
    master: Optional[str],
    java_major: Optional[int],
    extra_jars: Optional[List[Path]] = None,
    executor_memory_bytes: Optional[int] = None,
) -> Dict[str, str]:
    """Build the full Spark config needed to run Gluten.

    Every key here has to be set before the JVM starts. That is why this returns
    a plain dict for the session builder rather than applying anything itself.
    """
    all_jars = [str(jar)] + [str(j) for j in (extra_jars or [])]
    # spark.jars is comma-separated; the classpath keys are ':'-separated.
    jars_csv = ",".join(all_jars)
    classpath = ":".join(all_jars)
    extra_java = " ".join(jdk.jvm_options(java_major))

    conf: Dict[str, str] = {
        # --- the plugin itself -------------------------------------------
        "spark.plugins": GLUTEN_PLUGIN,
        GLUTEN_ENABLED_KEY: "true",
        # spark.jars ships the JARs to executors; extraClassPath puts them on
        # the driver JVM's classpath at launch, which spark.jars alone does
        # not do.
        "spark.jars": jars_csv,
        "spark.driver.extraClassPath": classpath,
        # --- memory -------------------------------------------------------
        # Velox works off-heap. Without these two, Gluten loads and then dies on
        # the first non-trivial query.
        "spark.memory.offHeap.enabled": "true",
        "spark.memory.offHeap.size": str(offheap_bytes),
        # --- shuffle ------------------------------------------------------
        # Keeps data columnar across a shuffle boundary. With the stock manager
        # every exchange round-trips through rows and gives back most of the win.
        "spark.shuffle.manager": COLUMNAR_SHUFFLE_MANAGER,
        # --- fallback thresholds -----------------------------------------
        # Gluten refuses expressions deeper than this and silently drops the
        # whole operator to the JVM. The upstream default (50) is exceeded by
        # any wide feature-engineering Project built with reduce(add, ...) --
        # a 176-column sum is depth 176. Raised so real workloads offload;
        # still finite so a pathological plan cannot stall the validator.
        "spark.gluten.sql.columnar.fallback.expressions.threshold": "250",
        # --- JVM flags ----------------------------------------------------
        "spark.driver.extraJavaOptions": extra_java,
        "spark.executor.extraJavaOptions": extra_java,
    }

    # spark.plugins is instantiated during executor bootstrap; in standalone
    # mode spark.jars are fetched from the driver's file server only *after*
    # the executor JVM is up -- too late for the plugin class. extraClassPath
    # is the only startup-time hook, so it is set unconditionally. On a
    # uniform fleet (same wheel, same path on every host) the path resolves on
    # the workers; where it would not, VELOX_SPARK_EXECUTOR_CLASSPATH
    # overrides it, and a nonexistent classpath entry is ignored by the JVM
    # rather than being an error. distribution_notes() warns about the
    # assumption on cluster masters.
    conf["spark.executor.extraClassPath"] = os.environ.get(
        "VELOX_SPARK_EXECUTOR_CLASSPATH", classpath
    )

    if driver_memory_bytes:
        conf["spark.driver.memory"] = memory.format_size(driver_memory_bytes)

    if not is_local_master(master):
        # On YARN/k8s the container request is
        #   executor.memory + memoryOverhead + offHeap.size (+ pyspark memory)
        # and Spark only accounts off-heap into that sum when the sizes are
        # explicit. Leaving executor memory implicit hands a 24g off-heap to a
        # container sized for the 1g default and gets it killed with no
        # Spark-side error. Defaults are deliberate but modest; real fleets
        # should override via extra_conf.
        executor_bytes = executor_memory_bytes or (
            driver_memory_bytes or 4 * 1024**3
        )
        conf["spark.executor.memory"] = memory.format_size(executor_bytes)
        conf["spark.executor.memoryOverhead"] = memory.format_size(
            max(1024**3, int(executor_bytes * 0.1))
        )

    return {k: v for k, v in conf.items() if v}


def distribution_notes(
    master: Optional[str], executor_memory_applied: bool = True
) -> List[str]:
    """Warnings about running on a real cluster, where this package's JARs
    live at a driver-local path that the workers may not share.

    Empty for local masters, where driver and executors are one process.
    ``executor_memory_applied`` states whether this package actually set
    spark.executor.memory at builder time -- under ``spark-submit --master
    yarn job.py`` the master is invisible until after startup, in which case
    the warning must say the sizing was NOT applied rather than claim
    protection that never happened.
    """
    if is_local_master(master):
        return []
    notes = [
        f"master={master}: executors must load GlutenPlugin at JVM startup, "
        "before spark.jars are fetched. spark.executor.extraClassPath has "
        "been pointed at this wheel's JAR paths, which assumes the same "
        "wheel is installed at the same path on every worker (uniform "
        "fleet). If workers differ, install the wheel there or set "
        "VELOX_SPARK_EXECUTOR_CLASSPATH to the JAR locations on the "
        "workers -- otherwise executors fail to load the plugin or run "
        "vanilla while the driver-side plan still claims offload. Verify "
        "with velox_spark.diagnostics.verify_executors(spark).",
    ]
    if executor_memory_applied:
        notes.append(
            "Executor memory has been set explicitly "
            "(spark.executor.memory/memoryOverhead) so the YARN/k8s "
            "container request covers heap + overhead + off-heap. Sized "
            "from the driver host; override via extra_conf for a "
            "non-uniform fleet."
        )
    else:
        notes.append(
            "Executor memory was NOT sized by this package: the cluster "
            "master only became visible after JVM startup (typically "
            "`spark-submit --master ...` with no master= in get_session). "
            "spark.executor.memory/memoryOverhead are at their defaults "
            "while off-heap is large -- on YARN/k8s the container request "
            "will not cover off-heap and the container will be killed. Pass "
            "master= to get_session() or set spark.executor.memory and "
            "spark.executor.memoryOverhead explicitly (e.g. via "
            "spark-submit --conf)."
        )
    return notes


def warnings_for(conf_view) -> List[str]:
    """Configuration that will quietly cost the user their acceleration.

    ``conf_view`` is any mapping-like object exposing ``.get(key, default)``.
    Returned strings are shown to the user; each one names a setting that leaves
    Gluten technically enabled but doing nothing.
    """
    notes: List[str] = []

    if str(conf_view.get("spark.sql.ansi.enabled", "false")).lower() == "true":
        notes.append(
            "spark.sql.ansi.enabled=true disables Velox offload entirely -- "
            "Gluten will load and then fall back on every operator. Turn ANSI "
            "off, or accept that this session is unaccelerated."
        )

    shuffle = conf_view.get("spark.shuffle.manager", "")
    if shuffle and shuffle != COLUMNAR_SHUFFLE_MANAGER:
        notes.append(
            f"spark.shuffle.manager={shuffle} overrides the columnar shuffle "
            "manager. Every exchange will convert columnar->row and back."
        )

    if str(conf_view.get("spark.sql.caseSensitive", "false")).lower() == "true":
        notes.append(
            "spark.sql.caseSensitive=true: Velox does not honour case-"
            "sensitive resolution everywhere, and the failure mode is WRONG "
            "RESULTS, not a fallback. Do not run Gluten with case-sensitive "
            "SQL enabled."
        )

    if str(conf_view.get("spark.memory.offHeap.enabled", "false")).lower() != "true":
        notes.append(
            "spark.memory.offHeap.enabled is not true. Velox has nowhere to "
            "allocate and queries will fail once they touch real data."
        )

    return notes
