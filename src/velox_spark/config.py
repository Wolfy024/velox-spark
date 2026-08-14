"""The Gluten configuration block.

Everything this package knows about how to turn Gluten on lives here, so there
is exactly one place to look when something needs to change.
"""

from __future__ import annotations

import os
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
        # spark-submit defaults to local[*] when no master is given anywhere.
        return True
    return resolved.startswith("local")


def gluten_config(
    jar: Path,
    offheap_bytes: int,
    driver_memory_bytes: Optional[int],
    master: Optional[str],
    java_major: Optional[int],
    extra_jars: Optional[List[Path]] = None,
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
        # --- JVM flags ----------------------------------------------------
        "spark.driver.extraJavaOptions": extra_java,
        "spark.executor.extraJavaOptions": extra_java,
    }

    if is_local_master(master):
        conf["spark.executor.extraClassPath"] = classpath

    if driver_memory_bytes:
        conf["spark.driver.memory"] = memory.format_size(driver_memory_bytes)

    return {k: v for k, v in conf.items() if v}


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

    if str(conf_view.get("spark.memory.offHeap.enabled", "false")).lower() != "true":
        notes.append(
            "spark.memory.offHeap.enabled is not true. Velox has nowhere to "
            "allocate and queries will fail once they touch real data."
        )

    return notes
