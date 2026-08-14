"""Answering "is Gluten actually doing anything?"

The failure mode that matters is not a crash -- it is a session where the
plugin loaded, every operator silently fell back to the JVM, and the user pays
columnar/row conversion overhead for no benefit. These helpers make that
visible.
"""

from __future__ import annotations

import re
from typing import Dict, List, Optional

from .config import GLUTEN_ENABLED_KEY, GLUTEN_PLUGIN

# Operators Gluten offloads to Velox are named "<Something>Transformer", plus a
# handful of Velox-specific batch operators. Counting these is far more reliable
# than looking for the "^" prefix in explain output, which only appears when
# native plan injection is switched on.
_OFFLOADED = re.compile(
    r"\b(\w+Transformer|ColumnarExchange|VeloxAppendBatches|VeloxResizeBatches"
    r"|ColumnarBroadcastExchange)\b"
)

# Boundaries where data crosses between Velox's columnar format and Spark rows.
# Each one is a real cost. A handful is normal (the final collect is always
# one); a lot means most of the plan is running on the JVM anyway.
_BOUNDARY = re.compile(
    r"\b(VeloxColumnarToRowExec|ColumnarToRowExec?|RowToVeloxColumnar\w*"
    r"|GlutenRowToArrowColumnar|GlutenColumnarToRow|ArrowColumnarToRow)\b"
)


def executed_plan(df, materialize: bool = True) -> str:
    """Return the physical plan as a string.

    With adaptive query execution on -- and it should be on -- the plan is
    rewritten while the query runs, so reading it before execution shows a plan
    that never actually ran. ``materialize`` forces execution first.

    Materialization executes the DataFrame's OWN QueryExecution from the JVM
    side. Two traps shape this implementation:

    * ``df.foreach(lambda: ...)`` pickles a Python lambda, which crashes on
      Python 3.14 (cloudpickle RecursionError) and drags rows through Python
      workers besides.
    * ``df.write.format("noop").save()`` runs a *separate* QueryExecution for
      the write job, so ``df``'s adaptive plan never finalizes and this
      function would report the pre-substitution operators -- zero offload on
      a perfectly healthy plan.

    Calling ``execute().count()`` on the physical plan drives this exact plan
    tree to completion, entirely in the JVM.
    """
    qe = df._jdf.queryExecution()
    if materialize:
        qe.executedPlan().execute().count()
    return qe.executedPlan().toString()


def plan_stats(plan: str) -> Dict[str, int]:
    """Count offloaded operators and conversion boundaries in a plan string."""
    return {
        "offloaded": len(_OFFLOADED.findall(plan)),
        "boundaries": len(_BOUNDARY.findall(plan)),
    }


def is_engaged(spark) -> bool:
    """True when the plugin is loaded *and* not switched off at runtime."""
    conf = spark.conf
    plugins = conf.get("spark.plugins", "") or ""
    if GLUTEN_PLUGIN not in plugins:
        return False
    return str(conf.get(GLUTEN_ENABLED_KEY, "true")).lower() == "true"


def status(spark) -> Dict[str, object]:
    """A snapshot of everything that determines whether Gluten is working."""
    conf = spark.conf
    return {
        "engaged": is_engaged(spark),
        "plugin_loaded": GLUTEN_PLUGIN in (conf.get("spark.plugins", "") or ""),
        "runtime_enabled": str(conf.get(GLUTEN_ENABLED_KEY, "true")).lower() == "true",
        "offheap_enabled": conf.get("spark.memory.offHeap.enabled", "false"),
        "offheap_size": conf.get("spark.memory.offHeap.size", "0"),
        "shuffle_manager": conf.get("spark.shuffle.manager", "<default>"),
        "ansi_enabled": conf.get("spark.sql.ansi.enabled", "false"),
        "spark_version": spark.version,
    }


def report(df, materialize: bool = True) -> str:
    """A short human-readable verdict on one DataFrame's plan."""
    plan = executed_plan(df, materialize=materialize)
    stats = plan_stats(plan)
    offloaded, boundaries = stats["offloaded"], stats["boundaries"]

    lines = [
        "velox_spark plan report",
        f"  operators offloaded to Velox : {offloaded}",
        f"  columnar<->row boundaries    : {boundaries}",
    ]

    if offloaded == 0:
        lines.append(
            "  VERDICT: nothing offloaded. This query ran entirely on the JVM. "
            "Check for ANSI mode, an unsupported file format (use Parquet), or "
            "a UDF in the plan."
        )
    elif boundaries > offloaded:
        lines.append(
            "  VERDICT: more conversions than offloaded operators. You are "
            "paying columnar<->row overhead for very little native execution. "
            "Enable Gluten's fallback reporter to see which operators bailed:\n"
            "    spark.sparkContext.setLogLevel('INFO')"
        )
    else:
        lines.append("  VERDICT: healthy -- most of this plan runs in Velox.")

    return "\n".join(lines)


def fallback_reasons(spark, limit: int = 20) -> List[str]:
    """Best-effort extraction of Gluten's per-operator fallback explanations.

    Gluten logs why each operator failed validation. There is no stable
    programmatic accessor for it, so this reads the log level and tells the
    caller how to see them rather than pretending to scrape driver logs.
    """
    notes: List[str] = []
    if not is_engaged(spark):
        notes.append("Gluten is not engaged in this session; nothing to report.")
        return notes
    notes.append(
        "Gluten logs a validation failure reason per fallen-back operator at "
        "INFO. To see them:\n"
        "  spark.sparkContext.setLogLevel('INFO')\n"
        "then re-run the query and grep the driver log for "
        "'Validation failed for plan'."
    )
    return notes[:limit]
