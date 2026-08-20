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
#
# Deliberately NOT here: plain ColumnarToRowExec. That is a stock Spark
# operator emitted by the vectorized Parquet reader with no Gluten involved;
# counting it would credit the vanilla baseline with boundaries it does not
# have. Only Velox/Gluten-specific transitions count.
# Gluten 1.6 prints the node as "VeloxColumnarToRow" -- no Exec suffix -- in
# executed-plan text, hence the \w* tails.
_BOUNDARY = re.compile(
    r"\b(VeloxColumnarToRow\w*|RowToVeloxColumnar\w*"
    r"|GlutenRowToArrowColumnar|GlutenColumnarToRow\w*|ArrowColumnarToRow)\b"
)

# First identifier on each plan line -- the operator name. Tree-drawing
# characters, the AQE "^" offload marker, "*(n)" codegen ids and "(n)" stage
# ids all precede it.
_NODE_NAME = re.compile(r"(?m)^[\s+:*!\-]*(?:\^ )?(?:\(\d+\)\s*)?([A-Za-z]\w+)")

# Plan-tree furniture that is neither offloaded nor "fallen back": wrappers,
# AQE bookkeeping, reuse markers, and the conversion nodes themselves.
_NOT_AN_OPERATOR = frozenset(
    {
        "AdaptiveSparkPlan", "WholeStageCodegen", "InputAdapter",
        "ReusedExchange", "ReusedSubquery", "Subquery", "SubqueryBroadcast",
        "AQEShuffleRead", "ShuffleQueryStage", "BroadcastQueryStage",
        "TableCacheQueryStage", "ResultQueryStage", "ColumnarToRow",
        "ColumnarToRowExec", "VeloxColumnarToRowExec", "RowToVeloxColumnar",
        "GlutenRowToArrowColumnar", "GlutenColumnarToRow",
        "ArrowColumnarToRow", "InputIteratorTransformer",
    }
)


def _final_plan_section(plan: str) -> str:
    """The part of the plan text that actually ran.

    ``AdaptiveSparkPlan.toString()`` prints the final plan *and* the initial
    plan; counting both doubles every operator. Cut at the initial-plan
    marker when present.
    """
    marker = plan.find("== Initial Plan ==")
    return plan[:marker] if marker != -1 else plan


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
    if getattr(df, "isStreaming", False):
        raise ValueError(
            "velox_spark: this DataFrame is a structured-streaming source. "
            "Gluten does not accelerate structured streaming -- micro-batch "
            "plans fall back to the JVM -- and a streaming plan cannot be "
            "materialized here. Point this at a batch DataFrame."
        )
    qe = df._jdf.queryExecution()
    if materialize:
        qe.executedPlan().execute().count()
    return qe.executedPlan().toString()


def plan_stats(plan: str) -> Dict[str, int]:
    """Count offloaded operators and conversion boundaries in a plan string.

    These are counts over the executed plan's *text* -- a proxy, not a
    measurement. A subtree the optimizer prints more than once inflates the
    count; a ReusedExchange contributes its reference line only. The numbers
    answer "is most of this plan native?", not "how much work ran where" --
    for the latter, use the Spark UI's stage metrics.
    """
    section = _final_plan_section(plan)
    return {
        "offloaded": len(_OFFLOADED.findall(section)),
        "boundaries": len(_BOUNDARY.findall(section)),
    }


def jvm_operators(plan: str) -> List[str]:
    """Operator names in the plan that ran on the JVM (i.e. fell back).

    Sorted and de-duplicated. Wrapper nodes, AQE bookkeeping and the
    columnar<->row conversion nodes are excluded -- what remains is the list
    of operators Velox refused, which is what you grep the INFO log for.
    """
    section = _final_plan_section(plan)
    names = set(_NODE_NAME.findall(section))
    return sorted(
        n
        for n in names
        if n not in _NOT_AN_OPERATOR
        and not _OFFLOADED.fullmatch(n)
        and not _BOUNDARY.fullmatch(n)
    )


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
    if getattr(df, "isStreaming", False):
        return (
            "velox_spark plan report\n"
            "  This is a structured-streaming DataFrame. Gluten does not "
            "accelerate structured streaming; micro-batch plans run on the "
            "JVM. Expect vanilla Spark performance for this query."
        )
    plan = executed_plan(df, materialize=materialize)
    stats = plan_stats(plan)
    offloaded, boundaries = stats["offloaded"], stats["boundaries"]

    lines = [
        "velox_spark plan report",
        f"  operators offloaded to Velox : {offloaded}",
        f"  columnar<->row boundaries    : {boundaries}",
    ]

    if offloaded == 0:
        fell_back = jvm_operators(plan)
        lines.append(
            "  VERDICT: nothing offloaded. This query ran entirely on the JVM. "
            "Check for ANSI mode, an unsupported file format (use Parquet), "
            "a UDF in the plan, or expression depth beyond "
            "spark.gluten.sql.columnar.fallback.expressions.threshold "
            "(deep reduce(add, ...) chains -- build wide sums as a balanced "
            "tree)."
        )
        if fell_back:
            lines.append(
            f"  JVM operators: {', '.join(fell_back)}"
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


def _flatten_reason_maps(buffer) -> List[str]:
    """Flatten Gluten's ``Seq[Map[operator, reason]]`` into strings.

    The FallbackSummary carries one Scala map per query execution; py4j
    exposes them only through the iterator protocol, hence the manual walk.
    """
    flat: List[str] = []
    outer = buffer.iterator()
    while outer.hasNext():
        inner = outer.next().iterator()
        while inner.hasNext():
            entry = inner.next()
            flat.append(f"{entry._1()}: {entry._2()}")
    return flat


def native_fallback_summary(spark, df) -> Optional[Dict[str, object]]:
    """Gluten's own per-query fallback accounting, when the JVM exposes it.

    Wraps ``GlutenImplicits.collectQueryExecutionFallbackSummary`` (present
    in Gluten 1.2+). Unlike the plan-text proxy this is the engine's own
    count of native vs fallen-back nodes, with per-operator reason strings.
    Returns None when the accessor is unavailable (older Gluten, no plugin).
    """
    try:
        summary = (
            spark._jvm.org.apache.spark.sql.execution.GlutenImplicits
            .collectQueryExecutionFallbackSummary(
                spark._jsparkSession, df._jdf.queryExecution()
            )
        )
        return {
            "num_gluten_nodes": int(summary.numGlutenNodes()),
            "num_fallback_nodes": int(summary.numFallbackNodes()),
            "reasons": _flatten_reason_maps(summary.fallbackNodeToReason()),
        }
    except Exception:  # noqa: BLE001 - introspection is best-effort
        return None


def fallback_reasons(spark, df=None, limit: int = 20) -> List[str]:
    """What fell back to the JVM, and why -- as far as Gluten will say.

    With ``df``, asks Gluten's own FallbackSummary first (real per-operator
    reason strings); if that accessor is unavailable, names the JVM
    operators from the executed plan text instead. The log pointer is
    appended only when the summary could not provide reasons, since the log
    is then the only place they exist.
    """
    if isinstance(df, int):
        # 1.6.0.4's signature was (spark, limit=20); keep old positional
        # callers working instead of treating their limit as a DataFrame.
        df, limit = None, df

    notes: List[str] = []
    if not is_engaged(spark):
        notes.append("Gluten is not engaged in this session; nothing to report.")
        return notes[:limit]

    have_reasons = False
    if df is not None:
        summary = native_fallback_summary(spark, df)
        if summary is not None:
            notes.append(
                f"Gluten fallback summary: {summary['num_gluten_nodes']} "
                f"native nodes, {summary['num_fallback_nodes']} fallen back."
            )
            for reason in summary["reasons"][: max(0, limit - len(notes))]:
                notes.append(reason)
            have_reasons = bool(summary["reasons"]) or (
                summary["num_fallback_nodes"] == 0
            )
        else:
            fell_back = jvm_operators(executed_plan(df))
            if fell_back:
                notes.append(
                    "Operators running on the JVM (fell back): "
                    + ", ".join(fell_back[:limit])
                )
            else:
                notes.append("No JVM operators in this plan; nothing fell back.")
                have_reasons = True

    if not have_reasons:
        notes.append(
            "Gluten logs a validation failure reason per fallen-back operator "
            "at INFO. To see them:\n"
            "  spark.sparkContext.setLogLevel('INFO')\n"
            "then re-run the query and grep the driver log for "
            "'Validation failed for plan'."
        )
    return notes[:limit]


def verify_executors(spark) -> List[str]:
    """Check that the Gluten JAR paths resolve on every executor host.

    Every driver-side signal (``status()``, ``report()``) can look healthy
    while cluster executors never loaded the plugin, because the classpath
    this package sets is a driver-local path. This runs several probe tasks
    per executor slot and reports hosts where the classpath entries do not
    resolve. An empty list means every host that ran a probe checked out (or
    the session is local, where there is nothing to verify) -- Spark offers
    no per-host scheduling guarantee, so treat it as strong evidence, not
    proof, on fleets with many more hosts than running executors.
    """
    sc = spark.sparkContext
    if sc.master.startswith("local"):
        return []
    classpath = spark.conf.get("spark.executor.extraClassPath", "") or ""
    entries = _classpath_jar_like_entries(classpath)
    if not entries:
        return [
            "spark.executor.extraClassPath is empty; executors cannot have "
            "loaded GlutenPlugin at startup."
        ]

    import glob as _glob
    import os as _os
    import socket as _socket

    def entry_resolves(entry):
        # A classpath entry may be a jar, a directory, or a `dir/*` wildcard.
        if entry.endswith("/*"):
            return bool(_glob.glob(entry[:-1] + "*.jar"))
        if entry.endswith(".jar"):
            return _os.path.isfile(entry)
        return _os.path.isdir(entry)

    def probe(_):
        missing = [e for e in entries if not entry_resolves(e)]
        return [(_socket.gethostname(), tuple(missing))] if missing else []

    # More tasks than slots to improve host coverage; scheduling still offers
    # no per-host guarantee, so an empty result certifies only the hosts that
    # actually ran probes -- see the docstring.
    slots = max(sc.defaultParallelism, 2) * 3
    results = sc.parallelize(range(slots), slots).mapPartitions(probe).collect()
    problems = sorted({host: miss for host, miss in results}.items())
    return [
        f"executor host {host}: classpath entries not found: "
        f"{', '.join(miss)} -- install the wheel there or set "
        "VELOX_SPARK_EXECUTOR_CLASSPATH"
        for host, miss in problems
    ]


def _classpath_jar_like_entries(classpath: str) -> List[str]:
    """Non-empty entries of a ':'-separated classpath, in order."""
    return [e for e in (classpath or "").split(":") if e]
