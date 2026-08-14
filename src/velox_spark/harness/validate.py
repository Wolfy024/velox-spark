"""Correctness / fallback / wall-clock validation for a single query."""

from __future__ import annotations

import json
import math
import statistics
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .. import diagnostics, session

# Sort keys are coarser than the comparison tolerance on purpose: two values
# that differ below the tolerance must still sort together, or matching rows
# would be paired up with the wrong partners and report spurious mismatches.
_SORT_PRECISION = 6


@dataclass
class ArmResult:
    """Measurements from one arm of the A/B (Gluten on, or Gluten off)."""

    label: str
    timings: List[float] = field(default_factory=list)
    row_count: int = 0
    offloaded: int = 0
    boundaries: int = 0
    plan: str = ""

    @property
    def median(self) -> float:
        return statistics.median(self.timings) if self.timings else float("nan")


@dataclass
class ValidationResult:
    """The verdict. ``passed`` gates promotion."""

    correctness_ok: bool
    fallback_ok: bool
    speed_ok: bool
    speedup: float
    mismatches: List[str]
    gluten: ArmResult
    baseline: ArmResult
    notes: List[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return self.correctness_ok and self.fallback_ok and self.speed_ok

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["passed"] = self.passed
        # Plans are long; keep them out of the summary JSON by default.
        for arm in ("gluten", "baseline"):
            data[arm]["median"] = getattr(self, arm).median
        return data


def _sort_key(row: Sequence[Any]) -> Tuple[str, ...]:
    parts = []
    for value in row:
        if value is None:
            parts.append("\x00None")
        elif isinstance(value, float):
            if math.isnan(value):
                parts.append("\x00NaN")
            else:
                # Normalise -0.0 to 0.0 so it does not sort apart from 0.0.
                parts.append(f"{value + 0.0:.{_SORT_PRECISION}g}")
        else:
            parts.append(repr(value))
    return tuple(parts)


def _values_match(a: Any, b: Any, rel_tol: float, abs_tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            fa, fb = float(a), float(b)
        except (TypeError, ValueError):
            return a == b
        if math.isnan(fa) and math.isnan(fb):
            return True
        return math.isclose(fa, fb, rel_tol=rel_tol, abs_tol=abs_tol)
    return a == b


def compare_rows(
    left: List[Sequence[Any]],
    right: List[Sequence[Any]],
    rel_tol: float = 1e-9,
    abs_tol: float = 1e-12,
    max_report: int = 10,
) -> List[str]:
    """Compare two result sets order-insensitively, tolerant of float drift.

    Velox reorders floating-point aggregations, so ``sum``/``avg`` over doubles
    legitimately differ in the last few units in the last place. An exact
    comparison here would report failures on every correct query.
    """
    problems: List[str] = []
    if len(left) != len(right):
        problems.append(f"row count differs: gluten={len(left)} baseline={len(right)}")
        return problems

    left_sorted = sorted(left, key=_sort_key)
    right_sorted = sorted(right, key=_sort_key)

    for index, (lrow, rrow) in enumerate(zip(left_sorted, right_sorted)):
        if len(lrow) != len(rrow):
            problems.append(f"row {index}: column count differs")
        else:
            for col, (lval, rval) in enumerate(zip(lrow, rrow)):
                if not _values_match(lval, rval, rel_tol, abs_tol):
                    problems.append(
                        f"row {index} col {col}: gluten={lval!r} baseline={rval!r}"
                    )
        if len(problems) >= max_report:
            problems.append("... further mismatches suppressed")
            break
    return problems


def _time_query(spark, sql: str, runs: int) -> List[float]:
    """Time full materialisation, discarding one warm-up run.

    Writing to the ``noop`` sink forces every row through the plan without
    paying to move results back to the driver, which is what ``collect`` would
    otherwise add to both arms.
    """
    spark.sql(sql).write.format("noop").mode("overwrite").save()  # warm-up
    timings = []
    for _ in range(runs):
        start = time.perf_counter()
        spark.sql(sql).write.format("noop").mode("overwrite").save()
        timings.append(time.perf_counter() - start)
    return timings


def _measure(spark, sql: str, runs: int, label: str) -> Tuple[ArmResult, List[Any]]:
    df = spark.sql(sql)
    rows = [tuple(r) for r in df.collect()]

    plan = diagnostics.executed_plan(df, materialize=False)
    stats = diagnostics.plan_stats(plan)

    arm = ArmResult(
        label=label,
        timings=_time_query(spark, sql, runs),
        row_count=len(rows),
        offloaded=stats["offloaded"],
        boundaries=stats["boundaries"],
        plan=plan,
    )
    return arm, rows


def validate(
    spark,
    sql: str,
    *,
    runs: int = 3,
    rel_tol: float = 1e-9,
    abs_tol: float = 1e-12,
    min_speedup: float = 1.0,
) -> ValidationResult:
    """Run ``sql`` with Gluten on and off and return the three-gate verdict.

    The session must have been built by :func:`velox_spark.get_session` with the
    plugin loaded. Both arms run in that same session -- toggling
    ``spark.gluten.enabled`` rather than building a second session keeps memory
    configuration identical, so wall clock reflects the engine and not a
    different heap size.
    """
    notes: List[str] = []
    if not diagnostics.is_engaged(spark):
        notes.append(
            "Gluten is not engaged in this session, so both arms are identical. "
            "The comparison below is meaningless -- run `velox-spark doctor`."
        )

    session.enable_gluten(spark)
    gluten_arm, gluten_rows = _measure(spark, sql, runs, "gluten")

    session.disable_gluten(spark)
    try:
        baseline_arm, baseline_rows = _measure(spark, sql, runs, "baseline")
    finally:
        session.enable_gluten(spark)

    mismatches = compare_rows(gluten_rows, baseline_rows, rel_tol, abs_tol)

    speedup = (
        baseline_arm.median / gluten_arm.median
        if gluten_arm.median and not math.isnan(gluten_arm.median)
        else float("nan")
    )

    fallback_ok = gluten_arm.offloaded > 0 and gluten_arm.boundaries <= gluten_arm.offloaded
    if gluten_arm.offloaded == 0:
        notes.append(
            "Nothing was offloaded to Velox. Common causes: ANSI mode enabled, "
            "a non-Parquet source, or a Python UDF in the plan."
        )
    elif gluten_arm.boundaries > gluten_arm.offloaded:
        notes.append(
            "More columnar<->row boundaries than offloaded operators -- this "
            "plan pays conversion overhead for very little native execution."
        )

    return ValidationResult(
        correctness_ok=not mismatches,
        fallback_ok=fallback_ok,
        speed_ok=bool(speedup >= min_speedup) if not math.isnan(speedup) else False,
        speedup=speedup,
        mismatches=mismatches,
        gluten=gluten_arm,
        baseline=baseline_arm,
        notes=notes,
    )


def format_result(result: ValidationResult) -> str:
    """Render the verdict as the table that goes in the handoff doc."""
    tick = lambda ok: "PASS" if ok else "FAIL"  # noqa: E731

    lines = [
        "",
        "=" * 62,
        "velox_spark validation",
        "=" * 62,
        f"  correctness   {tick(result.correctness_ok):>4}   "
        f"{result.gluten.row_count} rows compared",
        f"  fallback      {tick(result.fallback_ok):>4}   "
        f"{result.gluten.offloaded} offloaded / "
        f"{result.gluten.boundaries} boundaries",
        f"  wall clock    {tick(result.speed_ok):>4}   "
        f"{result.speedup:.2f}x  "
        f"({result.gluten.median:.2f}s gluten vs "
        f"{result.baseline.median:.2f}s baseline)",
        "-" * 62,
        f"  OVERALL       {tick(result.passed)}",
        "=" * 62,
    ]

    if result.mismatches:
        lines.append("\nresult mismatches:")
        lines += [f"  - {m}" for m in result.mismatches]

    if result.notes:
        lines.append("\nnotes:")
        lines += [f"  - {n}" for n in result.notes]

    return "\n".join(lines)


def add_arguments(parser) -> None:
    """Wire up the ``velox-spark validate`` subcommand."""
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--sql", help="Query to validate.")
    source.add_argument(
        "--sql-file", type=Path, help="File containing the query to validate."
    )

    parser.add_argument(
        "--register",
        action="append",
        default=[],
        metavar="NAME=PATH",
        help="Register a Parquet path as a temp view. Repeatable.",
    )
    parser.add_argument(
        "--setup-file",
        type=Path,
        help="SQL file of statements to run first, separated by semicolons.",
    )
    parser.add_argument("--master", help="Spark master URL.")
    parser.add_argument("--offheap", help="Off-heap size, e.g. 24g.")
    parser.add_argument(
        "--runs", type=int, default=3, help="Timed runs per arm (default 3)."
    )
    parser.add_argument(
        "--rel-tol",
        type=float,
        default=1e-9,
        help="Relative tolerance for float comparison (default 1e-9).",
    )
    parser.add_argument(
        "--min-speedup",
        type=float,
        default=1.0,
        help="Minimum speedup required to pass the wall-clock gate (default 1.0).",
    )
    parser.add_argument("--json", type=Path, help="Write the full verdict as JSON.")


def run_cli(args) -> int:
    sql = args.sql if args.sql else args.sql_file.read_text()

    spark = session.get_session(
        app_name="velox-spark-validate",
        master=args.master,
        offheap=args.offheap,
    )

    try:
        for spec in args.register:
            if "=" not in spec:
                print(f"velox_spark: --register expects NAME=PATH, got {spec!r}")
                return 2
            name, path = spec.split("=", 1)
            spark.read.parquet(path).createOrReplaceTempView(name)

        if args.setup_file:
            for statement in args.setup_file.read_text().split(";"):
                if statement.strip():
                    spark.sql(statement)

        result = validate(
            spark,
            sql,
            runs=args.runs,
            rel_tol=args.rel_tol,
            min_speedup=args.min_speedup,
        )
    finally:
        spark.stop()

    print(format_result(result))

    if args.json:
        args.json.write_text(json.dumps(result.to_dict(), indent=2, default=str))
        print(f"\nwrote {args.json}")

    return 0 if result.passed else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(run_cli(None))
