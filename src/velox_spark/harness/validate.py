"""Correctness / fallback / wall-clock validation for a single query."""

from __future__ import annotations

import json
import math
import pickle
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .. import diagnostics, memory, session

# Sort keys are coarse (%.6g) so that values equal within the comparison
# tolerance almost always share a key and sort together. The key alone cannot
# pair rows correctly in every case -- rows that differ between the key's
# resolution and the tolerance tie on the key and can zip up with the wrong
# partners -- so compare_rows() follows the sorted zip with a tolerance-aware
# rematch of the leftover pairs before reporting anything as a mismatch.
_SORT_PRECISION = 6

# Cap on rows pulled into the driver for the correctness comparison. Beyond
# it, both arms compare a deterministic prefix under a total ordering instead
# of OOMing the driver on `SELECT * FROM events`.
DEFAULT_MAX_COMPARE_ROWS = 100_000

# Formats accepted by `--register name=format:path`. A whitelist, so that a
# URI scheme (s3a://...) is never mistaken for a format.
_REGISTER_FORMATS = frozenset(
    {"parquet", "csv", "json", "orc", "text", "avro", "delta", "iceberg",
     "hudi", "paimon"}
)


def parse_register_spec(spec: str) -> Tuple[str, str, str]:
    """Parse ``NAME=[FORMAT:]PATH`` into (name, format, path).

    The format defaults to parquet. Only known format names before the first
    colon are treated as a format, so URI schemes pass through untouched.
    """
    if "=" not in spec:
        raise ValueError(
            f"velox_spark: --register expects NAME=[FORMAT:]PATH, got {spec!r}"
        )
    name, rest = spec.split("=", 1)
    fmt = "parquet"
    if ":" in rest:
        head, tail = rest.split(":", 1)
        if head.lower() in _REGISTER_FORMATS:
            fmt, rest = head.lower(), tail
    return name, fmt, rest


@dataclass
class ArmResult:
    """Measurements from one arm of the A/B (Gluten on, or Gluten off)."""

    label: str
    timings: List[float] = field(default_factory=list)
    row_count: int = 0
    offloaded: int = 0
    boundaries: int = 0
    plan: str = ""
    compare_note: str = ""

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


def _canon(value: Any) -> str:
    """Canonical sort string for one value, recursing into containers.

    Floats are coarsened everywhere -- including inside arrays, maps and
    structs -- so aggregation-order drift does not scatter matching rows.
    Decimals are normalised (1.10 == 1.1) and temporal types use ISO text, so
    ordering never depends on repr() details.
    """
    if value is None:
        return "\x00None"
    if isinstance(value, float):
        if math.isnan(value):
            return "\x00NaN"
        # Normalise -0.0 to 0.0 so it does not sort apart from 0.0.
        return f"{value + 0.0:.{_SORT_PRECISION}g}"
    if isinstance(value, Decimal):
        return f"\x01{value.normalize()}"
    if isinstance(value, (datetime, date)):
        return f"\x02{value.isoformat()}"
    if isinstance(value, dict):
        inner = ",".join(
            f"{_canon(k)}={_canon(v)}" for k, v in sorted(value.items(), key=repr)
        )
        return "{" + inner + "}"
    if isinstance(value, (list, tuple)):
        return "[" + ",".join(_canon(v) for v in value) + "]"
    return repr(value)


def _sort_key(row: Sequence[Any]) -> Tuple[str, ...]:
    return tuple(_canon(value) for value in row)


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
    # Containers recurse so a float nested in an array/map/struct still gets
    # the tolerance instead of falling through to ==.
    if isinstance(a, dict) and isinstance(b, dict):
        if set(a.keys()) != set(b.keys()):
            return False
        return all(_values_match(a[k], b[k], rel_tol, abs_tol) for k in a)
    if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
        return len(a) == len(b) and all(
            _values_match(x, y, rel_tol, abs_tol) for x, y in zip(a, b)
        )
    return a == b


def _rows_match(
    lrow: Sequence[Any], rrow: Sequence[Any], rel_tol: float, abs_tol: float
) -> bool:
    return len(lrow) == len(rrow) and all(
        _values_match(a, b, rel_tol, abs_tol) for a, b in zip(lrow, rrow)
    )


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

    # First pass: sorted zip. Almost every row pairs correctly here.
    unpaired: List[Tuple[Sequence[Any], Sequence[Any]]] = []
    for lrow, rrow in zip(left_sorted, right_sorted):
        if not _rows_match(lrow, rrow, rel_tol, abs_tol):
            unpaired.append((lrow, rrow))

    if not unpaired:
        return problems

    # Second pass: rows can tie on the coarse sort key while differing between
    # the key's resolution and the tolerance, in which case the zip pairs them
    # with the wrong partners. Re-match the leftovers against each other with
    # the real tolerance before calling anything a mismatch.
    lefts = [l for l, _ in unpaired]
    rights = [r for _, r in unpaired]
    for lrow in lefts:
        for i, rrow in enumerate(rights):
            if rrow is not None and _rows_match(lrow, rrow, rel_tol, abs_tol):
                rights[i] = None
                break
        else:
            remaining = [r for r in rights if r is not None]
            partner = remaining[0] if remaining else None
            problems.append(
                f"unmatched row: gluten={tuple(lrow)!r} "
                f"(nearest baseline={tuple(partner)!r})"
                if partner is not None
                else f"unmatched row: gluten={tuple(lrow)!r}"
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


def _collect_for_compare(
    df, cap: int
) -> Tuple[Optional[List[Tuple[Any, ...]]], int, str]:
    """Rows for the correctness comparison, without OOMing the driver.

    Results within ``cap`` are collected whole. Larger results are compared on
    the first ``cap`` rows under a total ordering (every column), which is the
    same deterministic subset in both arms. Results that cannot be totally
    ordered (map columns) skip the comparison with a note rather than
    collecting an unbounded result set.
    """
    total = df.count()
    if total <= cap:
        return [tuple(r) for r in df.collect()], total, ""
    try:
        rows = [tuple(r) for r in df.orderBy(*df.columns).limit(cap).collect()]
    except Exception as exc:  # noqa: BLE001 - unorderable schema
        return (
            None,
            total,
            f"result has {total} rows (cap {cap}) and cannot be totally "
            f"ordered ({exc}); correctness comparison skipped",
        )
    return (
        rows,
        total,
        f"result has {total} rows; correctness compared on the first {cap} "
        "under a total ordering over all columns",
    )


def _measure(
    spark,
    sql: str,
    runs: int,
    label: str,
    max_compare_rows: int = DEFAULT_MAX_COMPARE_ROWS,
) -> Tuple[ArmResult, Optional[List[Any]]]:
    df = spark.sql(sql)
    rows, total, note = _collect_for_compare(df, max_compare_rows)

    plan = diagnostics.executed_plan(df, materialize=False)
    stats = diagnostics.plan_stats(plan)

    arm = ArmResult(
        label=label,
        timings=_time_query(spark, sql, runs),
        row_count=total,
        offloaded=stats["offloaded"],
        boundaries=stats["boundaries"],
        plan=plan,
        compare_note=note,
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
    max_compare_rows: int = DEFAULT_MAX_COMPARE_ROWS,
) -> ValidationResult:
    """Run ``sql`` with Gluten on and off and return the three-gate verdict.

    The session must have been built by :func:`velox_spark.get_session` with the
    plugin loaded. Both arms run in that same session -- toggling
    ``spark.gluten.enabled`` rather than building a second session keeps memory
    configuration identical, so wall clock reflects the engine and not a
    different heap size.

    Caveat, stated rather than hidden: the in-session baseline is *not*
    vanilla Spark. The ColumnarShuffleManager stays installed and off-heap
    stays allocated (both are startup settings). For shuffle-heavy queries
    that flatters or penalises the baseline arm; use
    :func:`validate_isolated` for a true vanilla baseline in a separate
    process.
    """
    notes: List[str] = [
        "in-session baseline: ColumnarShuffleManager and off-heap remain "
        "active in the baseline arm (startup settings). For a true vanilla "
        "baseline use --isolated / validate_isolated()."
    ]
    if not diagnostics.is_engaged(spark):
        notes.append(
            "Gluten is not engaged in this session, so both arms are identical. "
            "The comparison below is meaningless -- run `velox-spark doctor`."
        )

    session.enable_gluten(spark)
    gluten_arm, gluten_rows = _measure(spark, sql, runs, "gluten", max_compare_rows)

    session.disable_gluten(spark)
    try:
        baseline_arm, baseline_rows = _measure(
            spark, sql, runs, "baseline", max_compare_rows
        )
    finally:
        session.enable_gluten(spark)

    mismatches, correctness_note = _compare_arm_rows(
        gluten_arm, gluten_rows, baseline_arm, baseline_rows, rel_tol, abs_tol
    )
    if correctness_note:
        notes.append(correctness_note)

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


def _compare_arm_rows(
    gluten_arm: ArmResult,
    gluten_rows: Optional[List[Any]],
    baseline_arm: ArmResult,
    baseline_rows: Optional[List[Any]],
    rel_tol: float,
    abs_tol: float,
) -> Tuple[List[str], str]:
    """Correctness comparison over what each arm could collect."""
    if gluten_arm.row_count != baseline_arm.row_count:
        return (
            [
                "row count differs: "
                f"gluten={gluten_arm.row_count} baseline={baseline_arm.row_count}"
            ],
            "",
        )
    if gluten_rows is None or baseline_rows is None:
        return [], (
            gluten_arm.compare_note
            or baseline_arm.compare_note
            or "correctness comparison skipped"
        )
    note = gluten_arm.compare_note
    return compare_rows(gluten_rows, baseline_rows, rel_tol, abs_tol), note


def _run_arm_in_subprocess(spec: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one arm in a fresh Python/JVM via ``harness._arm``."""
    with tempfile.TemporaryDirectory(prefix="velox-spark-validate-") as tmp:
        spec_path = Path(tmp) / "spec.json"
        out_path = Path(tmp) / "result.pkl"
        spec_path.write_text(json.dumps(spec))
        proc = subprocess.run(
            [sys.executable, "-m", "velox_spark.harness._arm",
             str(spec_path), str(out_path)],
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0 or not out_path.is_file():
            tail = "\n".join((proc.stderr or "").splitlines()[-15:])
            raise RuntimeError(
                f"velox_spark: {spec['label']} arm subprocess failed "
                f"(exit {proc.returncode}). Last stderr lines:\n{tail}"
            )
        with out_path.open("rb") as fh:
            return pickle.load(fh)


def validate_isolated(
    sql: str,
    *,
    registers: Optional[List[Tuple[str, str, str]]] = None,
    setup_sql: Optional[str] = None,
    master: Optional[str] = None,
    offheap: Optional[object] = None,
    driver_memory: Optional[object] = None,
    runs: int = 3,
    rel_tol: float = 1e-9,
    abs_tol: float = 1e-12,
    min_speedup: float = 1.0,
    max_compare_rows: int = DEFAULT_MAX_COMPARE_ROWS,
) -> ValidationResult:
    """A/B with a *true vanilla* baseline: each arm in its own process.

    The in-session toggle cannot remove the ColumnarShuffleManager or release
    off-heap memory -- both are fixed at JVM startup -- so its baseline arm is
    vanilla-with-asterisks. This runs each arm in a fresh interpreter and JVM:

    * gluten arm: ``get_session(enabled=True)``, heap H + off-heap O;
    * baseline arm: ``get_session(enabled=False)`` -- no plugin, default
      shuffle manager, no off-heap -- with heap H + O, so both arms hold the
      same total memory and wall clock isolates the engine.
    """
    offheap_bytes = memory.parse_size(offheap) if offheap else memory.default_offheap()
    heap_bytes = (
        memory.parse_size(driver_memory) if driver_memory else memory.default_heap()
    )

    common = {
        "sql": sql,
        "registers": [list(r) for r in (registers or [])],
        "setup_sql": setup_sql,
        "master": master,
        "runs": runs,
        "max_compare_rows": max_compare_rows,
    }
    gluten_raw = _run_arm_in_subprocess(
        {**common, "label": "gluten", "enabled": True,
         "offheap": offheap_bytes, "driver_memory": heap_bytes}
    )
    baseline_raw = _run_arm_in_subprocess(
        {**common, "label": "baseline", "enabled": False,
         "offheap": None, "driver_memory": heap_bytes + offheap_bytes}
    )

    def to_arm(raw: Dict[str, Any]) -> ArmResult:
        return ArmResult(
            label=raw["label"],
            timings=raw["timings"],
            row_count=raw["row_count"],
            offloaded=raw["offloaded"],
            boundaries=raw["boundaries"],
            plan=raw["plan"],
            compare_note=raw["compare_note"],
        )

    gluten_arm, baseline_arm = to_arm(gluten_raw), to_arm(baseline_raw)

    notes = [
        "isolated mode: baseline is a true vanilla session in its own "
        "process (no plugin, default shuffle manager, no off-heap). "
        f"Equal total memory: gluten {memory.format_size(heap_bytes)} heap "
        f"+ {memory.format_size(offheap_bytes)} off-heap vs baseline "
        f"{memory.format_size(heap_bytes + offheap_bytes)} heap."
    ]
    if gluten_raw.get("engaged") is False:
        notes.append(
            "Gluten did not engage in the gluten arm -- both arms are "
            "unaccelerated Spark and the comparison is meaningless. Run "
            "`velox-spark doctor`."
        )

    mismatches, correctness_note = _compare_arm_rows(
        gluten_arm, gluten_raw["rows"], baseline_arm, baseline_raw["rows"],
        rel_tol, abs_tol,
    )
    if correctness_note:
        notes.append(correctness_note)

    speedup = (
        baseline_arm.median / gluten_arm.median
        if gluten_arm.median and not math.isnan(gluten_arm.median)
        else float("nan")
    )

    fallback_ok = (
        gluten_arm.offloaded > 0
        and gluten_arm.boundaries <= gluten_arm.offloaded
    )
    if gluten_arm.offloaded == 0:
        notes.append(
            "Nothing was offloaded to Velox. Common causes: ANSI mode "
            "enabled, a non-Parquet source, a Python UDF, or expression "
            "depth beyond the fallback threshold."
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
        metavar="NAME=[FORMAT:]PATH",
        help="Register a path as a temp view; FORMAT defaults to parquet "
        "(csv, json, orc, avro, delta, iceberg, ... accepted). Repeatable.",
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
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="Run each arm in its own process: the baseline becomes true "
        "vanilla Spark (no plugin, default shuffle manager, no off-heap) "
        "with the same total memory as the gluten arm.",
    )
    parser.add_argument(
        "--max-compare-rows",
        type=int,
        default=DEFAULT_MAX_COMPARE_ROWS,
        help="Cap on rows collected to the driver for the correctness "
        f"comparison (default {DEFAULT_MAX_COMPARE_ROWS}). Larger results "
        "compare a deterministic sorted prefix.",
    )
    parser.add_argument("--driver-memory", help="Driver heap, e.g. 8g.")
    parser.add_argument("--json", type=Path, help="Write the full verdict as JSON.")


def _apply_setup(
    spark,
    registers: List[Tuple[str, str, str]],
    setup_sql: Optional[str],
) -> None:
    for name, fmt, path in registers:
        spark.read.format(fmt).load(path).createOrReplaceTempView(name)
    if setup_sql:
        for statement in setup_sql.split(";"):
            if statement.strip():
                spark.sql(statement)


def run_cli(args) -> int:
    sql = args.sql if args.sql else args.sql_file.read_text()

    try:
        registers = [parse_register_spec(spec) for spec in args.register]
    except ValueError as exc:
        print(exc)
        return 2

    setup_sql = args.setup_file.read_text() if args.setup_file else None

    if args.isolated:
        result = validate_isolated(
            sql,
            registers=registers,
            setup_sql=setup_sql,
            master=args.master,
            offheap=args.offheap,
            driver_memory=args.driver_memory,
            runs=args.runs,
            rel_tol=args.rel_tol,
            min_speedup=args.min_speedup,
            max_compare_rows=args.max_compare_rows,
        )
    else:
        spark = session.get_session(
            app_name="velox-spark-validate",
            master=args.master,
            offheap=args.offheap,
            driver_memory=args.driver_memory,
        )
        try:
            _apply_setup(spark, registers, setup_sql)
            result = validate(
                spark,
                sql,
                runs=args.runs,
                rel_tol=args.rel_tol,
                min_speedup=args.min_speedup,
                max_compare_rows=args.max_compare_rows,
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
