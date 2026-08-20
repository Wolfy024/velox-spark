"""Subprocess entry point for one arm of an isolated validation run.

Invoked as ``python -m velox_spark.harness._arm spec.json out.pkl`` by
:func:`velox_spark.harness.validate.validate_isolated`. Runs in its own
interpreter and JVM so the baseline arm can be genuinely vanilla Spark --
no plugin, default shuffle manager, no off-heap -- which the in-session
toggle cannot provide.
"""

from __future__ import annotations

import json
import pickle
import sys

from .. import diagnostics, session
from .validate import _apply_setup, _measure


def main(argv=None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    spec_path, out_path = argv

    with open(spec_path) as fh:
        spec = json.load(fh)

    spark = session.get_session(
        app_name=f"velox-spark-validate-{spec['label']}",
        master=spec.get("master"),
        enabled=spec["enabled"],
        offheap=spec.get("offheap"),
        driver_memory=spec.get("driver_memory"),
        quiet=True,
    )
    try:
        _apply_setup(
            spark,
            [tuple(r) for r in spec.get("registers", [])],
            spec.get("setup_sql"),
        )
        arm, rows = _measure(
            spark,
            spec["sql"],
            spec["runs"],
            spec["label"],
            spec.get("max_compare_rows", 100_000),
        )
        payload = {
            "label": arm.label,
            "timings": arm.timings,
            "row_count": arm.row_count,
            "offloaded": arm.offloaded,
            "boundaries": arm.boundaries,
            "plan": arm.plan,
            "compare_note": arm.compare_note,
            "rows": rows,
            "engaged": diagnostics.is_engaged(spark),
        }
    finally:
        spark.stop()

    with open(out_path, "wb") as fh:
        pickle.dump(payload, fh)
    return 0


if __name__ == "__main__":
    sys.exit(main())
