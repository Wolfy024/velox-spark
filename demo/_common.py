"""Shared plumbing for the demo queries.

Each query file is standalone (`python demo/aggregate.py`), and
test_queries.py runs them all in one session. This is a demo, not a
benchmark: 50k rows finish in milliseconds on any engine. What it shows is
*offload* -- each query prints a plan report saying how much of it ran in
Velox. For speedup numbers, run `velox-spark validate` on your own data at
real scale.
"""

import time

from velox_spark import demo_path, get_session, report


def load(spark):
    return spark.read.parquet(demo_path())


def run(title, frame):
    start = time.perf_counter()
    rows = frame.collect()
    elapsed = time.perf_counter() - start
    print(f"\n=== {title} ({elapsed * 1000:.0f} ms, {len(rows)} result rows) ===")
    for row in rows[:5]:
        print("  ", row)
    print(report(frame))


def run_standalone(build, title):
    spark = get_session("velox_spark_demo")
    run(title, build(load(spark)))
    spark.stop()
