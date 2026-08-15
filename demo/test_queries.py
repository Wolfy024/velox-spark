"""Run all demo queries in one session.

    python demo/test_queries.py

Each query also runs standalone: `python demo/aggregate.py`, etc. Works on
any machine with velox-spark installed -- the 50k-row synthetic dataset
ships inside the wheel, so nothing here needs your data.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import aggregate
import join
import python_udf
import window_rank
from _common import load, run

from velox_spark import get_session

spark = get_session("velox_spark_demo")
df = load(spark)

print(f"\ndemo data: {df.count():,} rows, {len(df.columns)} columns")
df.printSchema()

for query in (aggregate, window_rank, join, python_udf):
    run(query.TITLE, query.build(df))

spark.stop()
print("\ndemo complete.")
