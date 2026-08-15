"""CI smoke test: boot a real session with the native engine required.

Expects GLUTEN_JAR_PATH to point at a bundle JAR (CI extracts it from the
released platform wheel). Fails loudly if the engine does not engage.
"""

from velox_spark import get_session, report, status

spark = get_session("ci_native_smoke", require_native=True)

engine = status(spark)
assert engine["engaged"], engine

df = (
    spark.range(1_000_000)
    .selectExpr("id % 7 AS k", "id AS v")
    .groupBy("k")
    .sum("v")
)
rows = {r["k"]: r["sum(v)"] for r in df.collect()}
assert len(rows) == 7 and sum(rows.values()) == 499999500000, rows

local = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "s"])
assert local.count() == 2

print(report(df))
spark.stop()
print("native smoke: PASS")
