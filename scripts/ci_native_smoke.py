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

# Correctness regression: DISTINCT feeding a join that is then shuffled. With
# Gluten's flushable aggregation on, the DISTINCT's final stage is made
# flushable and leaks duplicates (400000 keys came back as 600000 rows).
# get_session() turns it off; this proves the shipped default is correct.
from pyspark.sql import functions as F  # noqa: E402

keys = spark.range(400_000).selectExpr("id % 100000 AS k", "CAST(id DIV 100000 AS INT) AS d")
a = keys.repartition(8, "k")
b = keys.where("d < 2").repartition(8, "k")
distinct = a.unionByName(b).distinct()
joined = distinct.join(b.withColumn("x", F.lit(1)), ["k", "d"], "left")
n = joined.repartition(8, "k").agg(F.count("*")).first()[0]
assert n == 400_000, f"DISTINCT returned duplicate rows: {n} (expected 400000)"

print(report(df))
spark.stop()
print("native smoke: PASS")
