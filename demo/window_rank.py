"""Window function: each country's best month by revenue, ranked."""

from pyspark.sql import Window
from pyspark.sql import functions as F

TITLE = "each country's best month (window rank)"


def build(df):
    monthly = df.groupBy(
        "country", F.date_format("event_date", "yyyy-MM").alias("month")
    ).agg(F.round(F.sum("amount"), 2).alias("revenue"))

    return (
        monthly.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("country").orderBy(F.desc("revenue"))
            ),
        )
        .filter(F.col("rank") == 1)
        .orderBy(F.desc("revenue"))
    )


if __name__ == "__main__":
    from _common import run_standalone

    run_standalone(build, TITLE)
