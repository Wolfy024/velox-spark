"""Scan + aggregate: the bread-and-butter shape that offloads completely."""

from pyspark.sql import functions as F

TITLE = "revenue by country"


def build(df):
    return (
        df.groupBy("country")
        .agg(
            F.count("*").alias("txns"),
            F.round(F.sum("amount"), 2).alias("revenue"),
            F.round(F.avg("amount"), 2).alias("avg_ticket"),
        )
        .orderBy(F.desc("revenue"))
    )


if __name__ == "__main__":
    from _common import run_standalone

    run_standalone(build, TITLE)
