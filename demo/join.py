"""Join: transactions against a per-category aggregate of themselves."""

from pyspark.sql import functions as F

TITLE = "big-ticket transactions vs category average (join)"


def build(df):
    category_avg = df.groupBy("category").agg(F.avg("amount").alias("cat_avg"))

    return (
        df.join(category_avg, "category")
        .filter(F.col("amount") > F.col("cat_avg") * 10)
        .groupBy("category")
        .agg(
            F.count("*").alias("outliers"),
            F.round(F.max("amount"), 2).alias("max_amount"),
        )
        .orderBy(F.desc("outliers"))
    )


if __name__ == "__main__":
    from _common import run_standalone

    run_standalone(build, TITLE)
