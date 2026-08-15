"""The cautionary tale: a Python UDF forces rows out of the native engine.

Same result as a built-in expression, but the plan report shows an extra
columnar<->row boundary -- this is what "quietly turns it off" looks like in
real pipelines. aggregate.py computes comparable things fully natively;
prefer built-in SQL functions whenever one exists.
"""

from pyspark.sql import functions as F

TITLE = "amount bands via Python UDF (see the boundaries)"


def build(df):
    band = F.udf(lambda a: "high" if a > 100 else "low")
    return df.withColumn("band", band("amount")).groupBy("band").count()


if __name__ == "__main__":
    from _common import run_standalone

    run_standalone(build, TITLE)
