# velox-spark

PySpark with the [Apache Gluten](https://gluten.apache.org/) (Velox) native
engine, preconfigured. Same code, same results, faster queries.

## Benchmarks

**TPC-H SF100** vs vanilla PySpark, same hardware, equal memory, identical
results:

| | Parquet | Iceberg |
|---|---|---|
| Overall speedup | **2.63×** | **3.12×** |
| Best query (q1) | 10.1× | 10.3× |
| Total runtime | 523 s → 199 s | 700 s → 225 s |

**Real-world feature-engineering workload** (anonymized dataset, 176 numeric
features; wide Parquet scan → per-row feature energy → grouped aggregation),
engine toggled in-session, identical results:

| Engine | Workload ×| Logical rows | Time | Throughput |
|---|---|---|---|---|
| JVM | 1 | 4.0 M | 25.1 s | 0.16 M rows/s |
| JVM | 14 | 56.5 M | 31.4 s | 1.80 M rows/s |
| Velox | 1 | 4.0 M | 5.7 s | **0.71 M rows/s** |
| Velox | 128 | 516.6 M | 19.4 s | **26.58 M rows/s** |

**4.4× at matched workload; 26.6M rows/s peak throughput** (14.8× the JVM's
peak). Methodology, per-query numbers, and how to reproduce on your own
workload: [NOTES.md](NOTES.md).

## Install

```bash
pip install velox-spark --extra-index-url https://wolfy024.github.io/velox-spark/simple/
```

Wheels are pre-built for x86_64 and aarch64 — nothing compiles on your
machine. Needs Linux and JDK 17
(`sudo apt-get install openjdk-17-jdk-headless`); the right `pyspark` comes
with it. On macOS/Windows the same code runs on standard Spark.

Check the install:

```bash
velox-spark doctor
```

## Use

```python
from velox_spark import get_session

spark = get_session("my_job")
spark.read.parquet("/data/events").groupBy("country").count().show()
```

## Migrating existing code

Change how the session is built. Nothing else.

```python
# Before
from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("daily_rollup")
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())

# After
from velox_spark import get_session
spark = get_session(
    "daily_rollup",
    extra_conf={"spark.sql.shuffle.partitions": "200"},
)
```

Your `spark.sql(...)`, DataFrame chains, reads and writes are untouched.
`extra_conf` is applied last, so your settings win.

Using Iceberg? Add `iceberg=True` and keep your catalog config in
`extra_conf` — the Iceberg jars and extensions are bundled and wired for you.

## Is it actually faster?

```python
from velox_spark import report
print(report(df))
# operators offloaded to Velox : 7
# columnar<->row boundaries    : 1
# VERDICT: healthy -- most of this plan runs in Velox.
```

Or run your own query through the built-in benchmark (correctness, offload
and speed, in one shot):

```bash
velox-spark validate --register events=/data/events \
  --sql "SELECT country, count(*) FROM events GROUP BY country"
```

What to expect: big scans, group-bys and aggregations get 2–3× (up to 10×);
queries that finish in a couple of seconds stay about the same. Parquet and
Iceberg accelerate; other formats just run as normal Spark.

## Things that quietly turn it off

- Creating another SparkSession before `get_session()` — call it first.
- `spark.sql.ansi.enabled=true` — disables all offload (you'll get a warning).
- Python UDFs — results stay correct, but each UDF pays a conversion cost;
  prefer built-in SQL functions.

## Turning it off on purpose

```bash
VELOX_SPARK_DISABLE=1 python my_job.py     # no code change
```

```python
spark = get_session("job", enabled=False)  # plain Spark session
```

For jobs that must not silently run slow, `get_session(..., require_native=True)`
fails at startup if acceleration is unavailable.

---

Building the wheels, publishing, platform internals, and benchmark
methodology: **[NOTES.md](NOTES.md)**.

License: Apache-2.0. The bundled Gluten binary retains its own
`LICENSE`/`NOTICE`, included in the wheel.
