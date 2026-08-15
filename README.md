# velox-spark

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI](https://img.shields.io/pypi/v/velox-spark.svg)](https://pypi.org/project/velox-spark/)
[![Python](https://img.shields.io/badge/Python-3.9%E2%80%933.13-blue.svg)](#requirements)
[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://adoptium.net/temurin/releases/?version=17)
[![CI](https://github.com/Wolfy024/velox-spark/actions/workflows/ci.yml/badge.svg)](https://github.com/Wolfy024/velox-spark/actions/workflows/ci.yml)
[![Publish](https://github.com/Wolfy024/velox-spark/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/Wolfy024/velox-spark/actions/workflows/publish-pypi.yml)

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

## Requirements

| Component | Supported |
|---|---|
| Python | 3.9 – 3.13 (full API) · 3.14 (SQL/DataFrame pipelines only — see below) |
| Java | JDK 17 — CI-tested with the native engine. `sudo apt-get install openjdk-17-jdk-headless` |
| OS (native engine) | Linux x86_64 (glibc ≥ 2.17) · Linux aarch64 (glibc ≥ 2.35) |
| macOS / Windows | runs as standard Spark — same code, no native engine |
| Apache Spark | 3.5.5 — installed with the package, version-pinned |
| Gluten / Velox | 1.6.0 — bundled inside the wheel |

Ubuntu 22.04 and 24.04 (and anything with a comparable glibc) work out of the
box. CI installs and tests the package on Python 3.9 through 3.14; the full
Spark API is additionally verified against a live session on 3.11–3.13. On
**Python 3.14**, pyspark 3.5's bundled serializer cannot pickle Python
callables yet: reading files, `spark.sql`, and DataFrame transformations all
work, but `createDataFrame` from local Python data, Python UDFs, and RDD
lambdas fail. On 3.14, stay on the SQL/DataFrame-over-files path or use a
3.13 venv.

## Install

In a virtual environment (recommended):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install velox-spark --extra-index-url https://wolfy024.github.io/velox-spark/simple/
```

Without one:

```bash
python3 -m pip install --user velox-spark --extra-index-url https://wolfy024.github.io/velox-spark/simple/
```

If pip refuses with `externally-managed-environment` (Ubuntu 23.04+,
Debian 12+), that distro requires the venv route above. In Jupyter, run
`%pip install velox-spark --extra-index-url https://wolfy024.github.io/velox-spark/simple/`
so it lands in the running kernel's environment, then restart the kernel.

Wheels are pre-built for x86_64 and aarch64 — nothing compiles on your
machine, and the right `pyspark` comes with it (don't install your own
alongside).

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

## Running tests

```bash
pip install -e ".[dev]"
pytest
```

The unit suite is JVM-free and runs in under a second. End-to-end checks
against a real SparkSession go through the validation harness instead:
`velox-spark validate` (see above).

## Build pipeline status

| Pipeline | Status |
|---|---|
| Tests — Python 3.9–3.14 on x86_64 + aarch64, plus a live native-engine smoke run on JDK 17 | [![CI](https://github.com/Wolfy024/velox-spark/actions/workflows/ci.yml/badge.svg)](https://github.com/Wolfy024/velox-spark/actions/workflows/ci.yml) |
| PyPI release — trusted publishing (OIDC) | [![Publish](https://github.com/Wolfy024/velox-spark/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/Wolfy024/velox-spark/actions/workflows/publish-pypi.yml) |
| Platform wheels | built by `scripts/build_wheels.sh`, published with sha256 checksums to the [package index](https://wolfy024.github.io/velox-spark/simple/velox-spark/) |

## A note about versions

velox-spark versions read `<gluten-version>.<packaging-revision>`: `1.6.0.2`
bundles Gluten 1.6.0, second packaging revision. `pyspark` is pinned to
exactly 3.5.5 because Gluten hooks Spark internals through per-version shims —
a bundle built against one patch release is not guaranteed to load against
another, so the package makes a mismatched pair impossible to install.

---

Building the wheels, publishing, platform internals, and benchmark
methodology: **[NOTES.md](NOTES.md)**.

License: Apache-2.0. The bundled Gluten binary retains its own
`LICENSE`/`NOTICE`, included in the wheel.
