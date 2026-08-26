# velox-spark

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI](https://img.shields.io/pypi/v/velox-spark.svg)](https://pypi.org/project/velox-spark/)
[![Python](https://img.shields.io/badge/Python-3.9%E2%80%933.13-blue.svg)](#requirements)
[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://adoptium.net/temurin/releases/?version=17)
[![CI](https://github.com/Wolfy024/velox-spark/actions/workflows/ci.yml/badge.svg)](https://github.com/Wolfy024/velox-spark/actions/workflows/ci.yml)
[![Publish](https://github.com/Wolfy024/velox-spark/actions/workflows/publish-pypi.yml/badge.svg)](https://github.com/Wolfy024/velox-spark/actions/workflows/publish-pypi.yml)

PySpark with the [Apache Gluten](https://gluten.apache.org/) (Velox) native
engine, preconfigured. Same code, same answers -- to a documented floating-point aggregation-order tolerance, not bit-identical (see [NOTES.md](NOTES.md#known-semantic-differences)) -- faster queries.

## Benchmarks

**TPC-H SF100** vs vanilla PySpark, same hardware, equal memory, results
verified equivalent (order-insensitive, 1e-9 relative float tolerance --
Velox reorders float aggregation, so sums over doubles differ in the last
ULPs):

| | Parquet | Iceberg |
|---|---|---|
| Overall speedup | **2.63×** | **3.12×** |
| Best query (q1) | 10.1× | 10.3× |
| Total runtime | 523 s → 199 s | 700 s → 225 s |

**Real-world feature-engineering workload** (anonymized dataset, 176 numeric
features; wide Parquet scan → per-row feature energy → grouped aggregation),
engine toggled in-session, results equivalent under the same tolerance:

| Engine | Workload ×| Logical rows | Time | Throughput |
|---|---|---|---|---|
| JVM | 1 | 4.0 M | 25.1 s | 0.16 M rows/s |
| JVM | 14 | 56.5 M | 31.4 s | 1.80 M rows/s |
| Velox | 1 | 4.0 M | 5.7 s | **0.71 M rows/s** |
| Velox | 128 | 516.6 M | 19.4 s | **26.58 M rows/s** |

**4.4× at matched workload; 26.6M rows/s peak throughput** (14.8× the JVM's
peak).

**DGX Spark (aarch64), strictest methodology** — each arm in its own
process, baseline a *true vanilla* session (no plugin, no columnar shuffle,
no off-heap) at equal total memory, 400M-row dataset: **3.09× overall**
(best query 3.73×). Methodology, per-query numbers, and how to reproduce on
your own workload: [NOTES.md](NOTES.md).

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
pip install velox-spark
```

Without one:

```bash
python3 -m pip install --user velox-spark
```

If pip refuses with `externally-managed-environment` (Ubuntu 23.04+,
Debian 12+), that distro requires the venv route above. In Jupyter, run
`%pip install velox-spark` so it lands in the running kernel's environment,
then restart the kernel.

If a release's platform wheel ever exceeds PyPI's file-size limit for this
project, that release falls back to `--extra-index-url
https://wolfy024.github.io/velox-spark/simple/` — see
[NOTES.md](NOTES.md#publishing).

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

No data handy? A 50k-row synthetic dataset ships inside the wheel:

```python
from velox_spark import get_session, demo_path

spark = get_session("try_it")
spark.read.parquet(demo_path()).groupBy("country").count().show()
```

For a guided tour — aggregate, window and join queries with per-query engine
reports, plus a worked example of what a Python UDF costs — run the test
queries in [demo/](demo/):

```bash
python demo/test_queries.py
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

## Memory

Velox needs off-heap memory, and there is no number that is right on every
machine — so `get_session()` sizes it from the machine it starts on, every
time: **35% of usable RAM for off-heap, 25% for the JVM heap**, rounded to
whole GiB, the rest left for the OS and your Python process. Inside a
container (JupyterHub, Kubernetes) the cgroup limit is respected, so a 16 GB
pod on a 512 GB host is sized from 16 GB — not OOM-killed.

The startup banner shows what was chosen (`off-heap 10g  driver heap 7g`).
When the default is wrong for the box, say so explicitly — your values win:

```python
spark = get_session("job", offheap="24g", driver_memory="8g")
```

On a real cluster (`spark://…`, YARN, k8s) the auto-sizing measures the
*driver's* machine. `get_session()` sets `spark.executor.memory` and
`spark.executor.memoryOverhead` explicitly on cluster masters so the
YARN/k8s container request covers heap + overhead + off-heap; the executor
classpath is pointed at this wheel's JAR paths, which assumes the same wheel
at the same path on every worker (you get a warning explaining this). For a
non-uniform fleet set `spark.executor.memory`, `spark.memory.offHeap.size`
and `VELOX_SPARK_EXECUTOR_CLASSPATH` deliberately, and check the fleet with
`velox_spark.diagnostics.verify_executors(spark)` -- driver-side signals
alone cannot prove the executors loaded the plugin. See
[NOTES.md](NOTES.md#distributed-mode) before pointing this at a cluster.

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

By default both arms share one session (identical memory, but the columnar
shuffle manager and off-heap allocation remain in the baseline arm). Add
`--isolated` to run each arm in its own process against a *true vanilla*
baseline -- no plugin, default shuffle manager, no off-heap -- with the same
total memory. `--register` accepts other formats too:
`--register events=csv:/data/events.csv`.

What to expect: big scans, group-bys and aggregations get 2–3× (up to 10×);
queries that finish in a couple of seconds stay about the same. Parquet and
Iceberg accelerate; other formats just run as normal Spark.

## Things that quietly turn it off

- Creating another SparkSession before `get_session()` — call it first.
- `spark.sql.ansi.enabled=true` — disables all offload (you'll get a warning).
- Python UDFs — results stay correct, but each UDF pays a conversion cost;
  prefer built-in SQL functions.
- Deeply nested expressions (a 100-term `reduce(add, ...)` sum) — Gluten
  drops the whole operator to the JVM past
  `spark.gluten.sql.columnar.fallback.expressions.threshold`.
  `get_session()` raises the threshold to 250 and `report()` names it as a
  suspect; build very wide sums as a balanced tree.
- **Structured streaming** — not accelerated at all. Micro-batch plans run
  on the JVM; `report()` on a streaming DataFrame says so instead of
  pretending to measure it. Batch only.

And one setting that does worse than turn it off:
`spark.sql.caseSensitive=true` produces **wrong results** under Velox rather
than a fallback — `get_session()` warns if it is set. Don't run Gluten with
case-sensitive SQL.

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
| Platform wheels | built by `scripts/build_wheels.sh`, published to PyPI (250 MB file-size exception); falls back to the [GitHub Pages index](https://wolfy024.github.io/velox-spark/simple/velox-spark/) with sha256 checksums if a wheel ever exceeds that limit |

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
