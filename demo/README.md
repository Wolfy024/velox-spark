# Demo

A zero-setup way to see the native engine work. The dataset — 50,000
entirely synthetic retail transactions — ships **inside the wheel**, so this
runs on any machine where `velox-spark` is installed:

```bash
python demo/test_queries.py
```

Four queries run against a real parquet scan, each followed by a plan report
showing how much of it executed in Velox. Each is its own file and also runs
standalone (`python demo/aggregate.py`):

| File | Query | What it shows |
|---|---|---|
| [aggregate.py](aggregate.py) | revenue by country | scan + group-by offloads completely |
| [window_rank.py](window_rank.py) | each country's best month | window functions offload |
| [join.py](join.py) | outliers vs category average | joins offload |
| [python_udf.py](python_udf.py) | amount bands via UDF | the columnar↔row boundary a Python UDF costs |

The UDF query is deliberately the bad example: the extra boundary it causes
is the main thing that quietly costs performance in real pipelines.

From your own code, the dataset is one import away:

```python
from velox_spark import get_session, demo_path

spark = get_session("try_it")
spark.read.parquet(demo_path()).groupBy("country").count().show()
```

This is a demo, not a benchmark — 50k rows finish in milliseconds on any
engine. For speedup measurements use `velox-spark validate` on your own data
at real scale, or see the benchmark methodology in [../NOTES.md](../NOTES.md).

`generate_data.py` regenerates the parquet deterministically (seeded; needs
`pyarrow`). The data is fake: every value comes from a seeded random
generator, derived from nothing.
