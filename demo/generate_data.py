"""Regenerate demo.parquet. Deterministic: same seed, same file.

The dataset is entirely synthetic -- 50,000 fake retail transactions across
2025. It exists so a fresh install can run real queries against a real
parquet scan without bringing any data of its own.

Needs pyarrow (`pip install pyarrow`), nothing else.
"""

import random
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROWS = 50_000
SEED = 20260815

COUNTRIES = ["IN", "US", "GB", "DE", "BR", "JP", "SG", "AU", "FR", "AE", "ZA", "CA"]
COUNTRY_WEIGHTS = [30, 20, 8, 7, 7, 6, 5, 5, 4, 3, 3, 2]
CATEGORIES = ["grocery", "electronics", "travel", "dining", "fuel",
              "clothing", "utilities", "entertainment"]

rng = random.Random(SEED)
start = date(2025, 1, 1)

cols = {"event_date": [], "country": [], "category": [],
        "account_id": [], "quantity": [], "amount": []}

for _ in range(ROWS):
    cols["event_date"].append(start + timedelta(days=rng.randrange(365)))
    cols["country"].append(rng.choices(COUNTRIES, COUNTRY_WEIGHTS)[0])
    cols["category"].append(rng.choice(CATEGORIES))
    cols["account_id"].append(rng.randrange(1, 20_001))
    cols["quantity"].append(rng.randrange(1, 11))
    cols["amount"].append(round(rng.lognormvariate(3.5, 1.0), 2))

table = pa.table(
    {
        "event_date": pa.array(cols["event_date"], pa.date32()),
        "country": pa.array(cols["country"], pa.string()),
        "category": pa.array(cols["category"], pa.string()),
        "account_id": pa.array(cols["account_id"], pa.int64()),
        "quantity": pa.array(cols["quantity"], pa.int32()),
        "amount": pa.array(cols["amount"], pa.float64()),
    }
)

# The canonical copy lives inside the package so it ships in every wheel
# (velox_spark.demo_path() points at it).
out = Path(__file__).resolve().parents[1] / "src" / "velox_spark" / "data" / "demo.parquet"
pq.write_table(table, out, compression="snappy")
print(f"wrote {out} ({out.stat().st_size / 1024:.0f} KiB, {table.num_rows} rows)")
