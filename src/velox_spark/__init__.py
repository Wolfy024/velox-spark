"""Apache Spark with the Gluten/Velox native engine, preconfigured.

    from velox_spark import get_session

    spark = get_session("my_job")
    spark.read.parquet("s3://bucket/data").groupBy("k").count().show()

On Linux the native engine is bundled in the wheel and switched on for you.
Everywhere else the same code runs on ordinary Spark, with a warning -- never a
silent difference.
"""

from __future__ import annotations

from .diagnostics import is_engaged, plan_stats, report
from .jar import resolve as resolve_jar
from .session import (
    NativeEngineUnavailable,
    disable_gluten,
    enable_gluten,
    get_session,
    status,
)

def demo_path() -> str:
    """Path to the bundled demo dataset: 50k synthetic retail transactions.

    Entirely fake data (see demo/generate_data.py in the repository), shipped
    in every wheel so a fresh install can run real queries against a real
    parquet scan without bringing any data of its own::

        spark.read.parquet(demo_path()).groupBy("country").count().show()
    """
    from pathlib import Path

    path = Path(__file__).parent / "data" / "demo.parquet"
    if not path.is_file():
        raise FileNotFoundError(
            "velox_spark: bundled demo.parquet is missing from this install."
        )
    return str(path)


try:  # pragma: no cover - trivial
    from importlib.metadata import PackageNotFoundError, version as _version

    __version__ = _version("velox-spark")
except Exception:  # noqa: BLE001 - running from a source tree, not installed
    __version__ = "0.0.0.dev0"

# The Gluten release whose bundle JAR these wheels carry. Derived from the
# package version, whose first three components are the Gluten version by
# convention (see pyproject.toml).
__gluten_version__ = ".".join(__version__.split(".")[:3])

__all__ = [
    "get_session",
    "disable_gluten",
    "enable_gluten",
    "status",
    "is_engaged",
    "report",
    "plan_stats",
    "resolve_jar",
    "demo_path",
    "NativeEngineUnavailable",
    "__version__",
    "__gluten_version__",
]
