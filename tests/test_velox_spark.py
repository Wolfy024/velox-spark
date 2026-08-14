"""Tests for the parts that do not need a JVM.

Anything requiring a real SparkSession belongs in the validation harness, which
runs against real data on a real host.
"""

from __future__ import annotations

import pytest

from velox_spark import config, diagnostics, jar, memory
from velox_spark.harness.validate import compare_rows


class TestMemory:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("8g", 8 * 1024**3),
            ("512m", 512 * 1024**2),
            ("1024k", 1024 * 1024),
            ("2G", 2 * 1024**3),
            ("16gb", 16 * 1024**3),
            (4096, 4096),
        ],
    )
    def test_parse_size(self, text, expected):
        assert memory.parse_size(text) == expected

    def test_parse_size_rejects_nonsense(self):
        with pytest.raises(ValueError):
            memory.parse_size("plenty")

    def test_format_size_round_trips_whole_units(self):
        assert memory.format_size(8 * 1024**3) == "8g"
        assert memory.format_size(512 * 1024**2) == "512m"

    def test_defaults_leave_headroom(self):
        """Off-heap plus heap must not claim the whole machine."""
        total = memory.usable_ram()
        assert memory.default_offheap() + memory.default_heap() < total


class TestJarResolution:
    def test_missing_env_jar_is_an_error_not_a_fallback(self, monkeypatch, tmp_path):
        # A typo'd GLUTEN_JAR_PATH must fail loudly rather than silently
        # dropping back to the bundled JAR -- the operator asked for a specific
        # build and got something else.
        monkeypatch.setenv("GLUTEN_JAR_PATH", str(tmp_path / "nope.jar"))
        with pytest.raises(jar.JarNotFoundError):
            jar.resolve()

    def test_env_jar_wins(self, monkeypatch, tmp_path):
        fake = tmp_path / "gluten.jar"
        fake.write_bytes(b"")
        monkeypatch.setenv("GLUTEN_JAR_PATH", str(fake))
        found, source = jar.resolve()
        assert found == fake and source == "env"

    def test_explicit_beats_env(self, monkeypatch, tmp_path):
        env_jar, explicit = tmp_path / "env.jar", tmp_path / "explicit.jar"
        env_jar.write_bytes(b"")
        explicit.write_bytes(b"")
        monkeypatch.setenv("GLUTEN_JAR_PATH", str(env_jar))
        found, source = jar.resolve(str(explicit))
        assert found == explicit and source == "explicit"

    def test_source_tree_has_no_bundled_jar(self):
        """JARs are build artifacts; committing one would bloat the repo."""
        assert jar.bundled_jar() is None


class TestPlanStats:
    GLUTEN_PLAN = """
    VeloxColumnarToRowExec
    +- ^ HashAggregateExecTransformer(keys=[country], functions=[count(1)])
       +- ColumnarExchange hashpartitioning(country, 200)
          +- ^ ProjectExecTransformer [country]
             +- ^ FileScanTransformer parquet [country]
    """

    VANILLA_PLAN = """
    HashAggregate(keys=[country], functions=[count(1)])
    +- Exchange hashpartitioning(country, 200)
       +- FileScan parquet [country]
    """

    def test_counts_offloaded_operators(self):
        stats = diagnostics.plan_stats(self.GLUTEN_PLAN)
        assert stats["offloaded"] == 4  # 3 Transformers + ColumnarExchange
        assert stats["boundaries"] == 1

    def test_vanilla_plan_shows_nothing_offloaded(self):
        stats = diagnostics.plan_stats(self.VANILLA_PLAN)
        assert stats == {"offloaded": 0, "boundaries": 0}


class TestConfigWarnings:
    def test_ansi_mode_is_flagged(self):
        notes = config.warnings_for({"spark.sql.ansi.enabled": "true"})
        assert any("ansi" in n.lower() for n in notes)

    def test_overridden_shuffle_manager_is_flagged(self):
        notes = config.warnings_for(
            {"spark.shuffle.manager": "org.apache.spark.shuffle.sort.SortShuffleManager"}
        )
        assert any("shuffle" in n.lower() for n in notes)

    def test_healthy_config_is_quiet(self):
        notes = config.warnings_for(
            {
                "spark.sql.ansi.enabled": "false",
                "spark.shuffle.manager": config.COLUMNAR_SHUFFLE_MANAGER,
                "spark.memory.offHeap.enabled": "true",
            }
        )
        assert notes == []


class TestLocalMasterDetection:
    @pytest.mark.parametrize(
        "master,expected",
        [
            ("local[*]", True),
            ("local", True),
            ("spark://head:7077", False),
            ("yarn", False),
            ("k8s://https://api:6443", False),
        ],
    )
    def test_detection(self, master, expected, monkeypatch):
        monkeypatch.delenv("MASTER", raising=False)
        monkeypatch.delenv("SPARK_MASTER", raising=False)
        assert config.is_local_master(master) is expected

    def test_unset_master_assumed_local(self, monkeypatch):
        """spark-submit defaults to local[*] when nothing sets a master."""
        monkeypatch.delenv("MASTER", raising=False)
        monkeypatch.delenv("SPARK_MASTER", raising=False)
        assert config.is_local_master(None) is True


class TestRowComparison:
    def test_identical_results_match(self):
        rows = [(1, "a", 1.5), (2, "b", 2.5)]
        assert compare_rows(rows, list(rows)) == []

    def test_row_order_is_ignored(self):
        assert compare_rows([(1, "a"), (2, "b")], [(2, "b"), (1, "a")]) == []

    def test_float_drift_within_tolerance_passes(self):
        """Velox reorders float aggregation; last-ULP drift is not a bug."""
        assert compare_rows([(1, 0.1 + 0.2)], [(1, 0.3)]) == []

    def test_real_difference_is_caught(self):
        problems = compare_rows([(1, 100.0)], [(1, 101.0)])
        assert len(problems) == 1 and "100.0" in problems[0]

    def test_row_count_difference_is_caught(self):
        problems = compare_rows([(1,), (2,)], [(1,)])
        assert "row count differs" in problems[0]

    def test_nulls_compare_equal_but_not_to_zero(self):
        assert compare_rows([(None,)], [(None,)]) == []
        assert compare_rows([(None,)], [(0.0,)]) != []

    def test_nan_matches_nan(self):
        assert compare_rows([(float("nan"),)], [(float("nan"),)]) == []

    def test_negative_zero_matches_zero(self):
        assert compare_rows([(-0.0,)], [(0.0,)]) == []
