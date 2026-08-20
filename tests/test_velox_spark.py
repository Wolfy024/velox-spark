"""Tests for the parts that do not need a JVM.

Anything requiring a real SparkSession belongs in the validation harness, which
runs against real data on a real host.
"""

from __future__ import annotations

import os
import sys

import pytest

import velox_spark
from velox_spark import config, diagnostics, jar, memory, session
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


class TestDemoData:
    def test_demo_parquet_ships_with_the_package(self):
        path = velox_spark.demo_path()
        assert os.path.isfile(path) and path.endswith("demo.parquet")

    def test_demo_parquet_stays_small(self):
        # It rides inside every wheel, including the pure one on PyPI.
        assert os.path.getsize(velox_spark.demo_path()) < 1024**2


class TestWorkerPython:
    def test_unset_pins_workers_to_current_interpreter(self, monkeypatch):
        # pyspark 3.5 defaults workers to bare `python3` from PATH, which
        # inside a venv is the wrong interpreter entirely.
        monkeypatch.delenv("PYSPARK_PYTHON", raising=False)
        session._ensure_worker_python()
        assert os.environ["PYSPARK_PYTHON"] == sys.executable

    def test_explicit_worker_python_is_respected(self, monkeypatch):
        monkeypatch.setenv("PYSPARK_PYTHON", "/opt/other/python")
        session._ensure_worker_python()
        assert os.environ["PYSPARK_PYTHON"] == "/opt/other/python"


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


# ---------------------------------------------------------------------------
# Regression tests for the review fixes (distributed wiring, warnings,
# boundary counting, comparison correctness, register formats).
# ---------------------------------------------------------------------------

from decimal import Decimal

from velox_spark.harness.validate import parse_register_spec


class TestClusterConfig:
    def _conf(self, master):
        from pathlib import Path

        return config.gluten_config(
            jar=Path("/opt/wheel/velox_spark/jars/bundle.jar"),
            offheap_bytes=8 * 1024**3,
            driver_memory_bytes=4 * 1024**3,
            master=master,
            java_major=17,
        )

    def test_executor_classpath_is_set_on_cluster_masters(self):
        # spark.plugins is instantiated during executor init; in standalone
        # mode spark.jars are fetched *after* the executor JVM is up, too late
        # for the plugin class. extraClassPath is the only startup-time hook,
        # so it must be set for cluster masters too (uniform-fleet assumption).
        conf = self._conf("spark://head:7077")
        assert "spark.executor.extraClassPath" in conf

    def test_executor_memory_is_set_on_cluster_masters(self):
        # On YARN/k8s the container request must cover heap + overhead +
        # off-heap; leaving executor memory implicit gets containers killed.
        conf = self._conf("yarn")
        assert "spark.executor.memory" in conf
        assert "spark.executor.memoryOverhead" in conf

    def test_local_master_needs_no_executor_memory(self):
        conf = self._conf("local[*]")
        assert "spark.executor.memory" not in conf

    def test_expression_depth_threshold_is_raised(self):
        # NOTES documents the 176-deep expression chain that silently dropped
        # a whole Project to the JVM at the default threshold of 50.
        conf = self._conf("local[*]")
        assert (
            int(conf["spark.gluten.sql.columnar.fallback.expressions.threshold"])
            >= 176
        )

    def test_distribution_notes_warn_on_cluster_master(self):
        notes = config.distribution_notes("spark://head:7077")
        assert notes and any("executor" in n.lower() for n in notes)

    def test_distribution_notes_quiet_on_local(self):
        assert config.distribution_notes("local[*]") == []
        assert config.distribution_notes(None) == []


class TestCaseSensitivityWarning:
    def test_case_sensitive_true_is_flagged(self):
        # Gluten + spark.sql.caseSensitive=true produces wrong results, not a
        # fallback -- strictly worse than every other warned condition.
        notes = config.warnings_for({"spark.sql.caseSensitive": "true"})
        assert any("casesensitive" in n.lower() for n in notes)

    def test_case_sensitive_false_is_quiet(self):
        notes = config.warnings_for({"spark.sql.caseSensitive": "false"})
        assert not any("casesensitive" in n.lower() for n in notes)


class TestBoundaryCounting:
    def test_vanilla_columnar_to_row_is_not_a_boundary(self):
        # Stock Spark's vectorized Parquet reader emits ColumnarToRowExec with
        # no Gluten involved; counting it gives the baseline arm boundaries it
        # does not have.
        plan = """
        ColumnarToRowExec
        +- FileScan parquet [country]
        """
        assert diagnostics.plan_stats(plan)["boundaries"] == 0

    def test_velox_boundary_still_counts(self):
        plan = "VeloxColumnarToRowExec\n+- ^ FileScanTransformer parquet"
        assert diagnostics.plan_stats(plan)["boundaries"] == 1

    def test_aqe_initial_plan_section_is_not_double_counted(self):
        # AdaptiveSparkPlan.toString() prints the final plan AND the initial
        # plan; counting both doubles every operator.
        plan = """
        AdaptiveSparkPlan isFinalPlan=true
        +- == Final Plan ==
           VeloxColumnarToRowExec
           +- ^ HashAggregateExecTransformer(keys=[country])
        +- == Initial Plan ==
           VeloxColumnarToRowExec
           +- ^ HashAggregateExecTransformer(keys=[country])
        """
        stats = diagnostics.plan_stats(plan)
        assert stats["offloaded"] == 1
        assert stats["boundaries"] == 1


class TestJvmOperators:
    def test_names_fallen_back_operators(self):
        plan = """
        VeloxColumnarToRowExec
        +- ^ ProjectExecTransformer [country]
           +- RowToVeloxColumnar
              +- HashAggregate(keys=[country], functions=[collect_list(x)])
                 +- Exchange hashpartitioning(country, 200)
        """
        ops = diagnostics.jvm_operators(plan)
        assert "HashAggregate" in ops
        assert "Exchange" in ops
        # Transformers and conversion nodes are not "fallen back".
        assert "ProjectExecTransformer" not in ops
        assert "VeloxColumnarToRowExec" not in ops
        assert "RowToVeloxColumnar" not in ops

    def test_wrapper_nodes_are_not_reported(self):
        plan = """
        AdaptiveSparkPlan isFinalPlan=true
        +- WholeStageCodegen (1)
           +- InputAdapter
              +- FileScan parquet
        """
        ops = diagnostics.jvm_operators(plan)
        assert "AdaptiveSparkPlan" not in ops
        assert "WholeStageCodegen" not in ops
        assert "InputAdapter" not in ops
        assert "FileScan" in ops


class TestCompareRowsPairing:
    def test_rows_matching_within_tolerance_are_not_mispaired(self):
        # Two rows share the coarse %.6g sort key but arrive in different
        # orders from the two arms. The naive zip pairs a-with-d and c-with-b
        # and reports two spurious mismatches; correct matching finds none.
        a, b = 1.00000012345, 1.00000012346  # differ ~1e-11 (within 1e-9 rel)
        c, d = 1.00000098765, 1.00000098766  # differ ~1e-11 (within 1e-9 rel)
        assert compare_rows([(a,), (c,)], [(d,), (b,)]) == []

    def test_genuine_mismatch_still_reported_after_rematch(self):
        assert compare_rows([(1.0,)], [(1.5,)]) != []


class TestCompareRowsNestedTypes:
    def test_floats_inside_arrays_use_the_tolerance(self):
        assert compare_rows([([0.1 + 0.2],)], [([0.3],)]) == []

    def test_floats_inside_maps_use_the_tolerance(self):
        assert compare_rows([({"k": 0.1 + 0.2},)], [({"k": 0.3},)]) == []

    def test_nested_real_difference_is_caught(self):
        assert compare_rows([([1.0],)], [([2.0],)]) != []

    def test_decimals_compare_exactly(self):
        assert compare_rows([(Decimal("1.10"),)], [(Decimal("1.1"),)]) == []
        assert compare_rows([(Decimal("1.10"),)], [(Decimal("1.2"),)]) != []


class TestRegisterSpecs:
    def test_bare_path_defaults_to_parquet(self):
        assert parse_register_spec("events=/data/events") == (
            "events", "parquet", "/data/events",
        )

    def test_explicit_format_prefix(self):
        assert parse_register_spec("events=csv:/data/events.csv") == (
            "events", "csv", "/data/events.csv",
        )

    def test_uri_scheme_is_not_mistaken_for_a_format(self):
        assert parse_register_spec("events=s3a://bucket/events") == (
            "events", "parquet", "s3a://bucket/events",
        )

    def test_malformed_spec_raises(self):
        with pytest.raises(ValueError):
            parse_register_spec("no-equals-sign")


class TestCompanionJars:
    def test_lakehouse_companions_are_discovered(self, monkeypatch, tmp_path):
        # Gluten 1.6 has Delta/Hudi/Paimon modules too; the wheel build accepts
        # them via --extra-jar, so resolution must pick them up when present.
        for name in (
            "gluten-iceberg-1.6.0.jar",
            "gluten-delta-1.6.0.jar",
            "gluten-hudi-1.6.0.jar",
            "gluten-paimon-1.6.0.jar",
            "iceberg-spark-runtime-3.5_2.12-1.10.0.jar",  # not a companion
        ):
            (tmp_path / name).write_bytes(b"")
        monkeypatch.setattr(jar, "_JAR_DIR", tmp_path)
        names = [j.name for j in jar.companion_jars()]
        assert "gluten-iceberg-1.6.0.jar" in names
        assert "gluten-delta-1.6.0.jar" in names
        assert "gluten-hudi-1.6.0.jar" in names
        assert "gluten-paimon-1.6.0.jar" in names
        assert "iceberg-spark-runtime-3.5_2.12-1.10.0.jar" not in names


class TestStreamingGuard:
    def test_report_names_streaming_instead_of_inspecting_the_plan(self):
        class FakeStreamingFrame:
            isStreaming = True

        text = diagnostics.report(FakeStreamingFrame())
        assert "streaming" in text.lower()


class TestReportSuspects:
    def test_zero_offload_names_expression_threshold(self):
        # NOTES documents expression-depth fallback as a lesson learned; the
        # report must name it as a suspect instead of leaving it to folklore.
        class FakeFrame:
            isStreaming = False

        plan = "HashAggregate(keys=[k])\n+- FileScan parquet"
        import unittest.mock as mock

        with mock.patch.object(
            diagnostics, "executed_plan", return_value=plan
        ):
            text = diagnostics.report(FakeFrame())
        assert "threshold" in text.lower()

    def test_velox_boundary_without_exec_suffix_counts(self):
        # Gluten 1.6's node prints as "VeloxColumnarToRow" (no Exec suffix)
        # in executed-plan text; observed live on the aarch64 bundle.
        plan = "VeloxColumnarToRow\n+- ^ FileScanTransformer parquet"
        stats = diagnostics.plan_stats(plan)
        assert stats["boundaries"] == 1
        assert "VeloxColumnarToRow" not in diagnostics.jvm_operators(plan)
