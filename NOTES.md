# Operator and maintainer notes

Everything that is not the data-scientist path: packaging, publishing,
platform internals, diagnostics detail, and benchmark methodology.

## Contents

- [Requirements and doctor](#requirements-and-doctor)
- [Session configuration details](#session-configuration-details)
- [Iceberg wiring](#iceberg-wiring)
- [Diagnostics](#diagnostics)
- [Validation harness methodology](#validation-harness-methodology)
- [Performance measurements](#performance-measurements)
- [Platform support](#platform-support)
- [Building the wheels](#building-the-wheels)
- [Building the aarch64 JAR](#building-the-aarch64-jar)
- [Container image](#container-image)
- [Publishing](#publishing)

## Requirements and doctor

JDK 17 is the one dependency pip cannot provide (Gluten supports 8 and 17;
the package targets 17 and sets the `--add-opens` flags Spark needs on
modern JDKs on both driver and executors — `spark-submit` normally does
this, but a session built from a plain Python process does not inherit it).

`velox-spark doctor` reports platform, Java, the bundled JAR, and computed
memory defaults, and exits non-zero when native acceleration will not
engage. Suitable as an environment gate in CI.

## Session configuration details

`get_session()` sets, before JVM start:

| Key | Value |
|---|---|
| `spark.plugins` | `org.apache.gluten.GlutenPlugin` |
| `spark.jars` / driver classpath | bundle JAR + companion jars |
| `spark.memory.offHeap.enabled/size` | on; 35% of usable RAM by default |
| `spark.driver.memory` | 25% of usable RAM by default |
| `spark.shuffle.manager` | `ColumnarShuffleManager` |
| driver/executor `extraJavaOptions` | `--add-opens` set for JDK 17 |

Memory sizing honours cgroup limits, so containers are sized by their limit
rather than host RAM. Overrides: `offheap=`, `driver_memory=`; anything else
via `extra_conf`, which is applied last and wins.

Do not set `spark.plugins`, `spark.memory.offHeap.*`, or
`spark.shuffle.manager` in `extra_conf`; the package warns when an override
disables acceleration (wrong shuffle manager, ANSI mode, off-heap off).

`disable_gluten(spark)` flips `spark.gluten.enabled`, which Spark evaluates
per query, so it works on a live session; off-heap allocation is fixed at
JVM startup and is not released. `enabled=False` at construction gives a
clean unaccelerated baseline. `VELOX_SPARK_DISABLE=1` does the same from the
environment.

JAR resolution order: explicit `jar_path=` argument → `GLUTEN_JAR_PATH`
environment variable → JAR bundled in the wheel → none (warn, or raise
under `require_native=True`). The env var exists so an operator can test a
locally built JAR without repackaging.

## Iceberg wiring

The wheel bundles two architecture-independent jars:

- `gluten-iceberg-1.6.0.jar` — Gluten's Iceberg module
  (`IcebergScanTransformer`); without it, Iceberg tables silently fall back
  to JVM scans even with the plugin engaged. Always placed on the classpath
  when the native engine is enabled.
- `iceberg-spark-runtime-3.5_2.12-1.10.0.jar` — Iceberg itself. Only added
  when `iceberg=True`, so an environment that manages its own Iceberg
  version is unaffected by default.

`iceberg=True` also registers `IcebergSparkSessionExtensions`. Catalog
configuration is intentionally left to the caller.

## Diagnostics

The engaged-but-idle failure mode: the plugin loads, every operator falls
back, and the job pays columnar↔row conversion for no benefit. `status()`
exposes the settings that decide engagement; `report(df)` counts operators
offloaded to Velox against conversion boundaries in the executed plan.

Interpretation: zero offloaded operators — the query ran on the JVM
(common causes: ANSI mode, non-Parquet/Iceberg source, UDF-dominated plan);
more boundaries than offloaded operators — conversion overhead likely
exceeds the native benefit.

Plan inspection materialises the DataFrame's own `QueryExecution` via a
JVM-side action. Two implementation traps documented in
`diagnostics.py`: `df.foreach(lambda ...)` pickles a Python lambda
(crashes on Python 3.14, and routes rows through Python workers), and a
`noop`-sink write materialises a *different* `QueryExecution`, leaving the
inspected AQE plan unfinalised — which reports zero offload on a healthy
plan.

Per-operator fallback reasons are logged by Gluten at INFO
(`Validation failed for plan: ...`).

## Validation harness methodology

`velox-spark validate` runs a query with the plugin enabled and disabled in
one session and gates on three criteria; exit code is non-zero unless all
pass:

1. **Correctness** — order-insensitive comparison with a relative float
   tolerance (`--rel-tol`, default 1e-9). Velox reorders floating-point
   aggregation, so `sum`/`avg` over doubles differ in the final ULPs on
   correct queries; exact comparison would fail them. NaN==NaN and
   -0.0==0.0 are treated as equal; integers and strings compare exactly.
2. **Fallback** — offloaded operators > 0 and boundaries ≤ offloaded.
3. **Wall clock** — median over `--runs` of full materialisation through
   the `noop` sink, after a discarded warm-up. `--min-speedup` sets the bar.

Both arms share one session (toggling `spark.gluten.enabled`), so memory
configuration is identical and the measurement isolates the engine.

Caveats: the `noop` write itself is not offloadable
(`OverwriteByExpression`), so both arms carry one fixed conversion — this
dilutes ratios on short queries. Validate on representative volumes:
sub-second queries are dominated by scheduling and fixed per-query native
overhead (~0.2–0.3 s) and measure near or below 1.0×, which the gate
reports accurately.

## Performance measurements

TPC-H SF100 (~100 GB scale), i9-13900HX, 30 GB RAM, equal total memory per
arm (vanilla 16 g heap; velox 8 g heap + 8 g off-heap), spill on real disk,
same Parquet files, results verified identical:

| Query | Vanilla | Velox | Speedup |
|---|---|---|---|
| q1 | 94.0 s | 9.4 s | 10.06× |
| q3 | 35.9 s | 10.5 s | 3.41× |
| q5 | 63.0 s | 20.4 s | 3.09× |
| q6 | 4.0 s | 2.9 s | 1.40× |
| q9 | 102.9 s | 33.9 s | 3.03× |
| q18 | 110.9 s | 53.7 s | 2.06× |
| q21 | 112.2 s | 68.3 s | 1.64× |
| **Total** | **522.9 s** | **199.1 s** | **2.63×** |

The same seven queries over Iceberg tables (zstd) measure 3.12× overall —
vanilla Spark reads Iceberg slower while Velox is largely format-neutral,
which widens the ratio.

### Real-world case study

A feature-engineering pipeline over an anonymized tabular dataset
(176 numeric features; wide Parquet scan
→ per-row squared-feature energy → xxhash64 row hashing →
explode-replicated grouped aggregation with `approx_count_distinct` and
`stddev`), engine toggled in-session, identical results both arms:

| Engine | Replication | Logical rows | Time | Throughput |
|---|---|---|---|---|
| JVM | 1 | 4.0 M | 25.1 s | 0.16 M rows/s |
| JVM | 14 | 56.5 M | 31.4 s | 1.80 M rows/s |
| Velox | 1 | 4.0 M | 5.7 s | 0.71 M rows/s |
| Velox | 128 | 516.6 M | 19.4 s | 26.58 M rows/s |

**4.4× at matched replication (rep=1); 14.8× peak-to-peak throughput.**
Quote the former as the speedup — the latter compares each engine at its own
saturation point, which is a throughput statement, not a matched-work one.
First-run times include cold-start (JIT, page cache) on both arms.

Two lessons from this workload, both general:

- **Expression depth causes silent whole-operator fallback.** The
  176-feature sum built with `reduce(add, ...)` produces a left-leaning
  expression chain 176 deep; Gluten refuses expressions past
  `spark.gluten.sql.columnar.fallback.expressions.threshold` (default 50)
  and dropped the entire Project to the JVM — turning the measured 4.4×
  into ~1× until caught. Fix: build wide sums as a balanced tree (depth
  ⌈log₂ n⌉) and/or raise the threshold. `report()` surfaces this class of
  fallback; the GlutenFallbackReporter WARN names the cause.
- **`MetricsUtil: Updating native metrics failed due to null`** is a known
  cosmetic warning around mixed native/JVM plans; silence with a log4j2
  level override for `org.apache.gluten.metrics.MetricsUtil` rather than
  raising the global log level.

Operator-level decomposition of the weak spots (600 M-row microbenchmarks):
the native engine carries a fixed ~0.2–0.3 s per-query overhead that is
invisible on long queries and dominant on short ones, and the broadcast
hash join probe is ~2× slower than JVM whole-stage codegen at small scale
(it still wins at TPC-H scale, q21 1.64×). Aggregation-heavy shapes are the
strongest case throughout. Memory split (heap vs off-heap) was measured to
have no material effect at equal totals.

## Platform support

| Platform | Native engine | Status |
|---|---|---|
| Linux x86_64 | Bundled | Official Apache Gluten 1.6.0 release binary; SHA-512 and GPG verified against the project KEYS. glibc floor 2.17. |
| Linux aarch64 | Bundled | Built from the v1.6.0 tag with `docker/Dockerfile.gluten-aarch64` (static vcpkg; glibc is the only runtime dependency). |
| macOS, Windows | — | Pure wheel; standard Spark with a warning. |

ANSI mode (`spark.sql.ansi.enabled=true`) disables offload entirely on any
platform.

## Building the wheels

```bash
scripts/build_wheels.sh \
  --x86-jar jars/gluten-velox-bundle-spark3.5_2.12-linux_amd64-1.6.0.jar \
  --arm-jar jars/gluten-velox-bundle-spark3.5_2.12-ubuntu_22.04_aarch64-1.6.0.jar \
  --extra-jar jars/gluten-iceberg-1.6.0.jar \
  --extra-jar jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar
```

Produces `manylinux_*_x86_64`, `manylinux_*_aarch64`, and `py3-none-any`
wheels; pip selects by platform tag.

- **Platform tags are applied post-build.** The wheels contain no compiled
  Python extension — the native code is inside the JAR — so setuptools tags
  them `py3-none-any`. The script retags with `wheel tags`; without this,
  pip would install an aarch64 JAR on x86 hosts.
- **The manylinux floor is derived from the binaries, not the filename.**
  The script extracts the shared libraries and reads the highest `GLIBC_x.y`
  symbol version and the ELF architecture. Upstream JAR naming has changed
  between releases and is not trusted. An architecture mismatch between a
  JAR and its `--x86-jar`/`--arm-jar` slot fails the build.
- **Version pairing is enforced.** The first three components of the package
  version must equal the Gluten version in the JAR name. `-SNAPSHOT` JARs
  are only accepted by `.devN` package versions, and release JARs are
  refused by `.devN` versions — an unreleased build cannot be published
  under a version string that reads as a release. Gluten binds to Spark
  internals through per-version shims; this check prevents shipping a
  mismatched pair.
- `--extra-jar` files are pure JVM bytecode and go into every platform wheel.
- Omitting one platform JAR is supported: affected hosts receive the pure
  wheel and run standard Spark with a warning.
- `LICENSE`/`NOTICE` are extracted from the bundle JAR into the wheel; the
  bundle statically links Velox, Arrow, folly and their dependency tree, and
  attribution must travel with redistribution.

## Building the aarch64 JAR

Upstream publishes no aarch64 binaries. `scripts/build_gluten_aarch64.sh`
builds one on an ARM host via `docker/Dockerfile.gluten-aarch64`
(Ubuntu 22.04, `--enable_vcpkg=ON` for static linking). Hard-won specifics,
all encoded in the Dockerfile:

- `CPU_TARGET=aarch64` is mandatory: `dev/vcpkg/env.sh` selects the vcpkg
  triplet from it with no host autodetect; unset, the ARM build is handed
  the `x64-linux-avx` triplet and fails in compiler detection. The value
  selects `arm64-linux-neon` — the baseline ARM triplet, the counterpart of
  the x86 `avx` default, not a `-march=native` equivalent.
- `autoconf automake libtool autoconf-archive` and `libfl-dev` are required.
  `libfl-dev` (which owns `FlexLexer.h`) is only a *Recommends* of `flex`
  and is dropped by `--no-install-recommends`; its absence surfaces 25
  minutes in, at CMake's generate step. A fail-fast guard checks for the
  header before the build starts.
- `TREAT_WARNINGS_AS_ERRORS=0`: Velox's aarch64 xsimd code emits `-Wpsabi`
  notes that are fatal under the default warnings-as-errors.
- A `RUN --mount=type=cache` holds vcpkg's binary cache across attempts, so
  a failed build resumes at the failing package rather than rebuilding all
  ~119 dependencies. `NUM_THREADS` (default 4) is the OOM lever; ~4 GB per
  compile job.
- The bundle JAR is named for the OS it was built on so the wheel build can
  cross-check it; the glibc floor itself is read from the ELF.

## Container image

`scripts/build_image.sh` builds a multi-arch runtime image (one tag,
`linux/amd64` + `linux/arm64`) with the wheels baked in — nothing is fetched
at runtime, so it serves air-gapped deployments. The image build runs
`velox-spark doctor` as a gate, so an image that accidentally picked up the
pure wheel fails at build time rather than shipping unaccelerated.

## Publishing

PyPI's default per-file limit (100 MB) is below the platform wheel size.
Distribution uses GitHub Releases (2 GB/asset) for bytes and a static
[PEP 503](https://peps.python.org/pep-0503/) index on GitHub Pages for name
resolution:

```bash
scripts/publish_release.sh --repo wolfy024/velox-spark
```

Uploads `dist/*.whl` to a versioned release and regenerates `docs/simple/`,
carrying forward previous releases' links so existing pins remain
installable. Commit `docs/` and serve it with GitHub Pages.

- Every index link embeds `#sha256=`, which pip verifies. Release assets
  are mutable; the hash is what makes installs reproducible.
- The repository must be public for unauthenticated `pip install`.
- The script refuses to publish the pure wheel alone — that would hand
  every Linux host an unaccelerated install with only a runtime warning.
- Air-gapped environments should mirror the wheels internally or use the
  container image.
