# Bundle JAR directory

This directory is **empty in source control** and empty in the pure-Python
(`py3-none-any`) wheel.

`scripts/build_wheels.sh` drops a single Gluten/Velox bundle JAR here, plus the
`LICENSE`/`NOTICE` files extracted from it, then builds a platform wheel and
cleans the directory again.

Do not commit a JAR here. The platform wheels are build artifacts.

At runtime `velox_spark.jar.resolve()` looks for exactly one `*.jar` in this
directory. Two or more is treated as a broken wheel and raises immediately,
rather than picking one arbitrarily and failing much later.
