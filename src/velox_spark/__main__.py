"""Command line entry point: ``velox-spark doctor`` and ``velox-spark validate``."""

from __future__ import annotations

import argparse
import platform
import sys


def _doctor() -> int:
    """Print everything needed to tell whether this install can accelerate."""
    from . import __gluten_version__, __version__, jar, jdk, memory

    print(f"velox-spark {__version__}  (Gluten {__gluten_version__})")
    print(f"  platform     : {platform.system()}/{platform.machine()}")
    print(f"  python       : {platform.python_version()}")

    try:
        import pyspark

        print(f"  pyspark      : {pyspark.__version__}")
    except ImportError:
        print("  pyspark      : MISSING")

    exe = jdk.java_executable()
    if exe is None:
        print("  java         : NOT FOUND  <-- Spark cannot start without a JVM")
    else:
        major = jdk.java_major_version(exe)
        supported = "ok" if major in jdk.SUPPORTED_MAJORS else "UNSUPPORTED"
        print(f"  java         : {major or 'unknown'} ({supported}) at {exe}")

    found, source = jar.resolve()
    if found is None:
        print("  gluten jar   : NOT BUNDLED")
        print(f"                 {jar.describe_missing()}")
    else:
        size_mb = found.stat().st_size / 1024**2
        print(f"  gluten jar   : {found.name} ({size_mb:.0f} MB, source={source})")

    ram = memory.usable_ram()
    print(
        f"  memory       : {memory.format_size(ram)} usable -> "
        f"off-heap {memory.format_size(memory.default_offheap())}, "
        f"driver heap {memory.format_size(memory.default_heap())}"
    )

    ready = found is not None and exe is not None
    print()
    print(
        "  READY: native acceleration will be enabled."
        if ready
        else "  NOT READY: sessions will run on unaccelerated Spark."
    )
    return 0 if ready else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="velox-spark",
        description="Spark with the Gluten/Velox native engine.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser(
        "doctor",
        help="Report whether this install can actually accelerate anything.",
    )

    validate = sub.add_parser(
        "validate",
        help="Run a query with and without Gluten and compare results, "
        "fallback counts and wall clock.",
    )
    from .harness.validate import add_arguments

    add_arguments(validate)

    args = parser.parse_args(argv)

    if args.command == "doctor":
        return _doctor()
    if args.command == "validate":
        from .harness.validate import run_cli

        return run_cli(args)
    parser.error(f"unknown command {args.command}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
