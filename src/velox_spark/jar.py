"""Locating the Gluten/Velox bundle JAR.

Resolution order, highest priority first:

1. ``GLUTEN_JAR_PATH`` environment variable -- an explicit operator override.
   Set this to point at a JAR you built yourself without reinstalling the wheel.
2. A JAR bundled inside this package (``velox_spark/jars/``). This is what the
   platform wheels ship and what almost every user gets.
3. Nothing. The pure-Python wheel installs on macOS/Windows and lands here; the
   session falls back to unaccelerated Spark with a warning.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional, Tuple

_ENV_VAR = "GLUTEN_JAR_PATH"

# Where the platform wheels drop the bundle JAR.
_JAR_DIR = Path(__file__).resolve().parent / "jars"


class JarNotFoundError(RuntimeError):
    """Raised when native acceleration was required but no JAR is available."""


def bundled_jar() -> Optional[Path]:
    """Return the Gluten *bundle* JAR shipped inside this wheel, if any.

    The wheel may carry companion jars alongside it (gluten-iceberg, the
    Iceberg Spark runtime); only the velox-bundle jar is the native engine.
    """
    if not _JAR_DIR.is_dir():
        return None
    jars = sorted(_JAR_DIR.glob("*velox-bundle*.jar"))
    if not jars:
        return None
    # More than one bundle JAR means a botched build. Picking arbitrarily
    # would give a confusing runtime failure much later, so refuse now.
    if len(jars) > 1:
        names = ", ".join(j.name for j in jars)
        raise RuntimeError(
            f"velox_spark: {len(jars)} bundle JARs found in {_JAR_DIR} "
            f"({names}). A wheel must bundle exactly one. Rebuild the wheel."
        )
    return jars[0]


def companion_jars() -> list:
    """Jars shipped alongside the bundle that extend Gluten itself.

    Currently the gluten-iceberg module, which holds IcebergScanTransformer --
    without it Iceberg tables silently fall back to JVM scans even with the
    plugin engaged.
    """
    if not _JAR_DIR.is_dir():
        return []
    return sorted(_JAR_DIR.glob("gluten-iceberg-*.jar"))


def iceberg_runtime_jar() -> Optional[Path]:
    """The iceberg-spark-runtime jar, when the wheel carries one.

    This is Iceberg itself, not part of Gluten. Only put on the classpath when
    the session asks for Iceberg, so a user's own Iceberg version never has to
    fight ours.
    """
    if not _JAR_DIR.is_dir():
        return None
    jars = sorted(_JAR_DIR.glob("iceberg-spark-runtime-*.jar"))
    return jars[0] if jars else None


def resolve(explicit: Optional[str] = None) -> Tuple[Optional[Path], str]:
    """Find the bundle JAR.

    Returns ``(path, source)`` where source is one of ``explicit``, ``env``,
    ``bundled`` or ``missing``. The source string exists so diagnostics can tell
    a user *why* they got the JAR they got.
    """
    if explicit:
        path = Path(explicit).expanduser().resolve()
        if not path.is_file():
            raise JarNotFoundError(f"velox_spark: jar_path={explicit!r} does not exist")
        return path, "explicit"

    from_env = os.environ.get(_ENV_VAR)
    if from_env:
        path = Path(from_env).expanduser().resolve()
        if not path.is_file():
            raise JarNotFoundError(
                f"velox_spark: {_ENV_VAR}={from_env!r} does not exist. "
                "Unset it to fall back to the JAR bundled in this wheel."
            )
        return path, "env"

    bundled = bundled_jar()
    if bundled is not None:
        return bundled, "bundled"

    return None, "missing"


def describe_missing() -> str:
    """Human-readable explanation of why no JAR is present, with a way forward."""
    import platform

    system = platform.system()
    machine = platform.machine()
    if system != "Linux":
        return (
            f"No Gluten JAR is bundled for {system}/{machine}. The Velox native "
            "engine is Linux-only, so this platform gets the pure-Python wheel "
            "and runs on unaccelerated Spark. Nothing is broken -- your code "
            "still works, just without the native backend."
        )
    return (
        f"No Gluten JAR is bundled in this installation ({system}/{machine}). "
        "You most likely installed the pure-Python wheel because pip could not "
        "reach the index hosting the platform wheels. Reinstall with the "
        "internal index configured, or set GLUTEN_JAR_PATH to a bundle JAR you "
        "already have."
    )
