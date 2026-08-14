"""Java detection and the JVM flags Spark and Gluten need on modern JDKs.

pip cannot install a JDK, so Java is the one prerequisite this package cannot
satisfy on the user's behalf. The best we can do is fail with a sentence a data
engineer can act on instead of a JVM stack trace.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional

# Gluten supports JDK 8 and 17. Anything else may work but is untested upstream
# and is not something we want to discover in production.
SUPPORTED_MAJORS = (8, 17)


class JavaNotFoundError(RuntimeError):
    """Raised when no usable JVM can be located."""


def java_executable() -> Optional[Path]:
    """Locate the ``java`` binary, preferring JAVA_HOME over PATH."""
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / "java"
        if candidate.is_file():
            return candidate
    found = shutil.which("java")
    return Path(found) if found else None


def java_major_version(executable: Optional[Path] = None) -> Optional[int]:
    """Return the JVM major version (8, 11, 17, ...) or None if undeterminable."""
    exe = executable or java_executable()
    if exe is None:
        return None
    try:
        # `java -version` writes to stderr on every JDK anyone still runs.
        proc = subprocess.run(
            [str(exe), "-version"],
            capture_output=True,
            text=True,
            timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    output = (proc.stderr or "") + (proc.stdout or "")
    match = re.search(r'version "(\d+)(?:\.(\d+))?', output)
    if not match:
        return None
    major = int(match.group(1))
    # Pre-9 JDKs report as 1.8.0_xxx; the real major is the second component.
    if major == 1 and match.group(2):
        return int(match.group(2))
    return major


def check(strict: bool = False) -> Optional[int]:
    """Verify a usable JVM is present.

    Returns the detected major version. Raises when Java is missing entirely;
    an unsupported-but-present version warns unless ``strict`` is set, because
    a wrong-version JVM usually still starts and we would rather the user see
    their own workload fail than be blocked by our opinion.
    """
    import warnings

    exe = java_executable()
    if exe is None:
        raise JavaNotFoundError(
            "velox_spark: no Java runtime found. Spark runs on the JVM and pip "
            "cannot install one.\n"
            "  Ubuntu/Debian: sudo apt-get install -y openjdk-17-jdk-headless\n"
            "  RHEL/Fedora:   sudo dnf install -y java-17-openjdk-devel\n"
            "Then either put java on PATH or set JAVA_HOME."
        )

    major = java_major_version(exe)
    if major is None:
        warnings.warn(
            f"velox_spark: could not determine the Java version of {exe}. "
            f"Gluten supports JDK {' and '.join(map(str, SUPPORTED_MAJORS))}.",
            RuntimeWarning,
            stacklevel=2,
        )
        return None

    if major not in SUPPORTED_MAJORS:
        message = (
            f"velox_spark: Java {major} detected at {exe}. Gluten supports JDK "
            f"{' and '.join(map(str, SUPPORTED_MAJORS))}; other versions are "
            "untested and may fail at native library load."
        )
        if strict:
            raise JavaNotFoundError(message)
        warnings.warn(message, RuntimeWarning, stacklevel=2)

    return major


# Spark 3.x needs the module system prised open on JDK 9+. spark-submit adds
# these itself, but a SparkSession built from a plain Python process does not
# always inherit them -- and this package *is* the session builder, so we set
# them here rather than leave it to chance.
#
# The final two entries are Gluten-specific: Arrow and the Velox JNI layer both
# reach for direct ByteBuffers through reflection.
_ADD_OPENS = (
    "java.base/java.lang",
    "java.base/java.lang.invoke",
    "java.base/java.lang.reflect",
    "java.base/java.io",
    "java.base/java.net",
    "java.base/java.nio",
    "java.base/java.util",
    "java.base/java.util.concurrent",
    "java.base/java.util.concurrent.atomic",
    "java.base/sun.nio.ch",
    "java.base/sun.nio.cs",
    "java.base/sun.security.action",
    "java.base/sun.util.calendar",
    "java.security.jgss/sun.security.krb5",
)


def jvm_options(major: Optional[int]) -> List[str]:
    """JVM flags to pass to both driver and executors for this JDK."""
    if major is not None and major < 9:
        return []
    opts = ["-XX:+IgnoreUnrecognizedVMOptions"]
    opts += [f"--add-opens={module}=ALL-UNNAMED" for module in _ADD_OPENS]
    opts += [
        "-Djdk.reflect.useDirectMethodHandle=false",
        # Arrow's allocator refuses to use direct memory without this.
        "-Dio.netty.tryReflectionSetAccessible=true",
    ]
    return opts
