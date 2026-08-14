"""Memory sizing.

Velox allocates its working set off-heap, so a Gluten session needs off-heap
memory configured or it will fail under any real workload. These are the two
settings users most often get wrong, so we compute defaults from the host
rather than shipping a number that is wrong on every machine but one.
"""

from __future__ import annotations

import math
import os
import re
from pathlib import Path
from typing import Optional

# Fractions of usable RAM. The remainder is deliberately left to the OS page
# cache, the Python process, and any other tenant on the box -- Spark's default
# posture of "take everything" is what gets containers OOM-killed.
OFFHEAP_FRACTION = 0.35
HEAP_FRACTION = 0.25

_FALLBACK_TOTAL = 8 * 1024**3  # Assume a modest laptop if detection fails.

_SIZE_PATTERN = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([kmgt]?)b?\s*$", re.IGNORECASE)
_UNITS = {"": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}


def parse_size(value) -> int:
    """Parse ``"8g"`` / ``"512m"`` / an int of bytes into bytes."""
    if isinstance(value, (int, float)):
        return int(value)
    match = _SIZE_PATTERN.match(str(value))
    if not match:
        raise ValueError(
            f"velox_spark: cannot parse memory size {value!r}. "
            "Use forms like '16g', '512m', or an integer number of bytes."
        )
    return int(float(match.group(1)) * _UNITS[match.group(2).lower()])


def format_size(num_bytes: int) -> str:
    """Render bytes as a Spark-style size string, rounded down to whole units."""
    if num_bytes >= 1024**3:
        return f"{num_bytes // 1024**3}g"
    if num_bytes >= 1024**2:
        return f"{num_bytes // 1024**2}m"
    return f"{max(1, num_bytes // 1024)}k"


def _cgroup_limit() -> Optional[int]:
    """Memory limit imposed by the container, if we are inside one.

    Without this, sizing from ``/proc/meminfo`` inside a container reads the
    *host's* RAM and hands Spark an off-heap budget several times larger than
    the cgroup allows. The result is a kernel OOM kill with no Spark-side error,
    which is a genuinely miserable thing to debug -- hence checking here.
    """
    candidates = (
        Path("/sys/fs/cgroup/memory.max"),  # cgroup v2
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),  # cgroup v1
    )
    for path in candidates:
        try:
            raw = path.read_text().strip()
        except OSError:
            continue
        if raw == "max":
            continue
        try:
            limit = int(raw)
        except ValueError:
            continue
        # cgroup v1 reports an absurd sentinel when unlimited.
        if 0 < limit < (1 << 62):
            return limit
    return None


def _physical_ram() -> Optional[int]:
    try:
        return os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        pass
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    except (OSError, IndexError, ValueError):
        pass
    return None


def usable_ram() -> int:
    """Memory this process may actually use, respecting container limits."""
    physical = _physical_ram()
    limit = _cgroup_limit()
    values = [v for v in (physical, limit) if v]
    return min(values) if values else _FALLBACK_TOTAL


def _round_down_gb(num_bytes: int) -> int:
    """Round to a whole GiB so the emitted config is readable in the Spark UI."""
    gb = math.floor(num_bytes / 1024**3)
    return max(1, gb) * 1024**3


def default_offheap() -> int:
    """Off-heap bytes for Velox on this host."""
    return _round_down_gb(usable_ram() * OFFHEAP_FRACTION)


def default_heap() -> int:
    """JVM heap bytes for the driver on this host."""
    return _round_down_gb(usable_ram() * HEAP_FRACTION)
