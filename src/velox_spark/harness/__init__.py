"""The validation harness.

Run the same query with Gluten on and off, then check three things:

1. **Correctness** -- do the result sets match? Velox has known
   Spark-semantics mismatches on some functions, so this is not a formality.
2. **Fallback** -- did anything actually run in Velox, or did the plan fall
   back to the JVM while still paying columnar/row conversion costs?
3. **Wall clock** -- is it faster? If not, you have a compatibility surface
   with no upside.

All three must pass. Ship this alongside the package so receiving teams can
check their own workload instead of trusting someone else's benchmark.
"""

from .validate import ValidationResult, validate, validate_isolated

__all__ = ["validate", "validate_isolated", "ValidationResult"]
