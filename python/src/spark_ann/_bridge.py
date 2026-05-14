"""Internal Py4J bridge utilities for spark-ann.

This module owns all the JVM gateway plumbing so the user-facing ``api``
and ``_dataframe`` modules can stay focused on signatures and DataFrame
returns. No business logic here — just type marshalling.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from pyspark.sql import SparkSession


def active_spark() -> SparkSession:
    """Return the active SparkSession or raise with a clear message.

    Auto-creating a SparkSession is intentionally avoided. The JAR must
    already be on the classpath (via ``--jars`` or ``--packages``) before
    any spark-ann call, and that requires the user to drive
    SparkSession construction.
    """
    session = SparkSession.getActiveSession()
    if session is None:
        raise RuntimeError(
            "No active SparkSession. Start one with the spark-ann JAR on the "
            "classpath, e.g.:\n"
            "  pyspark --jars /path/to/spark-ann-integration-assembly.jar\n"
            "or:\n"
            "  pyspark --packages com.wayblink:spark-ann-integration_2.12:0.1.0"
        )
    return session


def jvm() -> Any:
    """Return the JVM view of the active SparkSession.

    Each call validates that the spark-ann classes are reachable, so a
    missing JAR fails fast with a useful error rather than an opaque
    Py4JJavaError deep inside a call chain.
    """
    spark = active_spark()
    _jvm = spark.sparkContext._jvm
    if not hasattr(_jvm, "com") or not hasattr(_jvm.com, "company"):
        raise RuntimeError(
            "spark-ann JAR not found on the JVM classpath. Pass --jars or "
            "--packages when starting Spark — see the pyspark-ann README."
        )
    return _jvm


def to_jvm_float_array(values) -> Any:
    """Materialize a Python iterable of floats as a Java ``float[]``.

    Py4J cannot auto-convert Python lists to primitive arrays, so we go
    through ``gateway.new_array`` which is the documented bridge.
    """
    spark = active_spark()
    gateway = spark.sparkContext._gateway
    floats = [float(v) for v in values]
    jarr = gateway.new_array(gateway.jvm.float, len(floats))
    for i, v in enumerate(floats):
        jarr[i] = v
    return jarr


# Field order MUST match
# com.wayblink.ann.bundle.ANNIndexConfig case-class apply(...):
#   (M, efConstruction, groupingStrategy, targetVectorsPerIndex,
#    boundaryNodesPerIndex, distanceType, pk, algorithm)
# A change there without updating this list silently drops fields.
_CONFIG_FIELD_ORDER = (
    "M",
    "ef_construction",
    "grouping_strategy",
    "target_vectors_per_index",
    "boundary_nodes_per_index",
    "distance_type",
    "pk",
    "algorithm",
)

_DEFAULT_CONFIG: Dict[str, Any] = {
    "M": 16,
    "ef_construction": 200,
    "grouping_strategy": "SingleFile",
    "target_vectors_per_index": 500000,
    "boundary_nodes_per_index": 50,
    "distance_type": "euclidean",
    "pk": None,
    "algorithm": "hnsw",
}


def dict_to_config(config: Optional[Dict[str, Any]]) -> Any:
    """Convert a Python dict (or None) to a JVM ``ANNIndexConfig`` instance.

    Camel-case keys (``efConstruction``) are accepted as aliases for the
    snake-case keys for callers who lift values straight off the Scala
    config without rewriting them.
    """
    merged = dict(_DEFAULT_CONFIG)
    if config:
        for key, value in config.items():
            merged[_normalize_key(key)] = value

    j = jvm()
    grouping_name = merged["grouping_strategy"]
    bundle_pkg = j.com.wayblink.ann.bundle
    if grouping_name == "SingleFile":
        # Scala `case object` compiles to `<FQN>$.MODULE$` on the JVM.
        # Neither identifier is reachable via dotted attribute access in
        # Python because `$` is invalid in identifiers; getattr bypasses
        # that restriction at both levels.
        grouping = getattr(getattr(bundle_pkg, "SingleFile$"), "MODULE$")
    elif grouping_name == "MergeSmall":
        grouping = getattr(getattr(bundle_pkg, "MergeSmall$"), "MODULE$")
    else:
        raise ValueError(
            f"Unknown grouping_strategy: {grouping_name!r}. "
            "Use 'SingleFile' or 'MergeSmall'."
        )

    pk_jopt = _to_jvm_option(merged["pk"])
    algorithm_jvm = _algorithm_jvm(merged["algorithm"])

    return j.com.wayblink.ann.bundle.ANNIndexConfig.apply(
        int(merged["M"]),
        int(merged["ef_construction"]),
        grouping,
        int(merged["target_vectors_per_index"]),
        int(merged["boundary_nodes_per_index"]),
        str(merged["distance_type"]),
        pk_jopt,
        algorithm_jvm,
    )


def _algorithm_jvm(name: Any) -> Any:
    """Resolve an IndexAlgorithm string to its JVM case-object MODULE$.

    Currently the only legal value is "hnsw". Adding a future algorithm
    requires both a Scala case object in IndexAlgorithm.scala AND a
    branch here.
    """
    if name in (None, "", "hnsw", "HNSW"):
        return getattr(getattr(jvm().com.wayblink.ann.bundle.IndexAlgorithm, "HNSW$"), "MODULE$")
    raise ValueError(
        f"Unknown algorithm: {name!r}. Supported: 'hnsw'."
    )


def _to_jvm_option(value: Any) -> Any:
    """Wrap a Python value (or None) into a JVM scala.Option."""
    option_cls = jvm().scala.Option
    if value is None:
        return option_cls.empty()
    return option_cls.apply(str(value))


def _normalize_key(key: str) -> str:
    """Accept either snake_case or camelCase config keys."""
    camel_to_snake = {
        "efConstruction": "ef_construction",
        "groupingStrategy": "grouping_strategy",
        "targetVectorsPerIndex": "target_vectors_per_index",
        "boundaryNodesPerIndex": "boundary_nodes_per_index",
        "distanceType": "distance_type",
        # `pk` has no camelCase variant — it's already a single token.
    }
    return camel_to_snake.get(key, key)
