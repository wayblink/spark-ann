"""Functional (P1) spark-ann API.

Thin Py4J wrappers over ``com.wayblink.ann.spark.api.ANNIndexAPI``. No
business logic in Python — the JVM side already provides everything; we
just translate Python types to JVM types and DataFrame handles back to
PySpark.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from pyspark.sql import DataFrame, SparkSession

from ._bridge import active_spark, dict_to_config, jvm, to_jvm_float_array


def build_ann_index(
    df: DataFrame,
    vector_column: str,
    output_path: str,
    config: Optional[Dict[str, Any]] = None,
) -> Any:
    """Build an ANN index from a DataFrame.

    Parameters mirror ``ANNIndexAPI.buildIndex`` on the Scala side.

    Returns the JVM ``ANNIndexMetadata`` object. Useful fields can be
    accessed via attribute syntax (e.g. ``meta.totalVectors()``,
    ``meta.dimension()``).
    """
    api = jvm().com.wayblink.ann.spark.api.ANNIndexAPI
    cfg = dict_to_config(config)
    return api.buildIndex(df._jdf, vector_column, output_path, cfg)


def ann_search(
    spark: SparkSession,
    index_path: str,
    query_vector: Iterable[float],
    k: int,
    nprobe: int = 3,
    ef: int = 50,
) -> DataFrame:
    """Search a built ANN index for the nearest neighbours of a single query.

    Returns a DataFrame with columns ``(id, distance, indexId)``.
    """
    api = jvm().com.wayblink.ann.spark.api.ANNIndexAPI
    jvec = to_jvm_float_array(query_vector)
    jdf = api.search(spark._jsparkSession, index_path, jvec, k, nprobe, ef)
    return DataFrame(jdf, spark)


def ann_batch_search(
    spark: SparkSession,
    index_path: str,
    queries: DataFrame,
    query_vector_column: str,
    k: int,
    nprobe: int = 3,
    ef: int = 50,
) -> DataFrame:
    """Batch search for nearest neighbours of multiple query vectors.

    Runs distributed via the JVM ``batchSearch`` (mapPartitions + executor
    cache). Returns a DataFrame with columns
    ``(queryIndex, id, distance, indexId)``.
    """
    api = jvm().com.wayblink.ann.spark.api.ANNIndexAPI
    jdf = api.batchSearch(
        spark._jsparkSession,
        index_path,
        queries._jdf,
        query_vector_column,
        k,
        nprobe,
        ef,
    )
    return DataFrame(jdf, spark)


def load_searcher(spark: SparkSession, index_path: str) -> Any:
    """Load a JVM ``ANNSearcher`` (metadata-only; HNSW indexes are lazy).

    Returns the JVM object directly so callers can stash it and reuse it
    across multiple queries without paying the metadata load on each
    search.
    """
    return jvm().com.wayblink.ann.spark.api.ANNIndexAPI.loadSearcher(
        spark._jsparkSession, index_path
    )


def discover_data_files(
    spark: SparkSession, data_path: str, vector_column: str
) -> Any:
    """List parquet files in ``data_path`` with vector counts, JVM array."""
    return jvm().com.wayblink.ann.spark.api.ANNIndexAPI.discoverDataFiles(
        spark._jsparkSession, data_path, vector_column
    )
