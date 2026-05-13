"""Smoke tests for the functional spark-ann API.

The Scala side already has exhaustive coverage; here we only verify that
the Py4J bridge correctly translates Python types to JVM types and that
DataFrame round-trips look right.
"""

from __future__ import annotations

import os
import random


def _make_df(spark, n=300, dim=16, seed=11):
    rng = random.Random(seed)
    rows = [(i, [float(rng.random()) for _ in range(dim)]) for i in range(n)]
    return spark.createDataFrame(rows, ["id", "vector"])


def test_build_and_search_returns_dataframe(spark, tmp_index_dir):
    import spark_ann

    df = _make_df(spark)
    index_path = os.path.join(tmp_index_dir, "idx")
    meta = spark_ann.build_ann_index(
        df,
        vector_column="vector",
        output_path=index_path,
        config={"M": 16, "ef_construction": 100, "distance_type": "euclidean"},
    )
    # The JVM ANNIndexMetadata exposes accessors via Py4J.
    assert int(meta.dimension()) == 16
    assert int(meta.totalVectors()) == 300

    query = [0.5] * 16
    result = spark_ann.ann_search(spark, index_path, query, k=5)
    cols = result.columns
    assert cols == ["id", "distance", "indexId"]
    rows = result.collect()
    assert 1 <= len(rows) <= 5


def test_batch_search_returns_query_indexed_dataframe(spark, tmp_index_dir):
    import spark_ann

    df = _make_df(spark, n=200)
    index_path = os.path.join(tmp_index_dir, "idx")
    spark_ann.build_ann_index(df, "vector", index_path)

    queries = spark.createDataFrame(
        [(0, [0.1] * 16), (1, [0.9] * 16)],
        ["qid", "vector"],
    )
    result = spark_ann.ann_batch_search(spark, index_path, queries, "vector", k=3)
    assert result.columns == ["queryIndex", "id", "distance", "indexId"]
    by_query = {row.queryIndex for row in result.collect()}
    assert by_query == {0, 1}


def test_camelcase_config_keys_are_accepted(spark, tmp_index_dir):
    import spark_ann

    df = _make_df(spark, n=100)
    index_path = os.path.join(tmp_index_dir, "idx")
    meta = spark_ann.build_ann_index(
        df,
        "vector",
        index_path,
        # mixing both styles ensures the normaliser path is exercised
        config={"M": 12, "efConstruction": 120, "distanceType": "cosine"},
    )
    assert int(meta.dimension()) == 16
    # Spot-check config round-trips through to the JVM ANNIndexConfig.
    cfg = meta.config()
    assert int(cfg.M()) == 12
    assert int(cfg.efConstruction()) == 120
    assert str(cfg.distanceType()) == "cosine"
