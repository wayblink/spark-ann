"""Tests for the DataFrame method injection (P2) and accessor namespace."""

from __future__ import annotations

import os
import random


def _make_df(spark, n=200, dim=16, seed=11):
    rng = random.Random(seed)
    rows = [(i, [float(rng.random()) for _ in range(dim)]) for i in range(n)]
    return spark.createDataFrame(rows, ["id", "vector"])


def test_monkey_patched_methods_exist(spark):
    import spark_ann  # noqa: F401  side-effect: installs methods

    df = _make_df(spark, n=10)
    assert hasattr(df, "build_ann_index")
    assert hasattr(df, "ann_search")
    assert hasattr(df, "ann_batch_search")
    assert hasattr(df, "ann")


def test_flat_methods_match_functional_api(spark, tmp_index_dir):
    import spark_ann

    df = _make_df(spark)
    index_path = os.path.join(tmp_index_dir, "idx")

    df.build_ann_index("vector", index_path, {"M": 16, "ef_construction": 100})
    flat = df.ann_search(index_path, [0.5] * 16, k=3).collect()
    functional = spark_ann.ann_search(spark, index_path, [0.5] * 16, k=3).collect()

    flat_ids = sorted(r.id for r in flat)
    functional_ids = sorted(r.id for r in functional)
    assert flat_ids == functional_ids


def test_accessor_namespace(spark, tmp_index_dir):
    import spark_ann  # noqa: F401

    df = _make_df(spark)
    index_path = os.path.join(tmp_index_dir, "idx")

    df.ann.build_index("vector", index_path, {"M": 16, "ef_construction": 100})
    via_accessor = df.ann.search(index_path, [0.5] * 16, k=3).collect()
    via_flat = df.ann_search(index_path, [0.5] * 16, k=3).collect()

    assert sorted(r.id for r in via_accessor) == sorted(r.id for r in via_flat)


def test_accessor_batch_search(spark, tmp_index_dir):
    import spark_ann  # noqa: F401

    df = _make_df(spark, n=200)
    index_path = os.path.join(tmp_index_dir, "idx")
    df.ann.build_index("vector", index_path)

    queries = _make_df(spark, n=5, seed=99)
    result = queries.ann.batch_search(index_path, "vector", k=3)
    assert result.columns == ["queryIndex", "id", "distance", "indexId"]
    assert result.count() > 0
