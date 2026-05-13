"""05: Preserve user id through search.

Demonstrates plan D option A: when ANNIndexConfig.idColumn is set to a
parquet INT32 / INT64 column, the search result's `id` is the user's
own id, not the HNSW internal sequential offset. Without idColumn the
result id is the parquet row order.

Run:
    export PYSPARK_PYTHON=$(which python)
    export PYSPARK_SUBMIT_ARGS="--jars /abs/path/to/spark-ann-integration-assembly.jar pyspark-shell"
    python examples/python/05_id_column_preservation.py
"""
from __future__ import annotations

import random
import shutil
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    ArrayType,
    FloatType,
    LongType,
    StructField,
    StructType,
)

import spark_ann


def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-ann-example-05")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    workdir = tempfile.mkdtemp(prefix="spark-ann-ex05-")
    try:
        dim = 32
        # User-style ids spread across a wide range — clearly not parquet
        # row offsets. The schema MUST declare LongType so the parquet
        # column lands as INT64 (otherwise Spark may emit INT32 which is
        # also accepted, but LongType is the canonical case).
        schema = StructType([
            StructField("id", LongType(), nullable=False),
            StructField("vector", ArrayType(FloatType()), nullable=False),
        ])
        rng = random.Random(42)
        rows = [
            (10_000_000 + i * 7, [rng.random() for _ in range(dim)])
            for i in range(500)
        ]
        df = spark.createDataFrame(rows, schema=schema)

        index_path = f"{workdir}/idx"
        meta = spark_ann.build_ann_index(
            df,
            vector_column="vector",
            output_path=index_path,
            config={
                "M": 16,
                "ef_construction": 100,
                "id_column": "id",
            },
        )
        print(f"[example] built index with {int(meta.totalVectors())} vectors")

        # Self-probe row 100 — its user id should come back as the top hit.
        probe_id, probe_vec = rows[100]
        results = spark_ann.ann_search(
            spark, index_path, probe_vec, k=3, ef=200
        ).collect()
        print(f"[example] probe user id was {probe_id}")
        for r in results:
            print(
                f"           id={r.id} distance={r.distance:.6f} indexId={r.indexId}"
            )
        assert results[0].id == probe_id, (
            f"expected user id {probe_id}, got {results[0].id} "
            f"(this means idColumn passthrough is broken)"
        )

        # Sanity: every returned id must be one we inserted, not a
        # parquet row offset like 0..499.
        valid_ids = {row[0] for row in rows}
        for r in results:
            assert r.id in valid_ids, (
                f"returned id {r.id} is not a user id — looks like the "
                f"sequential-offset fallback path was taken"
            )
        print("[example] OK")
    finally:
        spark.stop()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
