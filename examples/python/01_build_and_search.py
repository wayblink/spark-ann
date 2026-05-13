"""01: Minimal end-to-end smoke test.

Generates a synthetic vector DataFrame, builds an ANN index on disk,
runs a single-query search, and prints the top-k. Exits 0 on success.

Run:
    export PYSPARK_PYTHON=$(which python)
    export PYSPARK_SUBMIT_ARGS="--jars /abs/path/to/spark-ann-integration-assembly.jar pyspark-shell"
    python examples/python/01_build_and_search.py
"""
from __future__ import annotations

import random
import shutil
import tempfile

from pyspark.sql import SparkSession

import spark_ann


def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-ann-example-01")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    workdir = tempfile.mkdtemp(prefix="spark-ann-ex01-")
    try:
        dim = 64
        n = 1000
        rng = random.Random(42)
        rows = [(i, [rng.random() for _ in range(dim)]) for i in range(n)]
        df = spark.createDataFrame(rows, ["id", "vector"])

        index_path = f"{workdir}/idx"
        meta = spark_ann.build_ann_index(
            df,
            vector_column="vector",
            output_path=index_path,
            config={
                "M": 16,
                "ef_construction": 200,
                "distance_type": "euclidean",
            },
        )
        n_vec = int(meta.totalVectors())
        print(f"[example] built index with {n_vec} vectors, dim={int(meta.dimension())}")
        assert n_vec == n

        # Use one of the inserted vectors as the query — search should
        # return distance ~0 for the matching base vector. (The returned
        # `id` column is the HNSW internal id, not the user-supplied id
        # column — see Known limitation in examples/README.md.)
        probe_id, probe_vec = rows[123]
        results = spark_ann.ann_search(
            spark, index_path, probe_vec, k=5, ef=200
        ).collect()

        print("[example] top-5 for probe row 123:")
        for r in results:
            print(f"           id={r.id} distance={r.distance:.6f} indexId={r.indexId}")

        top_distance = results[0].distance
        assert top_distance < 1e-3, (
            f"top distance {top_distance} should be near 0 for self-probe"
        )
        print("[example] OK")
    finally:
        spark.stop()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
