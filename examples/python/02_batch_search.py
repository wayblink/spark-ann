"""02: Distributed batch search.

Builds a base index, then runs a DataFrame of query vectors through the
distributed batchSearch path (mapPartitions + ExecutorIndexCache on
JVM). Prints results grouped by query.

Run:
    export PYSPARK_PYTHON=$(which python)
    export PYSPARK_SUBMIT_ARGS="--jars /abs/path/to/spark-ann-integration-assembly.jar pyspark-shell"
    python examples/python/02_batch_search.py
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
        .appName("spark-ann-example-02")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    workdir = tempfile.mkdtemp(prefix="spark-ann-ex02-")
    try:
        dim = 32
        n_base = 2000
        rng = random.Random(42)
        base_rows = [
            (i, [rng.random() for _ in range(dim)]) for i in range(n_base)
        ]
        base_df = spark.createDataFrame(base_rows, ["id", "vector"])

        index_path = f"{workdir}/idx"
        spark_ann.build_ann_index(base_df, "vector", index_path)

        # Build a query DataFrame; pick query 0 = base row 0, query 1 =
        # base row 100, etc. so we can verify the top hit per query.
        probe_indices = [0, 100, 250, 999, 1500]
        query_rows = [(qi, base_rows[probe_id][1]) for qi, probe_id in enumerate(probe_indices)]
        queries_df = spark.createDataFrame(query_rows, ["query_id", "vector"])

        results_df = spark_ann.ann_batch_search(
            spark,
            index_path,
            queries_df,
            query_vector_column="vector",
            k=3,
            ef=200,
        )
        print("[example] batchSearch schema:", results_df.columns)
        rows = results_df.collect()

        # Group hits by queryIndex. The id column returned is the HNSW
        # internal id (assigned at build time by parquet row order), not
        # the user-supplied id column, so we assert on distance instead.
        # When the query vector matches one of the base vectors exactly,
        # the nearest distance must be ~0.
        by_q: dict[int, list] = {}
        for r in rows:
            by_q.setdefault(r.queryIndex, []).append(r)
        for qi in sorted(by_q):
            top = sorted(by_q[qi], key=lambda x: x.distance)[0]
            print(
                f"           query {qi} top hit: id={top.id} "
                f"distance={top.distance:.6f}"
            )
            assert top.distance < 1e-3, (
                f"query {qi}: top distance {top.distance} should be near 0"
            )
        print("[example] OK")
    finally:
        spark.stop()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
