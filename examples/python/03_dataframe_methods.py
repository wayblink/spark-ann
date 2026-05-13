"""03: DataFrame method injection (P2).

Demonstrates both the flat (`df.build_ann_index`, `df.ann_search`,
`df.ann_batch_search`) and the accessor-namespace
(`df.ann.build_index`, `df.ann.search`, `df.ann.batch_search`) styles.
The two should produce identical results because they share the same
JVM call path.

Run:
    export PYSPARK_PYTHON=$(which python)
    export PYSPARK_SUBMIT_ARGS="--jars /abs/path/to/spark-ann-integration-assembly.jar pyspark-shell"
    python examples/python/03_dataframe_methods.py
"""
from __future__ import annotations

import random
import shutil
import tempfile

from pyspark.sql import SparkSession

# Importing spark_ann installs both flat and accessor methods on
# pyspark.sql.DataFrame as a side effect.
import spark_ann  # noqa: F401


def main() -> None:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("spark-ann-example-03")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    workdir = tempfile.mkdtemp(prefix="spark-ann-ex03-")
    try:
        dim = 32
        rng = random.Random(7)
        rows = [(i, [rng.random() for _ in range(dim)]) for i in range(500)]
        df = spark.createDataFrame(rows, ["id", "vector"])

        # Flat style:
        flat_idx = f"{workdir}/idx-flat"
        df.build_ann_index("vector", flat_idx, {"M": 16, "ef_construction": 100})
        flat_hits = df.ann_search(flat_idx, rows[0][1], k=3).collect()
        print("[example] flat df.ann_search top-3 for row 0:")
        for r in flat_hits:
            print(f"           id={r.id} distance={r.distance:.6f}")

        # Accessor style: same probe should give same ids/distances.
        acc_idx = f"{workdir}/idx-acc"
        df.ann.build_index("vector", acc_idx, {"M": 16, "ef_construction": 100})
        acc_hits = df.ann.search(acc_idx, rows[0][1], k=3).collect()

        flat_ids = sorted(r.id for r in flat_hits)
        acc_ids = sorted(r.id for r in acc_hits)
        assert flat_ids == acc_ids, f"flat vs accessor mismatch: {flat_ids} vs {acc_ids}"
        print("[example] flat and accessor styles return same ids ✓")

        # Mix them: build via accessor, batch-search via accessor on the
        # query DataFrame.
        queries = spark.createDataFrame(
            [(qi, rows[qi * 50][1]) for qi in range(5)],
            ["query_id", "vector"],
        )
        batch = queries.ann.batch_search(acc_idx, "vector", k=2)
        print("[example] accessor df.ann.batch_search rows:")
        for r in batch.collect():
            print(
                f"           queryIndex={r.queryIndex} id={r.id} "
                f"distance={r.distance:.6f} indexId={r.indexId}"
            )
        print("[example] OK")
    finally:
        spark.stop()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
