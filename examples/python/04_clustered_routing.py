"""04: Verify routing accuracy across multiple local indexes.

This exercises the global routing path that powers nprobe-based search.
Three linearly-separable clusters are written as separate parquet files
under one data directory, producing three local HNSW indexes plus one
small global routing index. Queries near a known cluster centroid should
route to that cluster's index.

Run:
    export PYSPARK_PYTHON=$(which python)
    export PYSPARK_SUBMIT_ARGS="--jars /abs/path/to/spark-ann-integration-assembly.jar pyspark-shell"
    python examples/python/04_clustered_routing.py
"""
from __future__ import annotations

import random
import shutil
import tempfile

from pyspark.sql import SparkSession

import spark_ann


def _cluster_vectors(n_per_cluster: int, dim: int, n_clusters: int, seed: int):
    rng = random.Random(seed)
    centers = [[rng.random() for _ in range(dim)] for _ in range(n_clusters)]
    rows = []
    rid = 0
    for c_idx, center in enumerate(centers):
        for _ in range(n_per_cluster):
            v = [c + rng.gauss(0.0, 0.02) for c in center]
            rows.append((rid, c_idx, v))
            rid += 1
    return centers, rows


def main() -> None:
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("spark-ann-example-04")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    workdir = tempfile.mkdtemp(prefix="spark-ann-ex04-")
    try:
        dim = 32
        per_cluster = 300
        n_clusters = 3
        centers, rows = _cluster_vectors(per_cluster, dim, n_clusters, seed=42)

        # Write each cluster to a separate parquet file so SingleFile
        # grouping yields one local index per cluster.
        data_root = f"{workdir}/data"
        for c_idx in range(n_clusters):
            cluster_rows = [(r[0], r[2]) for r in rows if r[1] == c_idx]
            (
                spark.createDataFrame(cluster_rows, ["id", "vector"])
                .coalesce(1)
                .write.mode("overwrite")
                .parquet(f"{data_root}/cluster_{c_idx}")
            )

        # Build the index from the file groups (one per cluster).
        from pyspark.sql.functions import col  # noqa: F401  (placeholder if needed)

        index_path = f"{workdir}/idx"
        # Use the directory-scan path via discover + group on the JVM
        # side; expose via the functional ANNIndexAPI bridge.
        jvm = spark.sparkContext._jvm
        api = jvm.com.wayblink.ann.spark.api.ANNIndexAPI
        files = api.discoverDataFiles(spark._jsparkSession, data_root, "vector")
        single_file = getattr(getattr(jvm.com.wayblink.ann.spark.builder,
                                       "SingleFile$"), "MODULE$")
        groups = api.groupFiles(files, single_file, 500000)
        # ANNIndexConfig.apply takes (M, efConstruction, groupingStrategy,
        # targetVectorsPerIndex, boundaryNodesPerIndex, distanceType,
        # pk: Option[String]). We pass scala.Option.empty() because
        # this example uses the sequential-id mode.
        none_opt = jvm.scala.Option.empty()
        cfg = jvm.com.wayblink.ann.spark.api.ANNIndexConfig.apply(
            16, 200, single_file, 500000, 40, "euclidean", none_opt
        )
        meta = api.buildIndexFromFileGroups(
            spark._jsparkSession, groups, "vector", index_path, cfg
        )
        # meta.localIndexes() returns a Scala Array; Py4J exposes its
        # length via the `length` field on the array proxy. The
        # statistics accessor is the cleaner read path.
        n_local = int(meta.statistics().numLocalIndexes())
        print(f"[example] built {n_local} local indexes")
        assert n_local == n_clusters

        # Discover the indexId → cluster mapping by inspecting which
        # file each local index points at. SingleFile grouping derives
        # the indexId from the parquet file name (auto-named by Spark),
        # so the cluster association lives in the directory portion of
        # the file path.
        local_metas = meta.localIndexes()
        index_id_to_cluster: dict[str, int] = {}
        for i in range(n_local):
            lm = local_metas[i]  # Scala Array via Py4J supports __getitem__
            idx_id = str(lm.indexId())
            file_path = str(lm.dataFiles()[0].filePath())
            for c_idx in range(n_clusters):
                if f"cluster_{c_idx}" in file_path:
                    index_id_to_cluster[idx_id] = c_idx
                    break
        print(f"[example] indexId → cluster: {index_id_to_cluster}")

        # For each cluster, fire 10 queries near its centroid with
        # nprobe=1 so routing accuracy is the only thing being tested.
        trials_per_cluster = 10
        rng = random.Random(99)
        hits_by_cluster = [0] * n_clusters
        for c_idx, center in enumerate(centers):
            for _ in range(trials_per_cluster):
                q = [c + rng.gauss(0.0, 0.005) for c in center]
                results = spark_ann.ann_search(
                    spark, index_path, q, k=5, nprobe=1, ef=150
                ).collect()
                if not results:
                    continue
                hit_clusters = {
                    index_id_to_cluster.get(r.indexId, -1) for r in results
                }
                if c_idx in hit_clusters:
                    hits_by_cluster[c_idx] += 1
        total = n_clusters * trials_per_cluster
        hits = sum(hits_by_cluster)
        accuracy = hits / total
        print(f"[example] routing accuracy = {accuracy:.2%}  ({hits}/{total})")
        for c_idx, h in enumerate(hits_by_cluster):
            print(f"           cluster_{c_idx}: {h}/{trials_per_cluster}")
        assert accuracy >= 0.85, f"routing accuracy too low: {accuracy}"
        print("[example] OK")
    finally:
        spark.stop()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    main()
