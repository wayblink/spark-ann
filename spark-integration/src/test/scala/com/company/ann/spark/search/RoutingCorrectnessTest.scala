package com.company.ann.spark.search

import com.company.ann.core.testutil.TestDataGenerator
import com.company.ann.spark.SharedSparkSession
import com.company.ann.spark.api.{ANNIndexAPI, ANNIndexConfig}
import com.company.ann.spark.builder.SingleFile
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

/**
 * Verifies that the routing index actually maps queries to the right local
 * indexes after the Phase 1 fix to findIndexIdForGlobalId. Pre-fix, every
 * routing hit collapsed to the first local index, so the test would either
 * fail or coincidentally pass via the all-indexes fallback only when
 * indexIds.isEmpty.
 */
class RoutingCorrectnessTest extends AnyFunSuite with SharedSparkSession {

  private val testBasePath = "/tmp/spark-ann-test/routing-correctness"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val base = new File(testBasePath)
    deleteRecursively(base)
    base.mkdirs()
  }

  override def afterAll(): Unit = {
    deleteRecursively(new File(testBasePath))
    super.afterAll()
  }

  test("queries near a cluster centroid route to the cluster's source index") {
    val dim = 32
    val perCluster = 400
    val clusters: Array[(Long, Array[Float])] = TestDataGenerator.generateClusteredVectors(
      numClusters = 3, vectorsPerCluster = perCluster, dimension = dim, spread = 0.02f, seed = 7L
    )

    // Each cluster's vectors get written to its own parquet file so the
    // SingleFile grouping produces three local indexes whose contents are
    // linearly separable in vector space.
    val localSpark = spark
    import localSpark.implicits._
    val dataPath = s"$testBasePath/data"
    new File(dataPath).mkdirs()

    val grouped: Seq[(Int, Array[(Long, Array[Float])])] =
      clusters.groupBy { tup => (tup._1 / perCluster).toInt }.toSeq.sortBy(_._1)
    grouped.foreach { case (clusterId, vecs) =>
      vecs.toSeq.map { case (id, v) => (id, v.toSeq) }
        .toDF("id", "vector")
        .coalesce(1)
        .write.mode("overwrite").parquet(s"$dataPath/cluster_${clusterId}")
    }

    val indexPath = s"$testBasePath/index"
    val cfg = ANNIndexConfig(
      M = 16, efConstruction = 200, groupingStrategy = SingleFile,
      boundaryNodesPerIndex = 40, distanceType = "euclidean"
    )

    // Build via the file-based path so each cluster lands in its own
    // local index — that is exactly what gives us a routing-meaningful
    // boundary map.
    val files = ANNIndexAPI.discoverDataFiles(spark, dataPath, "vector")
    val groups = ANNIndexAPI.groupFiles(files, SingleFile)
    val metadata = ANNIndexAPI.buildIndexFromFileGroups(spark, groups, "vector", indexPath, cfg)

    assert(metadata.localIndexes.length == 3,
      s"Expected 3 local indexes (one per cluster), got ${metadata.localIndexes.length}")
    assert(metadata.globalIndexPath.isDefined, "Global routing index should have been built")

    val searcher = ANNIndexAPI.loadSearcher(spark, indexPath)

    // Map each cluster to the indexId that holds it. The builder uses
    // the source parquet directory name (idx_cluster_N) for SingleFile,
    // so we infer the mapping by inspecting the dataFiles paths.
    val clusterToIndexId: Map[Int, String] = metadata.localIndexes.flatMap { lm =>
      lm.dataFiles.headOption.flatMap { f =>
        val name = new java.io.File(f.filePath).getParentFile.getName
        if (name.startsWith("cluster_")) Some(name.stripPrefix("cluster_").toInt -> lm.indexId)
        else None
      }
    }.toMap

    assert(clusterToIndexId.size == 3, s"Could not infer cluster→indexId map: $clusterToIndexId")

    // For each cluster, pick a representative query that is a noisy copy
    // of one of its members. nprobe=1 forces the test to rely entirely on
    // routing correctness — fallbacks won't mask a bug.
    var hits = 0
    val trials = 30
    val rng = new scala.util.Random(101L)
    (0 until trials).foreach { i =>
      val clusterId = i % 3
      val source = clusters((clusterId * perCluster) + rng.nextInt(perCluster))._2
      val query = source.map(v => v + (rng.nextGaussian().toFloat * 0.005f))

      val results = searcher.search(query, k = 5, nprobe = 1, ef = 100).collect()
      val expectedIndexId = clusterToIndexId(clusterId)
      val hit = results.exists(_.getString(2) == expectedIndexId)
      if (hit) hits += 1
    }

    val accuracy = hits.toDouble / trials
    assert(accuracy >= 0.85,
      s"Routing accuracy $accuracy is below the 0.85 threshold. Pre-fix code " +
        s"would collapse every routing hit to the first local index, producing accuracy ~= 1/3.")
  }
}
