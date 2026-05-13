package com.wayblink.ann.spark.search

import com.wayblink.ann.core.testutil.TestDataGenerator
import com.wayblink.ann.spark.SharedSparkSession
import com.wayblink.ann.spark.api.{ANNIndexAPI, ANNIndexConfig}
import com.wayblink.ann.spark.builder.SingleFile
import com.wayblink.ann.spark.util.{DriverIndexCache, ExecutorIndexCache}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
 * Verifies that ANNSearcher.load no longer eagerly loads all HNSW indexes
 * into driver memory and that the on-demand DriverIndexCache only loads
 * indexes actually searched (≤ nprobe per single-query search).
 */
class LazyLoadTest extends AnyFunSuite with SharedSparkSession with Matchers {

  private val testBasePath = "/tmp/spark-ann-test/lazy-load"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val base = new File(testBasePath)
    deleteRecursively(base)
    base.mkdirs()
  }

  override def afterAll(): Unit = {
    deleteRecursively(new File(testBasePath))
    DriverIndexCache.clear()
    ExecutorIndexCache.clear()
    super.afterAll()
  }

  private def buildThreeClusterIndex(): String = {
    val dim = 16
    val perCluster = 200
    val clusters = TestDataGenerator.generateClusteredVectors(
      numClusters = 3, vectorsPerCluster = perCluster, dimension = dim,
      spread = 0.02f, seed = 5L
    )
    val localSpark = spark
    import localSpark.implicits._

    val dataPath = s"$testBasePath/data"
    new File(dataPath).mkdirs()
    val grouped: Seq[(Int, Array[(Long, Array[Float])])] =
      clusters.groupBy(t => (t._1 / perCluster).toInt).toSeq.sortBy(_._1)
    grouped.foreach { case (cid, vecs) =>
      vecs.toSeq.map { case (id, v) => (id, v.toSeq) }
        .toDF("id", "vector")
        .coalesce(1)
        .write.mode("overwrite").parquet(s"$dataPath/cluster_$cid")
    }

    val indexPath = s"$testBasePath/index"
    val cfg = ANNIndexConfig(
      M = 16, efConstruction = 100, groupingStrategy = SingleFile,
      boundaryNodesPerIndex = 25, distanceType = "euclidean"
    )
    val files = ANNIndexAPI.discoverDataFiles(spark, dataPath, "vector")
    val groups = ANNIndexAPI.groupFiles(files, SingleFile)
    ANNIndexAPI.buildIndexFromFileGroups(spark, groups, "vector", indexPath, cfg)
    indexPath
  }

  test("ANNSearcher.load does not eagerly load any HNSW index into driver") {
    DriverIndexCache.clear()
    val indexPath = buildThreeClusterIndex()

    DriverIndexCache.loadedCount shouldBe 0
    val searcher = ANNIndexAPI.loadSearcher(spark, indexPath)

    // Metadata-only path: no HNSW indexes should be cached yet, no global
    // index either.
    DriverIndexCache.loadedCount shouldBe 0
    DriverIndexCache.hasGlobal shouldBe false

    // Searcher still knows the index ids from metadata.
    searcher.listIndexIds.length shouldBe 3
    searcher.totalVectors shouldBe 600L
  }

  test("single-query search loads at most nprobe local indexes plus global") {
    DriverIndexCache.clear()
    val indexPath = buildThreeClusterIndex()
    val searcher = ANNIndexAPI.loadSearcher(spark, indexPath)

    val query = Array.fill(16)(0.5f)
    val result = searcher.search(query, k = 3, nprobe = 1, ef = 100).collect()
    result.length should be > 0

    // Global routing index was needed → loaded once.
    DriverIndexCache.hasGlobal shouldBe true
    // With nprobe=1, exactly 1 local index should be in the cache.
    DriverIndexCache.loadedCount shouldBe 1

    // A second search hitting the same cluster reuses the cache.
    searcher.search(query, k = 3, nprobe = 1, ef = 100).collect()
    DriverIndexCache.loadedCount shouldBe 1
  }

  test("LRU cap evicts least-recently-used local indexes") {
    DriverIndexCache.clear()
    val indexPath = buildThreeClusterIndex()
    val searcher = ANNIndexAPI.loadSearcher(spark, indexPath)

    DriverIndexCache.setMaxLoaded(2)
    try {
      // Force all three local indexes to be visited.
      searcher.search(Array.fill(16)(0.0f), k = 3, nprobe = 3, ef = 100).collect()
      // Cap of 2 → at most 2 resident.
      DriverIndexCache.loadedCount should be <= 2
    } finally {
      DriverIndexCache.setMaxLoaded(10)
    }
  }
}
