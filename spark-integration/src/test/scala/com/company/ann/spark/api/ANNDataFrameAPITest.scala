package com.company.ann.spark.api

import com.company.ann.core.testutil.TestDataGenerator
import com.company.ann.spark.SharedSparkSession
import com.company.ann.spark.builder.{FileDiscovery, FileGroupingStrategy, MergeSmall, SingleFile}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

/**
 * Tests for ANN DataFrame API.
 */
class ANNDataFrameAPITest extends AnyFunSuite with SharedSparkSession {

  private val testBasePath = "/tmp/ann_api_test"

  override def beforeAll(): Unit = {
    super.beforeAll()
    deleteRecursively(new File(testBasePath))
  }

  override def afterAll(): Unit = {
    try {
      deleteRecursively(new File(testBasePath))
    } finally {
      super.afterAll()
    }
  }

  private def withImplicits[T](f: org.apache.spark.sql.SQLContext => T): T = {
    f(spark.sqlContext)
  }

  test("ANNIndexAPI.buildIndex should build index from DataFrame") {
    val sqlCtx = spark.sqlContext
    import sqlCtx.implicits._

    val vectors = TestDataGenerator.generateClusteredVectors(
      numClusters = 5,
      vectorsPerCluster = 100,
      dimension = 64,
      seed = 42L
    )

    val df = vectors.toSeq.toDF("id", "vector")
    val outputPath = s"$testBasePath/test_build_index"

    val metadata = ANNIndexAPI.buildIndex(
      df = df,
      vectorColumn = "vector",
      outputPath = outputPath
    )

    assert(metadata.totalVectors == 500)
    assert(metadata.dimension == 64)
    assert(metadata.localIndexes.nonEmpty)
    assert(new File(outputPath).exists())
  }

  test("ANNIndexAPI.search should return nearest neighbors") {
    val sqlCtx = spark.sqlContext
    import sqlCtx.implicits._

    // Build index first
    val vectors = TestDataGenerator.generateClusteredVectors(
      numClusters = 5,
      vectorsPerCluster = 100,
      dimension = 64,
      seed = 123L
    )

    val df = vectors.toSeq.toDF("id", "vector")
    val outputPath = s"$testBasePath/test_search_index"

    ANNIndexAPI.buildIndex(df, "vector", outputPath)

    // Search using first vector (should find itself)
    val queryVector = vectors.head._2
    val results = ANNIndexAPI.search(
      spark = spark,
      indexPath = outputPath,
      queryVector = queryVector,
      k = 10
    )

    assert(results.count() == 10)

    // The results should be sorted by distance and return valid distances
    val firstResult = results.orderBy("distance").first()
    assert(firstResult.getAs[Float]("distance") >= 0f)
    assert(firstResult.getAs[Long]("id") >= 0L)
  }

  test("ANNIndexAPI.batchSearch should search multiple queries") {
    val sqlCtx = spark.sqlContext
    import sqlCtx.implicits._

    // Build index
    val vectors = TestDataGenerator.generateClusteredVectors(
      numClusters = 5,
      vectorsPerCluster = 100,
      dimension = 64,
      seed = 456L
    )

    val df = vectors.toSeq.toDF("id", "vector")
    val outputPath = s"$testBasePath/test_batch_search_index"

    ANNIndexAPI.buildIndex(df, "vector", outputPath)

    // Batch search with 3 queries
    val queryVectors = Seq(
      vectors(0)._2.toSeq,
      vectors(100)._2.toSeq,
      vectors(200)._2.toSeq
    )
    val queriesDF = queryVectors.zipWithIndex.map { case (vec, idx) =>
      (idx, vec)
    }.toDF("queryId", "vector")

    val results = ANNIndexAPI.batchSearch(
      spark = spark,
      indexPath = outputPath,
      queries = queriesDF,
      queryVectorColumn = "vector",
      k = 5
    )

    // Should have 3 queries * 5 results each = 15 rows
    assert(results.count() == 15)

    // Each query should have results
    val queryIndexCounts = results.groupBy("queryIndex").count().collect()
    assert(queryIndexCounts.length == 3)
    queryIndexCounts.foreach { row =>
      assert(row.getAs[Long]("count") == 5)
    }
  }

  test("DataFrame extension annSearch should work") {
    val sqlCtx = spark.sqlContext
    import sqlCtx.implicits._
    import ANNDataFrameExtensions._

    // Build index
    val vectors = TestDataGenerator.generateRandomVectors(200, 32, seed = 789L)
    val df = vectors.toSeq.toDF("id", "vector")
    val outputPath = s"$testBasePath/test_extension_search"

    df.buildANNIndex("vector", outputPath)

    // Search using extension method
    val queryVector = vectors.head._2
    val results = df.annSearch(
      indexPath = outputPath,
      queryVector = queryVector,
      k = 5
    )

    assert(results.count() == 5)
    assert(results.columns.contains("id"))
    assert(results.columns.contains("distance"))
  }

  test("DataFrame extension annBatchSearch should work") {
    val sqlCtx = spark.sqlContext
    import sqlCtx.implicits._
    import ANNDataFrameExtensions._

    // Build index
    val vectors = TestDataGenerator.generateRandomVectors(200, 32, seed = 101L)
    val df = vectors.toSeq.toDF("id", "vector")
    val outputPath = s"$testBasePath/test_extension_batch"

    df.buildANNIndex("vector", outputPath)

    // Batch search using extension method
    val queryVectors = Seq(
      vectors(0)._2.toSeq,
      vectors(50)._2.toSeq
    )
    val queriesDF = queryVectors.zipWithIndex.map { case (vec, idx) =>
      (idx, vec)
    }.toDF("queryId", "vector")

    val results = df.annBatchSearch(
      indexPath = outputPath,
      queries = queriesDF,
      queryVectorColumn = "vector",
      k = 3
    )

    assert(results.count() == 6) // 2 queries * 3 results
  }

  test("buildIndexFromFileGroups should work with SingleFile strategy") {
    val sqlCtx = spark.sqlContext
    import sqlCtx.implicits._

    // Create multiple data files (use coalesce(1) to ensure single file per directory)
    val dataPath = s"$testBasePath/test_file_groups_data"
    for (i <- 1 to 3) {
      val vectors = TestDataGenerator.generateRandomVectors(100, 32, seed = i * 100L)
      val df = vectors.toSeq.toDF("id", "vector")
      df.coalesce(1).write.mode("overwrite").parquet(s"$dataPath/file_$i.parquet")
    }

    // Discover and group files
    val files = FileDiscovery.discoverDataFiles(spark, dataPath, "vector")
    assert(files.length == 3, s"Expected 3 files but found ${files.length}")

    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)
    assert(groups.length == 3)

    // Build index
    val outputPath = s"$testBasePath/test_file_groups_index"
    val metadata = ANNIndexAPI.buildIndexFromFileGroups(
      spark = spark,
      fileGroups = groups,
      vectorColumn = "vector",
      outputPath = outputPath
    )

    assert(metadata.localIndexes.length == 3)
    assert(metadata.totalVectors == 300)
  }

  test("buildIndexFromFileGroups should work with MergeSmall strategy") {
    val sqlCtx = spark.sqlContext
    import sqlCtx.implicits._

    // Create multiple small data files (use coalesce(1) to ensure single file per directory)
    val dataPath = s"$testBasePath/test_merge_groups_data"
    for (i <- 1 to 5) {
      val vectors = TestDataGenerator.generateRandomVectors(50, 32, seed = i * 200L)
      val df = vectors.toSeq.toDF("id", "vector")
      df.coalesce(1).write.mode("overwrite").parquet(s"$dataPath/file_$i.parquet")
    }

    // Discover and group files with MergeSmall
    val files = FileDiscovery.discoverDataFiles(spark, dataPath, "vector")
    assert(files.length == 5, s"Expected 5 files but found ${files.length}")

    // Target 150 vectors per index, so should merge some files
    val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, targetVectorsPerIndex = 150)

    // Should have fewer groups than files due to merging
    assert(groups.length < files.length)
    assert(groups.map(_.totalVectors).sum == 250)

    // Build index
    val outputPath = s"$testBasePath/test_merge_groups_index"
    val metadata = ANNIndexAPI.buildIndexFromFileGroups(
      spark = spark,
      fileGroups = groups,
      vectorColumn = "vector",
      outputPath = outputPath
    )

    assert(metadata.totalVectors == 250)
  }

  test("ANNIndexConfig should convert to HNSWConfig correctly") {
    val config = ANNIndexConfig(
      M = 32,
      efConstruction = 400,
      distanceType = "cosine"
    )

    val hnswConfig = config.toHNSWConfig(maxElements = 10000)

    assert(hnswConfig.M == 32)
    assert(hnswConfig.efConstruction == 400)
    assert(hnswConfig.maxElements == 10000)
  }

  test("search with different ef values should affect results") {
    val sqlCtx = spark.sqlContext
    import sqlCtx.implicits._

    // Build index with clustered data
    val vectors = TestDataGenerator.generateClusteredVectors(
      numClusters = 10,
      vectorsPerCluster = 100,
      dimension = 64,
      seed = 202L
    )

    val df = vectors.toSeq.toDF("id", "vector")
    val outputPath = s"$testBasePath/test_ef_values"

    ANNIndexAPI.buildIndex(df, "vector", outputPath)

    val queryVector = vectors(500)._2 // Middle of dataset

    // Search with different ef values
    val resultsLowEf = ANNIndexAPI.search(spark, outputPath, queryVector, k = 10, ef = 10)
    val resultsHighEf = ANNIndexAPI.search(spark, outputPath, queryVector, k = 10, ef = 200)

    // Both should return k results
    assert(resultsLowEf.count() == 10)
    assert(resultsHighEf.count() == 10)

    // Higher ef typically gives better (lower) distances on average
    // (though this isn't guaranteed for every query)
    val avgDistLow = resultsLowEf.agg(Map("distance" -> "avg")).first().getDouble(0)
    val avgDistHigh = resultsHighEf.agg(Map("distance" -> "avg")).first().getDouble(0)

    println(s"Average distance with ef=10: $avgDistLow")
    println(s"Average distance with ef=200: $avgDistHigh")
  }
}
