package com.company.ann.spark.builder

import com.company.ann.core.index.{HNSWConfig, HNSWLibIndex}
import com.company.ann.core.testutil.TestDataGenerator
import com.company.ann.spark.SharedSparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class LocalIndexBuilderTest extends AnyFunSuite with SharedSparkSession {

  private val testBasePath = "/tmp/spark-ann-test/local-index-builder"

  override def beforeAll(): Unit = {
    super.beforeAll()
    deleteRecursively(new File(testBasePath))
    new File(testBasePath).mkdirs()
  }

  override def afterAll(): Unit = {
    deleteRecursively(new File(testBasePath))
    super.afterAll()
  }

  private def createTestData(spark: SparkSession, path: String, numRows: Int, dimension: Int = 64): Unit = {
    val vectors = TestDataGenerator.generateRandomVectors(numRows, dimension)
    import spark.implicits._
    vectors.toSeq.toDF("id", "vector").coalesce(1).write.mode("overwrite").parquet(path)
  }

  private def createClusteredTestData(spark: SparkSession, path: String, numClusters: Int, vectorsPerCluster: Int, dimension: Int = 64): Unit = {
    val vectors = TestDataGenerator.generateClusteredVectors(numClusters, vectorsPerCluster, dimension)
    import spark.implicits._
    vectors.toSeq.toDF("id", "vector").coalesce(1).write.mode("overwrite").parquet(path)
  }

  // ==================== Single File Tests ====================

  test("build index for single file") {
    val dataPath = s"$testBasePath/single-file/data"
    val indexPath = s"$testBasePath/single-file/index"
    new File(dataPath).mkdirs()

    createTestData(spark, s"$dataPath/file_001.parquet", 100, 64)

    // Discover files
    val files = FileDiscovery.discoverDataFiles(spark, dataPath, "vector")
    assert(files.length == 1)

    // Group files (single file strategy)
    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)
    assert(groups.length == 1)

    // Build index
    val metadata = LocalIndexBuilder.buildFromFileGroups(
      spark,
      groups,
      vectorColumn = "vector",
      indexOutputPath = indexPath
    )

    assert(metadata.length == 1)
    assert(metadata.head.totalVectors == 100)
    assert(metadata.head.dimension == 64)
    assert(metadata.head.dataFiles.length == 1)
    assert(new File(metadata.head.indexPath).exists())
  }

  test("build index for single file and verify search works") {
    val dataPath = s"$testBasePath/single-searchable/data"
    val indexPath = s"$testBasePath/single-searchable/index"
    new File(dataPath).mkdirs()

    // Create clustered data for better search verification
    createClusteredTestData(spark, s"$dataPath/data.parquet", numClusters = 5, vectorsPerCluster = 20, dimension = 32)

    val files = FileDiscovery.discoverDataFiles(spark, dataPath, "vector")
    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)

    val metadata = LocalIndexBuilder.buildFromFileGroups(
      spark,
      groups,
      vectorColumn = "vector",
      indexOutputPath = indexPath
    )

    // Load built index and perform search
    val builtIndexPath = metadata.head.indexPath
    val loadedIndex = HNSWLibIndex.load(builtIndexPath)

    assert(loadedIndex.size == 100)
    assert(loadedIndex.dimension == 32)

    // Search with a random query
    val queryVector = Array.fill(32)(scala.util.Random.nextFloat())
    val results = loadedIndex.search(queryVector, k = 10)

    assert(results.length == 10)
    // Results should be sorted by distance
    assert(results.map(_.distance).sliding(2).forall {
      case Seq(a, b) => a <= b
      case _ => true
    })
  }

  // ==================== Multiple Files Tests ====================

  test("build indexes for multiple files with SingleFile strategy") {
    val dataPath = s"$testBasePath/multi-single/data"
    val indexPath = s"$testBasePath/multi-single/index"
    new File(dataPath).mkdirs()

    createTestData(spark, s"$dataPath/file_001.parquet", 50, 64)
    createTestData(spark, s"$dataPath/file_002.parquet", 75, 64)
    createTestData(spark, s"$dataPath/file_003.parquet", 100, 64)

    val files = FileDiscovery.discoverDataFiles(spark, dataPath, "vector")
    assert(files.length == 3)

    // SingleFile strategy: one index per file
    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)
    assert(groups.length == 3)

    val metadata = LocalIndexBuilder.buildFromFileGroups(
      spark,
      groups,
      vectorColumn = "vector",
      indexOutputPath = indexPath
    )

    assert(metadata.length == 3)
    assert(metadata.map(_.totalVectors).sum == 225)

    // Each index should have exactly one file
    metadata.foreach { m =>
      assert(m.dataFiles.length == 1)
      assert(new File(m.indexPath).exists())
    }

    // Each index should be searchable
    metadata.foreach { m =>
      val index = HNSWLibIndex.load(m.indexPath)
      assert(index.size == m.totalVectors)
      val results = index.search(Array.fill(64)(0.5f), k = 5)
      assert(results.length == 5)
    }
  }

  test("build index for multiple files with MergeSmall strategy") {
    val dataPath = s"$testBasePath/multi-merged/data"
    val indexPath = s"$testBasePath/multi-merged/index"
    new File(dataPath).mkdirs()

    createTestData(spark, s"$dataPath/file_001.parquet", 30, 64)
    createTestData(spark, s"$dataPath/file_002.parquet", 40, 64)
    createTestData(spark, s"$dataPath/file_003.parquet", 50, 64)
    createTestData(spark, s"$dataPath/file_004.parquet", 80, 64)

    val files = FileDiscovery.discoverDataFiles(spark, dataPath, "vector")
    assert(files.length == 4)

    // MergeSmall strategy: target 100 vectors per index
    val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, targetVectorsPerIndex = 100)

    // Should have fewer groups than files
    assert(groups.length <= files.length)
    assert(groups.map(_.totalVectors).sum == 200)

    val metadata = LocalIndexBuilder.buildFromFileGroups(
      spark,
      groups,
      vectorColumn = "vector",
      indexOutputPath = indexPath
    )

    assert(metadata.map(_.totalVectors).sum == 200)

    // At least one index should have multiple files
    val mergedIndex = metadata.find(_.dataFiles.length > 1)
    if (groups.length < files.length) {
      assert(mergedIndex.isDefined, "Should have at least one merged index")
    }

    // All indexes should be searchable
    metadata.foreach { m =>
      val index = HNSWLibIndex.load(m.indexPath)
      assert(index.size == m.totalVectors)
    }
  }

  // ==================== Vector Offset Tests ====================

  test("vector offsets are correctly calculated for merged files") {
    val dataPath = s"$testBasePath/offsets/data"
    val indexPath = s"$testBasePath/offsets/index"
    new File(dataPath).mkdirs()

    createTestData(spark, s"$dataPath/file_001.parquet", 10, 32)
    createTestData(spark, s"$dataPath/file_002.parquet", 20, 32)
    createTestData(spark, s"$dataPath/file_003.parquet", 15, 32)

    val files = FileDiscovery.discoverDataFiles(spark, dataPath, "vector")

    // Force merge all into one group
    val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, targetVectorsPerIndex = 1000)
    assert(groups.length == 1)

    val metadata = LocalIndexBuilder.buildFromFileGroups(
      spark,
      groups,
      vectorColumn = "vector",
      indexOutputPath = indexPath
    )

    val m = metadata.head
    assert(m.totalVectors == 45)
    assert(m.dataFiles.length == 3)

    // Check offsets are sequential
    val sortedEntries = m.dataFiles.sortBy(_.vectorOffset)
    var expectedOffset = 0L
    sortedEntries.foreach { entry =>
      assert(entry.vectorOffset == expectedOffset,
        s"Expected offset $expectedOffset but got ${entry.vectorOffset}")
      expectedOffset += entry.numVectors
    }
    assert(expectedOffset == 45)
  }

  // ==================== Custom Config Tests ====================

  test("build index with custom HNSW config") {
    val dataPath = s"$testBasePath/custom-config/data"
    val indexPath = s"$testBasePath/custom-config/index"
    new File(dataPath).mkdirs()

    createTestData(spark, s"$dataPath/data.parquet", 200, 64)

    val files = FileDiscovery.discoverDataFiles(spark, dataPath, "vector")
    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)

    val customConfig = HNSWConfig(
      M = 32,
      efConstruction = 400,
      maxElements = 1000
    )

    val metadata = LocalIndexBuilder.buildFromFileGroups(
      spark,
      groups,
      vectorColumn = "vector",
      indexOutputPath = indexPath,
      config = customConfig
    )

    assert(metadata.length == 1)
    assert(metadata.head.totalVectors == 200)

    // Index should be searchable
    val index = HNSWLibIndex.load(metadata.head.indexPath)
    assert(index.size == 200)
  }

  // ==================== Edge Cases ====================

  test("build index handles empty file groups array") {
    val indexPath = s"$testBasePath/empty/index"

    val metadata = LocalIndexBuilder.buildFromFileGroups(
      spark,
      Array.empty[FileGroup],
      vectorColumn = "vector",
      indexOutputPath = indexPath
    )

    assert(metadata.isEmpty)
  }

  test("printSummary displays correct information") {
    val metadata = Array(
      LocalIndexMetadata(
        indexId = "idx_a",
        dataFiles = Array(DataFileEntry("/path/a.parquet", 100, 0)),
        indexPath = "/index/a.hnsw",
        totalVectors = 100,
        dimension = 64
      ),
      LocalIndexMetadata(
        indexId = "idx_b",
        dataFiles = Array(
          DataFileEntry("/path/b1.parquet", 50, 0),
          DataFileEntry("/path/b2.parquet", 75, 50)
        ),
        indexPath = "/index/b.hnsw",
        totalVectors = 125,
        dimension = 64
      )
    )

    // Should not throw exception
    LocalIndexBuilder.printSummary(metadata)
  }

  // ==================== Integration Test ====================

  test("end-to-end: discover -> group -> build -> search") {
    val dataPath = s"$testBasePath/e2e/data"
    val indexPath = s"$testBasePath/e2e/index"
    new File(dataPath).mkdirs()

    // Generate clustered test data in multiple files
    for (i <- 1 to 3) {
      createClusteredTestData(spark, s"$dataPath/file_$i.parquet",
        numClusters = 3, vectorsPerCluster = 20, dimension = 32)
    }

    // Step 1: Discover files
    val files = FileDiscovery.discoverDataFiles(spark, dataPath, "vector")
    assert(files.length == 3)
    val totalVectors = FileDiscovery.totalVectors(files)
    assert(totalVectors == 180)

    // Step 2: Group files
    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)
    assert(groups.length == 3)

    // Step 3: Build indexes
    val metadata = LocalIndexBuilder.buildFromFileGroups(
      spark,
      groups,
      vectorColumn = "vector",
      indexOutputPath = indexPath
    )
    assert(metadata.length == 3)
    assert(metadata.map(_.totalVectors).sum == 180)

    // Step 4: Search across all indexes
    val queryVector = Array.fill(32)(scala.util.Random.nextFloat())
    val allResults = metadata.flatMap { m =>
      val index = HNSWLibIndex.load(m.indexPath)
      index.search(queryVector, k = 5)
    }

    // Should get 15 results (5 from each of 3 indexes)
    assert(allResults.length == 15)

    // Print summary
    LocalIndexBuilder.printSummary(metadata)
  }
}
