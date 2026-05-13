package com.wayblink.ann.spark.examples

import com.wayblink.ann.core.testutil.TestDataGenerator
import com.wayblink.ann.spark.api.{ANNDataFrameExtensions, ANNIndexAPI, ANNIndexConfig}
import com.wayblink.ann.spark.builder.{FileDiscovery, FileGroupingStrategy, SingleFile, MergeSmall}
import org.apache.spark.sql.SparkSession

/**
 * Quick start example demonstrating the ANN DataFrame API.
 *
 * This example shows:
 * 1. Generating test vector data
 * 2. Discovering and grouping data files
 * 3. Building an ANN index
 * 4. Performing single and batch searches
 */
object QuickStart {

  def main(args: Array[String]): Unit = {

    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("ANN Quick Start")
      .master("local[4]")
      .config("spark.driver.memory", "2g")
      .getOrCreate()

    try {
      runExample(spark)
    } finally {
      spark.stop()
    }
  }

  def runExample(spark: SparkSession): Unit = {
    import spark.implicits._

    println("=" * 60)
    println("ANN Index Quick Start Example")
    println("=" * 60)

    // Configuration
    val testDataPath = "/tmp/ann_quickstart/vectors"
    val indexPath = "/tmp/ann_quickstart/index"
    val dimension = 128
    val numFiles = 5
    val vectorsPerFile = 2000

    // Step 1: Generate test data files
    println("\n[Step 1] Generating test data files...")
    generateTestData(spark, testDataPath, numFiles, vectorsPerFile, dimension)
    println(s"Generated $numFiles data files with $vectorsPerFile vectors each")

    // Step 2: Discover data files
    println("\n[Step 2] Discovering data files...")
    val dataFiles = FileDiscovery.discoverDataFiles(spark, testDataPath, "vector")
    println(s"Found ${dataFiles.length} data files")
    dataFiles.foreach { f =>
      println(s"  - ${f.filePath}: ${f.numVectors} vectors")
    }

    // Step 3: Group files using SingleFile strategy
    println("\n[Step 3] Grouping files (SingleFile strategy)...")
    val fileGroups = FileGroupingStrategy.groupFiles(dataFiles, SingleFile)
    FileGroupingStrategy.printSummary(fileGroups)

    // Step 4: Build the ANN index
    println("\n[Step 4] Building ANN index...")
    val config = ANNIndexConfig(
      M = 16,
      efConstruction = 200,
      boundaryNodesPerIndex = 50,
      distanceType = "euclidean"
    )

    val metadata = ANNIndexAPI.buildIndexFromFileGroups(
      spark = spark,
      fileGroups = fileGroups,
      vectorColumn = "vector",
      outputPath = indexPath,
      config = config
    )

    println(s"\nIndex built successfully!")
    println(s"  Total vectors: ${metadata.statistics.totalVectors}")
    println(s"  Number of local indexes: ${metadata.statistics.numLocalIndexes}")
    println(s"  Build time: ${metadata.statistics.buildTimeMs}ms")

    // Step 5: Query the index - Single query
    println("\n[Step 5] Querying the index (single query)...")
    val queryVector = Array.fill(dimension)(scala.util.Random.nextFloat())

    val results = ANNIndexAPI.search(
      spark = spark,
      indexPath = indexPath,
      queryVector = queryVector,
      k = 10,
      nprobe = 3
    )

    println("Top 10 nearest neighbors:")
    results.show()

    // Step 6: Query using DataFrame API
    println("\n[Step 6] Querying using DataFrame implicit extension...")
    import ANNDataFrameExtensions._

    val df = spark.read.parquet(testDataPath)
    val results2 = df.annSearch(
      indexPath = indexPath,
      queryVector = queryVector,
      k = 5
    )

    println("Top 5 nearest neighbors (via DataFrame API):")
    results2.show()

    // Step 7: Batch search
    println("\n[Step 7] Batch search with multiple queries...")
    val queryVectors = Seq(
      Array.fill(dimension)(0.1f),
      Array.fill(dimension)(0.5f),
      Array.fill(dimension)(0.9f)
    )
    val queriesDF = queryVectors.zipWithIndex.map { case (vec, idx) =>
      (idx, vec.toSeq)
    }.toDF("queryId", "vector")

    val batchResults = ANNIndexAPI.batchSearch(
      spark = spark,
      indexPath = indexPath,
      queries = queriesDF,
      queryVectorColumn = "vector",
      k = 3
    )

    println("Batch search results (top 3 per query):")
    batchResults.orderBy("queryIndex", "distance").show(15)

    // Step 8: Using MergeSmall strategy
    println("\n[Step 8] Demonstrating MergeSmall grouping strategy...")
    val mergedGroups = FileGroupingStrategy.groupFiles(
      dataFiles,
      MergeSmall,
      targetVectorsPerIndex = 5000
    )
    println(s"With MergeSmall (target 5000 vectors/index):")
    FileGroupingStrategy.printSummary(mergedGroups)

    println("\n" + "=" * 60)
    println("Quick Start Example Complete!")
    println("=" * 60)
  }

  /**
   * Generate test data files with clustered vectors.
   */
  private def generateTestData(
    spark: SparkSession,
    basePath: String,
    numFiles: Int,
    vectorsPerFile: Int,
    dimension: Int
  ): Unit = {
    import spark.implicits._

    (1 to numFiles).foreach { fileIndex =>
      val vectors = TestDataGenerator.generateClusteredVectors(
        numClusters = 10,
        vectorsPerCluster = vectorsPerFile / 10,
        dimension = dimension,
        seed = fileIndex * 1000L
      )

      val df = vectors.toSeq.toDF("id", "vector")
      df.write
        .mode("overwrite")
        .parquet(s"$basePath/file_$fileIndex.parquet")
    }
  }
}

/**
 * Simpler example showing basic usage patterns.
 */
object SimpleExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ANN Simple Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    import ANNDataFrameExtensions._

    try {
      // Create sample data
      val vectors = Seq(
        (1L, Seq(0.1f, 0.2f, 0.3f, 0.4f)),
        (2L, Seq(0.2f, 0.3f, 0.4f, 0.5f)),
        (3L, Seq(0.3f, 0.4f, 0.5f, 0.6f)),
        (4L, Seq(0.4f, 0.5f, 0.6f, 0.7f)),
        (5L, Seq(0.5f, 0.6f, 0.7f, 0.8f))
      ).toDF("id", "vector")

      // Build index
      val indexPath = "/tmp/simple_ann_index"
      val metadata = vectors.buildANNIndex(
        vectorColumn = "vector",
        outputPath = indexPath
      )

      println(s"Built index with ${metadata.totalVectors} vectors")

      // Search
      val query = Array(0.15f, 0.25f, 0.35f, 0.45f)
      val results = vectors.annSearch(indexPath, query, k = 3)

      println("Search results:")
      results.show()

    } finally {
      spark.stop()
    }
  }
}
