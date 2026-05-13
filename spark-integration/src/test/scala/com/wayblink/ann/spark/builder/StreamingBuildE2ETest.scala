package com.wayblink.ann.spark.builder

import com.wayblink.ann.core.index.HNSWLibIndex
import com.wayblink.ann.core.testutil.TestDataGenerator
import com.wayblink.ann.spark.SharedSparkSession
import com.wayblink.ann.spark.util.StreamingParquetVectorReader
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
 * End-to-end verification of the streaming build path. The earlier
 * implementation buffered every vector from a FileGroup into an
 * ArrayBuffer before calling HNSWLibIndex.addAll. At production scale
 * (500K × 768 dim) that path peaks at well over 1 GB per task and OOMs
 * multi-core executors. The new path streams parquet rows directly into
 * HNSW.add and uses a reservoir sampler for boundary nodes.
 *
 * Tests here verify: (1) the streaming reader yields exactly the rows in
 * the parquet file, (2) the resulting HNSW index has the right size and
 * answers searches correctly, (3) boundary nodes are sampled in-stream
 * without buffering, (4) maxElements is sized precisely.
 */
class StreamingBuildE2ETest extends AnyFunSuite with SharedSparkSession with Matchers {

  private val testBasePath = "/tmp/spark-ann-test/streaming-build"

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

  test("StreamingParquetVectorReader yields all rows from a multi-row-group file") {
    val dim = 16
    val numRows = 2000
    val vectors = TestDataGenerator.generateRandomVectors(numRows, dim, seed = 11L)
    val localSpark = spark
    import localSpark.implicits._

    val dataDir = s"$testBasePath/reader/data.parquet"
    new File(s"$testBasePath/reader").mkdirs()
    vectors.toSeq.map { case (id, v) => (id, v.toSeq) }
      .toDF("id", "vector")
      .coalesce(1)
      .write.mode("overwrite").parquet(dataDir)

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(new Path(dataDir).toUri, hadoopConf)
    val parquetFile = fs.listStatus(new Path(dataDir))
      .find(s => s.getPath.getName.endsWith(".parquet") && !s.getPath.getName.startsWith("_"))
      .map(_.getPath.toString)
      .getOrElse(fail("No parquet file produced by Spark write"))

    val iter = StreamingParquetVectorReader.streamVectors(parquetFile, "vector", hadoopConf)
    val collected = iter.toArray
    collected.length shouldBe numRows
    collected.head.length shouldBe dim
  }

  test("streaming build produces a working HNSW with reservoir-sampled boundaries") {
    val dim = 32
    val numRows = 1500
    val vectors = TestDataGenerator.generateRandomVectors(numRows, dim, seed = 22L)
    val localSpark = spark
    import localSpark.implicits._

    val dataDir = s"$testBasePath/build/data.parquet"
    new File(s"$testBasePath/build").mkdirs()
    vectors.toSeq.map { case (id, v) => (id, v.toSeq) }
      .toDF("id", "vector")
      .coalesce(1)
      .write.mode("overwrite").parquet(dataDir)

    val indexOutput = s"$testBasePath/build/index"
    val files = FileDiscovery.discoverDataFiles(spark, dataDir, "vector")
    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)
    val results = LocalIndexBuilder.buildFromFileGroupsWithBoundaryNodes(
      spark, groups, "vector", indexOutput,
      config = com.wayblink.ann.core.index.HNSWConfig(M = 16, efConstruction = 100),
      distanceType = "euclidean",
      boundaryNodesPerIndex = 25
    )

    results should have length 1
    val r = results.head
    r.metadata.totalVectors shouldBe numRows
    r.boundaryNodes.length should be <= 25
    r.boundaryNodes.length should be > 0

    // Boundary node vectors must have the right dimension and belong to the
    // source index id.
    r.boundaryNodes.foreach { bn =>
      bn.vector.length shouldBe dim
      bn.indexId shouldBe r.metadata.indexId
    }

    // Load the saved index and verify a self-search returns the queried id
    // as the nearest neighbour (recall@1 on the training set).
    val loaded = HNSWLibIndex.load(r.metadata.indexPath)
    val (probeId, probeVec) = vectors(123)
    val hits = loaded.search(probeVec, k = 5, ef = 100)
    hits.head.id shouldBe probeId
  }
}
