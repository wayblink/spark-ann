package com.company.ann.spark

import com.company.ann.core.index.HNSWIndex
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files

/**
 * Verifies Spark environment is correctly set up and
 * project structure is valid.
 */
class SparkEnvironmentTest extends AnyFunSuite with SharedSparkSession {

  var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("spark-ann-test").toFile
  }

  test("Spark session should be created") {
    assert(spark != null)
    assert(spark.version.startsWith("3."))
  }

  test("Spark session should have correct configuration") {
    assert(spark.sparkContext.master == "local[4]")
  }

  test("Can read and write Parquet") {
    val sqlContext = spark
    import sqlContext.implicits._

    val data = Seq(
      (1L, Array(0.1f, 0.2f, 0.3f)),
      (2L, Array(0.4f, 0.5f, 0.6f))
    )
    val df = data.toDF("id", "vector")

    val path = new File(tempDir, "test_vectors.parquet").getAbsolutePath
    df.write.mode("overwrite").parquet(path)

    val readDf = spark.read.parquet(path)
    assert(readDf.count() == 2)
    assert(readDf.columns.toSeq.contains("id"))
    assert(readDf.columns.toSeq.contains("vector"))
  }

  test("Can handle array columns") {
    val sqlContext = spark
    import sqlContext.implicits._

    val dimension = 128
    val data = (0 until 100).map { i =>
      (i.toLong, Array.fill(dimension)(i.toFloat / 100))
    }
    val df = data.toDF("id", "vector")

    assert(df.count() == 100)

    // Verify array access
    val firstRow = df.first()
    val vector = firstRow.getSeq[Float](1)
    assert(vector.length == dimension)
  }

  test("Project structure is correct - core module classes accessible") {
    // Verify core module classes are accessible
    val indexClass = classOf[HNSWIndex]
    assert(indexClass != null)
    assert(indexClass.getName == "com.company.ann.core.index.HNSWIndex")
  }

  test("SQL functions work correctly") {
    val sqlContext = spark
    import sqlContext.implicits._

    val data = Seq(
      (1L, Array(1.0f, 2.0f, 3.0f)),
      (2L, Array(4.0f, 5.0f, 6.0f)),
      (3L, Array(7.0f, 8.0f, 9.0f))
    )
    val df = data.toDF("id", "vector")
    df.createOrReplaceTempView("vectors_env_test")

    val result = spark.sql("SELECT id, size(vector) as dim FROM vectors_env_test")
    val dims = result.collect().map(_.getAs[Int]("dim"))

    assert(dims.forall(_ == 3))
  }

  test("Parallel execution works") {
    val sqlContext = spark
    import sqlContext.implicits._

    val data = (0 until 1000).map(i => (i.toLong, i.toDouble))
    val df = data.toDF("id", "value")

    // Force multiple partitions
    val repartitioned = df.repartition(4)
    assert(repartitioned.rdd.getNumPartitions == 4)

    val sum = repartitioned.agg(Map("value" -> "sum")).collect().head.getDouble(0)
    val expected = (0 until 1000).map(_.toDouble).sum
    assert(math.abs(sum - expected) < 0.001)
  }

  override def afterAll(): Unit = {
    // Clean up temp directory
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
    super.afterAll()
  }
}
