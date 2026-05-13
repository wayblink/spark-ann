package com.wayblink.ann.spark.testutil

import com.wayblink.ann.spark.SharedSparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files

/**
 * Tests for SparkTestData utility class.
 */
class SparkTestDataTest extends AnyFunSuite with SharedSparkSession {

  var tempDir: File = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    tempDir = Files.createTempDirectory("spark-testdata-test").toFile
  }

  test("generateAndSave should create random vectors") {
    val path = new File(tempDir, "random_vectors").getAbsolutePath

    val df = SparkTestData.generateAndSave(
      spark,
      numVectors = 1000,
      dimension = 128,
      path = path,
      dataType = "random"
    )

    assert(df.count() == 1000)
    assert(df.columns.toSeq.contains("id"))
    assert(df.columns.toSeq.contains("vector"))

    // Verify vector dimension
    val firstVector = df.first().getSeq[Float](1)
    assert(firstVector.length == 128)
  }

  test("generateAndSave should create clustered vectors") {
    val path = new File(tempDir, "clustered_vectors").getAbsolutePath

    val df = SparkTestData.generateAndSave(
      spark,
      numVectors = 500,
      dimension = 64,
      path = path,
      dataType = "clustered"
    )

    assert(df.count() == 500)

    val firstVector = df.first().getSeq[Float](1)
    assert(firstVector.length == 64)
  }

  test("generateAndSave should create realistic vectors") {
    val path = new File(tempDir, "realistic_vectors").getAbsolutePath

    val df = SparkTestData.generateAndSave(
      spark,
      numVectors = 200,
      dimension = 256,
      path = path,
      dataType = "realistic"
    )

    assert(df.count() == 200)

    // Verify vectors are normalized (L2 norm ~= 1)
    val sqlContext = spark
    import sqlContext.implicits._
    val norms = df.select($"vector").as[Seq[Float]].collect().map { vector =>
      math.sqrt(vector.map(x => x * x).sum)
    }

    norms.foreach { norm =>
      assert(math.abs(norm - 1.0) < 0.01, s"Vector should be normalized, got norm=$norm")
    }
  }

  test("loadTestData should read saved data") {
    val path = new File(tempDir, "load_test").getAbsolutePath

    val originalDf = SparkTestData.generateAndSave(
      spark,
      numVectors = 100,
      dimension = 32,
      path = path,
      dataType = "random"
    )

    val loadedDf = SparkTestData.loadTestData(spark, path)

    assert(loadedDf.count() == 100)
    assert(loadedDf.schema == originalDf.schema)
  }

  test("generateDataFrame should create DataFrame without saving") {
    val df = SparkTestData.generateDataFrame(
      spark,
      numVectors = 300,
      dimension = 96,
      dataType = "random"
    )

    assert(df.count() == 300)
    assert(df.columns.toSeq.contains("id"))
    assert(df.columns.toSeq.contains("vector"))
  }

  test("generateAndSave should be reproducible with same seed") {
    val path1 = new File(tempDir, "seed_test_1").getAbsolutePath
    val path2 = new File(tempDir, "seed_test_2").getAbsolutePath

    val df1 = SparkTestData.generateAndSave(spark, 50, 16, path1, "random", seed = 12345)
    val df2 = SparkTestData.generateAndSave(spark, 50, 16, path2, "random", seed = 12345)

    val vectors1 = df1.collect().map(r => (r.getLong(0), r.getSeq[Float](1).toArray)).sortBy(_._1)
    val vectors2 = df2.collect().map(r => (r.getLong(0), r.getSeq[Float](1).toArray)).sortBy(_._1)

    vectors1.zip(vectors2).foreach { case ((id1, v1), (id2, v2)) =>
      assert(id1 == id2)
      assert(v1.sameElements(v2), s"Vectors with id=$id1 should be identical")
    }
  }

  test("generateAndSave should throw for unknown data type") {
    val path = new File(tempDir, "invalid_type").getAbsolutePath

    assertThrows[IllegalArgumentException] {
      SparkTestData.generateAndSave(
        spark,
        numVectors = 10,
        dimension = 8,
        path = path,
        dataType = "unknown_type"
      )
    }
  }

  test("generated data should work with Spark SQL") {
    val path = new File(tempDir, "sql_test").getAbsolutePath

    val df = SparkTestData.generateAndSave(spark, 100, 32, path, "random")
    df.createOrReplaceTempView("test_vectors_data")

    val result = spark.sql(
      """
        |SELECT
        |  COUNT(*) as cnt,
        |  MIN(id) as min_id,
        |  MAX(id) as max_id,
        |  AVG(size(vector)) as avg_dim
        |FROM test_vectors_data
        |""".stripMargin)

    val row = result.first()
    assert(row.getLong(0) == 100)  // count
    assert(row.getLong(1) == 0)    // min_id
    assert(row.getLong(2) == 99)   // max_id
    assert(row.getDouble(3) == 32) // avg_dim
  }

  test("generated data should support repartitioning") {
    val df = SparkTestData.generateDataFrame(spark, 1000, 64, "clustered")

    val repartitioned = df.repartition(10)
    assert(repartitioned.rdd.getNumPartitions == 10)
    assert(repartitioned.count() == 1000)
  }

  override def afterAll(): Unit = {
    if (tempDir != null && tempDir.exists()) {
      deleteRecursively(tempDir)
    }
    super.afterAll()
  }
}
