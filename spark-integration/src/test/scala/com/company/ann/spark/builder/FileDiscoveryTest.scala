package com.company.ann.spark.builder

import com.company.ann.spark.SharedSparkSession
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite

import java.io.File

class FileDiscoveryTest extends AnyFunSuite with SharedSparkSession {

  private val testBasePath = "/tmp/spark-ann-test/file-discovery"

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Clean up any previous test data
    deleteRecursively(new File(testBasePath))
    new File(testBasePath).mkdirs()
  }

  override def afterAll(): Unit = {
    deleteRecursively(new File(testBasePath))
    super.afterAll()
  }

  private def createTestData(spark: SparkSession, path: String, numRows: Int): Unit = {
    val data = (1 to numRows).map { i =>
      (i.toLong, Array(i.toFloat / 100, (i + 1).toFloat / 100, (i + 2).toFloat / 100))
    }
    import spark.implicits._
    // Use coalesce(1) to create a single partition file for predictable testing
    data.toDF("id", "vector").coalesce(1).write.mode("overwrite").parquet(path)
  }

  private def createTextData(spark: SparkSession, path: String, numRows: Int): Unit = {
    val data = (1 to numRows).map { i =>
      (i.toLong, s"text_$i")
    }
    import spark.implicits._
    // Use coalesce(1) to create a single partition file for predictable testing
    data.toDF("id", "text_column").coalesce(1).write.mode("overwrite").parquet(path)
  }

  test("discover single parquet file") {
    val testPath = s"$testBasePath/single"
    new File(testPath).mkdirs()

    createTestData(spark, s"$testPath/data.parquet", 3)

    val files = FileDiscovery.discoverDataFiles(spark, testPath, "vector")

    assert(files.length == 1)
    assert(files.head.numVectors == 3)
    assert(files.head.filePath.contains("data.parquet"))
  }

  test("discover multiple parquet files") {
    val testPath = s"$testBasePath/multiple"
    new File(testPath).mkdirs()

    // Create multiple files with different sizes
    createTestData(spark, s"$testPath/file_1.parquet", 100)
    createTestData(spark, s"$testPath/file_2.parquet", 200)
    createTestData(spark, s"$testPath/file_3.parquet", 300)

    val files = FileDiscovery.discoverDataFiles(spark, testPath, "vector")

    assert(files.length == 3)
    assert(files.map(_.numVectors).sum == 600)

    // Each file should have expected count
    val sortedFiles = files.sortBy(_.numVectors)
    assert(sortedFiles(0).numVectors == 100)
    assert(sortedFiles(1).numVectors == 200)
    assert(sortedFiles(2).numVectors == 300)
  }

  test("discover files in nested directories") {
    val testPath = s"$testBasePath/nested"
    new File(s"$testPath/year=2024/month=01").mkdirs()
    new File(s"$testPath/year=2024/month=02").mkdirs()

    createTestData(spark, s"$testPath/year=2024/month=01/data.parquet", 50)
    createTestData(spark, s"$testPath/year=2024/month=02/data.parquet", 75)

    val files = FileDiscovery.discoverDataFiles(spark, testPath, "vector")

    assert(files.length == 2)
    assert(files.map(_.numVectors).sum == 125)
  }

  test("skip hidden files and directories") {
    val testPath = s"$testBasePath/hidden"
    new File(testPath).mkdirs()
    new File(s"$testPath/_hidden_dir").mkdirs()

    createTestData(spark, s"$testPath/visible.parquet", 10)
    createTestData(spark, s"$testPath/_hidden_dir/hidden.parquet", 10)

    val files = FileDiscovery.discoverDataFiles(spark, testPath, "vector")

    // Should only find the visible file, not files in hidden directory
    assert(files.length == 1)
    assert(files.head.filePath.contains("visible.parquet"))
  }

  test("throw exception for missing vector column") {
    val testPath = s"$testBasePath/missing-column"
    new File(testPath).mkdirs()

    createTextData(spark, s"$testPath/data.parquet", 5)

    val exception = intercept[IllegalArgumentException] {
      FileDiscovery.discoverDataFiles(spark, testPath, "vector")
    }

    assert(exception.getMessage.contains("Vector column 'vector' not found"))
  }

  test("throw exception for empty directory") {
    val testPath = s"$testBasePath/empty"
    new File(testPath).mkdirs()

    val exception = intercept[IllegalArgumentException] {
      FileDiscovery.discoverDataFiles(spark, testPath, "vector")
    }

    assert(exception.getMessage.contains("No Parquet files found"))
  }

  test("totalVectors calculates sum correctly") {
    val files = Array(
      DataFileInfo("/path/a.parquet", 100),
      DataFileInfo("/path/b.parquet", 200),
      DataFileInfo("/path/c.parquet", 300)
    )

    assert(FileDiscovery.totalVectors(files) == 600)
  }

  test("totalVectors returns 0 for empty array") {
    assert(FileDiscovery.totalVectors(Array.empty) == 0)
  }
}
