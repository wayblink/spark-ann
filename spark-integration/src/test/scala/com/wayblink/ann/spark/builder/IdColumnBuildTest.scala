package com.wayblink.ann.spark.builder

import com.wayblink.ann.spark.SharedSparkSession
import com.wayblink.ann.spark.api.{ANNIndexAPI, ANNIndexConfig}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
 * Verifies the option-A path for plan D: when `idColumn` is set on
 * `ANNIndexConfig`, the user's id column is used directly as the HNSW
 * internal id, so search results carry the user's id back instead of a
 * sequential offset.
 */
class IdColumnBuildTest extends AnyFunSuite with SharedSparkSession with Matchers {

  private val testBasePath = "/tmp/spark-ann-test/id-column-build"

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

  private def writeData(numRows: Int, dim: Int, dataPath: String): Seq[(Long, Array[Float])] = {
    // Spread ids over a wide range so we can verify the index returns the
    // user id, not a sequential rebase.
    val rng = new scala.util.Random(13L)
    val rows: Seq[(Long, Array[Float])] = (0 until numRows).map { i =>
      val id = 1000000L + i.toLong * 7L
      val vec = Array.fill(dim)(rng.nextFloat())
      (id, vec)
    }
    val localSpark = spark
    import localSpark.implicits._
    rows.map { case (id, v) => (id, v.toSeq) }
      .toDF("id", "vector")
      .coalesce(1)
      .write.mode("overwrite").parquet(dataPath)
    rows
  }

  test("search returns user id when idColumn is set") {
    val dataPath = s"$testBasePath/preserve/data"
    val indexPath = s"$testBasePath/preserve/index"
    val rows = writeData(numRows = 800, dim = 32, dataPath = dataPath)

    val cfg = ANNIndexConfig(
      M = 16, efConstruction = 100,
      idColumn = Some("id")
    )
    val localSpark = spark
    import localSpark.implicits._
    val df = localSpark.read.parquet(dataPath)
    val meta = ANNIndexAPI.buildIndex(df, "vector", indexPath, cfg)

    meta.totalVectors shouldBe 800

    // Self-probe a known row; the top hit's id must be the user id.
    val (probeId, probeVec) = rows(123)
    val results = ANNIndexAPI.search(spark, indexPath, probeVec, k = 3, ef = 100).collect()
    results should not be empty
    val topId = results.head.getLong(0)
    topId shouldBe probeId
  }

  test("sequential ids preserved when idColumn is unset (regression guard)") {
    val dataPath = s"$testBasePath/sequential/data"
    val indexPath = s"$testBasePath/sequential/index"
    writeData(numRows = 300, dim = 16, dataPath = dataPath)

    // No idColumn → sequential ids [0, 300)
    val cfg = ANNIndexConfig(M = 16, efConstruction = 100)
    val df = spark.read.parquet(dataPath)
    val meta = ANNIndexAPI.buildIndex(df, "vector", indexPath, cfg)
    meta.totalVectors shouldBe 300

    // Use a synthetic query and just check the returned id range stays
    // inside [0, 300). With idColumn unset the search id should never
    // be a large user-style id.
    val probeVec = Array.fill(16)(0.5f)
    val results = ANNIndexAPI.search(spark, indexPath, probeVec, k = 5, ef = 100).collect()
    results.foreach { r =>
      val id = r.getLong(0)
      id should be >= 0L
      id should be < 300L
    }
  }

  test("string id column fails fast with helpful error") {
    val dataPath = s"$testBasePath/str/data"
    val indexPath = s"$testBasePath/str/index"
    val localSpark = spark
    import localSpark.implicits._

    val rows: Seq[(String, Seq[Float])] =
      (0 until 50).map(i => (s"item_$i", Seq.fill(8)(i.toFloat)))
    rows.toDF("id", "vector").coalesce(1).write.mode("overwrite").parquet(dataPath)

    val cfg = ANNIndexConfig(idColumn = Some("id"))
    val df = spark.read.parquet(dataPath)

    val ex = intercept[Exception] {
      ANNIndexAPI.buildIndex(df, "vector", indexPath, cfg)
    }
    // Spark wraps the executor-side IllegalArgumentException; the
    // underlying message must mention the type-rejection guidance.
    val message = Option(ex.getMessage).getOrElse("") +
      Option(ex.getCause).map(_.getMessage).getOrElse("")
    message should include("INT32 or INT64")
  }
}
