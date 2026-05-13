package com.wayblink.ann.spark.builder

import com.wayblink.ann.spark.SharedSparkSession
import com.wayblink.ann.spark.api.{ANNIndexAPI, ANNIndexConfig}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.File

/**
 * Verifies the option-A path for plan D: when `pk` is set on
 * `ANNIndexConfig`, the user's primary-key column is used directly as
 * the HNSW internal id, so search results carry the user's pk back
 * instead of a sequential offset.
 */
class PkBuildTest extends AnyFunSuite with SharedSparkSession with Matchers {

  private val testBasePath = "/tmp/spark-ann-test/pk-build"

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
    // Spread pks over a wide range so we can verify the index returns the
    // user pk, not a sequential rebase.
    val rng = new scala.util.Random(13L)
    val rows: Seq[(Long, Array[Float])] = (0 until numRows).map { i =>
      val pk = 1000000L + i.toLong * 7L
      val vec = Array.fill(dim)(rng.nextFloat())
      (pk, vec)
    }
    val localSpark = spark
    import localSpark.implicits._
    rows.map { case (pk, v) => (pk, v.toSeq) }
      .toDF("pk", "vector")
      .coalesce(1)
      .write.mode("overwrite").parquet(dataPath)
    rows
  }

  test("search returns user pk when pk is set") {
    val dataPath = s"$testBasePath/preserve/data"
    val indexPath = s"$testBasePath/preserve/index"
    val rows = writeData(numRows = 800, dim = 32, dataPath = dataPath)

    val cfg = ANNIndexConfig(
      M = 16, efConstruction = 100,
      pk = Some("pk")
    )
    val localSpark = spark
    import localSpark.implicits._
    val df = localSpark.read.parquet(dataPath)
    val meta = ANNIndexAPI.buildIndex(df, "vector", indexPath, cfg)

    meta.totalVectors shouldBe 800

    // Self-probe a known row; the top hit's id must be the user pk.
    val (probePk, probeVec) = rows(123)
    val results = ANNIndexAPI.search(spark, indexPath, probeVec, k = 3, ef = 100).collect()
    results should not be empty
    val topId = results.head.getLong(0)
    topId shouldBe probePk
  }

  test("sequential ids preserved when pk is unset (regression guard)") {
    val dataPath = s"$testBasePath/sequential/data"
    val indexPath = s"$testBasePath/sequential/index"
    writeData(numRows = 300, dim = 16, dataPath = dataPath)

    // No pk → sequential ids [0, 300)
    val cfg = ANNIndexConfig(M = 16, efConstruction = 100)
    val df = spark.read.parquet(dataPath)
    val meta = ANNIndexAPI.buildIndex(df, "vector", indexPath, cfg)
    meta.totalVectors shouldBe 300

    // Use a synthetic query and just check the returned id range stays
    // inside [0, 300). With pk unset the search id should never
    // be a large user-style pk value.
    val probeVec = Array.fill(16)(0.5f)
    val results = ANNIndexAPI.search(spark, indexPath, probeVec, k = 5, ef = 100).collect()
    results.foreach { r =>
      val id = r.getLong(0)
      id should be >= 0L
      id should be < 300L
    }
  }

  test("string pk column fails fast with helpful error") {
    val dataPath = s"$testBasePath/str/data"
    val indexPath = s"$testBasePath/str/index"
    val localSpark = spark
    import localSpark.implicits._

    val rows: Seq[(String, Seq[Float])] =
      (0 until 50).map(i => (s"item_$i", Seq.fill(8)(i.toFloat)))
    rows.toDF("pk", "vector").coalesce(1).write.mode("overwrite").parquet(dataPath)

    val cfg = ANNIndexConfig(pk = Some("pk"))
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
