package com.wayblink.ann.spark.examples

import com.wayblink.ann.bundle.ANNIndexConfig
import com.wayblink.ann.spark.api.{ANNDataFrameExtensions, ANNIndexAPI}
import org.apache.spark.sql.SparkSession

/**
 * Offline driver used by the STARTUP end-to-end smoke. Builds a tiny
 * bundle, runs a single-query search against it, and prints results.
 * Intentionally self-contained so it can be invoked via
 *   spark-submit --class com.wayblink.ann.spark.examples.OfflineSmoke
 * or via
 *   sbt "sparkIntegration/runMain com.wayblink.ann.spark.examples.OfflineSmoke <args>"
 *
 * Args:
 *   <output-path>   directory to write the bundle into (will be created)
 *   <num-vectors>   how many vectors to generate (default 1000)
 *   <dim>           vector dimensionality (default 32)
 *
 * The bundle is built with `pk = Some("pk")` so the on-disk HNSW
 * internal ids equal the user-supplied pk values, matching what the
 * api-server bundle path expects.
 */
object OfflineSmoke {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("usage: OfflineSmoke <output-path> [num-vectors] [dim]")
      sys.exit(2)
    }
    val outputPath = args(0)
    val numVectors = if (args.length > 1) args(1).toInt else 1000
    val dim = if (args.length > 2) args(2).toInt else 32

    val spark = SparkSession.builder()
      .appName("spark-ann-offline-smoke")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[2]"))
      .config("spark.ui.enabled", "false")
      .config("spark.sql.adaptive.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try {
      import spark.implicits._
      val rng = new scala.util.Random(13L)
      val baseId = 10000000L
      val rows: Seq[(Long, Seq[Float])] = (0 until numVectors).map { i =>
        val pk = baseId + i.toLong * 7L
        val vec = (0 until dim).map(_ => rng.nextFloat()).toSeq
        (pk, vec)
      }
      val df = rows.toDF("pk", "vector")

      println(s"[offline] building bundle with $numVectors vectors of dim $dim at $outputPath")
      val cfg = ANNIndexConfig(
        M = 16,
        efConstruction = 100,
        boundaryNodesPerIndex = 50,
        distanceType = "euclidean",
        pk = Some("pk")
      )
      val metadata = ANNIndexAPI.buildIndex(df, "vector", outputPath, cfg)
      println(s"[offline] bundle metadata: " +
        s"local=${metadata.localIndexes.length}, total=${metadata.totalVectors}, dim=${metadata.dimension}")

      // Self-probe to verify pk passthrough on the offline path.
      val (probePk, probeVecSeq) = rows(123)
      val probeVec = probeVecSeq.toArray
      val results = ANNIndexAPI.search(spark, outputPath, probeVec, k = 5, ef = 200).collect()
      println(s"[offline] self-probe pk=$probePk, top-5:")
      results.foreach { r =>
        val id = r.getLong(0)
        val dist = r.getFloat(1)
        val idxId = r.getString(2)
        println(f"           id=$id%d distance=$dist%.6f indexId=$idxId%s")
      }
      val topId = results.head.getLong(0)
      val topDist = results.head.getFloat(1)
      if (topId == probePk && topDist < 1e-3f) {
        println("[offline] OK — pk passthrough verified")
      } else {
        System.err.println(s"[offline] FAIL — expected pk=$probePk distance≈0, got id=$topId distance=$topDist")
        sys.exit(3)
      }
    } finally {
      spark.stop()
    }
  }
}
