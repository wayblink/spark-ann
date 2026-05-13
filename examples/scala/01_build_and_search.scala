// 01: Scala equivalent of examples/python/01_build_and_search.py
//
// Run with:
//   sbt sparkIntegration/assembly
//   spark-shell --jars spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar \
//       -i examples/scala/01_build_and_search.scala

import com.wayblink.ann.spark.api.ANNIndexAPI
import com.wayblink.ann.spark.api.ANNIndexConfig
import com.wayblink.ann.spark.builder.SingleFile

val dim = 64
val n = 1000
val rng = new scala.util.Random(42L)

val rows: Seq[(Long, Seq[Float])] =
  (0 until n).map { i =>
    val v = (0 until dim).map(_ => rng.nextFloat()).toSeq
    (i.toLong, v)
  }

import spark.implicits._
val df = rows.toDF("id", "vector")

val indexPath = "/tmp/spark-ann-ex01-idx"
new java.io.File(indexPath).getParentFile.mkdirs()

val cfg = ANNIndexConfig(
  M = 16,
  efConstruction = 200,
  groupingStrategy = SingleFile,
  distanceType = "euclidean"
)
val meta = ANNIndexAPI.buildIndex(df, "vector", indexPath, cfg)
println(s"[example] built ${meta.statistics.totalVectors} vectors, dim=${meta.dimension}")
assert(meta.statistics.totalVectors == n)

// Probe the 124th inserted vector — the search should return distance ~0.
// (The returned `id` is the HNSW internal id, NOT the user `id` column;
//  see examples/README.md "Known limitation" for context.)
val (probeId, probeVecSeq) = rows(123)
val probeVec = probeVecSeq.toArray

val results = ANNIndexAPI.search(spark, indexPath, probeVec, k = 5, ef = 200).collect()
println("[example] top-5 for probe row 123:")
results.foreach { r =>
  println(s"           id=${r.getLong(0)} distance=${r.getFloat(1)} indexId=${r.getString(2)}")
}
val topDistance = results.head.getFloat(1)
assert(topDistance < 1e-3f, s"top distance $topDistance should be near 0")
println("[example] OK")

System.exit(0)
