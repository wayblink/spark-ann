// 02: Scala equivalent of examples/python/02_batch_search.py
//
// Run with:
//   sbt sparkIntegration/assembly
//   spark-shell --jars spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar \
//       -i examples/scala/02_batch_search.scala

import com.wayblink.ann.spark.api.ANNIndexAPI
import com.wayblink.ann.spark.api.ANNIndexConfig

val dim = 32
val nBase = 2000
val rng = new scala.util.Random(42L)

val baseRows: Seq[(Long, Seq[Float])] =
  (0 until nBase).map { i =>
    val v = (0 until dim).map(_ => rng.nextFloat()).toSeq
    (i.toLong, v)
  }

import spark.implicits._
val baseDF = baseRows.toDF("id", "vector")

val indexPath = "/tmp/spark-ann-ex02-idx"
new java.io.File(indexPath).getParentFile.mkdirs()

ANNIndexAPI.buildIndex(baseDF, "vector", indexPath, ANNIndexConfig())

val probeIndices = Seq(0, 100, 250, 999, 1500)
val queryRows: Seq[(Long, Seq[Float])] = probeIndices.zipWithIndex.map { case (probeId, qi) =>
  (qi.toLong, baseRows(probeId)._2)
}
val queriesDF = queryRows.toDF("query_id", "vector")

val resultsDF = ANNIndexAPI.batchSearch(
  spark, indexPath, queriesDF, "vector", k = 3, ef = 200
)
println(s"[example] batchSearch schema: ${resultsDF.schema.fieldNames.mkString(", ")}")

val rows = resultsDF.collect()
val byQuery = rows.groupBy(_.getInt(0))
// id column is HNSW internal id (not user `id`); assert on distance.
probeIndices.zipWithIndex.foreach { case (_, qi) =>
  val top = byQuery(qi).sortBy(_.getFloat(2)).head
  val id = top.getLong(1)
  val dist = top.getFloat(2)
  println(f"           query $qi%d top: id=$id%d distance=$dist%.6f")
  assert(dist < 1e-3f, s"query $qi: distance $dist should be near 0")
}
println("[example] OK")

System.exit(0)
