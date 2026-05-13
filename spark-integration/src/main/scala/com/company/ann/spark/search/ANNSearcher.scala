package com.company.ann.spark.search

import com.company.ann.core.index.{HNSWLibIndex, SearchResult}
import com.company.ann.spark.api.ANNIndexMetadata
import com.company.ann.spark.builder.{LocalIndexMetadata, MetadataJson}
import com.company.ann.spark.util.{DriverIndexCache, ExecutorIndexCache}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Paths

/**
 * ANN Searcher for querying built indexes.
 *
 * Driver-side state is intentionally lightweight: metadata and boundary
 * routing map only. HNSW indexes themselves are loaded lazily on first
 * access — via [[DriverIndexCache]] for single-query `search`, via
 * [[ExecutorIndexCache]] for distributed `batchSearch`. This keeps the
 * driver heap bounded even when the full index set is orders of magnitude
 * larger than the driver's memory.
 *
 * `preloadedLocal` and `preloadedGlobal` exist for tests and small
 * interactive sessions that want to skip the cache.
 */
class ANNSearcher private (
  spark: SparkSession,
  val indexMetadata: ANNIndexMetadata,
  private val preloadedLocal: Map[String, HNSWLibIndex],
  private val preloadedGlobal: Option[HNSWLibIndex],
  private val boundaryMap: Array[String]
) {

  private val dimension = indexMetadata.dimension
  private val localIndexPaths: Map[String, String] =
    indexMetadata.localIndexes.map(m => m.indexId -> m.indexPath).toMap
  private val allIndexIds: Seq[String] =
    indexMetadata.localIndexes.map(_.indexId).toSeq

  private def resolveLocal(indexId: String): Option[HNSWLibIndex] = {
    preloadedLocal.get(indexId).orElse {
      localIndexPaths.get(indexId).map { path =>
        DriverIndexCache.getOrLoadLocal(indexId, path)
      }
    }
  }

  private def resolveGlobal(): Option[HNSWLibIndex] = {
    preloadedGlobal.orElse {
      indexMetadata.globalIndexPath.map(DriverIndexCache.getOrLoadGlobal)
    }
  }

  /**
   * Search for nearest neighbors of a single query vector. Loads only the
   * target local indexes selected by the routing step (or, in the no-global
   * fallback, all indexes, which is why global routing matters at scale).
   *
   * @param queryVector Query vector
   * @param k           Number of neighbors to return
   * @param nprobe      Number of local indexes to search (for routing)
   * @param ef          Search ef parameter (higher = more accurate but slower)
   * @return DataFrame with columns: id, distance, indexId
   */
  def search(
    queryVector: Array[Float],
    k: Int,
    nprobe: Int = 3,
    ef: Int = 50
  ): DataFrame = {
    require(queryVector.length == dimension,
      s"Query dimension ${queryVector.length} doesn't match index dimension $dimension")
    require(k > 0, "k must be positive")

    val targetIndexIds = ANNSearcher.selectTargetIndexes(
      queryVector, nprobe, resolveGlobal(), allIndexIds, boundaryMap
    )

    val allResults = scala.collection.mutable.ArrayBuffer.empty[(Long, Float, String)]
    targetIndexIds.foreach { indexId =>
      resolveLocal(indexId).foreach { index =>
        index.search(queryVector, k, ef).foreach { r =>
          allResults += ((r.id, r.distance, indexId))
        }
      }
    }

    val topK = allResults.sortBy(_._2).take(k)
    val localSpark = spark
    import localSpark.implicits._
    topK.toSeq.toDF("id", "distance", "indexId")
  }

  /**
   * Search a specific local index. Loads the index on demand via the
   * driver cache if not already resident.
   */
  def searchIndex(
    indexId: String,
    queryVector: Array[Float],
    k: Int,
    ef: Int = 50
  ): Seq[SearchResult] = {
    require(queryVector.length == dimension,
      s"Query dimension ${queryVector.length} doesn't match index dimension $dimension")

    resolveLocal(indexId) match {
      case Some(index) => index.search(queryVector, k, ef)
      case None => throw new IllegalArgumentException(s"Index '$indexId' not found")
    }
  }

  /**
   * Batch search for multiple query vectors.
   * Distributes query processing across Spark executors using mapPartitions.
   * Each executor loads indexes from shared storage and processes its partition of queries.
   *
   * @param queries           DataFrame containing query vectors
   * @param queryVectorColumn Name of the column containing query vectors
   * @param k                 Number of neighbors per query
   * @param nprobe            Number of local indexes to search per query
   * @param ef                Search ef parameter
   * @return DataFrame with columns: queryIndex, id, distance, indexId
   */
  def batchSearch(
    queries: DataFrame,
    queryVectorColumn: String,
    k: Int,
    nprobe: Int = 3,
    ef: Int = 50
  ): DataFrame = {
    require(k > 0, "k must be positive")

    val bcIndexPaths = spark.sparkContext.broadcast(localIndexPaths)
    val bcGlobalIndexPath = spark.sparkContext.broadcast(indexMetadata.globalIndexPath)
    val bcBoundaryMap = spark.sparkContext.broadcast(boundaryMap)
    val bcAllIndexIds = spark.sparkContext.broadcast(allIndexIds)
    val bcK = spark.sparkContext.broadcast(k)
    val bcNprobe = spark.sparkContext.broadcast(nprobe)
    val bcEf = spark.sparkContext.broadcast(ef)

    val queriesRDD = queries.select(queryVectorColumn).rdd.zipWithIndex().map {
      case (row, idx) =>
        // Python-created DataFrames store float lists as DOUBLE; Scala
        // Array[Float] ends up as FLOAT. Cover both by reading Any and
        // coercing each element.
        val raw = row.get(0).asInstanceOf[Seq[_]]
        val arr = new Array[Float](raw.size)
        var i = 0
        raw.foreach { v =>
          arr(i) = v match {
            case f: java.lang.Float  => f.floatValue()
            case d: java.lang.Double => d.doubleValue().toFloat
            case n: Number           => n.floatValue()
            case other =>
              throw new IllegalArgumentException(
                s"Unsupported vector element type: ${other.getClass}")
          }
          i += 1
        }
        (idx.toInt, arr)
    }

    val resultsRDD = queriesRDD.mapPartitions { iter =>
      val localIdxs = ExecutorIndexCache.getOrLoadLocal(bcIndexPaths.value)
      val globalIdx = bcGlobalIndexPath.value.map(ExecutorIndexCache.getOrLoadGlobal)
      val boundary = bcBoundaryMap.value
      val allIds = bcAllIndexIds.value
      val searchK = bcK.value
      val searchNprobe = bcNprobe.value
      val searchEf = bcEf.value

      iter.flatMap { case (queryIdx, queryVector) =>
        val targetIndexIds = ANNSearcher.selectTargetIndexes(
          queryVector, searchNprobe, globalIdx, allIds, boundary
        )

        val queryResults = scala.collection.mutable.ArrayBuffer.empty[(Long, Float, String)]
        targetIndexIds.foreach { indexId =>
          localIdxs.get(indexId).foreach { index =>
            index.search(queryVector, searchK, searchEf).foreach { r =>
              queryResults += ((r.id, r.distance, indexId))
            }
          }
        }

        queryResults.sortBy(_._2).take(searchK).map { case (id, dist, indexId) =>
          (queryIdx, id, dist, indexId)
        }
      }
    }

    val localSpark = spark
    import localSpark.implicits._
    resultsRDD.toDF("queryIndex", "id", "distance", "indexId")
  }

  /**
   * Get list of all local index IDs (from metadata; does not require
   * indexes to be loaded).
   */
  def listIndexIds: Seq[String] = allIndexIds

  /**
   * Total number of vectors across all local indexes.
   */
  def totalVectors: Long = indexMetadata.totalVectors

  /**
   * Get index statistics.
   */
  def statistics: String = {
    s"""ANNSearcher Statistics:
       |  Total vectors: $totalVectors
       |  Dimension: $dimension
       |  Local indexes (metadata): ${allIndexIds.length}
       |  Local indexes (preloaded): ${preloadedLocal.size}
       |  Global index: ${indexMetadata.globalIndexPath.isDefined}
       |""".stripMargin
  }
}

object ANNSearcher {

  /**
   * Load an ANNSearcher from disk. Reads only JSON metadata and the
   * boundary routing map; HNSW indexes are loaded lazily on first
   * access. Calling this against a large index set is O(metadata size),
   * not O(indexes).
   */
  def load(spark: SparkSession, indexPath: String): ANNSearcher = {
    val metadataPath = Paths.get(indexPath, "ann_index.json")
    val metadata = MetadataJson.readMetadata(metadataPath)

    val boundaryMap: Array[String] = if (metadata.globalIndexPath.isDefined) {
      val mappingPath = Paths.get(indexPath, "global", "boundary_mapping.json")
      val entries = MetadataJson.readBoundaryMapping(mappingPath)
      val arr = new Array[String](entries.length)
      var i = 0
      while (i < entries.length) {
        val e = entries(i)
        arr(e.globalId) = e.indexId
        i += 1
      }
      arr
    } else {
      Array.empty[String]
    }

    new ANNSearcher(spark, metadata, Map.empty, None, boundaryMap)
  }

  /**
   * Create an ANNSearcher directly from metadata and already-loaded
   * indexes. Useful for tests or when indexes are pre-materialized in
   * memory.
   */
  def create(
    spark: SparkSession,
    metadata: ANNIndexMetadata,
    localIndexes: Map[String, HNSWLibIndex],
    globalIndex: Option[HNSWLibIndex] = None,
    boundaryMap: Array[String] = Array.empty
  ): ANNSearcher = {
    new ANNSearcher(spark, metadata, localIndexes, globalIndex, boundaryMap)
  }

  /**
   * Select which local indexes to search based on query vector.
   *
   * @param queryVector  Query vector
   * @param nprobe       Number of indexes to select
   * @param globalIndex  Optional global routing index
   * @param allIndexIds  All known local indexIds (used as fallback when
   *                     routing is unavailable or yields nothing)
   * @param boundaryMap  Array indexed by global routing id, value is
   *                     source local indexId
   */
  private[search] def selectTargetIndexes(
    queryVector: Array[Float],
    nprobe: Int,
    globalIndex: Option[HNSWLibIndex],
    allIndexIds: Seq[String],
    boundaryMap: Array[String]
  ): Seq[String] = {
    globalIndex match {
      case Some(global) if boundaryMap.nonEmpty =>
        val routingResults = global.search(queryVector, nprobe * 2, ef = 100)
        val indexIds = routingResults.flatMap { r =>
          val gid = r.id.toInt
          if (gid >= 0 && gid < boundaryMap.length) Some(boundaryMap(gid)) else None
        }.distinct.take(nprobe)

        if (indexIds.isEmpty) allIndexIds else indexIds

      case _ =>
        allIndexIds
    }
  }
}
