package com.company.ann.spark.search

import com.company.ann.core.index.{HNSWLibIndex, SearchResult}
import com.company.ann.spark.api.ANNIndexMetadata
import com.company.ann.spark.builder.LocalIndexMetadata
import com.company.ann.spark.util.ExecutorIndexCache
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.{ObjectInputStream}
import java.nio.file.{Files, Paths}

/**
 * ANN Searcher for querying built indexes.
 * Supports single query search, batch search, and multi-index search.
 *
 * @param spark          SparkSession instance
 * @param indexMetadata  Metadata for the loaded index
 * @param localIndexes   Map of indexId -> loaded HNSW index
 * @param globalIndex    Optional global routing index
 */
class ANNSearcher private (
  spark: SparkSession,
  val indexMetadata: ANNIndexMetadata,
  private val localIndexes: Map[String, HNSWLibIndex],
  private val globalIndex: Option[HNSWLibIndex]
) {

  private val dimension = indexMetadata.dimension

  /**
   * Search for nearest neighbors of a single query vector.
   * Runs on the driver since single-query latency is already fast.
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

    // Determine which local indexes to search
    val targetIndexIds = ANNSearcher.selectTargetIndexes(
      queryVector, nprobe, globalIndex, localIndexes, indexMetadata
    )

    // Search each target index and merge results
    val allResults = scala.collection.mutable.ArrayBuffer.empty[(Long, Float, String)]

    targetIndexIds.foreach { indexId =>
      localIndexes.get(indexId).foreach { index =>
        val results = index.search(queryVector, k, ef)
        results.foreach { r =>
          allResults += ((r.id, r.distance, indexId))
        }
      }
    }

    // Sort by distance and take top k
    val topK = allResults.sortBy(_._2).take(k)

    // Create DataFrame
    import spark.implicits._
    topK.toSeq.toDF("id", "distance", "indexId")
  }

  /**
   * Search a specific local index.
   *
   * @param indexId     ID of the local index to search
   * @param queryVector Query vector
   * @param k           Number of neighbors to return
   * @param ef          Search ef parameter
   * @return Sequence of SearchResult
   */
  def searchIndex(
    indexId: String,
    queryVector: Array[Float],
    k: Int,
    ef: Int = 50
  ): Seq[SearchResult] = {
    require(queryVector.length == dimension,
      s"Query dimension ${queryVector.length} doesn't match index dimension $dimension")

    localIndexes.get(indexId) match {
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

    // Broadcast index paths and metadata (small data)
    val indexPaths = indexMetadata.localIndexes.map(m => (m.indexId, m.indexPath)).toMap
    val bcIndexPaths = spark.sparkContext.broadcast(indexPaths)
    val bcGlobalIndexPath = spark.sparkContext.broadcast(indexMetadata.globalIndexPath)
    val bcMetadata = spark.sparkContext.broadcast(indexMetadata)
    val bcK = spark.sparkContext.broadcast(k)
    val bcNprobe = spark.sparkContext.broadcast(nprobe)
    val bcEf = spark.sparkContext.broadcast(ef)
    val bcDimension = spark.sparkContext.broadcast(dimension)

    // Convert query vectors to RDD with index
    val queriesRDD = queries.select(queryVectorColumn).rdd.zipWithIndex().map {
      case (row, idx) => (idx.toInt, row.getAs[Seq[Float]](0).toArray)
    }

    // Process queries on executors using mapPartitions
    val resultsRDD = queriesRDD.mapPartitions { iter =>
      // Load indexes once per partition (cached per executor JVM)
      val localIdxs = ExecutorIndexCache.getOrLoadLocal(bcIndexPaths.value)
      val globalIdx = bcGlobalIndexPath.value.map(ExecutorIndexCache.getOrLoadGlobal)
      val meta = bcMetadata.value
      val searchK = bcK.value
      val searchNprobe = bcNprobe.value
      val searchEf = bcEf.value

      iter.flatMap { case (queryIdx, queryVector) =>
        // Route query to target indexes
        val targetIndexIds = ANNSearcher.selectTargetIndexes(
          queryVector, searchNprobe, globalIdx, localIdxs, meta
        )

        val queryResults = scala.collection.mutable.ArrayBuffer.empty[(Long, Float, String)]
        targetIndexIds.foreach { indexId =>
          localIdxs.get(indexId).foreach { index =>
            val results = index.search(queryVector, searchK, searchEf)
            results.foreach(r => queryResults += ((r.id, r.distance, indexId)))
          }
        }

        // Take top k for this query
        queryResults.sortBy(_._2).take(searchK).map { case (id, dist, indexId) =>
          (queryIdx, id, dist, indexId)
        }
      }
    }

    import spark.implicits._
    resultsRDD.toDF("queryIndex", "id", "distance", "indexId")
  }

  /**
   * Get list of all local index IDs.
   */
  def listIndexIds: Seq[String] = localIndexes.keys.toSeq

  /**
   * Get total number of vectors in the index.
   */
  def totalVectors: Long = indexMetadata.totalVectors

  /**
   * Get index statistics.
   */
  def statistics: String = {
    s"""ANNSearcher Statistics:
       |  Total vectors: $totalVectors
       |  Dimension: $dimension
       |  Local indexes: ${localIndexes.size}
       |  Global index: ${globalIndex.isDefined}
       |""".stripMargin
  }
}

object ANNSearcher {

  /**
   * Load an ANNSearcher from disk.
   *
   * @param spark     SparkSession instance
   * @param indexPath Path to the index directory
   * @return Loaded ANNSearcher instance
   */
  def load(spark: SparkSession, indexPath: String): ANNSearcher = {
    val metadataPath = Paths.get(indexPath, "ann_index.meta")

    // Load metadata
    val ois = new ObjectInputStream(Files.newInputStream(metadataPath))
    val metadata = try {
      ois.readObject().asInstanceOf[ANNIndexMetadata]
    } finally {
      ois.close()
    }

    // Load local indexes
    val localIndexes = metadata.localIndexes.map { localMeta =>
      val index = HNSWLibIndex.load(localMeta.indexPath)
      (localMeta.indexId, index)
    }.toMap

    // Load global index if present
    val globalIndex = metadata.globalIndexPath.map { path =>
      HNSWLibIndex.load(path)
    }

    new ANNSearcher(spark, metadata, localIndexes, globalIndex)
  }

  /**
   * Create an ANNSearcher directly from metadata and loaded indexes.
   * Useful for testing or when indexes are already in memory.
   */
  def create(
    spark: SparkSession,
    metadata: ANNIndexMetadata,
    localIndexes: Map[String, HNSWLibIndex],
    globalIndex: Option[HNSWLibIndex] = None
  ): ANNSearcher = {
    new ANNSearcher(spark, metadata, localIndexes, globalIndex)
  }

  /**
   * Select which local indexes to search based on query vector.
   * Uses global routing index if available, otherwise searches all indexes.
   * This is a static method so it can be called from both driver and executor closures.
   *
   * @param queryVector  Query vector
   * @param nprobe       Number of indexes to select
   * @param globalIndex  Optional global routing index
   * @param localIndexes Map of available local indexes
   * @param metadata     Index metadata for routing lookup
   * @return List of index IDs to search
   */
  private[search] def selectTargetIndexes(
    queryVector: Array[Float],
    nprobe: Int,
    globalIndex: Option[HNSWLibIndex],
    localIndexes: Map[String, HNSWLibIndex],
    metadata: ANNIndexMetadata
  ): Seq[String] = {
    globalIndex match {
      case Some(global) =>
        // Use global index to route query to most relevant local indexes
        val routingResults = global.search(queryVector, nprobe * 2, ef = 100)

        // Extract unique index IDs from routing results
        val indexIds = routingResults.map { r =>
          findIndexIdForGlobalId(r.id, metadata)
        }.distinct.take(nprobe)

        if (indexIds.isEmpty) {
          // Fallback to all indexes if routing fails
          localIndexes.keys.take(nprobe).toSeq
        } else {
          indexIds
        }

      case None =>
        // No global index, search all local indexes
        localIndexes.keys.toSeq
    }
  }

  /**
   * Find the local index ID that contains a global vector ID.
   */
  private def findIndexIdForGlobalId(globalId: Long, metadata: ANNIndexMetadata): String = {
    metadata.localIndexes.find { meta =>
      val minId = meta.dataFiles.head.vectorOffset
      val maxId = minId + meta.totalVectors
      globalId >= minId && globalId < maxId
    }.map(_.indexId).getOrElse(metadata.localIndexes.head.indexId)
  }
}
