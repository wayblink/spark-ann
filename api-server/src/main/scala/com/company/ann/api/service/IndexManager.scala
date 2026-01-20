package com.company.ann.api.service

import com.company.ann.core.index.{HNSWConfig, HNSWLibIndex}

import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Metadata about a loaded index.
 *
 * @param indexId    Unique identifier for the index
 * @param index      The HNSW index instance
 * @param indexPath  Path where the index is stored (if loaded from disk)
 * @param distanceType Distance metric used
 * @param loadedAt   Timestamp when the index was loaded
 */
case class LoadedIndexInfo(
  indexId: String,
  index: HNSWLibIndex,
  indexPath: Option[String],
  distanceType: String,
  loadedAt: Long = System.currentTimeMillis()
)

/**
 * Thread-safe manager for in-memory HNSW indexes.
 * Provides operations to load, unload, and query indexes.
 */
class IndexManager {

  private val indexes = new ConcurrentHashMap[String, LoadedIndexInfo]()

  /**
   * Load an index from disk.
   *
   * @param indexId   Unique identifier for the index
   * @param indexPath Path to the index file
   * @return Either an error message or the loaded index info
   */
  def loadIndex(indexId: String, indexPath: String): Either[String, LoadedIndexInfo] = {
    if (indexes.containsKey(indexId)) {
      return Left(s"Index '$indexId' already exists")
    }

    try {
      val index = HNSWLibIndex.load(indexPath)
      val info = LoadedIndexInfo(
        indexId = indexId,
        index = index,
        indexPath = Some(indexPath),
        distanceType = "euclidean" // TODO: read from metadata
      )
      indexes.put(indexId, info)
      Right(info)
    } catch {
      case e: Exception =>
        Left(s"Failed to load index from '$indexPath': ${e.getMessage}")
    }
  }

  /**
   * Create a new index from vectors.
   *
   * @param indexId        Unique identifier for the index
   * @param dimension      Vector dimension
   * @param vectors        Vectors to add (id, vector pairs)
   * @param config         HNSW configuration
   * @param distanceType   Distance metric ("euclidean" or "cosine")
   * @return Either an error message or the created index info
   */
  def createIndex(
    indexId: String,
    dimension: Int,
    vectors: Seq[(Long, Array[Float])],
    config: HNSWConfig = HNSWConfig(),
    distanceType: String = "euclidean"
  ): Either[String, LoadedIndexInfo] = {
    if (indexes.containsKey(indexId)) {
      return Left(s"Index '$indexId' already exists")
    }

    try {
      val maxElements = math.max(config.maxElements, vectors.size)
      val adjustedConfig = config.copy(maxElements = maxElements)
      val index = HNSWLibIndex(dimension, adjustedConfig, distanceType)
      index.addAll(vectors)

      val info = LoadedIndexInfo(
        indexId = indexId,
        index = index,
        indexPath = None,
        distanceType = distanceType
      )
      indexes.put(indexId, info)
      Right(info)
    } catch {
      case e: Exception =>
        Left(s"Failed to create index: ${e.getMessage}")
    }
  }

  /**
   * Get an index by ID.
   */
  def getIndex(indexId: String): Option[LoadedIndexInfo] = {
    Option(indexes.get(indexId))
  }

  /**
   * List all loaded indexes.
   */
  def listIndexes(): Seq[LoadedIndexInfo] = {
    indexes.values().asScala.toSeq
  }

  /**
   * Unload an index from memory.
   *
   * @param indexId Index to unload
   * @return true if the index was unloaded, false if it didn't exist
   */
  def unloadIndex(indexId: String): Boolean = {
    indexes.remove(indexId) != null
  }

  /**
   * Save an index to disk.
   *
   * @param indexId Index to save
   * @param path    Path to save to
   * @return Either an error message or success
   */
  def saveIndex(indexId: String, path: String): Either[String, Unit] = {
    getIndex(indexId) match {
      case Some(info) =>
        try {
          info.index.save(path)
          // Update the index info with the new path
          val updated = info.copy(indexPath = Some(path))
          indexes.put(indexId, updated)
          Right(())
        } catch {
          case e: Exception =>
            Left(s"Failed to save index: ${e.getMessage}")
        }
      case None =>
        Left(s"Index '$indexId' not found")
    }
  }

  /**
   * Add vectors to an existing index.
   *
   * @param indexId Index to add vectors to
   * @param vectors Vectors to add
   * @return Either an error message or the updated index info
   */
  def addVectors(indexId: String, vectors: Seq[(Long, Array[Float])]): Either[String, LoadedIndexInfo] = {
    getIndex(indexId) match {
      case Some(info) =>
        try {
          info.index.addAll(vectors)
          Right(info)
        } catch {
          case e: Exception =>
            Left(s"Failed to add vectors: ${e.getMessage}")
        }
      case None =>
        Left(s"Index '$indexId' not found")
    }
  }

  /**
   * Get the total number of loaded indexes.
   */
  def indexCount: Int = indexes.size()

  /**
   * Get the total number of vectors across all loaded indexes.
   */
  def totalVectors: Long = {
    indexes.values().asScala.map(_.index.size.toLong).sum
  }

  /**
   * Check if an index exists.
   */
  def exists(indexId: String): Boolean = {
    indexes.containsKey(indexId)
  }
}

object IndexManager {
  /**
   * Create a new IndexManager instance.
   */
  def apply(): IndexManager = new IndexManager()
}
