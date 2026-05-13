package com.wayblink.ann.spark.api

import com.wayblink.ann.core.index.HNSWConfig
import com.wayblink.ann.spark.builder.GroupingStrategy

/**
 * Configuration for building ANN indexes.
 *
 * @param M                      HNSW M parameter (connections per node)
 * @param efConstruction         HNSW efConstruction parameter (construction quality)
 * @param groupingStrategy       How to group data files for indexing
 * @param targetVectorsPerIndex  Target vectors per local index (for MergeSmall strategy)
 * @param boundaryNodesPerIndex  Number of boundary nodes per local index for global routing
 * @param distanceType           Distance metric: "euclidean" or "cosine"
 * @param pk                     Optional primary-key column name. If set, must be
 *                               parquet INT32/INT64; the value becomes the HNSW internal id
 *                               and is preserved through to search results. If unset,
 *                               internal ids are sequential offsets within each local
 *                               index (preserves the original behavior).
 */
@SerialVersionUID(3L)
case class ANNIndexConfig(
  M: Int = 16,
  efConstruction: Int = 200,
  groupingStrategy: GroupingStrategy = com.wayblink.ann.spark.builder.SingleFile,
  targetVectorsPerIndex: Long = 500000,
  boundaryNodesPerIndex: Int = 50,
  distanceType: String = "euclidean",
  pk: Option[String] = None
) extends Serializable {
  /**
   * Convert to core HNSWConfig.
   */
  def toHNSWConfig(maxElements: Int = 1000000): HNSWConfig = {
    HNSWConfig(
      M = M,
      efConstruction = efConstruction,
      maxElements = maxElements
    )
  }
}

/**
 * Statistics about a built ANN index.
 *
 * @param totalVectors      Total number of vectors indexed
 * @param totalFiles        Total number of data files covered
 * @param numLocalIndexes   Number of local indexes built
 * @param dimension         Vector dimensionality
 * @param buildTimeMs       Index build time in milliseconds
 */
@SerialVersionUID(1L)
case class ANNIndexStatistics(
  totalVectors: Long,
  totalFiles: Int,
  numLocalIndexes: Int,
  dimension: Int,
  buildTimeMs: Long
) extends Serializable {
  override def toString: String = {
    s"""ANNIndexStatistics(
       |  totalVectors: $totalVectors,
       |  totalFiles: $totalFiles,
       |  numLocalIndexes: $numLocalIndexes,
       |  dimension: $dimension,
       |  buildTimeMs: $buildTimeMs
       |)""".stripMargin
  }
}

/**
 * Metadata for a complete ANN index (local indexes + global routing index).
 *
 * @param indexPath      Base path where the index is stored
 * @param localIndexes   Metadata for each local index
 * @param globalIndexPath Path to the global routing index (if built)
 * @param config         Configuration used to build the index
 * @param statistics     Build statistics
 * @param createdAt      Timestamp when the index was created
 */
@SerialVersionUID(1L)
case class ANNIndexMetadata(
  indexPath: String,
  localIndexes: Array[com.wayblink.ann.spark.builder.LocalIndexMetadata],
  globalIndexPath: Option[String],
  config: ANNIndexConfig,
  statistics: ANNIndexStatistics,
  createdAt: Long = System.currentTimeMillis()
) extends Serializable {
  /**
   * Get total number of vectors in the index.
   */
  def totalVectors: Long = statistics.totalVectors

  /**
   * Get the dimension of vectors in the index.
   */
  def dimension: Int = statistics.dimension

  override def toString: String = {
    s"ANNIndexMetadata($indexPath, ${localIndexes.length} local indexes, ${statistics.totalVectors} vectors)"
  }
}
