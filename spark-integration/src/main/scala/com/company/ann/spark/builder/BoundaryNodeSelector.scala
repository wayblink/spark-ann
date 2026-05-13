package com.company.ann.spark.builder

/**
 * Utility for selecting boundary nodes from an already-materialized set of
 * vectors. Deprecated: the streaming build path uses
 * [[StreamingBoundaryReservoir]] instead, which samples in O(k) heap during
 * a single iterator pass and never requires the full vector array in memory.
 *
 * Kept for any caller that already has an in-memory `Array[(Long,
 * Array[Float])]` and wants a deterministic even-spaced subsample.
 */
@deprecated("Use StreamingBoundaryReservoir for memory-safe sampling during streaming builds", "0.2.0")
object BoundaryNodeSelector {

  /**
   * Select boundary nodes from a set of vectors using evenly-spaced sampling.
   *
   * @param vectors     Array of (id, vector) pairs
   * @param indexId     ID of the source local index
   * @param targetCount Number of boundary nodes to select
   * @return Array of GlobalBoundaryNode
   */
  def selectBoundaryNodes(
    vectors: Array[(Long, Array[Float])],
    indexId: String,
    targetCount: Int
  ): Array[GlobalBoundaryNode] = {

    val actualCount = math.min(targetCount, vectors.length)

    if (actualCount <= 0) {
      return Array.empty
    }

    // Evenly-spaced sampling across the vector set
    val step = vectors.length.toDouble / actualCount
    (0 until actualCount).map { i =>
      val idx = (i * step).toInt
      val (localId, vector) = vectors(idx)
      GlobalBoundaryNode(
        globalId = s"$indexId:$localId",
        indexId = indexId,
        localId = localId,
        vector = vector
      )
    }.toArray
  }
}
