package com.company.ann.spark.builder

/**
 * Utility for selecting boundary nodes from a set of vectors.
 * Boundary nodes are representative vectors used for routing queries
 * to the correct local index in a multi-index setup.
 *
 * This is a static utility so it can be called from both the driver
 * and executor closures.
 */
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
