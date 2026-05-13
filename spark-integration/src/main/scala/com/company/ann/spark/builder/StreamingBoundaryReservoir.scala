package com.company.ann.spark.builder

import scala.util.Random

/**
 * Reservoir sampler (Vitter Algorithm R) for boundary-node selection during
 * a streaming HNSW build. The reservoir maintains exactly `targetCount`
 * uniformly-sampled vectors regardless of stream length, in O(1) space per
 * slot, so the boundary-sampling step adds no heap pressure to scale-out
 * builds.
 *
 * Thread-safety: NOT thread-safe. One reservoir per build task; the build
 * task itself runs single-threaded inside a Spark task closure.
 *
 * @param targetCount Number of boundary nodes to retain
 * @param seed        Random seed for deterministic sampling
 */
final class StreamingBoundaryReservoir(
  targetCount: Int,
  seed: Long = 42L
) {
  require(targetCount >= 0, s"targetCount must be non-negative, got $targetCount")

  private val ids = new Array[Long](targetCount)
  private val vectors = new Array[Array[Float]](targetCount)
  private val rng = new Random(seed)
  private var seen: Long = 0L
  private var filled: Int = 0

  /**
   * Offer the next vector from the stream to the reservoir. Vectors are
   * retained or evicted according to Algorithm R; the caller passes the
   * vector by reference and must NOT mutate it afterwards (the reservoir
   * may store the same array).
   */
  def offer(id: Long, vector: Array[Float]): Unit = {
    if (targetCount == 0) {
      seen += 1
      return
    }
    if (filled < targetCount) {
      ids(filled) = id
      vectors(filled) = vector
      filled += 1
    } else {
      // Algorithm R: replace a random reservoir slot with probability
      // targetCount / seen. seen is 1-indexed here (this is the
      // (seen+1)-th vector).
      val j = (rng.nextDouble() * (seen + 1)).toLong
      if (j < targetCount) {
        ids(j.toInt) = id
        vectors(j.toInt) = vector
      }
    }
    seen += 1
  }

  /**
   * Materialize the boundary nodes for the given source local index. Called
   * once at the end of the build pass. The returned array is freshly
   * allocated and does not alias the reservoir's internal storage for the
   * vectors themselves (the vector arrays inside are still shared).
   */
  def result(indexId: String): Array[GlobalBoundaryNode] = {
    val out = new Array[GlobalBoundaryNode](filled)
    var i = 0
    while (i < filled) {
      out(i) = GlobalBoundaryNode(
        globalId = s"$indexId:${ids(i)}",
        indexId = indexId,
        localId = ids(i),
        vector = vectors(i)
      )
      i += 1
    }
    out
  }

  /**
   * Number of vectors currently held in the reservoir. Equals min(seen, targetCount).
   */
  def size: Int = filled

  /**
   * Total vectors offered to the reservoir so far.
   */
  def totalSeen: Long = seen
}
