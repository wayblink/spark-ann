package com.wayblink.ann.bundle

import com.wayblink.ann.core.index.HNSWLibIndex

/**
 * Pure routing logic shared between the offline Spark batchSearch path
 * and online runtimes (api-server, future C++/Rust servers). Lives in
 * the bundle module so neither caller has to drag along the other's
 * runtime.
 */
object Routing {

  /**
   * Given a query vector, decide which local indexes to search.
   *
   * When a global routing index is available and the boundary map is
   * non-empty: search the routing index, then translate the global ids
   * to local indexIds via O(1) array indexing. The boundary map is
   * positional — array index i holds the indexId of the local index
   * that contributed boundary node i.
   *
   * Falls back to "search every local index" when routing is
   * unavailable or yields nothing usable. That fallback exists for
   * correctness; production deployments should always have a global
   * routing index when there are >1 local indexes.
   *
   * @param queryVector  Query
   * @param nprobe       Number of distinct local indexes to return
   * @param globalIndex  Optional routing HNSW (boundary nodes only)
   * @param allIndexIds  All local indexIds known to the bundle, used
   *                     as the fallback when routing yields nothing
   * @param boundaryMap  Positional array, index = global routing id,
   *                     value = source local indexId
   */
  def selectTargetIndexes(
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
