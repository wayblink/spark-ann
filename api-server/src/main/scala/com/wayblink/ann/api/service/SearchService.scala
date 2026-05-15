package com.wayblink.ann.api.service

import com.wayblink.ann.api.error.ApiError
import com.wayblink.ann.bundle.Routing
import com.wayblink.ann.core.index.SearchResult

/**
 * Result of a search operation on a single index.
 */
case class SingleSearchResult(
  indexId: String,
  results: Seq[SearchResult],
  queryTimeMs: Long
)

/** Result item with index information for multi-index searches. */
case class MergedResultItem(
  id: Long,
  distance: Float,
  indexId: String
)

case class MultiSearchResult(
  perIndexResults: Map[String, Seq[SearchResult]],
  merged: Seq[MergedResultItem],
  totalTimeMs: Long
)

/**
 * Service for executing search operations across bundle indexes.
 *
 * The api-server is bundle-only: every index comes from a loaded
 * bundle, and search always routes through the bundle metadata and
 * local HNSW indexes.
 */
class SearchService(indexManager: IndexManager) {

  private val DefaultEf = 50
  private val RoutingNprobe = 3

  def search(
    indexId: String,
    query: Array[Float],
    k: Int,
    ef: Option[Int] = None
  ): Either[ApiError, SingleSearchResult] = {
    if (k <= 0) return Left(ApiError.InvalidRequest("k must be positive"))

    indexManager.getBundle(indexId) match {
      case Some(bundle) => searchBundle(bundle, query, k, ef)
      case None         => Left(ApiError.IndexNotFound(indexId))
    }
  }

  /**
   * Search a bundle by routing through the global index (if any) and
   * fanning out to the selected local indexes.
   */
  private def searchBundle(
    bundle: LoadedBundleInfo,
    query: Array[Float],
    k: Int,
    ef: Option[Int]
  ): Either[ApiError, SingleSearchResult] = {
    if (query.length != bundle.dimension) {
      return Left(ApiError.DimensionMismatch(bundle.dimension, query.length))
    }
    try {
      val start = System.currentTimeMillis()
      val targetIds = Routing.selectTargetIndexes(
        queryVector = query,
        nprobe = RoutingNprobe,
        globalIndex = bundle.globalIndex,
        allIndexIds = bundle.localIndexes.keys.toSeq,
        boundaryMap = bundle.boundaryMap
      )
      val effectiveEf = ef.getOrElse(DefaultEf)
      val raw = scala.collection.mutable.ArrayBuffer.empty[SearchResult]
      targetIds.foreach { id =>
        bundle.localIndexes.get(id).foreach { idx =>
          raw ++= idx.search(query, k, effectiveEf)
        }
      }
      val merged = raw.sortBy(_.distance).take(k).toSeq
      Right(SingleSearchResult(bundle.indexId, merged, System.currentTimeMillis() - start))
    } catch {
      case e: Exception => Left(ApiError.SearchFailed(e.getMessage))
    }
  }

  // ── Multi-index search ─────────────────────────────────────────────

  def multiSearch(
    query: Array[Float],
    k: Int,
    ef: Option[Int] = None,
    indexIds: Option[Seq[String]] = None
  ): Either[ApiError, MultiSearchResult] = {
    if (k <= 0) return Left(ApiError.InvalidRequest("k must be positive"))

    val targets: Seq[LoadedBundleInfo] = indexIds match {
      case Some(ids) =>
        val missing = ids.filterNot(indexManager.exists)
        if (missing.nonEmpty) {
          return Left(ApiError.IndexNotFound(missing.head))
        }
        ids.flatMap(indexManager.getBundle)
      case None =>
        indexManager.listBundles()
    }

    if (targets.isEmpty) return Left(ApiError.NoIndexesAvailable)
    if (targets.exists(_.dimension != targets.head.dimension)) {
      return Left(ApiError.InvalidRequest("Bundles have inconsistent dimensions"))
    }
    if (query.length != targets.head.dimension) {
      return Left(ApiError.DimensionMismatch(targets.head.dimension, query.length))
    }

    try {
      val start = System.currentTimeMillis()
      val effectiveEf = ef.getOrElse(DefaultEf)

      val perIndex = scala.collection.mutable.LinkedHashMap.empty[String, Seq[SearchResult]]
      val merged   = scala.collection.mutable.ArrayBuffer.empty[MergedResultItem]

      targets.foreach { bundle =>
        val targetIds = Routing.selectTargetIndexes(
          query, RoutingNprobe, bundle.globalIndex,
          bundle.localIndexes.keys.toSeq, bundle.boundaryMap
        )
        val raw = scala.collection.mutable.ArrayBuffer.empty[SearchResult]
        targetIds.foreach { id =>
          bundle.localIndexes.get(id).foreach { idx =>
            val results = idx.search(query, k, effectiveEf)
            raw ++= results
            results.foreach(r => merged += MergedResultItem(r.id, r.distance, bundle.indexId))
          }
        }
        perIndex(bundle.indexId) = raw.sortBy(_.distance).take(k).toSeq
      }

      val topK = merged.sortBy(_.distance).take(k).toSeq
      Right(MultiSearchResult(perIndex.toMap, topK, System.currentTimeMillis() - start))
    } catch {
      case e: Exception => Left(ApiError.SearchFailed(e.getMessage))
    }
  }

  // ── Batch search (bundle only) ─────────────────────────────────────

  def batchSearch(
    indexId: String,
    queries: Seq[(Array[Float], Int)],
    ef: Option[Int] = None
  ): Either[ApiError, Seq[SingleSearchResult]] = {
    indexManager.getBundle(indexId) match {
      case Some(bundle) =>
        val invalid = queries.zipWithIndex.collect {
          case ((q, _), i) if q.length != bundle.dimension => i
        }
        if (invalid.nonEmpty) {
          return Left(ApiError.InvalidRequest(
            s"Query dimension mismatch at indices: ${invalid.mkString(", ")}" 
          ))
        }

        try {
          val results = queries.map { case (q, k) =>
            searchBundle(bundle, q, k, ef) match {
              case Right(r) => r
              case Left(err) => throw new RuntimeException(err.message)
            }
          }
          Right(results)
        } catch {
          case e: Exception => Left(ApiError.SearchFailed(e.getMessage))
        }

      case None => Left(ApiError.IndexNotFound(indexId))
    }
  }
}

object SearchService {
  def apply(indexManager: IndexManager): SearchService = new SearchService(indexManager)
}
