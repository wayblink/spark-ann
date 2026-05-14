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
 * Service for executing search operations across indexes.
 *
 * Service-layer errors are typed ([[ApiError]]); the route layer maps
 * them to HTTP status codes. This replaces the legacy
 * `Either[String, _]` shape whose error routing relied on
 * substring-matching the message text (todo.md #6).
 *
 * Dispatches between flat (single-HNSW) and bundle (routed multi-HNSW)
 * indexes via [[IndexManager.getEntry]] — callers don't need to know
 * which mode an indexId is in.
 */
class SearchService(indexManager: IndexManager) {

  private val DefaultEf = 50
  private val RoutingNprobe = 3

  // ── Single-index search ────────────────────────────────────────────

  def search(
    indexId: String,
    query: Array[Float],
    k: Int,
    ef: Option[Int] = None
  ): Either[ApiError, SingleSearchResult] = {
    if (k <= 0) return Left(ApiError.InvalidRequest("k must be positive"))

    indexManager.getEntry(indexId) match {
      case Some(IndexEntry.Flat(info)) =>
        searchFlat(info, query, k, ef)
      case Some(IndexEntry.Bundle(info)) =>
        searchBundle(info, query, k, ef)
      case None =>
        Left(ApiError.IndexNotFound(indexId))
    }
  }

  private def searchFlat(
    info: LoadedIndexInfo,
    query: Array[Float],
    k: Int,
    ef: Option[Int]
  ): Either[ApiError, SingleSearchResult] = {
    if (query.length != info.index.dimension) {
      return Left(ApiError.DimensionMismatch(info.index.dimension, query.length))
    }
    try {
      val start = System.currentTimeMillis()
      val results = info.index.search(query, k, ef.getOrElse(DefaultEf))
      Right(SingleSearchResult(info.indexId, results, System.currentTimeMillis() - start))
    } catch {
      case e: Exception => Left(ApiError.SearchFailed(e.getMessage))
    }
  }

  /**
   * Search a bundle by routing through the global index (if any) and
   * fanning out to the selected local indexes. Results are tagged with
   * the local indexId via SearchResult's id alone — callers needing
   * (id, indexId) pairs should prefer the multiSearch shape.
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
      // Merge across the probed local indexes; we lose the
      // per-local indexId tag here, but it's preserved at the
      // multiSearch entry point. The flat-style single-search shape
      // intentionally returns SearchResult to keep wire compat.
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

    val targets: Seq[IndexEntry] = indexIds match {
      case Some(ids) =>
        val missing = ids.filterNot(indexManager.exists)
        if (missing.nonEmpty) {
          // Surface the first missing id; the legacy behaviour listed
          // them all in a single string, but a typed error is clearer.
          return Left(ApiError.IndexNotFound(missing.head))
        }
        ids.flatMap(indexManager.getEntry)
      case None =>
        indexManager.listEntries()
    }

    if (targets.isEmpty) return Left(ApiError.NoIndexesAvailable)

    // Dimension check across the union of underlying indexes.
    val dims = targets.flatMap {
      case IndexEntry.Flat(i)   => Seq(i.index.dimension)
      case IndexEntry.Bundle(b) => Seq(b.dimension)
    }.distinct
    if (dims.size > 1) {
      return Left(ApiError.InvalidRequest(s"Indexes have inconsistent dimensions: ${dims.mkString(", ")}"))
    }
    if (query.length != dims.head) {
      return Left(ApiError.DimensionMismatch(dims.head, query.length))
    }

    try {
      val start = System.currentTimeMillis()
      val effectiveEf = ef.getOrElse(DefaultEf)

      val perIndex = scala.collection.mutable.LinkedHashMap.empty[String, Seq[SearchResult]]
      val merged   = scala.collection.mutable.ArrayBuffer.empty[MergedResultItem]

      targets.foreach {
        case IndexEntry.Flat(info) =>
          val results = info.index.search(query, k, effectiveEf)
          perIndex(info.indexId) = results
          results.foreach(r => merged += MergedResultItem(r.id, r.distance, info.indexId))
        case IndexEntry.Bundle(bundle) =>
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

  // ── Batch search (single index only) ───────────────────────────────

  def batchSearch(
    indexId: String,
    queries: Seq[(Array[Float], Int)],
    ef: Option[Int] = None
  ): Either[ApiError, Seq[SingleSearchResult]] = {
    indexManager.getEntry(indexId) match {
      case Some(entry) =>
        val dim = entry match {
          case IndexEntry.Flat(i)   => i.index.dimension
          case IndexEntry.Bundle(b) => b.dimension
        }
        val invalid = queries.zipWithIndex.collect {
          case ((q, _), i) if q.length != dim => i
        }
        if (invalid.nonEmpty) {
          return Left(ApiError.InvalidRequest(
            s"Query dimension mismatch at indices: ${invalid.mkString(", ")}"
          ))
        }

        try {
          val results = queries.map { case (q, k) =>
            val single = entry match {
              case IndexEntry.Flat(info)   => searchFlat(info, q, k, ef)
              case IndexEntry.Bundle(info) => searchBundle(info, q, k, ef)
            }
            single match {
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
