package com.company.ann.api.service

import com.company.ann.core.index.SearchResult

/**
 * Result of a search operation on a single index.
 *
 * @param indexId     Index that was searched
 * @param results     Search results
 * @param queryTimeMs Time taken to execute the search
 */
case class SingleSearchResult(
  indexId: String,
  results: Seq[SearchResult],
  queryTimeMs: Long
)

/**
 * Result item with index information for multi-index searches.
 *
 * @param id        Vector ID
 * @param distance  Distance from query
 * @param indexId   Source index ID
 */
case class MergedResultItem(
  id: Long,
  distance: Float,
  indexId: String
)

/**
 * Result of a multi-index search operation.
 *
 * @param perIndexResults Results from each index
 * @param merged          Merged and sorted results
 * @param totalTimeMs     Total time taken
 */
case class MultiSearchResult(
  perIndexResults: Map[String, Seq[SearchResult]],
  merged: Seq[MergedResultItem],
  totalTimeMs: Long
)

/**
 * Service for executing search operations across indexes.
 *
 * @param indexManager The index manager containing loaded indexes
 */
class SearchService(indexManager: IndexManager) {

  private val DefaultEf = 50

  /**
   * Search a single index for nearest neighbors.
   *
   * @param indexId Index to search
   * @param query   Query vector
   * @param k       Number of neighbors to return
   * @param ef      Search ef parameter (optional)
   * @return Either an error message or the search result
   */
  def search(
    indexId: String,
    query: Array[Float],
    k: Int,
    ef: Option[Int] = None
  ): Either[String, SingleSearchResult] = {
    indexManager.getIndex(indexId) match {
      case Some(info) =>
        // Validate dimension
        if (query.length != info.index.dimension) {
          return Left(s"Query dimension (${query.length}) does not match index dimension (${info.index.dimension})")
        }

        // Validate k
        if (k <= 0) {
          return Left("k must be positive")
        }

        try {
          val startTime = System.currentTimeMillis()
          val results = info.index.search(query, k, ef.getOrElse(DefaultEf))
          val queryTime = System.currentTimeMillis() - startTime

          Right(SingleSearchResult(indexId, results, queryTime))
        } catch {
          case e: Exception =>
            Left(s"Search failed: ${e.getMessage}")
        }

      case None =>
        Left(s"Index '$indexId' not found")
    }
  }

  /**
   * Search multiple indexes and merge results.
   *
   * @param query    Query vector
   * @param k        Number of neighbors per index
   * @param ef       Search ef parameter (optional)
   * @param indexIds Specific indexes to search (None = all indexes)
   * @return Either an error message or the multi-search result
   */
  def multiSearch(
    query: Array[Float],
    k: Int,
    ef: Option[Int] = None,
    indexIds: Option[Seq[String]] = None
  ): Either[String, MultiSearchResult] = {
    val startTime = System.currentTimeMillis()

    // Determine which indexes to search
    val targetIndexes = indexIds match {
      case Some(ids) =>
        // Validate all specified indexes exist
        val missing = ids.filterNot(indexManager.exists)
        if (missing.nonEmpty) {
          return Left(s"Indexes not found: ${missing.mkString(", ")}")
        }
        ids.flatMap(indexManager.getIndex)
      case None =>
        indexManager.listIndexes()
    }

    if (targetIndexes.isEmpty) {
      return Left("No indexes available to search")
    }

    // Validate dimension consistency
    val dimensions = targetIndexes.map(_.index.dimension).distinct
    if (dimensions.size > 1) {
      return Left(s"Indexes have inconsistent dimensions: ${dimensions.mkString(", ")}")
    }

    val expectedDim = dimensions.head
    if (query.length != expectedDim) {
      return Left(s"Query dimension (${query.length}) does not match index dimension ($expectedDim)")
    }

    // Validate k
    if (k <= 0) {
      return Left("k must be positive")
    }

    try {
      // Search all indexes
      val perIndexResults = targetIndexes.map { info =>
        val results = info.index.search(query, k, ef.getOrElse(DefaultEf))
        info.indexId -> results
      }.toMap

      // Merge and sort results by distance
      val merged = perIndexResults.flatMap { case (indexId, results) =>
        results.map(r => MergedResultItem(r.id, r.distance, indexId))
      }.toSeq.sortBy(_.distance).take(k)

      val totalTime = System.currentTimeMillis() - startTime

      Right(MultiSearchResult(perIndexResults, merged, totalTime))
    } catch {
      case e: Exception =>
        Left(s"Multi-search failed: ${e.getMessage}")
    }
  }

  /**
   * Batch search on a single index with multiple queries.
   *
   * @param indexId Index to search
   * @param queries Query vectors with individual k values
   * @param ef      Search ef parameter (optional, applies to all queries)
   * @return Either an error message or results for each query
   */
  def batchSearch(
    indexId: String,
    queries: Seq[(Array[Float], Int)],
    ef: Option[Int] = None
  ): Either[String, Seq[SingleSearchResult]] = {
    indexManager.getIndex(indexId) match {
      case Some(info) =>
        val startTime = System.currentTimeMillis()

        // Validate all query dimensions
        val invalidQueries = queries.zipWithIndex.filter { case ((q, _), _) =>
          q.length != info.index.dimension
        }
        if (invalidQueries.nonEmpty) {
          val indices = invalidQueries.map(_._2).mkString(", ")
          return Left(s"Query dimension mismatch at indices: $indices")
        }

        try {
          val results = queries.map { case (query, k) =>
            val queryStart = System.currentTimeMillis()
            val searchResults = info.index.search(query, k, ef.getOrElse(DefaultEf))
            val queryTime = System.currentTimeMillis() - queryStart
            SingleSearchResult(indexId, searchResults, queryTime)
          }
          Right(results)
        } catch {
          case e: Exception =>
            Left(s"Batch search failed: ${e.getMessage}")
        }

      case None =>
        Left(s"Index '$indexId' not found")
    }
  }
}

object SearchService {
  def apply(indexManager: IndexManager): SearchService = new SearchService(indexManager)
}
