package com.company.ann.api.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive0, Route}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.company.ann.api.model._
import com.company.ann.api.model.ApiJsonProtocol._
import com.company.ann.api.service.{SearchService, IndexManager}

/**
 * HTTP routes for search operations.
 *
 * @param searchService The search service for executing queries
 * @param indexManager  The index manager for validation
 */
class SearchRoutes(searchService: SearchService, indexManager: IndexManager) {

  val routes: Route = concat(
    // POST /indexes/{indexId}/search - Search a single index
    pathPrefix("indexes" / Segment / "search") { indexId =>
      post {
        entity(as[SearchRequest]) { request =>
          validateSearchRequest(request) {
            searchService.search(indexId, request.vector, request.k, request.ef) match {
              case Right(result) =>
                val response = SearchResponse(
                  indexId = result.indexId,
                  results = result.results.map(r => SearchResultItem(r.id, r.distance)),
                  queryTimeMs = result.queryTimeMs
                )
                complete(response)
              case Left(error) if error.contains("not found") =>
                complete(StatusCodes.NotFound -> ErrorResponse("IndexNotFound", error))
              case Left(error) if error.contains("dimension") =>
                complete(StatusCodes.UnprocessableEntity -> ErrorResponse("DimensionMismatch", error))
              case Left(error) =>
                complete(StatusCodes.InternalServerError -> ErrorResponse("SearchError", error))
            }
          }
        }
      }
    },
    // POST /search - Multi-index search
    path("search") {
      post {
        entity(as[MultiSearchRequest]) { request =>
          validateSearchRequest(SearchRequest(request.vector, request.k, request.ef)) {
            searchService.multiSearch(request.vector, request.k, request.ef, request.indexIds) match {
              case Right(result) =>
                val response = MultiSearchResponse(
                  results = result.perIndexResults.map { case (indexId, results) =>
                    indexId -> results.map(r => SearchResultItem(r.id, r.distance))
                  },
                  merged = result.merged.map(r => MergedSearchResultItem(r.id, r.distance, r.indexId)),
                  totalTimeMs = result.totalTimeMs
                )
                complete(response)
              case Left(error) if error.contains("not found") =>
                complete(StatusCodes.NotFound -> ErrorResponse("IndexNotFound", error))
              case Left(error) if error.contains("dimension") =>
                complete(StatusCodes.UnprocessableEntity -> ErrorResponse("DimensionMismatch", error))
              case Left(error) if error.contains("No indexes") =>
                complete(StatusCodes.BadRequest -> ErrorResponse("NoIndexes", error))
              case Left(error) =>
                complete(StatusCodes.InternalServerError -> ErrorResponse("SearchError", error))
            }
          }
        }
      }
    },
    // POST /search/batch - Batch search
    path("search" / "batch") {
      post {
        entity(as[BatchSearchRequest]) { request =>
          if (request.queries.isEmpty) {
            complete(StatusCodes.BadRequest -> ErrorResponse("InvalidRequest", "queries cannot be empty"))
          } else {
            val queries = request.queries.map(q => (q.vector, q.k))
            searchService.batchSearch(request.indexId, queries, request.ef) match {
              case Right(results) =>
                val response = BatchSearchResponse(
                  results = results.zipWithIndex.map { case (r, idx) =>
                    BatchSearchResultItem(
                      queryIndex = idx,
                      results = r.results.map(sr => SearchResultItem(sr.id, sr.distance))
                    )
                  },
                  totalTimeMs = results.map(_.queryTimeMs).sum
                )
                complete(response)
              case Left(error) if error.contains("not found") =>
                complete(StatusCodes.NotFound -> ErrorResponse("IndexNotFound", error))
              case Left(error) if error.contains("dimension") =>
                complete(StatusCodes.UnprocessableEntity -> ErrorResponse("DimensionMismatch", error))
              case Left(error) =>
                complete(StatusCodes.InternalServerError -> ErrorResponse("SearchError", error))
            }
          }
        }
      }
    }
  )

  /**
   * Validate basic search request parameters.
   */
  private def validateSearchRequest(request: SearchRequest): Directive0 = {
    if (request.vector.isEmpty) {
      complete(StatusCodes.BadRequest -> ErrorResponse("InvalidVector", "vector cannot be empty"))
    } else if (request.k <= 0) {
      complete(StatusCodes.BadRequest -> ErrorResponse("InvalidParameter", "k must be positive"))
    } else if (request.k > 1000) {
      complete(StatusCodes.BadRequest -> ErrorResponse("InvalidParameter", "k cannot exceed 1000"))
    } else if (request.ef.exists(_ <= 0)) {
      complete(StatusCodes.BadRequest -> ErrorResponse("InvalidParameter", "ef must be positive"))
    } else {
      pass
    }
  }
}

object SearchRoutes {
  def apply(searchService: SearchService, indexManager: IndexManager): SearchRoutes =
    new SearchRoutes(searchService, indexManager)
}
