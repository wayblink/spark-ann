package com.wayblink.ann.api.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.{Directive0, Route}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.wayblink.ann.api.model._
import com.wayblink.ann.api.model.ApiJsonProtocol._
import com.wayblink.ann.api.service.{SearchService, IndexManager}
import io.swagger.v3.oas.annotations.{Operation, Parameter}
import io.swagger.v3.oas.annotations.enums.ParameterIn
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.parameters.RequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.ws.rs.{POST, Path}

/**
 * HTTP routes for search operations.
 *
 * @param searchService The search service for executing queries
 * @param indexManager  The index manager for validation
 */
@Path("/")
@Tag(name = "Search", description = "Vector similarity search operations")
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

  @POST
  @Path("/indexes/{indexId}/search")
  @Operation(
    summary = "Search a single index",
    description = "Find k nearest neighbors in the specified index",
    parameters = Array(
      new Parameter(
        name = "indexId",
        in = ParameterIn.PATH,
        description = "ID of the index to search",
        required = true,
        schema = new Schema(implementation = classOf[String])
      )
    ),
    requestBody = new RequestBody(
      description = "Search parameters",
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[SearchRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Search results",
        content = Array(new Content(schema = new Schema(implementation = classOf[SearchResponse])))
      ),
      new ApiResponse(responseCode = "404", description = "Index not found"),
      new ApiResponse(responseCode = "422", description = "Dimension mismatch")
    )
  )
  def searchIndex(): Unit = {}

  @POST
  @Path("/search")
  @Operation(
    summary = "Multi-index search",
    description = "Search across multiple indexes and merge results",
    requestBody = new RequestBody(
      description = "Multi-search parameters",
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[MultiSearchRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Merged search results",
        content = Array(new Content(schema = new Schema(implementation = classOf[MultiSearchResponse])))
      ),
      new ApiResponse(responseCode = "400", description = "No indexes available"),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def multiSearch(): Unit = {}

  @POST
  @Path("/search/batch")
  @Operation(
    summary = "Batch search",
    description = "Execute multiple search queries against a single index",
    requestBody = new RequestBody(
      description = "Batch search parameters",
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[BatchSearchRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Batch search results",
        content = Array(new Content(schema = new Schema(implementation = classOf[BatchSearchResponse])))
      ),
      new ApiResponse(responseCode = "400", description = "Empty queries"),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def batchSearch(): Unit = {}

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
