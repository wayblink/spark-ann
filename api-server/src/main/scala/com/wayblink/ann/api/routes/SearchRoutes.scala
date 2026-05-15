package com.wayblink.ann.api.routes

import akka.http.scaladsl.model.StatusCode
import akka.http.scaladsl.server.{Directive0, Rejection, RejectionHandler, Route}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.wayblink.ann.api.error.ApiError
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
 * Custom Akka HTTP rejection emitted by the search-request validation
 * directive. Mapped to a typed 400 response by [[SearchRoutes.routes]].
 */
final case class ValidationRejection(reason: String) extends Rejection

/**
 * HTTP routes for search operations. Service-layer errors come back as
 * [[ApiError]] and are mapped to HTTP status codes via a single helper,
 * eliminating the substring-based routing flagged in todo.md #6.
 */
@Path("/")
@Tag(name = "Search", description = "Vector similarity search operations")
class SearchRoutes(searchService: SearchService, indexManager: IndexManager) {

  /** Map every ApiError to a typed HTTP response. */
  private def respond(err: ApiError): Route = {
    val status: StatusCode = ApiError.toHttpStatus(err)
    complete(status -> ErrorResponse(err.code, err.message))
  }

  /** Handle ValidationRejection emitted by validateSearchParams. */
  private val rejectionHandler: RejectionHandler =
    RejectionHandler.newBuilder()
      .handle { case ValidationRejection(reason) =>
        respond(ApiError.InvalidRequest(reason))
      }
      .result()

  val routes: Route = handleRejections(rejectionHandler) {
    concat(
      pathPrefix("indexes" / Segment / "search") { indexId =>
        post {
          entity(as[SearchRequest]) { request =>
            validateSearchParams(request) {
              searchService.search(indexId, request.vector, request.k, request.ef) match {
                case Right(result) =>
                  complete(SearchResponse(
                    indexId = result.indexId,
                    results = result.results.map(r => SearchResultItem(r.id, r.distance)),
                    queryTimeMs = result.queryTimeMs
                  ))
                case Left(err) => respond(err)
              }
            }
          }
        }
      },
      path("search") {
        post {
          entity(as[MultiSearchRequest]) { request =>
            validateSearchParams(SearchRequest(request.vector, request.k, request.ef)) {
              searchService.multiSearch(request.vector, request.k, request.ef, request.indexIds) match {
                case Right(result) =>
                  complete(MultiSearchResponse(
                    results = result.perIndexResults.map { case (indexId, results) =>
                      indexId -> results.map(r => SearchResultItem(r.id, r.distance))
                    },
                    merged = result.merged.map(r => MergedSearchResultItem(r.id, r.distance, r.indexId)),
                    totalTimeMs = result.totalTimeMs
                  ))
                case Left(err) => respond(err)
              }
            }
          }
        }
      },
      path("search" / "batch") {
        post {
          entity(as[BatchSearchRequest]) { request =>
            if (request.queries.isEmpty) {
              respond(ApiError.InvalidRequest("queries cannot be empty"))
            } else {
              val queries = request.queries.map(q => (q.vector, q.k))
              searchService.batchSearch(request.indexId, queries, request.ef) match {
                case Right(results) =>
                  complete(BatchSearchResponse(
                    results = results.zipWithIndex.map { case (r, idx) =>
                      BatchSearchResultItem(
                        queryIndex = idx,
                        results = r.results.map(sr => SearchResultItem(sr.id, sr.distance))
                      )
                    },
                    totalTimeMs = results.map(_.queryTimeMs).sum
                  ))
                case Left(err) => respond(err)
              }
            }
          }
        }
      }
    )
  }

  @POST
  @Path("/indexes/{indexId}/search")
  @Operation(
    summary = "Search a single index",
    description = "Find k nearest neighbors in the specified bundle index",
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
   * Validate basic search request parameters. Replaces the previous
   * `validateSearchRequest` that returned Directive0 but called
   * `complete(...)` inline (todo.md #13). Now emits a typed
   * ValidationRejection that the route-level rejection handler maps
   * to a 400 response. The shape Directive0 is correct: the directive
   * either passes through (parameters valid) or rejects.
   */
  private def validateSearchParams(request: SearchRequest): Directive0 = {
    if (request.vector.isEmpty)
      reject(ValidationRejection("vector cannot be empty"))
    else if (request.k <= 0)
      reject(ValidationRejection("k must be positive"))
    else if (request.k > 1000)
      reject(ValidationRejection("k cannot exceed 1000"))
    else if (request.ef.exists(_ <= 0))
      reject(ValidationRejection("ef must be positive"))
    else
      pass
  }
}

object SearchRoutes {
  def apply(searchService: SearchService, indexManager: IndexManager): SearchRoutes =
    new SearchRoutes(searchService, indexManager)
}
