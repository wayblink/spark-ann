package com.company.ann.api.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{ExceptionHandler, RejectionHandler, Route}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.company.ann.api.model.{ApiJsonProtocol, ErrorResponse}
import com.company.ann.api.model.ApiJsonProtocol._
import com.company.ann.api.service.{IndexManager, SearchService}

/**
 * Main API routes combining all endpoint groups.
 *
 * @param indexManager  The index manager
 * @param searchService The search service
 * @param version       API version string
 */
class ApiRoutes(
  indexManager: IndexManager,
  searchService: SearchService,
  version: String = "1.0.0"
) {

  private val healthRoutes = HealthRoutes(indexManager, version)
  private val searchRoutes = SearchRoutes(searchService, indexManager)
  private val indexRoutes = IndexRoutes(indexManager)

  /**
   * Custom exception handler for unhandled exceptions.
   */
  private implicit val exceptionHandler: ExceptionHandler = ExceptionHandler {
    case e: spray.json.DeserializationException =>
      complete(StatusCodes.BadRequest -> ErrorResponse("InvalidRequest", e.getMessage))
    case e: Exception =>
      complete(StatusCodes.InternalServerError -> ErrorResponse("InternalError", e.getMessage))
  }

  /**
   * Custom rejection handler for route rejections.
   */
  private implicit val rejectionHandler: RejectionHandler = RejectionHandler.newBuilder()
    .handleNotFound {
      complete(StatusCodes.NotFound -> ErrorResponse("NotFound", "The requested resource was not found"))
    }
    .handle {
      case akka.http.scaladsl.server.MalformedRequestContentRejection(msg, _) =>
        complete(StatusCodes.BadRequest -> ErrorResponse("InvalidRequest", msg))
      case akka.http.scaladsl.server.MethodRejection(supported) =>
        complete(StatusCodes.MethodNotAllowed -> ErrorResponse(
          "MethodNotAllowed",
          s"HTTP method not allowed. Supported: ${supported.value}"
        ))
    }
    .result()

  /**
   * All API routes under /api/v1 prefix.
   */
  val routes: Route = handleExceptions(exceptionHandler) {
    handleRejections(rejectionHandler) {
      pathPrefix("api" / "v1") {
        concat(
          healthRoutes.routes,
          searchRoutes.routes,
          indexRoutes.routes
        )
      }
    }
  }
}

object ApiRoutes {
  def apply(
    indexManager: IndexManager,
    searchService: SearchService,
    version: String = "1.0.0"
  ): ApiRoutes = new ApiRoutes(indexManager, searchService, version)
}
