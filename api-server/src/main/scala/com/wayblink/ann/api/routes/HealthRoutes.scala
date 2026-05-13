package com.wayblink.ann.api.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.wayblink.ann.api.model._
import com.wayblink.ann.api.model.ApiJsonProtocol._
import com.wayblink.ann.api.service.IndexManager
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.ws.rs.{GET, Path}

/**
 * HTTP routes for health and status endpoints.
 *
 * @param indexManager The index manager for status information
 * @param version      API version string
 */
@Path("/health")
@Tag(name = "Health", description = "Service health and status endpoints")
class HealthRoutes(indexManager: IndexManager, version: String = "1.0.0") {

  val routes: Route = pathPrefix("health") {
    concat(
      pathEnd {
        get {
          complete(getHealth())
        }
      },
      path("ready") {
        get {
          complete(getReady())
        }
      },
      path("live") {
        get {
          complete(getLive())
        }
      }
    )
  }

  @GET
  @Operation(
    summary = "Get service health status",
    description = "Returns full health status including version and statistics",
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Service health status",
        content = Array(new Content(schema = new Schema(implementation = classOf[HealthResponse])))
      )
    )
  )
  def getHealth(): HealthResponse = {
    HealthResponse(
      status = "healthy",
      version = version,
      indexCount = indexManager.indexCount,
      totalVectors = indexManager.totalVectors
    )
  }

  @GET
  @Path("/ready")
  @Operation(
    summary = "Readiness probe",
    description = "Kubernetes readiness probe endpoint",
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Service is ready",
        content = Array(new Content(schema = new Schema(implementation = classOf[ReadinessResponse])))
      )
    )
  )
  def getReady(): ReadinessResponse = {
    ReadinessResponse(ready = true)
  }

  @GET
  @Path("/live")
  @Operation(
    summary = "Liveness probe",
    description = "Kubernetes liveness probe endpoint",
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Service is alive",
        content = Array(new Content(schema = new Schema(implementation = classOf[LivenessResponse])))
      )
    )
  )
  def getLive(): LivenessResponse = {
    LivenessResponse(alive = true)
  }
}

object HealthRoutes {
  def apply(indexManager: IndexManager, version: String = "1.0.0"): HealthRoutes =
    new HealthRoutes(indexManager, version)
}
