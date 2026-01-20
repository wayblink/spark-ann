package com.company.ann.api.routes

import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.company.ann.api.model._
import com.company.ann.api.model.ApiJsonProtocol._
import com.company.ann.api.service.IndexManager

/**
 * HTTP routes for health and status endpoints.
 *
 * @param indexManager The index manager for status information
 * @param version      API version string
 */
class HealthRoutes(indexManager: IndexManager, version: String = "1.0.0") {

  val routes: Route = pathPrefix("health") {
    concat(
      // GET /health - Full health check with statistics
      pathEnd {
        get {
          complete(HealthResponse(
            status = "healthy",
            version = version,
            indexCount = indexManager.indexCount,
            totalVectors = indexManager.totalVectors
          ))
        }
      },
      // GET /health/ready - Readiness probe
      path("ready") {
        get {
          complete(ReadinessResponse(ready = true))
        }
      },
      // GET /health/live - Liveness probe
      path("live") {
        get {
          complete(LivenessResponse(alive = true))
        }
      }
    )
  }
}

object HealthRoutes {
  def apply(indexManager: IndexManager, version: String = "1.0.0"): HealthRoutes =
    new HealthRoutes(indexManager, version)
}
