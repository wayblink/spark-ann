package com.company.ann.api.swagger

import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.swagger.v3.oas.models.ExternalDocumentation

/**
 * Swagger documentation service for the ANN API.
 * Provides OpenAPI 3.0 specification at /api-docs/swagger.json
 */
object SwaggerDocService extends SwaggerHttpService {
  override val apiClasses: Set[Class[_]] = Set(
    classOf[com.company.ann.api.routes.HealthRoutes],
    classOf[com.company.ann.api.routes.IndexRoutes],
    classOf[com.company.ann.api.routes.SearchRoutes]
  )

  override val host: String = ""  // Empty for relative paths
  override val basePath: String = "/api/v1"

  override val info: Info = Info(
    title = "Spark-ANN API",
    version = "1.0.0",
    description =
      """REST API for Spark-ANN vector similarity search.
        |
        |Spark-ANN provides high-performance approximate nearest neighbor (ANN) search
        |using the HNSW algorithm. This API allows you to:
        |
        |* Manage vector indexes (create, load, save, delete)
        |* Search for similar vectors (single, multi-index, batch)
        |* Monitor service health
        |""".stripMargin
  )

  override val externalDocs: Option[ExternalDocumentation] = Some(
    new ExternalDocumentation()
      .description("Spark-ANN Documentation")
      .url("https://github.com/wayblink/spark-ann")
  )

  // Swagger UI will be available at /api-docs/swagger.json
  // Use swagger-ui-dist or ReDoc to render the spec
}
