package com.company.ann.api.swagger

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.github.swagger.akka.SwaggerHttpService
import com.github.swagger.akka.model.Info
import io.swagger.v3.oas.models.ExternalDocumentation

/**
 * Swagger documentation service for the ANN API.
 * Provides OpenAPI 3.0 specification and Swagger UI.
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
        |- Manage vector indexes (create, load, save, delete)
        |- Search for similar vectors (single, multi-index, batch)
        |- Monitor service health
        |""".stripMargin
  )

  override val externalDocs: Option[ExternalDocumentation] = Some(
    new ExternalDocumentation()
      .description("Spark-ANN Documentation")
      .url("https://github.com/wayblink/spark-ann")
  )

  // Swagger UI version for CDN
  private val swaggerUiVersion = "5.9.0"

  // HTML page that loads Swagger UI from CDN and points to our API spec.
  // Note: Using separate strings to avoid Scala compiler interpreting CSS selectors as comments
  private val swaggerUiHtml: String = {
    val star = "*"
    val cdnBase = s"https://unpkg.com/swagger-ui-dist@$swaggerUiVersion"
    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |  <meta charset="UTF-8">
       |  <title>Spark-ANN API - Swagger UI</title>
       |  <link rel="stylesheet" type="text/css" href="$cdnBase/swagger-ui.css">
       |  <link rel="icon" type="image/png" href="$cdnBase/favicon-32x32.png" sizes="32x32">
       |  <style>
       |    html { box-sizing: border-box; overflow-y: scroll; }
       |    $star, $star:before, $star:after { box-sizing: inherit; }
       |    body { margin: 0; background: #fafafa; }
       |  </style>
       |</head>
       |<body>
       |  <div id="swagger-ui"></div>
       |  <script src="$cdnBase/swagger-ui-bundle.js" charset="UTF-8"></script>
       |  <script src="$cdnBase/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
       |  <script>
       |    window.onload = function() {
       |      window.ui = SwaggerUIBundle({
       |        url: "/api-docs/swagger.json",
       |        dom_id: '#swagger-ui',
       |        deepLinking: true,
       |        presets: [
       |          SwaggerUIBundle.presets.apis,
       |          SwaggerUIStandalonePreset
       |        ],
       |        plugins: [
       |          SwaggerUIBundle.plugins.DownloadUrl
       |        ],
       |        layout: "StandaloneLayout"
       |      });
       |    };
       |  </script>
       |</body>
       |</html>""".stripMargin
  }

  // Routes for Swagger UI and API documentation.
  // - /api/v1/swagger - Swagger UI (loads assets from CDN)
  // - /api-docs/swagger.json - OpenAPI JSON spec
  def swaggerRoutes: Route = concat(
    // Swagger UI page at /api/v1/swagger
    path("api" / "v1" / "swagger") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, swaggerUiHtml))
      }
    },
    // OpenAPI JSON spec (from parent trait)
    routes
  )
}
