package com.wayblink.ann.api.swagger

import akka.http.scaladsl.model.{ContentTypes, HttpEntity, MediaTypes}
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
    classOf[com.wayblink.ann.api.routes.HealthRoutes],
    classOf[com.wayblink.ann.api.routes.IndexRoutes],
    classOf[com.wayblink.ann.api.routes.SearchRoutes]
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

  // Swagger UI version from webjar
  private val swaggerUiVersion = "5.9.0"
  private val webjarPath = s"META-INF/resources/webjars/swagger-ui/$swaggerUiVersion"

  // HTML page that loads Swagger UI and points to our API spec.
  private val swaggerUiHtml: String = {
    val star = "*"
    s"""<!DOCTYPE html>
       |<html lang="en">
       |<head>
       |  <meta charset="UTF-8">
       |  <title>Spark-ANN API - Swagger UI</title>
       |  <link rel="stylesheet" type="text/css" href="/swagger-ui/swagger-ui.css">
       |  <link rel="icon" type="image/png" href="/swagger-ui/favicon-32x32.png" sizes="32x32">
       |  <style>
       |    html { box-sizing: border-box; overflow-y: scroll; }
       |    $star, $star:before, $star:after { box-sizing: inherit; }
       |    body { margin: 0; background: #fafafa; }
       |  </style>
       |</head>
       |<body>
       |  <div id="swagger-ui"></div>
       |  <script src="/swagger-ui/swagger-ui-bundle.js" charset="UTF-8"></script>
       |  <script src="/swagger-ui/swagger-ui-standalone-preset.js" charset="UTF-8"></script>
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

  // Load resource as bytes from classpath
  private def loadResource(path: String): Option[Array[Byte]] = {
    Option(getClass.getClassLoader.getResourceAsStream(path)).map { stream =>
      try {
        Iterator.continually(stream.read()).takeWhile(_ != -1).map(_.toByte).toArray
      } finally {
        stream.close()
      }
    }
  }

  // Determine content type from file extension
  private def contentTypeFor(fileName: String): akka.http.scaladsl.model.ContentType = {
    import akka.http.scaladsl.model.HttpCharsets
    fileName.split('.').lastOption.map(_.toLowerCase) match {
      case Some("css") => MediaTypes.`text/css`.toContentType(HttpCharsets.`UTF-8`)
      case Some("js") => MediaTypes.`application/javascript`.toContentType(HttpCharsets.`UTF-8`)
      case Some("html") => ContentTypes.`text/html(UTF-8)`
      case Some("json") => ContentTypes.`application/json`
      case Some("png") => MediaTypes.`image/png`.toContentType
      case Some("svg") => MediaTypes.`image/svg+xml`.toContentType
      case Some("map") => ContentTypes.`application/json`
      case _ => ContentTypes.`application/octet-stream`
    }
  }

  // Routes for Swagger UI and API documentation.
  def swaggerRoutes: Route = concat(
    // Swagger UI page at /api/v1/swagger
    path("api" / "v1" / "swagger") {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, swaggerUiHtml))
      }
    },
    // Serve Swagger UI assets from webjar
    pathPrefix("swagger-ui") {
      extractUnmatchedPath { path =>
        get {
          val fileName = path.toString.stripPrefix("/")
          val resourcePath = s"$webjarPath/$fileName"
          loadResource(resourcePath) match {
            case Some(bytes) =>
              complete(HttpEntity(contentTypeFor(fileName), bytes))
            case None =>
              complete(akka.http.scaladsl.model.StatusCodes.NotFound)
          }
        }
      }
    },
    // OpenAPI JSON spec (from parent trait)
    routes
  )
}
