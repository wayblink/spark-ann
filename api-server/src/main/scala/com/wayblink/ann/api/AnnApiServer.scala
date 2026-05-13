package com.wayblink.ann.api

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
import com.wayblink.ann.api.routes.ApiRoutes
import com.wayblink.ann.api.service.{IndexManager, SearchService}
import com.typesafe.config.ConfigFactory

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

/**
 * Main entry point for the ANN API Server.
 */
object AnnApiServer extends App {

  // Load configuration
  private val config = ConfigFactory.load()
  private val serverConfig = config.getConfig("ann-service.server")
  private val host = serverConfig.getString("host")
  private val port = serverConfig.getInt("port")
  private val version = config.getString("ann-service.version")

  // Create actor system
  implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "ann-api-server")
  implicit val executionContext: ExecutionContext = system.executionContext

  // Initialize services
  private val indexManager = IndexManager()
  private val searchService = SearchService(indexManager)

  // Create routes
  private val apiRoutes = ApiRoutes(indexManager, searchService, version)

  // Start HTTP server
  private val bindingFuture: Future[Http.ServerBinding] =
    Http().newServerAt(host, port).bind(apiRoutes.routes)

  bindingFuture.onComplete {
    case Success(binding) =>
      val address = binding.localAddress
      system.log.info(s"ANN API Server started at http://${address.getHostString}:${address.getPort}/")
      system.log.info(s"API version: $version")
      system.log.info(s"Health check: http://${address.getHostString}:${address.getPort}/api/v1/health")

    case Failure(ex) =>
      system.log.error("Failed to bind HTTP server", ex)
      system.terminate()
  }

  // Handle shutdown
  sys.addShutdownHook {
    bindingFuture
      .flatMap(_.unbind())
      .onComplete { _ =>
        system.terminate()
      }
  }

  // Block main thread to keep server running
  Await.result(system.whenTerminated, Duration.Inf)
}

/**
 * Programmatic API server for embedding in other applications.
 *
 * @param host    Host to bind to
 * @param port    Port to bind to
 * @param version API version string
 */
class AnnApiServer(
  host: String = "0.0.0.0",
  port: Int = 8080,
  version: String = "1.0.0"
) {

  private var system: ActorSystem[Nothing] = _
  private var bindingFuture: Future[Http.ServerBinding] = _
  private var _indexManager: IndexManager = _
  private var _searchService: SearchService = _

  /**
   * Start the API server.
   *
   * @return Future that completes when the server is bound
   */
  def start(): Future[Http.ServerBinding] = {
    system = ActorSystem(Behaviors.empty, "ann-api-server")
    implicit val sys: ActorSystem[Nothing] = system
    implicit val ec: ExecutionContext = system.executionContext

    _indexManager = IndexManager()
    _searchService = SearchService(_indexManager)

    val apiRoutes = ApiRoutes(_indexManager, _searchService, version)

    bindingFuture = Http().newServerAt(host, port).bind(Route.toFunction(apiRoutes.routes))
    bindingFuture
  }

  /**
   * Stop the API server.
   *
   * @return Future that completes when the server is stopped
   */
  def stop(): Future[Unit] = {
    implicit val ec: ExecutionContext = system.executionContext
    bindingFuture
      .flatMap(_.unbind())
      .flatMap { _ =>
        system.terminate()
        system.whenTerminated
      }
      .map(_ => ())
  }

  /**
   * Get the index manager for programmatic access.
   */
  def indexManager: IndexManager = _indexManager

  /**
   * Get the search service for programmatic access.
   */
  def searchService: SearchService = _searchService
}
