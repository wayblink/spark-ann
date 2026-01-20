package com.company.ann.api.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.company.ann.api.model._
import com.company.ann.api.model.ApiJsonProtocol._
import com.company.ann.api.service.{IndexManager, LoadedIndexInfo}
import com.company.ann.core.index.HNSWConfig

/**
 * HTTP routes for index management operations.
 *
 * @param indexManager The index manager for index operations
 */
class IndexRoutes(indexManager: IndexManager) {

  val routes: Route = pathPrefix("indexes") {
    concat(
      // GET /indexes - List all loaded indexes
      pathEnd {
        get {
          val indexes = indexManager.listIndexes().map(toIndexInfo)
          complete(IndexListResponse(
            indexes = indexes,
            totalIndexes = indexes.size,
            totalVectors = indexManager.totalVectors
          ))
        }
      },
      // POST /indexes - Load or create an index
      pathEnd {
        post {
          entity(as[spray.json.JsValue]) { json =>
            // Determine if this is a load or create request
            val fields = json.asJsObject.fields
            if (fields.contains("indexPath")) {
              // Load from disk
              handleLoadIndex(json.convertTo[LoadIndexRequest])
            } else if (fields.contains("vectors")) {
              // Create from vectors
              handleCreateIndex(json.convertTo[CreateIndexRequest])
            } else {
              complete(StatusCodes.BadRequest -> ErrorResponse(
                "InvalidRequest",
                "Request must contain either 'indexPath' (to load) or 'vectors' (to create)"
              ))
            }
          }
        }
      },
      // GET /indexes/{indexId} - Get index details
      path(Segment) { indexId =>
        get {
          indexManager.getIndex(indexId) match {
            case Some(info) =>
              complete(toIndexInfo(info))
            case None =>
              complete(StatusCodes.NotFound -> ErrorResponse("IndexNotFound", s"Index '$indexId' not found"))
          }
        }
      },
      // DELETE /indexes/{indexId} - Unload an index
      path(Segment) { indexId =>
        delete {
          parameter("deleteFile".as[Boolean].?(false)) { deleteFile =>
            if (deleteFile) {
              // TODO: Implement file deletion in Phase 2
              complete(StatusCodes.NotImplemented -> ErrorResponse(
                "NotImplemented",
                "File deletion not yet implemented"
              ))
            } else {
              if (indexManager.unloadIndex(indexId)) {
                complete(IndexOperationResponse(
                  success = true,
                  message = s"Index '$indexId' unloaded"
                ))
              } else {
                complete(StatusCodes.NotFound -> ErrorResponse("IndexNotFound", s"Index '$indexId' not found"))
              }
            }
          }
        }
      },
      // POST /indexes/{indexId}/vectors - Add vectors to an index
      path(Segment / "vectors") { indexId =>
        post {
          entity(as[AddVectorsRequest]) { request =>
            if (request.vectors.isEmpty) {
              complete(StatusCodes.BadRequest -> ErrorResponse("InvalidRequest", "vectors cannot be empty"))
            } else {
              val vectors = request.vectors.map(v => (v.id, v.vector))
              indexManager.addVectors(indexId, vectors) match {
                case Right(info) =>
                  complete(IndexOperationResponse(
                    success = true,
                    message = s"Added ${request.vectors.size} vectors to index '$indexId'",
                    index = Some(toIndexInfo(info))
                  ))
                case Left(error) if error.contains("not found") =>
                  complete(StatusCodes.NotFound -> ErrorResponse("IndexNotFound", error))
                case Left(error) if error.contains("dimension") =>
                  complete(StatusCodes.UnprocessableEntity -> ErrorResponse("DimensionMismatch", error))
                case Left(error) =>
                  complete(StatusCodes.InternalServerError -> ErrorResponse("IndexError", error))
              }
            }
          }
        }
      },
      // POST /indexes/{indexId}/save - Save an index to disk
      path(Segment / "save") { indexId =>
        post {
          entity(as[SaveIndexRequest]) { request =>
            indexManager.saveIndex(indexId, request.path) match {
              case Right(_) =>
                complete(IndexOperationResponse(
                  success = true,
                  message = s"Index '$indexId' saved to ${request.path}"
                ))
              case Left(error) if error.contains("not found") =>
                complete(StatusCodes.NotFound -> ErrorResponse("IndexNotFound", error))
              case Left(error) =>
                complete(StatusCodes.InternalServerError -> ErrorResponse("SaveError", error))
            }
          }
        }
      }
    )
  }

  /**
   * Handle loading an index from disk.
   */
  private def handleLoadIndex(request: LoadIndexRequest): Route = {
    if (request.indexId.isEmpty) {
      complete(StatusCodes.BadRequest -> ErrorResponse("InvalidRequest", "indexId cannot be empty"))
    } else if (request.indexPath.isEmpty) {
      complete(StatusCodes.BadRequest -> ErrorResponse("InvalidRequest", "indexPath cannot be empty"))
    } else {
      indexManager.loadIndex(request.indexId, request.indexPath) match {
        case Right(info) =>
          complete(StatusCodes.Created -> IndexOperationResponse(
            success = true,
            message = s"Index '${request.indexId}' loaded successfully",
            index = Some(toIndexInfo(info))
          ))
        case Left(error) if error.contains("already exists") =>
          complete(StatusCodes.Conflict -> ErrorResponse("IndexAlreadyExists", error))
        case Left(error) if error.contains("Failed to load") =>
          complete(StatusCodes.UnprocessableEntity -> ErrorResponse("FileNotFound", error))
        case Left(error) =>
          complete(StatusCodes.InternalServerError -> ErrorResponse("LoadError", error))
      }
    }
  }

  /**
   * Handle creating an index from vectors.
   */
  private def handleCreateIndex(request: CreateIndexRequest): Route = {
    if (request.indexId.isEmpty) {
      complete(StatusCodes.BadRequest -> ErrorResponse("InvalidRequest", "indexId cannot be empty"))
    } else if (request.vectors.isEmpty) {
      complete(StatusCodes.BadRequest -> ErrorResponse("InvalidRequest", "vectors cannot be empty"))
    } else {
      // Validate all vectors have the same dimension
      val dimensions = request.vectors.map(_.vector.length).distinct
      if (dimensions.size > 1) {
        complete(StatusCodes.BadRequest -> ErrorResponse(
          "InvalidRequest",
          s"All vectors must have the same dimension, found: ${dimensions.mkString(", ")}"
        ))
      } else {
        val dimension = dimensions.head
        val config = request.config.map { c =>
          HNSWConfig(
            M = c.m.getOrElse(16),
            efConstruction = c.efConstruction.getOrElse(200),
            maxElements = math.max(1000000, request.vectors.size * 2)
          )
        }.getOrElse(HNSWConfig())

        val distanceType = request.config.flatMap(_.distanceType).getOrElse("euclidean")
        val vectors = request.vectors.map(v => (v.id, v.vector))

        indexManager.createIndex(request.indexId, dimension, vectors, config, distanceType) match {
          case Right(info) =>
            complete(StatusCodes.Created -> IndexOperationResponse(
              success = true,
              message = s"Index '${request.indexId}' created with ${request.vectors.size} vectors",
              index = Some(toIndexInfo(info))
            ))
          case Left(error) if error.contains("already exists") =>
            complete(StatusCodes.Conflict -> ErrorResponse("IndexAlreadyExists", error))
          case Left(error) =>
            complete(StatusCodes.InternalServerError -> ErrorResponse("CreateError", error))
        }
      }
    }
  }

  /**
   * Convert internal index info to API response model.
   */
  private def toIndexInfo(info: LoadedIndexInfo): IndexInfo = {
    IndexInfo(
      indexId = info.indexId,
      dimension = info.index.dimension,
      size = info.index.size,
      indexPath = info.indexPath,
      distanceType = Some(info.distanceType)
    )
  }
}

object IndexRoutes {
  def apply(indexManager: IndexManager): IndexRoutes = new IndexRoutes(indexManager)
}
