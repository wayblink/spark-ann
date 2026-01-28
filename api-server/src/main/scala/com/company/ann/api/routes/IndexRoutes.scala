package com.company.ann.api.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.company.ann.api.model._
import com.company.ann.api.model.ApiJsonProtocol._
import com.company.ann.api.service.{IndexManager, LoadedIndexInfo}
import com.company.ann.core.index.HNSWConfig
import io.swagger.v3.oas.annotations.{Operation, Parameter}
import io.swagger.v3.oas.annotations.enums.ParameterIn
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.parameters.RequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.ws.rs.{DELETE, GET, POST, Path}

/**
 * HTTP routes for index management operations.
 *
 * @param indexManager The index manager for index operations
 */
@Path("/indexes")
@Tag(name = "Indexes", description = "Index management operations")
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

  @GET
  @Operation(
    summary = "List all indexes",
    description = "Returns a list of all loaded indexes with their metadata",
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "List of indexes",
        content = Array(new Content(schema = new Schema(implementation = classOf[IndexListResponse])))
      )
    )
  )
  def listIndexes(): Unit = {}

  @POST
  @Operation(
    summary = "Create or load an index",
    description = "Create a new index from vectors or load an existing index from disk. Include 'indexPath' to load from disk, or 'vectors' to create from data.",
    requestBody = new RequestBody(
      description = "Index creation or load parameters",
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[CreateIndexRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "201",
        description = "Index created/loaded successfully",
        content = Array(new Content(schema = new Schema(implementation = classOf[IndexOperationResponse])))
      ),
      new ApiResponse(responseCode = "400", description = "Invalid request"),
      new ApiResponse(responseCode = "409", description = "Index already exists")
    )
  )
  def createIndex(): Unit = {}

  @GET
  @Path("/{indexId}")
  @Operation(
    summary = "Get index details",
    description = "Returns detailed information about a specific index",
    parameters = Array(
      new Parameter(
        name = "indexId",
        in = ParameterIn.PATH,
        description = "Index identifier",
        required = true,
        schema = new Schema(implementation = classOf[String])
      )
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Index details",
        content = Array(new Content(schema = new Schema(implementation = classOf[IndexInfo])))
      ),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def getIndex(): Unit = {}

  @DELETE
  @Path("/{indexId}")
  @Operation(
    summary = "Delete/unload an index",
    description = "Unload an index from memory. Use deleteFile=true to also delete from disk.",
    parameters = Array(
      new Parameter(
        name = "indexId",
        in = ParameterIn.PATH,
        description = "Index identifier",
        required = true,
        schema = new Schema(implementation = classOf[String])
      ),
      new Parameter(
        name = "deleteFile",
        in = ParameterIn.QUERY,
        description = "Also delete the index file from disk",
        required = false,
        schema = new Schema(implementation = classOf[Boolean], defaultValue = "false")
      )
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Index unloaded",
        content = Array(new Content(schema = new Schema(implementation = classOf[IndexOperationResponse])))
      ),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def deleteIndex(): Unit = {}

  @POST
  @Path("/{indexId}/vectors")
  @Operation(
    summary = "Add vectors to an index",
    description = "Add new vectors to an existing index",
    parameters = Array(
      new Parameter(
        name = "indexId",
        in = ParameterIn.PATH,
        description = "Index identifier",
        required = true,
        schema = new Schema(implementation = classOf[String])
      )
    ),
    requestBody = new RequestBody(
      description = "Vectors to add",
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[AddVectorsRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Vectors added",
        content = Array(new Content(schema = new Schema(implementation = classOf[IndexOperationResponse])))
      ),
      new ApiResponse(responseCode = "404", description = "Index not found"),
      new ApiResponse(responseCode = "422", description = "Dimension mismatch")
    )
  )
  def addVectors(): Unit = {}

  @POST
  @Path("/{indexId}/save")
  @Operation(
    summary = "Save index to disk",
    description = "Persist an in-memory index to disk",
    parameters = Array(
      new Parameter(
        name = "indexId",
        in = ParameterIn.PATH,
        description = "Index identifier",
        required = true,
        schema = new Schema(implementation = classOf[String])
      )
    ),
    requestBody = new RequestBody(
      description = "Save destination",
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[SaveIndexRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Index saved",
        content = Array(new Content(schema = new Schema(implementation = classOf[IndexOperationResponse])))
      ),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def saveIndex(): Unit = {}

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
