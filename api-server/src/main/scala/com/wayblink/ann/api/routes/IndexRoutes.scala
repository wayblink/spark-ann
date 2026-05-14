package com.wayblink.ann.api.routes

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.wayblink.ann.api.error.ApiError
import com.wayblink.ann.api.model._
import com.wayblink.ann.api.model.ApiJsonProtocol._
import com.wayblink.ann.api.service.{IndexEntry, IndexManager, LoadedBundleInfo, LoadedIndexInfo}
import com.wayblink.ann.core.index.HNSWConfig
import io.swagger.v3.oas.annotations.{Operation, Parameter}
import io.swagger.v3.oas.annotations.enums.ParameterIn
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.parameters.RequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.ws.rs.{DELETE, GET, POST, Path}

/**
 * HTTP routes for index management. Handles both legacy flat indexes
 * (single .hnsw file) and bundle indexes (pattern B; directory with
 * ann_index.json). Service-layer errors are typed via [[ApiError]]
 * and translated to HTTP codes by a single helper, replacing the
 * substring-routing flagged in todo.md #6.
 */
@Path("/indexes")
@Tag(name = "Indexes", description = "Index management operations")
class IndexRoutes(indexManager: IndexManager) {

  private def respond(err: ApiError): Route = {
    val status: StatusCode = ApiError.toHttpStatus(err)
    complete(status -> ErrorResponse(err.code, err.message))
  }

  val routes: Route = pathPrefix("indexes") {
    concat(
      // POST /indexes/bundle  — load a bundle from disk
      pathPrefix("bundle") {
        pathEnd {
          post {
            entity(as[BundleLoadRequest]) { request =>
              if (request.indexId.isEmpty)
                respond(ApiError.InvalidRequest("indexId cannot be empty"))
              else if (request.bundlePath.isEmpty)
                respond(ApiError.InvalidRequest("bundlePath cannot be empty"))
              else
                indexManager.loadBundle(request.indexId, request.bundlePath) match {
                  case Right(info) =>
                    complete(StatusCodes.Created -> IndexOperationResponse(
                      success = true,
                      message = s"Bundle '${request.indexId}' loaded from ${request.bundlePath}"
                    ))
                  case Left(err) => respond(err)
                }
            }
          }
        }
      },

      // GET /indexes  — unified listing (flat + bundle)
      pathEnd {
        get {
          val entries: Seq[UnifiedIndexEntry] = indexManager.listEntries().map(toUnifiedEntry)
          complete(UnifiedIndexListResponse(
            indexes = entries,
            totalIndexes = entries.size,
            totalVectors = indexManager.totalVectors
          ))
        }
      },

      // POST /indexes  — legacy: load flat .hnsw OR create from inline vectors
      pathEnd {
        post {
          entity(as[spray.json.JsValue]) { json =>
            val fields = json.asJsObject.fields
            if (fields.contains("indexPath")) {
              handleLoadIndex(json.convertTo[LoadIndexRequest])
            } else if (fields.contains("vectors")) {
              handleCreateIndex(json.convertTo[CreateIndexRequest])
            } else {
              respond(ApiError.InvalidRequest(
                "Request must contain either 'indexPath' (flat load), 'vectors' (create), " +
                  "or use POST /indexes/bundle for bundle loads"
              ))
            }
          }
        }
      },

      // GET /indexes/{id}  — flat or bundle detail
      path(Segment) { indexId =>
        get {
          indexManager.getEntry(indexId) match {
            case Some(entry) => complete(toUnifiedEntry(entry))
            case None        => respond(ApiError.IndexNotFound(indexId))
          }
        }
      },

      // DELETE /indexes/{id}  — unload from memory (flat or bundle)
      path(Segment) { indexId =>
        delete {
          parameter("deleteFile".as[Boolean].?(false)) { deleteFile =>
            if (deleteFile) {
              complete(StatusCodes.NotImplemented -> ErrorResponse(
                "not_implemented",
                "File deletion not yet implemented"
              ))
            } else {
              if (indexManager.unloadIndex(indexId))
                complete(IndexOperationResponse(
                  success = true,
                  message = s"Index '$indexId' unloaded"
                ))
              else
                respond(ApiError.IndexNotFound(indexId))
            }
          }
        }
      },

      // POST /indexes/{id}/vectors  — flat only (bundles are read-only here)
      path(Segment / "vectors") { indexId =>
        post {
          entity(as[AddVectorsRequest]) { request =>
            if (request.vectors.isEmpty) {
              respond(ApiError.InvalidRequest("vectors cannot be empty"))
            } else {
              val vectors = request.vectors.map(v => (v.id, v.vector))
              indexManager.addVectors(indexId, vectors) match {
                case Right(info) =>
                  complete(IndexOperationResponse(
                    success = true,
                    message = s"Added ${request.vectors.size} vectors to index '$indexId'",
                    index = Some(toIndexInfo(info))
                  ))
                case Left(err) => respond(err)
              }
            }
          }
        }
      },

      // POST /indexes/{id}/save  — flat only
      path(Segment / "save") { indexId =>
        post {
          entity(as[SaveIndexRequest]) { request =>
            indexManager.saveIndex(indexId, request.path) match {
              case Right(_) =>
                complete(IndexOperationResponse(
                  success = true,
                  message = s"Index '$indexId' saved to ${request.path}"
                ))
              case Left(err) => respond(err)
            }
          }
        }
      }
    )
  }

  // ── Swagger annotations ─────────────────────────────────────────────

  @GET
  @Operation(
    summary = "List all indexes",
    description = "Returns flat and bundle indexes with a `kind` discriminator",
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "List of indexes",
        content = Array(new Content(schema = new Schema(implementation = classOf[UnifiedIndexListResponse])))
      )
    )
  )
  def listIndexes(): Unit = {}

  @POST
  @Operation(
    summary = "Create or load a flat index",
    description = "Create from inline vectors or load a single .hnsw file. For pattern-B bundles, use POST /indexes/bundle.",
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

  @POST
  @Path("/bundle")
  @Operation(
    summary = "Load a bundle (pattern B)",
    description = "Load a directory bundle produced by the offline Spark builder.",
    requestBody = new RequestBody(
      description = "Bundle path",
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[BundleLoadRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "201",
        description = "Bundle loaded",
        content = Array(new Content(schema = new Schema(implementation = classOf[IndexOperationResponse])))
      ),
      new ApiResponse(responseCode = "400", description = "Invalid bundle"),
      new ApiResponse(responseCode = "404", description = "Bundle not found"),
      new ApiResponse(responseCode = "409", description = "Index already exists")
    )
  )
  def loadBundle(): Unit = {}

  @GET
  @Path("/{indexId}")
  @Operation(
    summary = "Get index details",
    parameters = Array(
      new Parameter(
        name = "indexId", in = ParameterIn.PATH, required = true,
        schema = new Schema(implementation = classOf[String])
      )
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200", description = "Index details",
        content = Array(new Content(schema = new Schema(implementation = classOf[UnifiedIndexEntry])))
      ),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def getIndex(): Unit = {}

  @DELETE
  @Path("/{indexId}")
  @Operation(
    summary = "Unload an index",
    parameters = Array(
      new Parameter(
        name = "indexId", in = ParameterIn.PATH, required = true,
        schema = new Schema(implementation = classOf[String])
      ),
      new Parameter(
        name = "deleteFile", in = ParameterIn.QUERY, required = false,
        schema = new Schema(implementation = classOf[Boolean], defaultValue = "false")
      )
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200", description = "Index unloaded",
        content = Array(new Content(schema = new Schema(implementation = classOf[IndexOperationResponse])))
      ),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def deleteIndex(): Unit = {}

  @POST
  @Path("/{indexId}/vectors")
  @Operation(
    summary = "Add vectors to a flat index",
    parameters = Array(
      new Parameter(
        name = "indexId", in = ParameterIn.PATH, required = true,
        schema = new Schema(implementation = classOf[String])
      )
    ),
    requestBody = new RequestBody(
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[AddVectorsRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200", description = "Vectors added",
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
    summary = "Save a flat index to disk",
    parameters = Array(
      new Parameter(
        name = "indexId", in = ParameterIn.PATH, required = true,
        schema = new Schema(implementation = classOf[String])
      )
    ),
    requestBody = new RequestBody(
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[SaveIndexRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200", description = "Index saved",
        content = Array(new Content(schema = new Schema(implementation = classOf[IndexOperationResponse])))
      ),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def saveIndex(): Unit = {}

  // ── Internal handlers ───────────────────────────────────────────────

  private def handleLoadIndex(request: LoadIndexRequest): Route = {
    if (request.indexId.isEmpty)
      respond(ApiError.InvalidRequest("indexId cannot be empty"))
    else if (request.indexPath.isEmpty)
      respond(ApiError.InvalidRequest("indexPath cannot be empty"))
    else
      indexManager.loadIndex(request.indexId, request.indexPath) match {
        case Right(info) =>
          complete(StatusCodes.Created -> IndexOperationResponse(
            success = true,
            message = s"Index '${request.indexId}' loaded successfully",
            index = Some(toIndexInfo(info))
          ))
        case Left(err) => respond(err)
      }
  }

  private def handleCreateIndex(request: CreateIndexRequest): Route = {
    if (request.indexId.isEmpty)
      respond(ApiError.InvalidRequest("indexId cannot be empty"))
    else if (request.vectors.isEmpty)
      respond(ApiError.InvalidRequest("vectors cannot be empty"))
    else {
      val dimensions = request.vectors.map(_.vector.length).distinct
      if (dimensions.size > 1) {
        respond(ApiError.InvalidRequest(
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
          case Left(err) => respond(err)
        }
      }
    }
  }

  // ── DTO converters ─────────────────────────────────────────────────

  private def toIndexInfo(info: LoadedIndexInfo): IndexInfo =
    IndexInfo(
      indexId = info.indexId,
      dimension = info.index.dimension,
      size = info.index.size,
      indexPath = info.indexPath,
      distanceType = Some(info.distanceType)
    )

  private def toUnifiedEntry(entry: IndexEntry): UnifiedIndexEntry = entry match {
    case IndexEntry.Flat(info) =>
      UnifiedIndexEntry(
        kind = "flat",
        indexId = info.indexId,
        dimension = info.index.dimension,
        size = info.index.size.toLong,
        distanceType = info.distanceType,
        indexPath = info.indexPath,
        bundlePath = None,
        numLocalIndexes = None,
        hasGlobalIndex = None,
        algorithm = None,
        loadedAt = info.loadedAt
      )
    case IndexEntry.Bundle(b) =>
      UnifiedIndexEntry(
        kind = "bundle",
        indexId = b.indexId,
        dimension = b.dimension,
        size = b.totalVectors,
        distanceType = b.distanceType,
        indexPath = None,
        bundlePath = Some(b.bundlePath),
        numLocalIndexes = Some(b.metadata.localIndexes.length),
        hasGlobalIndex = Some(b.metadata.globalIndexPath.isDefined),
        algorithm = Some(b.algorithmId),
        loadedAt = b.loadedAt
      )
  }
}

object IndexRoutes {
  def apply(indexManager: IndexManager): IndexRoutes = new IndexRoutes(indexManager)
}
