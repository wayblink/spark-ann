package com.wayblink.ann.api.routes

import akka.http.scaladsl.model.{StatusCode, StatusCodes}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.wayblink.ann.api.error.ApiError
import com.wayblink.ann.api.model.ApiJsonProtocol._
import com.wayblink.ann.api.model._
import com.wayblink.ann.api.service.{IndexManager, LoadedBundleInfo}
import io.swagger.v3.oas.annotations.{Operation, Parameter}
import io.swagger.v3.oas.annotations.enums.ParameterIn
import io.swagger.v3.oas.annotations.media.{Content, Schema}
import io.swagger.v3.oas.annotations.parameters.RequestBody
import io.swagger.v3.oas.annotations.responses.ApiResponse
import io.swagger.v3.oas.annotations.tags.Tag
import jakarta.ws.rs.{DELETE, GET, POST, Path}

/**
 * HTTP routes for bundle-only index management.
 */
@Path("/indexes")
@Tag(name = "Indexes", description = "Bundle index management operations")
class IndexRoutes(indexManager: IndexManager) {

  private def respond(err: ApiError): Route = {
    val status: StatusCode = ApiError.toHttpStatus(err)
    complete(status -> ErrorResponse(err.code, err.message))
  }

  val routes: Route = pathPrefix("indexes") {
    concat(
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
                  case Right(info) => complete(StatusCodes.Created -> toBundleInfo(info))
                  case Left(err) => respond(err)
                }
            }
          }
        }
      },
      pathEnd {
        get {
          val entries = indexManager.listBundles().map(toUnifiedEntry)
          complete(UnifiedIndexListResponse(entries, entries.size, indexManager.totalVectors))
        }
      },
      path(Segment) { indexId =>
        get {
          indexManager.getBundle(indexId) match {
            case Some(info) => complete(toBundleInfo(info))
            case None       => respond(ApiError.IndexNotFound(indexId))
          }
        }
      },
      path(Segment) { indexId =>
        delete {
          if (indexManager.unloadIndex(indexId)) complete(StatusCodes.OK)
          else respond(ApiError.IndexNotFound(indexId))
        }
      }
    )
  }

  @GET
  @Path("")
  @Operation(
    summary = "List loaded bundles",
    description = "Return all loaded bundle indexes",
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Bundle list",
        content = Array(new Content(schema = new Schema(implementation = classOf[UnifiedIndexListResponse])))
      )
    )
  )
  def listIndexes(): Unit = {}

  @POST
  @Path("/bundle")
  @Operation(
    summary = "Load a bundle",
    description = "Load a Spark-built bundle from disk",
    requestBody = new RequestBody(
      description = "Bundle load parameters",
      required = true,
      content = Array(new Content(schema = new Schema(implementation = classOf[BundleLoadRequest])))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "201",
        description = "Bundle loaded",
        content = Array(new Content(schema = new Schema(implementation = classOf[BundleInfo])))
      ),
      new ApiResponse(responseCode = "404", description = "Bundle not found"),
      new ApiResponse(responseCode = "409", description = "Index already exists")
    )
  )
  def loadBundle(): Unit = {}

  @GET
  @Path("/{indexId}")
  @Operation(
    summary = "Get bundle info",
    description = "Return a loaded bundle's metadata",
    parameters = Array(
      new Parameter(name = "indexId", in = ParameterIn.PATH, required = true, schema = new Schema(implementation = classOf[String]))
    ),
    responses = Array(
      new ApiResponse(
        responseCode = "200",
        description = "Bundle details",
        content = Array(new Content(schema = new Schema(implementation = classOf[BundleInfo])))
      ),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def getIndex(): Unit = {}

  @DELETE
  @Path("/{indexId}")
  @Operation(
    summary = "Unload a bundle",
    description = "Remove a loaded bundle from memory",
    parameters = Array(
      new Parameter(name = "indexId", in = ParameterIn.PATH, required = true, schema = new Schema(implementation = classOf[String]))
    ),
    responses = Array(
      new ApiResponse(responseCode = "200", description = "Bundle unloaded"),
      new ApiResponse(responseCode = "404", description = "Index not found")
    )
  )
  def unloadIndex(): Unit = {}

  private def toBundleInfo(info: LoadedBundleInfo): BundleInfo =
    BundleInfo(
      indexId = info.indexId,
      bundlePath = info.bundlePath,
      totalVectors = info.totalVectors,
      dimension = info.dimension,
      numLocalIndexes = info.metadata.localIndexes.length,
      hasGlobalIndex = info.metadata.globalIndexPath.isDefined,
      algorithm = info.algorithmId,
      distanceType = info.distanceType,
      loadedAt = info.loadedAt
    )

  private def toUnifiedEntry(info: LoadedBundleInfo): UnifiedIndexEntry =
    UnifiedIndexEntry(
      kind = "bundle",
      indexId = info.indexId,
      dimension = info.dimension,
      size = info.totalVectors,
      distanceType = info.distanceType,
      bundlePath = Some(info.bundlePath),
      numLocalIndexes = Some(info.metadata.localIndexes.length),
      hasGlobalIndex = Some(info.metadata.globalIndexPath.isDefined),
      algorithm = Some(info.algorithmId),
      loadedAt = info.loadedAt
    )
}

object IndexRoutes {
  def apply(indexManager: IndexManager): IndexRoutes = new IndexRoutes(indexManager)
}
