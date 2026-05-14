package com.wayblink.ann.api.model

import spray.json._

/**
 * API request/response models for ANN service.
 */

// ==================== Search API ====================

/**
 * Request to search for nearest neighbors.
 *
 * @param vector Query vector
 * @param k      Number of neighbors to return
 * @param ef     Search ef parameter (optional, default 50)
 */
case class SearchRequest(
  vector: Array[Float],
  k: Int,
  ef: Option[Int] = None
)

/**
 * Single search result.
 *
 * @param id       Vector ID in the index
 * @param distance Distance from query vector
 */
case class SearchResultItem(
  id: Long,
  distance: Float
)

/**
 * Search result with source index information.
 *
 * @param id       Vector ID in the index
 * @param distance Distance from query vector
 * @param indexId  Source index ID
 */
case class MergedSearchResultItem(
  id: Long,
  distance: Float,
  indexId: String
)

/**
 * Response for search request.
 *
 * @param indexId      Index that was searched
 * @param results      List of nearest neighbors
 * @param queryTimeMs  Query execution time in milliseconds
 */
case class SearchResponse(
  indexId: String,
  results: Seq[SearchResultItem],
  queryTimeMs: Long
)

/**
 * Request to search across multiple indexes.
 *
 * @param vector   Query vector
 * @param k        Number of neighbors to return per index
 * @param ef       Search ef parameter (optional)
 * @param indexIds Specific indexes to search (optional, searches all if empty)
 */
case class MultiSearchRequest(
  vector: Array[Float],
  k: Int,
  ef: Option[Int] = None,
  indexIds: Option[Seq[String]] = None
)

/**
 * Response for multi-index search.
 *
 * @param results     Results from each index
 * @param merged      Merged and re-ranked top-k results
 * @param totalTimeMs Total query time
 */
case class MultiSearchResponse(
  results: Map[String, Seq[SearchResultItem]],
  merged: Seq[MergedSearchResultItem],
  totalTimeMs: Long
)

/**
 * Single query in a batch search request.
 *
 * @param vector Query vector
 * @param k      Number of neighbors to return
 */
case class BatchQueryItem(
  vector: Array[Float],
  k: Int
)

/**
 * Request for batch search.
 *
 * @param queries List of queries to execute
 * @param indexId Index to search
 * @param ef      Search ef parameter (optional)
 */
case class BatchSearchRequest(
  queries: Seq[BatchQueryItem],
  indexId: String,
  ef: Option[Int] = None
)

/**
 * Single result in a batch search response.
 *
 * @param queryIndex Index of the query in the batch
 * @param results    Search results for this query
 */
case class BatchSearchResultItem(
  queryIndex: Int,
  results: Seq[SearchResultItem]
)

/**
 * Response for batch search.
 *
 * @param results     Results for each query
 * @param totalTimeMs Total execution time
 */
case class BatchSearchResponse(
  results: Seq[BatchSearchResultItem],
  totalTimeMs: Long
)

// ==================== Index Management API ====================

/**
 * Request to load an index from disk.
 *
 * @param indexId   Unique ID for this index
 * @param indexPath Path to the index file
 */
case class LoadIndexRequest(
  indexId: String,
  indexPath: String
)

/**
 * Configuration for index creation.
 *
 * @param m              HNSW M parameter (optional)
 * @param efConstruction HNSW efConstruction parameter (optional)
 * @param distanceType   Distance type: "euclidean" or "cosine" (optional)
 */
case class IndexConfig(
  m: Option[Int] = None,
  efConstruction: Option[Int] = None,
  distanceType: Option[String] = None
)

/**
 * Request to create an index from vectors.
 *
 * @param indexId   Unique ID for this index
 * @param vectors   Vectors to add (id, vector pairs)
 * @param config    Index configuration (optional)
 */
case class CreateIndexRequest(
  indexId: String,
  vectors: Seq[VectorData],
  config: Option[IndexConfig] = None
)

/**
 * Request to add vectors to an existing index.
 *
 * @param vectors Vectors to add
 */
case class AddVectorsRequest(
  vectors: Seq[VectorData]
)

/**
 * Request to save an index to disk.
 *
 * @param path Path to save the index to
 */
case class SaveIndexRequest(
  path: String
)

/**
 * Vector data for index creation.
 *
 * @param id     Vector ID
 * @param vector Vector values
 */
case class VectorData(
  id: Long,
  vector: Array[Float]
)

/**
 * Information about a loaded index.
 *
 * @param indexId      Index identifier
 * @param dimension    Vector dimension
 * @param size         Number of vectors
 * @param indexPath    Path where index is stored (if loaded from disk)
 * @param distanceType Distance metric used
 */
case class IndexInfo(
  indexId: String,
  dimension: Int,
  size: Int,
  indexPath: Option[String],
  distanceType: Option[String] = None
)

/**
 * Response for listing all indexes.
 *
 * @param indexes      List of loaded indexes
 * @param totalIndexes Total number of indexes
 * @param totalVectors Total vectors across all indexes
 */
case class IndexListResponse(
  indexes: Seq[IndexInfo],
  totalIndexes: Int,
  totalVectors: Long
)

/**
 * Response for index operations.
 *
 * @param success Whether the operation succeeded
 * @param message Description of result
 * @param index   Index info (if applicable)
 */
case class IndexOperationResponse(
  success: Boolean,
  message: String,
  index: Option[IndexInfo] = None
)

// ==================== Health & Status API ====================

/**
 * Service health status.
 *
 * @param status      Service status (healthy/unhealthy)
 * @param version     API version
 * @param indexCount  Number of loaded indexes
 * @param totalVectors Total vectors across all indexes
 */
case class HealthResponse(
  status: String,
  version: String,
  indexCount: Int,
  totalVectors: Long
)

/**
 * Readiness probe response.
 *
 * @param ready Whether the service is ready to accept requests
 */
case class ReadinessResponse(
  ready: Boolean
)

/**
 * Liveness probe response.
 *
 * @param alive Whether the service is alive
 */
case class LivenessResponse(
  alive: Boolean
)

/**
 * Error response.
 *
 * @param error   Error type
 * @param message Error description
 */
case class ErrorResponse(
  error: String,
  message: String
)

// ==================== Bundle-mode DTOs (pattern B) ====================

/**
 * Request payload for loading an offline-built bundle into the
 * api-server. Distinct from LoadIndexRequest (single flat .hnsw) so
 * each path has a clear shape on the wire.
 *
 * @param indexId    Identifier the bundle will be served under
 * @param bundlePath Filesystem path to the bundle root directory
 */
case class BundleLoadRequest(
  indexId: String,
  bundlePath: String
)

/**
 * Summary of a loaded bundle. Returned by bundle CRUD endpoints and
 * by the listing endpoint when the entry is a bundle (distinguished
 * from flat IndexInfo via the `kind` discriminator).
 */
case class BundleInfo(
  indexId: String,
  bundlePath: String,
  totalVectors: Long,
  dimension: Int,
  numLocalIndexes: Int,
  hasGlobalIndex: Boolean,
  algorithm: String,
  distanceType: String,
  loadedAt: Long
)

/**
 * Unified listing entry. `kind` is the JSON discriminator:
 *   - "flat"   → flat field set (size, distanceType, ...) matches IndexInfo
 *   - "bundle" → bundle field set (numLocalIndexes, ...) matches BundleInfo
 *
 * Encoded inline so clients can branch on `kind` without a wrapper.
 */
case class UnifiedIndexEntry(
  kind: String,
  indexId: String,
  dimension: Int,
  size: Long,
  distanceType: String,
  indexPath: Option[String] = None,
  bundlePath: Option[String] = None,
  numLocalIndexes: Option[Int] = None,
  hasGlobalIndex: Option[Boolean] = None,
  algorithm: Option[String] = None,
  loadedAt: Long = 0L
)

case class UnifiedIndexListResponse(
  indexes: Seq[UnifiedIndexEntry],
  totalIndexes: Int,
  totalVectors: Long
)

// ==================== JSON Protocols ====================

object ApiJsonProtocol extends DefaultJsonProtocol {

  // Custom format for Array[Float]
  implicit object FloatArrayFormat extends JsonFormat[Array[Float]] {
    def write(arr: Array[Float]): JsValue = JsArray(arr.map(JsNumber(_)).toVector)
    def read(value: JsValue): Array[Float] = value match {
      case JsArray(elements) => elements.map {
        case JsNumber(n) => n.toFloat
        case other => deserializationError(s"Expected number, got $other")
      }.toArray
      case other => deserializationError(s"Expected array, got $other")
    }
  }

  // Search models
  implicit val vectorDataFormat: RootJsonFormat[VectorData] = jsonFormat2(VectorData)
  implicit val searchRequestFormat: RootJsonFormat[SearchRequest] = jsonFormat3(SearchRequest)
  implicit val searchResultItemFormat: RootJsonFormat[SearchResultItem] = jsonFormat2(SearchResultItem)
  implicit val mergedSearchResultItemFormat: RootJsonFormat[MergedSearchResultItem] = jsonFormat3(MergedSearchResultItem)
  implicit val searchResponseFormat: RootJsonFormat[SearchResponse] = jsonFormat3(SearchResponse)
  implicit val multiSearchRequestFormat: RootJsonFormat[MultiSearchRequest] = jsonFormat4(MultiSearchRequest)
  implicit val multiSearchResponseFormat: RootJsonFormat[MultiSearchResponse] = jsonFormat3(MultiSearchResponse)

  // Batch search models
  implicit val batchQueryItemFormat: RootJsonFormat[BatchQueryItem] = jsonFormat2(BatchQueryItem)
  implicit val batchSearchRequestFormat: RootJsonFormat[BatchSearchRequest] = jsonFormat3(BatchSearchRequest)
  implicit val batchSearchResultItemFormat: RootJsonFormat[BatchSearchResultItem] = jsonFormat2(BatchSearchResultItem)
  implicit val batchSearchResponseFormat: RootJsonFormat[BatchSearchResponse] = jsonFormat2(BatchSearchResponse)

  // Index management models
  implicit val loadIndexRequestFormat: RootJsonFormat[LoadIndexRequest] = jsonFormat2(LoadIndexRequest)
  implicit val indexConfigFormat: RootJsonFormat[IndexConfig] = jsonFormat3(IndexConfig)
  implicit val createIndexRequestFormat: RootJsonFormat[CreateIndexRequest] = jsonFormat3(CreateIndexRequest)
  implicit val addVectorsRequestFormat: RootJsonFormat[AddVectorsRequest] = jsonFormat1(AddVectorsRequest)
  implicit val saveIndexRequestFormat: RootJsonFormat[SaveIndexRequest] = jsonFormat1(SaveIndexRequest)
  implicit val indexInfoFormat: RootJsonFormat[IndexInfo] = jsonFormat5(IndexInfo)
  implicit val indexListResponseFormat: RootJsonFormat[IndexListResponse] = jsonFormat3(IndexListResponse)
  implicit val indexOperationResponseFormat: RootJsonFormat[IndexOperationResponse] = jsonFormat3(IndexOperationResponse)

  // Health models
  implicit val healthResponseFormat: RootJsonFormat[HealthResponse] = jsonFormat4(HealthResponse)
  implicit val readinessResponseFormat: RootJsonFormat[ReadinessResponse] = jsonFormat1(ReadinessResponse)
  implicit val livenessResponseFormat: RootJsonFormat[LivenessResponse] = jsonFormat1(LivenessResponse)
  implicit val errorResponseFormat: RootJsonFormat[ErrorResponse] = jsonFormat2(ErrorResponse)

  // Bundle DTOs (pattern B)
  implicit val bundleLoadRequestFormat: RootJsonFormat[BundleLoadRequest] = jsonFormat2(BundleLoadRequest)
  implicit val bundleInfoFormat: RootJsonFormat[BundleInfo] = jsonFormat9(BundleInfo)
  implicit val unifiedIndexEntryFormat: RootJsonFormat[UnifiedIndexEntry] = jsonFormat11(UnifiedIndexEntry)
  implicit val unifiedIndexListResponseFormat: RootJsonFormat[UnifiedIndexListResponse] = jsonFormat3(UnifiedIndexListResponse)
}
