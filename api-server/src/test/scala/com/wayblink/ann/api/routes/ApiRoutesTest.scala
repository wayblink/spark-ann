package com.wayblink.ann.api.routes

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import com.wayblink.ann.api.model._
import com.wayblink.ann.api.model.ApiJsonProtocol._
import com.wayblink.ann.api.service.{IndexManager, SearchService}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class ApiRoutesTest extends AnyFunSuite with Matchers with ScalatestRouteTest with BeforeAndAfterEach {

  var indexManager: IndexManager = _
  var searchService: SearchService = _
  var apiRoutes: ApiRoutes = _

  override def beforeEach(): Unit = {
    indexManager = IndexManager()
    searchService = SearchService(indexManager)
    apiRoutes = ApiRoutes(indexManager, searchService, "1.0.0")
  }

  // ==================== Health Routes ====================

  test("GET /api/v1/health should return health status") {
    Get("/api/v1/health") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[HealthResponse]
      response.status shouldBe "healthy"
      response.version shouldBe "1.0.0"
      response.indexCount shouldBe 0
      response.totalVectors shouldBe 0
    }
  }

  test("GET /api/v1/health/ready should return readiness") {
    Get("/api/v1/health/ready") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[ReadinessResponse]
      response.ready shouldBe true
    }
  }

  test("GET /api/v1/health/live should return liveness") {
    Get("/api/v1/health/live") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[LivenessResponse]
      response.alive shouldBe true
    }
  }

  // ==================== Index Routes ====================

  test("GET /api/v1/indexes should return empty list initially") {
    Get("/api/v1/indexes") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[IndexListResponse]
      response.indexes shouldBe empty
      response.totalIndexes shouldBe 0
      response.totalVectors shouldBe 0
    }
  }

  test("POST /api/v1/indexes should create index from vectors") {
    val request = CreateIndexRequest(
      indexId = "test-index",
      vectors = Seq(
        VectorData(1L, Array(0.1f, 0.2f, 0.3f)),
        VectorData(2L, Array(0.4f, 0.5f, 0.6f))
      )
    )

    Post("/api/v1/indexes", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.Created
      val response = responseAs[IndexOperationResponse]
      response.success shouldBe true
      response.index.isDefined shouldBe true
      response.index.get.indexId shouldBe "test-index"
      response.index.get.size shouldBe 2
      response.index.get.dimension shouldBe 3
    }
  }

  test("POST /api/v1/indexes should fail for duplicate index") {
    val request = CreateIndexRequest(
      indexId = "test-index",
      vectors = Seq(VectorData(1L, Array(0.1f, 0.2f, 0.3f)))
    )

    Post("/api/v1/indexes", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.Created
    }

    Post("/api/v1/indexes", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.Conflict
      val response = responseAs[ErrorResponse]
      response.error shouldBe "IndexAlreadyExists"
    }
  }

  test("GET /api/v1/indexes/{indexId} should return index info") {
    indexManager.createIndex("test-index", 3, Seq((1L, Array(0.1f, 0.2f, 0.3f))))

    Get("/api/v1/indexes/test-index") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[IndexInfo]
      response.indexId shouldBe "test-index"
      response.dimension shouldBe 3
      response.size shouldBe 1
    }
  }

  test("GET /api/v1/indexes/{indexId} should return 404 for non-existent index") {
    Get("/api/v1/indexes/non-existent") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.NotFound
      val response = responseAs[ErrorResponse]
      response.error shouldBe "IndexNotFound"
    }
  }

  test("DELETE /api/v1/indexes/{indexId} should unload index") {
    indexManager.createIndex("test-index", 3, Seq((1L, Array(0.1f, 0.2f, 0.3f))))

    Delete("/api/v1/indexes/test-index") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[IndexOperationResponse]
      response.success shouldBe true
    }

    indexManager.exists("test-index") shouldBe false
  }

  test("POST /api/v1/indexes/{indexId}/vectors should add vectors") {
    indexManager.createIndex("test-index", 3, Seq((1L, Array(0.1f, 0.2f, 0.3f))))

    val request = AddVectorsRequest(
      vectors = Seq(VectorData(2L, Array(0.4f, 0.5f, 0.6f)))
    )

    Post("/api/v1/indexes/test-index/vectors", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[IndexOperationResponse]
      response.success shouldBe true
      response.index.get.size shouldBe 2
    }
  }

  // ==================== Search Routes ====================

  test("POST /api/v1/indexes/{indexId}/search should return search results") {
    indexManager.createIndex("test-index", 3, Seq(
      (1L, Array(0.0f, 0.0f, 0.0f)),
      (2L, Array(1.0f, 1.0f, 1.0f))
    ))

    val request = SearchRequest(
      vector = Array(0.0f, 0.0f, 0.0f),
      k = 2
    )

    Post("/api/v1/indexes/test-index/search", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[SearchResponse]
      response.indexId shouldBe "test-index"
      response.results.size shouldBe 2
      response.results.head.id shouldBe 1L
      response.results.head.distance shouldBe 0.0f
    }
  }

  test("POST /api/v1/indexes/{indexId}/search should return 404 for non-existent index") {
    val request = SearchRequest(
      vector = Array(0.0f, 0.0f, 0.0f),
      k = 2
    )

    Post("/api/v1/indexes/non-existent/search", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.NotFound
      val response = responseAs[ErrorResponse]
      response.error shouldBe "IndexNotFound"
    }
  }

  test("POST /api/v1/indexes/{indexId}/search should return 422 for dimension mismatch") {
    indexManager.createIndex("test-index", 3, Seq((1L, Array(0.0f, 0.0f, 0.0f))))

    val request = SearchRequest(
      vector = Array(0.0f, 0.0f), // Wrong dimension
      k = 2
    )

    Post("/api/v1/indexes/test-index/search", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.UnprocessableEntity
      val response = responseAs[ErrorResponse]
      response.error shouldBe "DimensionMismatch"
    }
  }

  test("POST /api/v1/search should search all indexes") {
    indexManager.createIndex("index-1", 3, Seq((1L, Array(0.0f, 0.0f, 0.0f))))
    indexManager.createIndex("index-2", 3, Seq((2L, Array(1.0f, 1.0f, 1.0f))))

    val request = MultiSearchRequest(
      vector = Array(0.5f, 0.5f, 0.5f),
      k = 1
    )

    Post("/api/v1/search", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[MultiSearchResponse]
      response.results.size shouldBe 2
      response.merged.nonEmpty shouldBe true
    }
  }

  test("POST /api/v1/search/batch should execute batch search") {
    indexManager.createIndex("test-index", 3, Seq(
      (1L, Array(0.0f, 0.0f, 0.0f)),
      (2L, Array(1.0f, 1.0f, 1.0f))
    ))

    val request = BatchSearchRequest(
      queries = Seq(
        BatchQueryItem(Array(0.0f, 0.0f, 0.0f), 1),
        BatchQueryItem(Array(1.0f, 1.0f, 1.0f), 1)
      ),
      indexId = "test-index"
    )

    Post("/api/v1/search/batch", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[BatchSearchResponse]
      response.results.size shouldBe 2
    }
  }

  test("POST /api/v1/indexes/{indexId}/search should validate k parameter") {
    indexManager.createIndex("test-index", 3, Seq((1L, Array(0.0f, 0.0f, 0.0f))))

    val request = SearchRequest(
      vector = Array(0.0f, 0.0f, 0.0f),
      k = 0 // Invalid
    )

    Post("/api/v1/indexes/test-index/search", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.BadRequest
      val response = responseAs[ErrorResponse]
      response.error shouldBe "InvalidParameter"
    }
  }

  test("POST /api/v1/indexes/{indexId}/search should validate empty vector") {
    indexManager.createIndex("test-index", 3, Seq((1L, Array(0.0f, 0.0f, 0.0f))))

    val request = SearchRequest(
      vector = Array.empty[Float],
      k = 2
    )

    Post("/api/v1/indexes/test-index/search", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.BadRequest
      val response = responseAs[ErrorResponse]
      response.error shouldBe "InvalidVector"
    }
  }
}
