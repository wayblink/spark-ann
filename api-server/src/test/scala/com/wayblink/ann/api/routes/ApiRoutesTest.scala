package com.wayblink.ann.api.routes

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.testkit.ScalatestRouteTest
import com.wayblink.ann.api.model._
import com.wayblink.ann.api.model.ApiJsonProtocol._
import com.wayblink.ann.api.service.{IndexManager, SearchService}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ApiRoutesTest extends AnyFunSuite with Matchers with ScalatestRouteTest with BeforeAndAfterEach {

  var indexManager: IndexManager = _
  var searchService: SearchService = _
  var apiRoutes: ApiRoutes = _
  var bundleRoot: java.nio.file.Path = _

  override def beforeEach(): Unit = {
    indexManager = IndexManager()
    searchService = SearchService(indexManager)
    apiRoutes = ApiRoutes(indexManager, searchService, "1.0.0")
    bundleRoot = com.wayblink.ann.api.service.BundleIndexManagerTestHelper.writeBundle()
  }

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

  test("GET /api/v1/indexes should return empty list initially") {
    Get("/api/v1/indexes") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[UnifiedIndexListResponse]
      response.indexes shouldBe empty
      response.totalIndexes shouldBe 0
      response.totalVectors shouldBe 0
    }
  }

  test("POST /api/v1/indexes/bundle should load a bundle") {
    val request = BundleLoadRequest("test-bundle", bundleRoot.toString)

    Post("/api/v1/indexes/bundle", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.Created
      val response = responseAs[BundleInfo]
      response.indexId shouldBe "test-bundle"
      response.totalVectors shouldBe 4L
      response.numLocalIndexes shouldBe 2
    }
  }

  test("GET /api/v1/indexes/{indexId} should return bundle info") {
    indexManager.loadBundle("test-bundle", bundleRoot.toString).isRight shouldBe true

    Get("/api/v1/indexes/test-bundle") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[BundleInfo]
      response.indexId shouldBe "test-bundle"
      response.dimension shouldBe 4
    }
  }

  test("GET /api/v1/indexes/{indexId} should return 404 for non-existent index") {
    Get("/api/v1/indexes/non-existent") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.NotFound
      val response = responseAs[ErrorResponse]
      response.error shouldBe "index_not_found"
    }
  }

  test("DELETE /api/v1/indexes/{indexId} should unload bundle") {
    indexManager.loadBundle("test-bundle", bundleRoot.toString).isRight shouldBe true

    Delete("/api/v1/indexes/test-bundle") ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
    }

    indexManager.exists("test-bundle") shouldBe false
  }

  test("POST /api/v1/indexes/{indexId}/search should return search results") {
    indexManager.loadBundle("test-bundle", bundleRoot.toString).isRight shouldBe true

    val request = SearchRequest(
      vector = Array(0.0f, 0.0f, 0.0f, 0.0f),
      k = 2
    )

    Post("/api/v1/indexes/test-bundle/search", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[SearchResponse]
      response.indexId shouldBe "test-bundle"
      response.results.nonEmpty shouldBe true
    }
  }

  test("POST /api/v1/search should search all bundles") {
    indexManager.loadBundle("bundle-a", bundleRoot.toString).isRight shouldBe true
    val bundleB = com.wayblink.ann.api.service.BundleIndexManagerTestHelper.writeBundle()
    indexManager.loadBundle("bundle-b", bundleB.toString).isRight shouldBe true

    val request = MultiSearchRequest(
      vector = Array(0.5f, 0.5f, 0.5f, 0.5f),
      k = 1
    )

    Post("/api/v1/search", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[MultiSearchResponse]
      response.results.keySet shouldBe Set("bundle-a", "bundle-b")
      response.merged.nonEmpty shouldBe true
    }
  }

  test("POST /api/v1/search/batch should execute batch search") {
    indexManager.loadBundle("test-bundle", bundleRoot.toString).isRight shouldBe true

    val request = BatchSearchRequest(
      queries = Seq(
        BatchQueryItem(Array(0.0f, 0.0f, 0.0f, 0.0f), 1),
        BatchQueryItem(Array(5.0f, 5.0f, 5.0f, 5.0f), 1)
      ),
      indexId = "test-bundle"
    )

    Post("/api/v1/search/batch", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.OK
      val response = responseAs[BatchSearchResponse]
      response.results.size shouldBe 2
    }
  }

  test("POST /api/v1/search should validate k parameter") {
    val request = MultiSearchRequest(
      vector = Array(0.0f, 0.0f, 0.0f, 0.0f),
      k = 0
    )

    Post("/api/v1/search", request) ~> apiRoutes.routes ~> check {
      status shouldBe StatusCodes.BadRequest
      val response = responseAs[ErrorResponse]
      response.error shouldBe "invalid_request"
    }
  }
}
