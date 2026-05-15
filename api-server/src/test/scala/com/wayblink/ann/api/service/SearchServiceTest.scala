package com.wayblink.ann.api.service

import com.wayblink.ann.api.error.ApiError
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class SearchServiceTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var indexManager: IndexManager = _
  var searchService: SearchService = _
  var bundlePath: java.nio.file.Path = _

  override def beforeEach(): Unit = {
    indexManager = IndexManager()
    searchService = SearchService(indexManager)
    bundlePath = BundleIndexManagerTestHelper.writeBundle()
    indexManager.loadBundle("svc", bundlePath.toString).isRight shouldBe true
  }

  test("search should return nearest neighbors from a loaded bundle") {
    val result = searchService.search("svc", Array(0.0f, 0.0f, 0.0f, 0.0f), k = 3)

    result.isRight shouldBe true
    val searchResult = result.right.get
    searchResult.indexId shouldBe "svc"
    searchResult.results.size shouldBe 3
    searchResult.results.head.id should (be(1L) or be(2L))
  }

  test("search should fail for non-existent bundle") {
    val result = searchService.search("missing", Array(0.0f, 0.0f, 0.0f, 0.0f), k = 3)

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.IndexNotFound]
  }

  test("search should fail for dimension mismatch") {
    val result = searchService.search("svc", Array(0.0f, 0.0f), k = 3)

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.DimensionMismatch]
  }

  test("search should fail for invalid k") {
    val result = searchService.search("svc", Array(0.0f, 0.0f, 0.0f, 0.0f), k = 0)

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.InvalidRequest]
  }

  test("multiSearch should search all loaded bundles when indexIds is None") {
    val second = BundleIndexManagerTestHelper.writeBundle()
    indexManager.loadBundle("svc-2", second.toString).isRight shouldBe true

    val result = searchService.multiSearch(Array(0.5f, 0.5f, 0.5f, 0.5f), k = 2)

    result.isRight shouldBe true
    val multiResult = result.right.get
    multiResult.perIndexResults.keySet shouldBe Set("svc", "svc-2")
    multiResult.merged.nonEmpty shouldBe true
  }

  test("multiSearch should search only specified bundles") {
    val second = BundleIndexManagerTestHelper.writeBundle()
    indexManager.loadBundle("svc-2", second.toString).isRight shouldBe true

    val result = searchService.multiSearch(
      Array(0.5f, 0.5f, 0.5f, 0.5f),
      k = 2,
      indexIds = Some(Seq("svc"))
    )

    result.isRight shouldBe true
    val multiResult = result.right.get
    multiResult.perIndexResults.keySet shouldBe Set("svc")
  }

  test("multiSearch should fail for non-existent bundle in indexIds") {
    val result = searchService.multiSearch(
      Array(0.5f, 0.5f, 0.5f, 0.5f),
      k = 2,
      indexIds = Some(Seq("svc", "missing"))
    )

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.IndexNotFound]
  }

  test("multiSearch should fail when no bundles are available") {
    val emptyManager = IndexManager()
    val emptyService = SearchService(emptyManager)
    val result = emptyService.multiSearch(Array(0.5f, 0.5f, 0.5f, 0.5f), k = 2)

    result.isLeft shouldBe true
    result.left.get shouldBe ApiError.NoIndexesAvailable
  }

  test("batchSearch should execute multiple queries") {
    val queries = Seq(
      (Array(0.0f, 0.0f, 0.0f, 0.0f), 2),
      (Array(5.0f, 5.0f, 5.0f, 5.0f), 2)
    )

    val result = searchService.batchSearch("svc", queries)

    result.isRight shouldBe true
    val batchResult = result.right.get
    batchResult.size shouldBe 2
    batchResult(0).results.nonEmpty shouldBe true
    batchResult(1).results.nonEmpty shouldBe true
  }

  test("batchSearch should fail for non-existent bundle") {
    val result = searchService.batchSearch("missing", Seq((Array(0.0f, 0.0f, 0.0f, 0.0f), 2)))

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.IndexNotFound]
  }
}
