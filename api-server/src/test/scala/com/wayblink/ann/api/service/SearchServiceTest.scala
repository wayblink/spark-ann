package com.wayblink.ann.api.service

import com.wayblink.ann.api.error.ApiError
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class SearchServiceTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var indexManager: IndexManager = _
  var searchService: SearchService = _

  // Sample vectors for testing
  private val testVectors = Seq(
    (1L, Array(0.0f, 0.0f, 0.0f)),
    (2L, Array(1.0f, 0.0f, 0.0f)),
    (3L, Array(0.0f, 1.0f, 0.0f)),
    (4L, Array(0.0f, 0.0f, 1.0f)),
    (5L, Array(1.0f, 1.0f, 1.0f))
  )

  override def beforeEach(): Unit = {
    indexManager = IndexManager()
    searchService = SearchService(indexManager)
  }

  test("search should return nearest neighbors") {
    indexManager.createIndex("test-index", 3, testVectors)

    val result = searchService.search("test-index", Array(0.0f, 0.0f, 0.0f), k = 3)

    result.isRight shouldBe true
    val searchResult = result.right.get
    searchResult.indexId shouldBe "test-index"
    searchResult.results.size shouldBe 3
    // The closest vector to origin should be vector 1 (at origin)
    searchResult.results.head.id shouldBe 1L
    searchResult.results.head.distance shouldBe 0.0f
  }

  test("search should fail for non-existent index") {
    val result = searchService.search("non-existent", Array(0.0f, 0.0f, 0.0f), k = 3)

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.IndexNotFound]
  }

  test("search should fail for dimension mismatch") {
    indexManager.createIndex("test-index", 3, testVectors)

    val result = searchService.search("test-index", Array(0.0f, 0.0f), k = 3)

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.DimensionMismatch]
  }

  test("search should fail for invalid k") {
    indexManager.createIndex("test-index", 3, testVectors)

    val result = searchService.search("test-index", Array(0.0f, 0.0f, 0.0f), k = 0)

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.InvalidRequest]
  }

  test("search should respect ef parameter") {
    indexManager.createIndex("test-index", 3, testVectors)

    val result = searchService.search("test-index", Array(0.0f, 0.0f, 0.0f), k = 3, ef = Some(100))

    result.isRight shouldBe true
  }

  test("multiSearch should search all indexes when indexIds is None") {
    indexManager.createIndex("index-1", 3, testVectors.take(3))
    indexManager.createIndex("index-2", 3, testVectors.drop(3))

    val result = searchService.multiSearch(Array(0.5f, 0.5f, 0.5f), k = 2)

    result.isRight shouldBe true
    val multiResult = result.right.get
    multiResult.perIndexResults.size shouldBe 2
    multiResult.perIndexResults.contains("index-1") shouldBe true
    multiResult.perIndexResults.contains("index-2") shouldBe true
    multiResult.merged.nonEmpty shouldBe true
  }

  test("multiSearch should search only specified indexes") {
    indexManager.createIndex("index-1", 3, testVectors.take(3))
    indexManager.createIndex("index-2", 3, testVectors.drop(3))

    val result = searchService.multiSearch(
      Array(0.5f, 0.5f, 0.5f),
      k = 2,
      indexIds = Some(Seq("index-1"))
    )

    result.isRight shouldBe true
    val multiResult = result.right.get
    multiResult.perIndexResults.size shouldBe 1
    multiResult.perIndexResults.contains("index-1") shouldBe true
  }

  test("multiSearch should fail for non-existent index in indexIds") {
    indexManager.createIndex("index-1", 3, testVectors)

    val result = searchService.multiSearch(
      Array(0.5f, 0.5f, 0.5f),
      k = 2,
      indexIds = Some(Seq("index-1", "non-existent"))
    )

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.IndexNotFound]
  }

  test("multiSearch should fail when no indexes are available") {
    val result = searchService.multiSearch(Array(0.5f, 0.5f, 0.5f), k = 2)

    result.isLeft shouldBe true
    result.left.get shouldBe ApiError.NoIndexesAvailable
  }

  test("multiSearch merged results should be sorted by distance") {
    indexManager.createIndex("index-1", 3, Seq(
      (1L, Array(0.1f, 0.1f, 0.1f))
    ))
    indexManager.createIndex("index-2", 3, Seq(
      (2L, Array(0.9f, 0.9f, 0.9f))
    ))

    val result = searchService.multiSearch(Array(0.0f, 0.0f, 0.0f), k = 2)

    result.isRight shouldBe true
    val merged = result.right.get.merged
    merged.size shouldBe 2
    // Vector 1 should be closer to origin than vector 2
    merged.head.id shouldBe 1L
    merged.head.indexId shouldBe "index-1"
  }

  test("batchSearch should execute multiple queries") {
    indexManager.createIndex("test-index", 3, testVectors)

    val queries = Seq(
      (Array(0.0f, 0.0f, 0.0f), 2),
      (Array(1.0f, 1.0f, 1.0f), 2)
    )

    val result = searchService.batchSearch("test-index", queries)

    result.isRight shouldBe true
    val batchResult = result.right.get
    batchResult.size shouldBe 2
    batchResult(0).results.size shouldBe 2
    batchResult(1).results.size shouldBe 2
  }

  test("batchSearch should fail for non-existent index") {
    val result = searchService.batchSearch("non-existent", Seq((Array(0.0f, 0.0f, 0.0f), 2)))

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.IndexNotFound]
  }
}
