package com.wayblink.ann.api.service

import com.wayblink.ann.api.error.ApiError
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

class IndexManagerTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var indexManager: IndexManager = _

  override def beforeEach(): Unit = {
    indexManager = IndexManager()
  }

  test("createIndex should create an index from vectors") {
    val vectors = Seq(
      (1L, Array(0.1f, 0.2f, 0.3f)),
      (2L, Array(0.4f, 0.5f, 0.6f)),
      (3L, Array(0.7f, 0.8f, 0.9f))
    )

    val result = indexManager.createIndex("test-index", 3, vectors)

    result.isRight shouldBe true
    result.right.get.indexId shouldBe "test-index"
    result.right.get.index.size shouldBe 3
    result.right.get.index.dimension shouldBe 3
  }

  test("createIndex should fail if index already exists") {
    val vectors = Seq((1L, Array(0.1f, 0.2f, 0.3f)))
    indexManager.createIndex("test-index", 3, vectors)

    val result = indexManager.createIndex("test-index", 3, vectors)

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.IndexAlreadyExists]
  }

  test("getIndex should return None for non-existent index") {
    indexManager.getIndex("non-existent") shouldBe None
  }

  test("getIndex should return index info for existing index") {
    val vectors = Seq((1L, Array(0.1f, 0.2f, 0.3f)))
    indexManager.createIndex("test-index", 3, vectors)

    val result = indexManager.getIndex("test-index")

    result.isDefined shouldBe true
    result.get.indexId shouldBe "test-index"
  }

  test("listIndexes should return all loaded indexes") {
    indexManager.createIndex("index-1", 3, Seq((1L, Array(0.1f, 0.2f, 0.3f))))
    indexManager.createIndex("index-2", 3, Seq((2L, Array(0.4f, 0.5f, 0.6f))))

    val indexes = indexManager.listIndexes()

    indexes.size shouldBe 2
    indexes.map(_.indexId).toSet shouldBe Set("index-1", "index-2")
  }

  test("unloadIndex should remove index from memory") {
    indexManager.createIndex("test-index", 3, Seq((1L, Array(0.1f, 0.2f, 0.3f))))

    indexManager.unloadIndex("test-index") shouldBe true
    indexManager.getIndex("test-index") shouldBe None
  }

  test("unloadIndex should return false for non-existent index") {
    indexManager.unloadIndex("non-existent") shouldBe false
  }

  test("addVectors should add vectors to existing index") {
    indexManager.createIndex("test-index", 3, Seq((1L, Array(0.1f, 0.2f, 0.3f))))

    val result = indexManager.addVectors("test-index", Seq(
      (2L, Array(0.4f, 0.5f, 0.6f)),
      (3L, Array(0.7f, 0.8f, 0.9f))
    ))

    result.isRight shouldBe true
    indexManager.getIndex("test-index").get.index.size shouldBe 3
  }

  test("addVectors should fail for non-existent index") {
    val result = indexManager.addVectors("non-existent", Seq((1L, Array(0.1f, 0.2f, 0.3f))))

    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.IndexNotFound]
  }

  test("indexCount should return correct count") {
    indexManager.indexCount shouldBe 0

    indexManager.createIndex("index-1", 3, Seq((1L, Array(0.1f, 0.2f, 0.3f))))
    indexManager.indexCount shouldBe 1

    indexManager.createIndex("index-2", 3, Seq((2L, Array(0.4f, 0.5f, 0.6f))))
    indexManager.indexCount shouldBe 2
  }

  test("totalVectors should return sum of all vectors") {
    indexManager.totalVectors shouldBe 0

    indexManager.createIndex("index-1", 3, Seq(
      (1L, Array(0.1f, 0.2f, 0.3f)),
      (2L, Array(0.4f, 0.5f, 0.6f))
    ))
    indexManager.createIndex("index-2", 3, Seq(
      (3L, Array(0.7f, 0.8f, 0.9f))
    ))

    indexManager.totalVectors shouldBe 3
  }

  test("exists should return correct result") {
    indexManager.exists("test-index") shouldBe false

    indexManager.createIndex("test-index", 3, Seq((1L, Array(0.1f, 0.2f, 0.3f))))

    indexManager.exists("test-index") shouldBe true
  }
}
