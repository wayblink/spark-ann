package com.wayblink.ann.api.service

import com.wayblink.ann.api.error.ApiError
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class BundleIndexManagerTest extends AnyFunSuite with Matchers {

  test("loadBundle reads metadata, locals, and routing index from disk") {
    val mgr = IndexManager()
    val root = BundleIndexManagerTestHelper.writeBundle()
    val Right(info) = mgr.loadBundle("svc", root.toString)

    info.indexId shouldBe "svc"
    info.dimension shouldBe 4
    info.totalVectors shouldBe 4L
    info.distanceType shouldBe "euclidean"
    info.algorithmId shouldBe "hnsw"
    info.localIndexes.keySet shouldBe Set("idx_a", "idx_b")
    info.globalIndex shouldBe defined
    info.boundaryMap should have length 2
  }

  test("loadBundle rejects a path that is not a bundle directory") {
    val mgr = IndexManager()
    val empty = java.nio.file.Files.createTempDirectory("not-a-bundle")
    val result = mgr.loadBundle("svc", empty.toString)
    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.BundleNotFound]
  }

  test("loadBundle returns IndexAlreadyExists when the id is taken") {
    val mgr = IndexManager()
    val root = BundleIndexManagerTestHelper.writeBundle()
    mgr.loadBundle("svc", root.toString).isRight shouldBe true
    val again = mgr.loadBundle("svc", root.toString)
    again.left.get shouldBe a [ApiError.IndexAlreadyExists]
  }

  test("bundle ids share a single id namespace") {
    val mgr = IndexManager()
    val root = BundleIndexManagerTestHelper.writeBundle()
    mgr.loadBundle("shared", root.toString).isRight shouldBe true
    val duplicate = mgr.loadBundle("shared", root.toString)
    duplicate.left.get shouldBe a [ApiError.IndexAlreadyExists]
  }

  test("listBundles enumerates loaded bundles") {
    val mgr = IndexManager()
    val root = BundleIndexManagerTestHelper.writeBundle()
    mgr.loadBundle("svc", root.toString)

    val bundles = mgr.listBundles()
    bundles.map(_.indexId) shouldBe Seq("svc")
    mgr.indexCount shouldBe 1
    mgr.totalVectors shouldBe 4L
  }

  test("unloadIndex removes a loaded bundle") {
    val mgr = IndexManager()
    val root = BundleIndexManagerTestHelper.writeBundle()
    mgr.loadBundle("svc", root.toString)

    mgr.unloadIndex("svc") shouldBe true
    mgr.getBundle("svc") shouldBe None
    mgr.exists("svc") shouldBe false
  }

  test("getBundle returns None for unknown id") {
    val mgr = IndexManager()
    mgr.getBundle("missing") shouldBe None
  }

  test("loadBundle returns CapacityExceeded once max-loaded-indexes is reached") {
    val mgr = IndexManager(maxLoadedIndexes = 1)
    val root = BundleIndexManagerTestHelper.writeBundle()
    mgr.loadBundle("first", root.toString).isRight shouldBe true

    val secondRoot = BundleIndexManagerTestHelper.writeBundle()
    val rejected = mgr.loadBundle("second", secondRoot.toString)
    rejected.isLeft shouldBe true
    val err = rejected.left.get.asInstanceOf[ApiError.CapacityExceeded]
    err.loaded shouldBe 1
    err.max shouldBe 1

    // After unloading, capacity is freed.
    mgr.unloadIndex("first") shouldBe true
    mgr.loadBundle("second", secondRoot.toString).isRight shouldBe true
  }
}
