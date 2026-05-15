package com.wayblink.ann.api.service

import com.wayblink.ann.api.error.ApiError
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IndexManagerTest extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  var indexManager: IndexManager = _

  override def beforeEach(): Unit = {
    indexManager = IndexManager()
  }

  test("legacy flat-index methods are removed") {
    val methods = classOf[IndexManager].getMethods.map(_.getName).toSet
    methods should not contain "loadIndex"
    methods should not contain "createIndex"
    methods should not contain "getIndex"
    methods should not contain "listIndexes"
    methods should not contain "addVectors"
    methods should not contain "saveIndex"
  }

  test("getBundle should return None for non-existent bundle") {
    indexManager.getBundle("non-existent") shouldBe None
  }

  test("listBundles should return all loaded bundles") {
    val root = BundleIndexManagerTestHelper.writeBundle()
    indexManager.loadBundle("bundle-1", root.toString).isRight shouldBe true

    val bundles = indexManager.listBundles()
    bundles.map(_.indexId) shouldBe Seq("bundle-1")
  }

  test("loadBundle should fail if bundle id already exists") {
    val root = BundleIndexManagerTestHelper.writeBundle()
    indexManager.loadBundle("bundle-1", root.toString).isRight shouldBe true
    val result = indexManager.loadBundle("bundle-1", root.toString)
    result.left.get shouldBe a [ApiError.IndexAlreadyExists]
  }

  test("unloadIndex should remove a loaded bundle") {
    val root = BundleIndexManagerTestHelper.writeBundle()
    indexManager.loadBundle("bundle-1", root.toString).isRight shouldBe true

    indexManager.unloadIndex("bundle-1") shouldBe true
    indexManager.getBundle("bundle-1") shouldBe None
  }

  test("exists should reflect loaded bundles") {
    val root = BundleIndexManagerTestHelper.writeBundle()
    indexManager.loadBundle("bundle-1", root.toString).isRight shouldBe true

    indexManager.exists("bundle-1") shouldBe true
    indexManager.exists("missing") shouldBe false
  }
}
