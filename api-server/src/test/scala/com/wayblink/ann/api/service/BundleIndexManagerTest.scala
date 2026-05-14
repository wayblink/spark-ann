package com.wayblink.ann.api.service

import com.wayblink.ann.api.error.ApiError
import com.wayblink.ann.bundle._
import com.wayblink.ann.core.index.{HNSWConfig, HNSWLibIndex}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

/**
 * Pattern-B integration test: build a bundle on disk by hand (without
 * Spark — this module doesn't depend on it), load it through
 * IndexManager.loadBundle, route + search via SearchService, and
 * assert that the typed-error contract holds for the unhappy paths.
 */
class BundleIndexManagerTest extends AnyFunSuite with Matchers {

  /** Build a tiny bundle with two local indexes and a routing index. */
  private def writeBundle(): Path = {
    val root = Files.createTempDirectory("api-bundle-test")
    val localDir = Files.createDirectories(root.resolve("local"))
    val globalDir = Files.createDirectories(root.resolve("global"))

    val cfg = HNSWConfig(M = 8, efConstruction = 50, maxElements = 16)

    val a = HNSWLibIndex(4, cfg, "euclidean")
    a.add(1L, Array(0.0f, 0.0f, 0.0f, 0.0f))
    a.add(2L, Array(0.1f, 0.1f, 0.1f, 0.1f))
    val aPath = localDir.resolve("idx_a.hnsw").toString
    a.save(aPath)

    val b = HNSWLibIndex(4, cfg, "euclidean")
    b.add(10L, Array(5.0f, 5.0f, 5.0f, 5.0f))
    b.add(11L, Array(5.1f, 5.1f, 5.1f, 5.1f))
    val bPath = localDir.resolve("idx_b.hnsw").toString
    b.save(bPath)

    val global = HNSWLibIndex(4, cfg, "euclidean")
    global.add(0L, Array(0.05f, 0.05f, 0.05f, 0.05f))   // anchor for idx_a
    global.add(1L, Array(5.05f, 5.05f, 5.05f, 5.05f))   // anchor for idx_b
    val globalPath = globalDir.resolve("global_routing.hnsw").toString
    global.save(globalPath)

    val metadata = ANNIndexMetadata(
      indexPath = root.toString,
      localIndexes = Array(
        LocalIndexMetadata("idx_a",
          Array(DataFileEntry("/x/a.parquet", 2L, 0L)), aPath, 2L, 4),
        LocalIndexMetadata("idx_b",
          Array(DataFileEntry("/x/b.parquet", 2L, 0L)), bPath, 2L, 4)
      ),
      globalIndexPath = Some(globalPath),
      config = ANNIndexConfig(distanceType = "euclidean"),
      statistics = ANNIndexStatistics(4L, 2, 2, 4, 0L)
    )
    MetadataJson.writeMetadata(metadata, root.resolve("ann_index.json"))

    val mapping = Array(
      BoundaryMappingEntry(0, "idx_a", 1L),
      BoundaryMappingEntry(1, "idx_b", 10L)
    )
    MetadataJson.writeBoundaryMapping(mapping, globalDir.resolve("boundary_mapping.json"))
    root
  }

  test("loadBundle reads metadata, locals, and routing index from disk") {
    val mgr = IndexManager()
    val root = writeBundle()
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
    val empty = Files.createTempDirectory("not-a-bundle")
    val result = mgr.loadBundle("svc", empty.toString)
    result.isLeft shouldBe true
    result.left.get shouldBe a [ApiError.BundleNotFound]
  }

  test("loadBundle returns IndexAlreadyExists when the id is taken") {
    val mgr = IndexManager()
    val root = writeBundle()
    mgr.loadBundle("svc", root.toString).isRight shouldBe true
    val again = mgr.loadBundle("svc", root.toString)
    again.left.get shouldBe a [ApiError.IndexAlreadyExists]
  }

  test("flat indexes and bundles share a single id namespace") {
    val mgr = IndexManager()
    mgr.createIndex("shared", 4, Seq((1L, Array(0.1f, 0.1f, 0.1f, 0.1f))))
    val root = writeBundle()
    val result = mgr.loadBundle("shared", root.toString)
    result.left.get shouldBe a [ApiError.IndexAlreadyExists]
  }

  test("search dispatches through routing for a bundle and finds the right cluster") {
    val mgr = IndexManager()
    val svc = SearchService(mgr)
    val root = writeBundle()
    mgr.loadBundle("svc", root.toString)

    // Query close to idx_a's region — top result should come from idx_a.
    val Right(near) = svc.search("svc", Array(0.0f, 0.0f, 0.0f, 0.0f), k = 1)
    near.results.head.id should (be(1L) or be(2L))

    val Right(far) = svc.search("svc", Array(5.0f, 5.0f, 5.0f, 5.0f), k = 1)
    far.results.head.id should (be(10L) or be(11L))
  }

  test("search returns DimensionMismatch when query length differs from bundle dim") {
    val mgr = IndexManager()
    val svc = SearchService(mgr)
    val root = writeBundle()
    mgr.loadBundle("svc", root.toString)

    val result = svc.search("svc", Array(0.1f, 0.2f), k = 1)
    result.left.get shouldBe a [ApiError.DimensionMismatch]
  }

  test("listEntries enumerates flat and bundle entries together") {
    val mgr = IndexManager()
    mgr.createIndex("flat", 4, Seq((1L, Array(0.1f, 0.1f, 0.1f, 0.1f))))
    val root = writeBundle()
    mgr.loadBundle("svc", root.toString)

    val entries = mgr.listEntries()
    entries.collect { case IndexEntry.Flat(_) => 1 }.size shouldBe 1
    entries.collect { case IndexEntry.Bundle(_) => 1 }.size shouldBe 1
    mgr.totalVectors shouldBe (1L + 4L) // 1 flat + 4 bundle vectors
  }

  test("unloadIndex removes either flat or bundle entries") {
    val mgr = IndexManager()
    val root = writeBundle()
    mgr.loadBundle("svc", root.toString)
    mgr.unloadIndex("svc") shouldBe true
    mgr.exists("svc") shouldBe false
  }
}
