package com.wayblink.ann.bundle

import com.wayblink.ann.core.index.{HNSWConfig, HNSWLibIndex}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

class BundleReaderTest extends AnyFunSuite with Matchers {

  /** Build a small bundle on disk and return its root path. */
  private def writeBundle(): Path = {
    val root = Files.createTempDirectory("bundle-reader-test")
    val localDir = Files.createDirectories(root.resolve("local"))
    val globalDir = Files.createDirectories(root.resolve("global"))

    // Two tiny local HNSW indexes so loadAllLocalIndexes has something
    // to chew on. Vectors are 4-d so the test stays fast.
    val cfg = HNSWConfig(M = 8, efConstruction = 50, maxElements = 16)
    val a = HNSWLibIndex(4, cfg, "euclidean")
    a.add(1L, Array(0.1f, 0.2f, 0.3f, 0.4f))
    a.add(2L, Array(0.5f, 0.6f, 0.7f, 0.8f))
    val aPath = localDir.resolve("idx_a.hnsw").toString
    a.save(aPath)

    val b = HNSWLibIndex(4, cfg, "euclidean")
    b.add(10L, Array(0.9f, 0.8f, 0.7f, 0.6f))
    val bPath = localDir.resolve("idx_b.hnsw").toString
    b.save(bPath)

    val global = HNSWLibIndex(4, cfg, "euclidean")
    global.add(0L, Array(0.1f, 0.2f, 0.3f, 0.4f))
    global.add(1L, Array(0.9f, 0.8f, 0.7f, 0.6f))
    val globalPath = globalDir.resolve("global_routing.hnsw").toString
    global.save(globalPath)

    val metadata = ANNIndexMetadata(
      indexPath = root.toString,
      localIndexes = Array(
        LocalIndexMetadata("idx_a", Array(DataFileEntry("/x/a.parquet", 2L, 0L)),
          aPath, 2L, 4),
        LocalIndexMetadata("idx_b", Array(DataFileEntry("/x/b.parquet", 1L, 0L)),
          bPath, 1L, 4)
      ),
      globalIndexPath = Some(globalPath),
      config = ANNIndexConfig(distanceType = "euclidean"),
      statistics = ANNIndexStatistics(3L, 2, 2, 4, 0L)
    )
    MetadataJson.writeMetadata(metadata, root.resolve("ann_index.json"))

    val mapping = Array(
      BoundaryMappingEntry(0, "idx_a", 1L),
      BoundaryMappingEntry(1, "idx_b", 10L)
    )
    MetadataJson.writeBoundaryMapping(mapping, globalDir.resolve("boundary_mapping.json"))

    root
  }

  test("isBundle recognises directories with ann_index.json") {
    val root = writeBundle()
    BundleReader.isBundle(root) shouldBe true
    BundleReader.isBundle(root.toString) shouldBe true
  }

  test("isBundle rejects non-bundle paths") {
    val empty = Files.createTempDirectory("not-a-bundle")
    BundleReader.isBundle(empty) shouldBe false
    val missing = empty.resolve("does-not-exist")
    BundleReader.isBundle(missing) shouldBe false
  }

  test("loadMetadata round-trips and surfaces metadata fields") {
    val root = writeBundle()
    BundleReader.loadMetadata(root) match {
      case Right(meta) =>
        meta.localIndexes should have length 2
        meta.statistics.totalVectors shouldBe 3L
        meta.globalIndexPath shouldBe defined
      case Left(err) =>
        fail(s"Expected Right(metadata) but got Left($err)")
    }
  }

  test("loadMetadata returns BundleNotFound for nonexistent paths") {
    val missing = Files.createTempDirectory("dir-without-bundle").resolve("missing")
    BundleReader.loadMetadata(missing) match {
      case Left(BundleError.BundleNotFound(_)) => succeed
      case other => fail(s"Expected BundleNotFound, got $other")
    }
  }

  test("loadMetadata returns InvalidBundle when ann_index.json is missing") {
    val empty = Files.createTempDirectory("empty-dir")
    BundleReader.loadMetadata(empty) match {
      case Left(BundleError.InvalidBundle(_, reason)) =>
        reason should include("ann_index.json")
      case other => fail(s"Expected InvalidBundle, got $other")
    }
  }

  test("loadBoundaryMap produces a positional Array[String] indexed by globalId") {
    val root = writeBundle()
    val Right(meta) = BundleReader.loadMetadata(root)
    val boundary = BundleReader.loadBoundaryMap(root, meta)

    boundary should have length 2
    boundary(0) shouldBe "idx_a"
    boundary(1) shouldBe "idx_b"
  }

  test("loadBoundaryMap returns empty array when bundle has no global index") {
    val root = Files.createTempDirectory("single-local-bundle")
    Files.createDirectories(root.resolve("local"))
    val cfg = HNSWConfig(M = 8, efConstruction = 50, maxElements = 4)
    val solo = HNSWLibIndex(4, cfg, "euclidean")
    solo.add(1L, Array(0.1f, 0.2f, 0.3f, 0.4f))
    val soloPath = root.resolve("local").resolve("idx_solo.hnsw").toString
    solo.save(soloPath)
    val meta = ANNIndexMetadata(
      indexPath = root.toString,
      localIndexes = Array(
        LocalIndexMetadata("idx_solo", Array(DataFileEntry("/x/solo.parquet", 1L, 0L)),
          soloPath, 1L, 4)
      ),
      globalIndexPath = None,
      config = ANNIndexConfig(),
      statistics = ANNIndexStatistics(1L, 1, 1, 4, 0L)
    )
    MetadataJson.writeMetadata(meta, root.resolve("ann_index.json"))

    BundleReader.loadBoundaryMap(root, meta) shouldBe empty
  }

  test("loadAllLocalIndexes loads every entry and returns a working HNSW") {
    val root = writeBundle()
    val Right(meta) = BundleReader.loadMetadata(root)
    val locals = BundleReader.loadAllLocalIndexes(meta)

    locals.keySet shouldBe Set("idx_a", "idx_b")
    val hits = locals("idx_a").search(Array(0.1f, 0.2f, 0.3f, 0.4f), k = 1, ef = 10)
    hits.head.id shouldBe 1L
  }

  test("loadGlobalIndex returns None when bundle has no global routing") {
    val root = Files.createTempDirectory("no-global")
    val meta = ANNIndexMetadata(
      indexPath = root.toString,
      localIndexes = Array.empty,
      globalIndexPath = None,
      config = ANNIndexConfig(),
      statistics = ANNIndexStatistics(0L, 0, 0, 0, 0L)
    )
    BundleReader.loadGlobalIndex(meta) shouldBe None
  }

  test("loadMetadata wraps unknown-version errors as InvalidBundle") {
    val root = Files.createTempDirectory("future-version")
    val futureJson = """{"version": 999, "type": "ANNIndexMetadata", "payload": {}}"""
    Files.write(root.resolve("ann_index.json"), futureJson.getBytes(StandardCharsets.UTF_8))
    BundleReader.loadMetadata(root) match {
      case Left(BundleError.InvalidBundle(_, msg)) =>
        msg should include("newer than supported")
      case other => fail(s"Expected InvalidBundle, got $other")
    }
  }
}
