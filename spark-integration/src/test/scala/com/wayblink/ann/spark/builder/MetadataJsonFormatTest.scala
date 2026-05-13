package com.wayblink.ann.spark.builder

import com.wayblink.ann.spark.api.{ANNIndexConfig, ANNIndexMetadata, ANNIndexStatistics}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

class MetadataJsonFormatTest extends AnyFunSuite with Matchers {

  private def tempPath(name: String) = {
    val p = Files.createTempDirectory("metadata-json-test").resolve(name)
    p.getParent.toFile.deleteOnExit()
    p
  }

  test("ANNIndexMetadata round-trips through JSON") {
    val local = LocalIndexMetadata(
      indexId = "idx_a",
      dataFiles = Array(
        DataFileEntry("hdfs://ns/foo.parquet", 1000L, 0L),
        DataFileEntry("hdfs://ns/bar.parquet", 500L, 1000L)
      ),
      indexPath = "hdfs://ns/idx/local/idx_a.hnsw",
      totalVectors = 1500L,
      dimension = 128
    )
    val stats = ANNIndexStatistics(1500L, 2, 1, 128, 4567L)
    val cfg = ANNIndexConfig(
      M = 24,
      efConstruction = 250,
      groupingStrategy = MergeSmall,
      targetVectorsPerIndex = 1000,
      boundaryNodesPerIndex = 32,
      distanceType = "cosine"
    )
    val metadata = ANNIndexMetadata(
      indexPath = "hdfs://ns/idx",
      localIndexes = Array(local),
      globalIndexPath = Some("hdfs://ns/idx/global/global_routing.hnsw"),
      config = cfg,
      statistics = stats,
      createdAt = 1700000000000L
    )

    val target = tempPath("ann_index.json")
    MetadataJson.writeMetadata(metadata, target)

    val text = new String(Files.readAllBytes(target), StandardCharsets.UTF_8)
    text should include("\"version\"")
    text should include("\"ANNIndexMetadata\"")
    text should include("MergeSmall")
    text should include("cosine")

    val read = MetadataJson.readMetadata(target)
    read.indexPath shouldBe metadata.indexPath
    read.globalIndexPath shouldBe metadata.globalIndexPath
    read.config.M shouldBe cfg.M
    read.config.groupingStrategy shouldBe MergeSmall
    read.config.distanceType shouldBe "cosine"
    read.statistics.totalVectors shouldBe 1500L
    read.localIndexes.length shouldBe 1
    read.localIndexes.head.indexId shouldBe "idx_a"
    read.localIndexes.head.dataFiles.length shouldBe 2
    read.localIndexes.head.dataFiles(1).vectorOffset shouldBe 1000L
  }

  test("boundary_mapping.json round-trips and preserves order") {
    val entries = Array(
      BoundaryMappingEntry(0, "idx_a", 17L),
      BoundaryMappingEntry(1, "idx_b", 42L),
      BoundaryMappingEntry(2, "idx_a", 88L)
    )
    val target = tempPath("boundary_mapping.json")
    MetadataJson.writeBoundaryMapping(entries, target)

    val read = MetadataJson.readBoundaryMapping(target)
    read should have length 3
    read(0).indexId shouldBe "idx_a"
    read(1).indexId shouldBe "idx_b"
    read(2).indexId shouldBe "idx_a"
    read(1).localId shouldBe 42L
  }

  test("rejects mismatched envelope type") {
    val entries = Array(BoundaryMappingEntry(0, "idx_a", 1L))
    val target = tempPath("wrong_type.json")
    MetadataJson.writeBoundaryMapping(entries, target)

    val ex = intercept[IllegalStateException] {
      MetadataJson.readMetadata(target)
    }
    ex.getMessage should include("type mismatch")
  }

  test("rejects unknown future version") {
    val target = tempPath("future.json")
    val json =
      """{"version": 99, "type": "ANNIndexMetadata", "payload": {}}"""
    Files.createDirectories(target.getParent)
    Files.write(target, json.getBytes(StandardCharsets.UTF_8))

    val ex = intercept[IllegalStateException] {
      MetadataJson.readMetadata(target)
    }
    ex.getMessage should include("newer than supported")
  }
}
