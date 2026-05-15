package com.wayblink.ann.api.service

import com.wayblink.ann.bundle._
import com.wayblink.ann.core.index.{HNSWConfig, HNSWLibIndex}

import java.nio.file.{Files, Path}

object BundleIndexManagerTestHelper {
  def writeBundle(): Path = {
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
    global.add(0L, Array(0.05f, 0.05f, 0.05f, 0.05f))
    global.add(1L, Array(5.05f, 5.05f, 5.05f, 5.05f))
    val globalPath = globalDir.resolve("global_routing.hnsw").toString
    global.save(globalPath)

    val metadata = ANNIndexMetadata(
      indexPath = root.toString,
      localIndexes = Array(
        LocalIndexMetadata("idx_a", Array(DataFileEntry("/x/a.parquet", 2L, 0L)), aPath, 2L, 4),
        LocalIndexMetadata("idx_b", Array(DataFileEntry("/x/b.parquet", 2L, 0L)), bPath, 2L, 4)
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
}
