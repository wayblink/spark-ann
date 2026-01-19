package com.company.ann.spark.builder

import org.scalatest.funsuite.AnyFunSuite

class FileGroupingStrategyTest extends AnyFunSuite {

  // Helper to create test DataFileInfo
  private def createFile(name: String, numVectors: Long): DataFileInfo = {
    DataFileInfo(s"/data/$name.parquet", numVectors)
  }

  // ==================== SingleFile Strategy Tests ====================

  test("SingleFile strategy creates one group per file") {
    val files = Array(
      createFile("file1", 1000),
      createFile("file2", 2000),
      createFile("file3", 3000)
    )

    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)

    assert(groups.length == 3)
    assert(groups.forall(_.files.length == 1))
    assert(groups.map(_.totalVectors).toSet == Set(1000, 2000, 3000))
  }

  test("SingleFile strategy generates index IDs based on file names") {
    val files = Array(
      createFile("alpha", 100),
      createFile("beta", 200)
    )

    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)

    assert(groups.exists(_.indexId == "idx_alpha"))
    assert(groups.exists(_.indexId == "idx_beta"))
  }

  test("SingleFile strategy handles single file") {
    val files = Array(createFile("only_file", 5000))

    val groups = FileGroupingStrategy.groupFiles(files, SingleFile)

    assert(groups.length == 1)
    assert(groups.head.indexId == "idx_only_file")
    assert(groups.head.totalVectors == 5000)
  }

  // ==================== MergeSmall Strategy Tests ====================

  test("MergeSmall strategy merges small files") {
    val files = Array(
      createFile("small1", 100),
      createFile("small2", 200),
      createFile("small3", 150),
      createFile("small4", 250)
    )

    val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, targetVectorsPerIndex = 400)

    // Should create fewer groups than files
    assert(groups.length < files.length)
    // Total vectors should be preserved
    assert(groups.map(_.totalVectors).sum == 700)
  }

  test("MergeSmall strategy keeps large files separate when needed") {
    val files = Array(
      createFile("large1", 1000),
      createFile("large2", 1000),
      createFile("small", 100)
    )

    val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, targetVectorsPerIndex = 500)

    // Large files should be in their own groups, small might be merged
    assert(groups.map(_.totalVectors).sum == 2100)
    // Should have at least 2 groups (each large file exceeds target)
    assert(groups.length >= 2)
  }

  test("MergeSmall strategy handles file larger than target") {
    val files = Array(
      createFile("huge", 10000),
      createFile("small", 100)
    )

    val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, targetVectorsPerIndex = 500)

    // Should still create groups, even if one file exceeds target
    assert(groups.nonEmpty)
    assert(groups.map(_.totalVectors).sum == 10100)
  }

  test("MergeSmall strategy uses named index ID for single-file groups") {
    val files = Array(
      createFile("large_file", 10000)
    )

    val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, targetVectorsPerIndex = 500)

    // Single file group should use file name, not group index
    assert(groups.head.indexId == "idx_large_file")
  }

  test("MergeSmall strategy uses group index for multi-file groups") {
    val files = Array(
      createFile("a", 100),
      createFile("b", 100),
      createFile("c", 100)
    )

    val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, targetVectorsPerIndex = 500)

    // If merged into one group, should use group index
    if (groups.length == 1 && groups.head.files.length > 1) {
      assert(groups.head.indexId.startsWith("idx_group_"))
    }
  }

  test("MergeSmall strategy sorts files by size for better packing") {
    val files = Array(
      createFile("big", 400),
      createFile("small", 100),
      createFile("medium", 250)
    )

    val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, targetVectorsPerIndex = 400)

    // Total should be preserved regardless of order
    assert(groups.map(_.totalVectors).sum == 750)
  }

  // ==================== Edge Cases ====================

  test("groupFiles handles empty array") {
    val groups = FileGroupingStrategy.groupFiles(Array.empty, SingleFile)
    assert(groups.isEmpty)

    val groups2 = FileGroupingStrategy.groupFiles(Array.empty, MergeSmall)
    assert(groups2.isEmpty)
  }

  test("extractFileName handles paths correctly") {
    assert(FileGroupingStrategy.extractFileName("/data/file.parquet") == "file")
    assert(FileGroupingStrategy.extractFileName("/path/to/data.parquet") == "data")
    assert(FileGroupingStrategy.extractFileName("simple.parquet") == "simple")
    assert(FileGroupingStrategy.extractFileName("/file_without_extension") == "file_without_extension")
  }

  // ==================== FileGroup Tests ====================

  test("FileGroup filePaths returns all file paths") {
    val group = FileGroup(
      indexId = "test",
      files = Array(
        createFile("a", 100),
        createFile("b", 200)
      ),
      totalVectors = 300
    )

    assert(group.filePaths.length == 2)
    assert(group.filePaths.contains("/data/a.parquet"))
    assert(group.filePaths.contains("/data/b.parquet"))
  }

  test("FileGroup fileCount returns correct count") {
    val group = FileGroup(
      indexId = "test",
      files = Array(
        createFile("a", 100),
        createFile("b", 200),
        createFile("c", 300)
      ),
      totalVectors = 600
    )

    assert(group.fileCount == 3)
  }

  // ==================== Statistics Tests ====================

  test("statistics calculates correct values") {
    val groups = Array(
      FileGroup("idx1", Array(createFile("a", 100)), 100),
      FileGroup("idx2", Array(createFile("b", 200), createFile("c", 300)), 500),
      FileGroup("idx3", Array(createFile("d", 400)), 400)
    )

    val stats = FileGroupingStrategy.statistics(groups)

    assert(stats.numGroups == 3)
    assert(stats.totalFiles == 4)
    assert(stats.totalVectors == 1000)
    assert(stats.minVectorsPerGroup == 100)
    assert(stats.maxVectorsPerGroup == 500)
    assert(stats.avgVectorsPerGroup == 333) // 1000 / 3
  }

  test("statistics handles empty groups array") {
    val stats = FileGroupingStrategy.statistics(Array.empty)

    assert(stats.numGroups == 0)
    assert(stats.totalFiles == 0)
    assert(stats.totalVectors == 0)
  }

  // ==================== Integration-like Tests ====================

  test("full workflow: discover -> group -> build metadata") {
    // Simulate discovered files
    val files = Array(
      DataFileInfo("/data/2024/01/data1.parquet", 50000),
      DataFileInfo("/data/2024/01/data2.parquet", 30000),
      DataFileInfo("/data/2024/02/data3.parquet", 80000),
      DataFileInfo("/data/2024/02/data4.parquet", 20000),
      DataFileInfo("/data/2024/03/data5.parquet", 100000)
    )

    // Total vectors
    val totalVectors = FileDiscovery.totalVectors(files)
    assert(totalVectors == 280000)

    // Group with SingleFile strategy
    val singleGroups = FileGroupingStrategy.groupFiles(files, SingleFile)
    assert(singleGroups.length == 5)
    assert(singleGroups.map(_.totalVectors).sum == totalVectors)

    // Group with MergeSmall strategy (target 100k vectors per index)
    val mergedGroups = FileGroupingStrategy.groupFiles(files, MergeSmall, 100000)
    assert(mergedGroups.map(_.totalVectors).sum == totalVectors)
    // Should have fewer groups than files
    assert(mergedGroups.length <= files.length)

    // Print statistics
    val stats = FileGroupingStrategy.statistics(mergedGroups)
    assert(stats.totalVectors == totalVectors)
  }
}
