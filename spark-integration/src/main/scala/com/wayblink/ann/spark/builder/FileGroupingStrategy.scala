package com.wayblink.ann.spark.builder

import com.wayblink.ann.bundle.{GroupingStrategy, MergeSmall, SingleFile}
import org.apache.hadoop.fs.Path

import scala.collection.mutable

// GroupingStrategy / SingleFile / MergeSmall live in
// com.wayblink.ann.bundle because they're persisted in ANNIndexConfig
// metadata; this file keeps only the Spark-side grouping logic.

/**
 * Represents a group of files that will share a single index.
 *
 * @param indexId      Unique identifier for this index group
 * @param files        Array of data files in this group
 * @param totalVectors Total number of vectors across all files in the group
 */
case class FileGroup(
  indexId: String,
  files: Array[DataFileInfo],
  totalVectors: Long
) {
  /**
   * Returns the file paths in this group.
   */
  def filePaths: Array[String] = files.map(_.filePath)

  /**
   * Returns the number of files in this group.
   */
  def fileCount: Int = files.length

  override def toString: String = {
    s"FileGroup($indexId, ${files.length} files, $totalVectors vectors)"
  }
}

/**
 * Utility object for grouping data files according to different strategies.
 * This determines how files are combined when building local HNSW indexes.
 */
object FileGroupingStrategy {

  /** Default target vectors per index when using MergeSmall strategy */
  val DefaultTargetVectorsPerIndex: Long = 500000L

  /** Minimum vectors per index to avoid creating too small indexes */
  val MinVectorsPerIndex: Long = 1000L

  /**
   * Groups files according to the specified strategy.
   *
   * @param files                 Array of data files to group
   * @param strategy              Grouping strategy to use
   * @param targetVectorsPerIndex Target number of vectors per index (for MergeSmall)
   * @return Array of FileGroups
   */
  def groupFiles(
    files: Array[DataFileInfo],
    strategy: GroupingStrategy,
    targetVectorsPerIndex: Long = DefaultTargetVectorsPerIndex
  ): Array[FileGroup] = {

    if (files.isEmpty) {
      return Array.empty[FileGroup]
    }

    strategy match {
      case SingleFile =>
        groupAsSingleFiles(files)

      case MergeSmall =>
        mergeSmallFiles(files, targetVectorsPerIndex)
    }
  }

  /**
   * Creates one index per file (1:1 mapping).
   */
  private def groupAsSingleFiles(files: Array[DataFileInfo]): Array[FileGroup] = {
    files.map { file =>
      val fileName = extractFileName(file.filePath)
      FileGroup(
        indexId = s"idx_$fileName",
        files = Array(file),
        totalVectors = file.numVectors
      )
    }
  }

  /**
   * Merges small files into groups to reduce index fragmentation.
   * Uses a greedy algorithm to pack files into groups.
   *
   * @param files         Files to merge
   * @param targetVectors Target vectors per group
   * @return Array of FileGroups
   */
  private def mergeSmallFiles(
    files: Array[DataFileInfo],
    targetVectors: Long
  ): Array[FileGroup] = {

    val effectiveTarget = math.max(targetVectors, MinVectorsPerIndex)

    // Sort files by size (ascending) for better packing
    val sortedFiles = files.sortBy(_.numVectors)

    val groups = mutable.ArrayBuffer.empty[FileGroup]
    var currentGroup = mutable.ArrayBuffer.empty[DataFileInfo]
    var currentCount = 0L
    var groupIndex = 0

    sortedFiles.foreach { file =>
      // Check if adding this file would exceed the target
      // Allow exceeding if:
      // 1. Current group is empty (file is larger than target), or
      // 2. Current count is less than half the target (pack more aggressively)
      val wouldExceed = currentCount + file.numVectors > effectiveTarget
      val shouldStartNewGroup = wouldExceed && currentGroup.nonEmpty &&
        currentCount >= effectiveTarget / 2

      if (shouldStartNewGroup) {
        // Finalize current group
        groups += createFileGroup(groupIndex, currentGroup.toArray, currentCount)
        currentGroup.clear()
        currentCount = 0
        groupIndex += 1
      }

      // Add file to current group
      currentGroup += file
      currentCount += file.numVectors
    }

    // Don't forget the last group
    if (currentGroup.nonEmpty) {
      groups += createFileGroup(groupIndex, currentGroup.toArray, currentCount)
    }

    groups.toArray
  }

  /**
   * Creates a FileGroup with a formatted index ID.
   */
  private def createFileGroup(
    groupIndex: Int,
    files: Array[DataFileInfo],
    totalVectors: Long
  ): FileGroup = {
    // Use file name for single-file groups, group ID for multi-file groups
    val indexId = if (files.length == 1) {
      s"idx_${extractFileName(files.head.filePath)}"
    } else {
      f"idx_group_$groupIndex%05d"
    }

    FileGroup(
      indexId = indexId,
      files = files,
      totalVectors = totalVectors
    )
  }

  /**
   * Extracts the file name without extension from a path.
   */
  private[builder] def extractFileName(filePath: String): String = {
    val path = new Path(filePath)
    val name = path.getName
    // Remove .parquet extension if present
    if (name.endsWith(".parquet")) {
      name.stripSuffix(".parquet")
    } else {
      name
    }
  }

  /**
   * Prints a summary of the file groups.
   */
  def printSummary(groups: Array[FileGroup]): Unit = {
    println(s"Created ${groups.length} file groups:")
    groups.foreach { g =>
      val fileList = if (g.files.length <= 3) {
        g.files.map(f => extractFileName(f.filePath)).mkString(", ")
      } else {
        val first = g.files.take(2).map(f => extractFileName(f.filePath)).mkString(", ")
        s"$first, ... (${g.files.length} files)"
      }
      println(s"  ${g.indexId}: $fileList [${g.totalVectors} vectors]")
    }
    println(s"Total vectors: ${groups.map(_.totalVectors).sum}")
  }

  /**
   * Calculates statistics about the grouping.
   */
  def statistics(groups: Array[FileGroup]): GroupingStatistics = {
    if (groups.isEmpty) {
      return GroupingStatistics(0, 0, 0, 0, 0, 0)
    }

    val vectorCounts = groups.map(_.totalVectors)
    GroupingStatistics(
      numGroups = groups.length,
      totalFiles = groups.map(_.files.length).sum,
      totalVectors = vectorCounts.sum,
      minVectorsPerGroup = vectorCounts.min,
      maxVectorsPerGroup = vectorCounts.max,
      avgVectorsPerGroup = vectorCounts.sum / groups.length
    )
  }
}

/**
 * Statistics about file grouping results.
 */
case class GroupingStatistics(
  numGroups: Int,
  totalFiles: Int,
  totalVectors: Long,
  minVectorsPerGroup: Long,
  maxVectorsPerGroup: Long,
  avgVectorsPerGroup: Long
) {
  override def toString: String = {
    s"""GroupingStatistics(
       |  numGroups: $numGroups,
       |  totalFiles: $totalFiles,
       |  totalVectors: $totalVectors,
       |  minVectorsPerGroup: $minVectorsPerGroup,
       |  maxVectorsPerGroup: $maxVectorsPerGroup,
       |  avgVectorsPerGroup: $avgVectorsPerGroup
       |)""".stripMargin
  }
}
