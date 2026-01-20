package com.company.ann.spark.builder

import com.company.ann.core.index.{HNSWConfig, HNSWLibIndex}
import org.apache.spark.sql.SparkSession

import java.io.File

/**
 * Entry representing a data file within a local index.
 *
 * @param filePath     Full path to the data file
 * @param numVectors   Number of vectors from this file
 * @param vectorOffset Starting offset for vectors from this file in the index
 */
@SerialVersionUID(1L)
case class DataFileEntry(
  filePath: String,
  numVectors: Long,
  vectorOffset: Long
) extends Serializable

/**
 * Metadata for a built local index.
 *
 * @param indexId      Unique identifier for this index
 * @param dataFiles    Array of data file entries covered by this index
 * @param indexPath    Path where the index is stored
 * @param totalVectors Total number of vectors in the index
 * @param dimension    Vector dimensionality
 */
@SerialVersionUID(1L)
case class LocalIndexMetadata(
  indexId: String,
  dataFiles: Array[DataFileEntry],
  indexPath: String,
  totalVectors: Long,
  dimension: Int
) extends Serializable {
  override def toString: String = {
    s"LocalIndexMetadata($indexId, ${dataFiles.length} files, $totalVectors vectors, dim=$dimension)"
  }
}

/**
 * Builder for constructing local HNSW indexes from file groups.
 * Each file group becomes a single local index containing vectors
 * from all files in the group.
 */
object LocalIndexBuilder {

  /**
   * Build local HNSW indexes from file groups.
   *
   * @param spark           SparkSession instance
   * @param fileGroups      Array of file groups to build indexes for
   * @param vectorColumn    Name of the column containing vectors
   * @param indexOutputPath Base path for storing built indexes
   * @param config          HNSW configuration parameters
   * @return Array of metadata for built indexes
   */
  def buildFromFileGroups(
    spark: SparkSession,
    fileGroups: Array[FileGroup],
    vectorColumn: String,
    indexOutputPath: String,
    config: HNSWConfig = HNSWConfig()
  ): Array[LocalIndexMetadata] = {

    if (fileGroups.isEmpty) {
      return Array.empty[LocalIndexMetadata]
    }

    // Get vector dimension from first file
    val firstFile = fileGroups.head.files.head.filePath
    val dimension = getVectorDimension(spark, firstFile, vectorColumn)

    println(s"Building local indexes with dimension=$dimension for ${fileGroups.length} file groups")

    // Build index for each file group
    // Note: Processing sequentially in driver because SparkSession cannot be
    // used inside RDD transformations. For large numbers of groups, consider
    // implementing distributed index building.
    val metadata = fileGroups.zipWithIndex.map { case (group, idx) =>
      println(s"[${idx + 1}/${fileGroups.length}] Building index for group: ${group.indexId}")
      buildIndexForFileGroup(
        spark,
        group,
        vectorColumn,
        dimension,
        indexOutputPath,
        config
      )
    }

    println(s"Built ${metadata.length} local indexes")
    metadata
  }

  /**
   * Build a single local index for a file group.
   */
  private def buildIndexForFileGroup(
    spark: SparkSession,
    group: FileGroup,
    vectorColumn: String,
    dimension: Int,
    outputPath: String,
    config: HNSWConfig
  ): LocalIndexMetadata = {

    var vectorOffset = 0L
    val allVectors = scala.collection.mutable.ArrayBuffer.empty[(Long, Array[Float])]
    val fileEntries = scala.collection.mutable.ArrayBuffer.empty[DataFileEntry]

    // Read vectors from all files in the group
    group.files.foreach { fileInfo =>
      val df = spark.read.parquet(fileInfo.filePath)
      val vectors = df.select(vectorColumn).collect().map { row =>
        row.getAs[Seq[Float]](0).toArray
      }

      // Assign global IDs based on offset
      vectors.zipWithIndex.foreach { case (vec, localIdx) =>
        val globalIdx = vectorOffset + localIdx
        allVectors += ((globalIdx, vec))
      }

      fileEntries += DataFileEntry(
        filePath = fileInfo.filePath,
        numVectors = vectors.length,
        vectorOffset = vectorOffset
      )

      vectorOffset += vectors.length
    }

    println(s"  [${group.indexId}] Building index for ${allVectors.length} vectors from ${group.files.length} files")

    // Build HNSW index with appropriate maxElements
    val indexConfig = config.copy(maxElements = math.max(allVectors.length * 2, 1000))
    val index = HNSWLibIndex(dimension, indexConfig)
    index.addAll(allVectors.toSeq)

    // Save index to disk
    val indexPath = s"$outputPath/local/${group.indexId}.hnsw"
    val indexDir = new File(indexPath).getParentFile
    if (!indexDir.exists()) {
      indexDir.mkdirs()
    }
    index.save(indexPath)

    println(s"  [${group.indexId}] Index saved to $indexPath")

    LocalIndexMetadata(
      indexId = group.indexId,
      dataFiles = fileEntries.toArray,
      indexPath = indexPath,
      totalVectors = allVectors.length,
      dimension = dimension
    )
  }

  /**
   * Get vector dimension from a parquet file.
   */
  private def getVectorDimension(
    spark: SparkSession,
    filePath: String,
    vectorColumn: String
  ): Int = {
    val df = spark.read.parquet(filePath)
    val firstRow = df.select(vectorColumn).first()
    firstRow.getAs[Seq[Float]](0).length
  }

  /**
   * Print summary of built indexes.
   */
  def printSummary(metadata: Array[LocalIndexMetadata]): Unit = {
    println(s"Built ${metadata.length} local indexes:")
    metadata.foreach { m =>
      println(s"  ${m.indexId}: ${m.totalVectors} vectors from ${m.dataFiles.length} files")
    }
    val totalVectors = metadata.map(_.totalVectors).sum
    val totalFiles = metadata.map(_.dataFiles.length).sum
    println(s"Total: $totalVectors vectors from $totalFiles files")
  }
}
