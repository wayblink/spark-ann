package com.wayblink.ann.spark.builder

import com.wayblink.ann.core.index.{HNSWConfig, HNSWLibIndex}
import com.wayblink.ann.spark.util.{IndexStorageUtils, SerializableConfiguration, StreamingParquetVectorReader}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.SparkSession

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
 * Result of building a local index, containing both metadata and sampled boundary nodes.
 *
 * @param metadata       Metadata for the built index
 * @param boundaryNodes  Sampled boundary nodes for global routing
 */
@SerialVersionUID(1L)
case class LocalIndexBuildResult(
  metadata: LocalIndexMetadata,
  boundaryNodes: Array[GlobalBoundaryNode]
) extends Serializable

/**
 * Builder for constructing local HNSW indexes from file groups.
 * Each file group becomes a single local index containing vectors
 * from all files in the group.
 */
object LocalIndexBuilder {

  /**
   * Build local HNSW indexes from file groups.
   * Distributes index building across Spark executors — one task per file group.
   *
   * @param spark           SparkSession instance
   * @param fileGroups      Array of file groups to build indexes for
   * @param vectorColumn    Name of the column containing vectors
   * @param indexOutputPath Base path for storing built indexes
   * @param config          HNSW configuration parameters
   * @param distanceType    Distance metric type
   * @return Array of metadata for built indexes
   */
  def buildFromFileGroups(
    spark: SparkSession,
    fileGroups: Array[FileGroup],
    vectorColumn: String,
    indexOutputPath: String,
    config: HNSWConfig = HNSWConfig(),
    distanceType: String = "euclidean",
    idColumn: Option[String] = None
  ): Array[LocalIndexMetadata] = {
    buildFromFileGroupsWithBoundaryNodes(
      spark, fileGroups, vectorColumn, indexOutputPath, config, distanceType,
      boundaryNodesPerIndex = 0, idColumn = idColumn
    ).map(_.metadata)
  }

  /**
   * Build local HNSW indexes and co-collect boundary nodes in a single pass.
   * Distributes index building across Spark executors — one task per file group.
   * Boundary nodes are sampled from vectors already in memory on each executor,
   * avoiding a separate data re-read.
   *
   * @param spark                 SparkSession instance
   * @param fileGroups            Array of file groups to build indexes for
   * @param vectorColumn          Name of the column containing vectors
   * @param indexOutputPath       Base path for storing built indexes
   * @param config                HNSW configuration parameters
   * @param distanceType          Distance metric type
   * @param boundaryNodesPerIndex Number of boundary nodes to sample per index
   * @param idColumn              Optional user id column (INT32/INT64). When set, its
   *                              value is used as the HNSW internal id directly so
   *                              search results carry the user's id back.
   * @return Array of build results with metadata and boundary nodes
   */
  def buildFromFileGroupsWithBoundaryNodes(
    spark: SparkSession,
    fileGroups: Array[FileGroup],
    vectorColumn: String,
    indexOutputPath: String,
    config: HNSWConfig = HNSWConfig(),
    distanceType: String = "euclidean",
    boundaryNodesPerIndex: Int = 50,
    idColumn: Option[String] = None
  ): Array[LocalIndexBuildResult] = {

    if (fileGroups.isEmpty) {
      return Array.empty[LocalIndexBuildResult]
    }

    // Get vector dimension from first file (cheap — reads one row group footer)
    val dimension = getVectorDimension(
      fileGroups.head.files.head.filePath,
      vectorColumn,
      spark.sparkContext.hadoopConfiguration
    )

    // Broadcast shared configuration to executors
    val bcConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    )
    val bcHnswConfig = spark.sparkContext.broadcast(config)
    val bcOutputPath = spark.sparkContext.broadcast(indexOutputPath)
    val bcVectorColumn = spark.sparkContext.broadcast(vectorColumn)
    val bcDistanceType = spark.sparkContext.broadcast(distanceType)
    val bcDimension = spark.sparkContext.broadcast(dimension)
    val bcBoundaryNodesPerIndex = spark.sparkContext.broadcast(boundaryNodesPerIndex)
    val bcIdColumn = spark.sparkContext.broadcast(idColumn)

    // Parallelize file groups — one partition per file group for maximum parallelism
    val numPartitions = math.min(fileGroups.length, spark.sparkContext.defaultParallelism)
    val fileGroupRDD = spark.sparkContext.parallelize(fileGroups.toSeq, numPartitions)

    val resultsRDD = fileGroupRDD.map { group =>
      // This closure runs on executors
      val hadoopConf = bcConf.value.value
      val hnswConfig = bcHnswConfig.value
      val outputPath = bcOutputPath.value
      val vecColumn = bcVectorColumn.value
      val distType = bcDistanceType.value
      val dim = bcDimension.value
      val numBoundaryNodes = bcBoundaryNodesPerIndex.value
      val idCol = bcIdColumn.value

      buildIndexForFileGroup(
        group, vecColumn, dim, outputPath, hnswConfig, distType, hadoopConf,
        numBoundaryNodes, idCol
      )
    }

    val results = resultsRDD.collect()

    // Clean up broadcasts
    bcConf.destroy()
    bcHnswConfig.destroy()
    bcOutputPath.destroy()
    bcVectorColumn.destroy()
    bcDistanceType.destroy()
    bcDimension.destroy()
    bcBoundaryNodesPerIndex.destroy()
    bcIdColumn.destroy()

    results
  }

  /**
   * Build a single local index for a file group by streaming vectors
   * directly into the HNSW index. Peak heap is bounded to one parquet row
   * group plus the HNSW graph itself; the full vector set is never
   * materialized in memory. Boundary nodes are sampled in the same pass
   * via reservoir sampling.
   *
   * When `idColumn` is set, each row's id value is used as the HNSW internal
   * id (after INT32 widening if necessary). When unset, sequential ids
   * starting from 0 are assigned per local index (original behavior).
   */
  private def buildIndexForFileGroup(
    group: FileGroup,
    vectorColumn: String,
    dimension: Int,
    outputPath: String,
    config: HNSWConfig,
    distanceType: String,
    hadoopConf: Configuration,
    boundaryNodesPerIndex: Int,
    idColumn: Option[String]
  ): LocalIndexBuildResult = {

    val total = math.max(group.totalVectors.toInt, 1)
    val indexConfig = config.copy(maxElements = total)
    val index = HNSWLibIndex(dimension, indexConfig, distanceType)
    val reservoir = new StreamingBoundaryReservoir(boundaryNodesPerIndex)

    var rowsSeen = 0L
    val fileEntries = scala.collection.mutable.ArrayBuffer.empty[DataFileEntry]

    group.files.foreach { fileInfo =>
      val startOffset = rowsSeen
      var fileCount = 0L

      idColumn match {
        case Some(col) =>
          // User-id mode: read both id and vector; HNSW internal id == user id.
          val iter = StreamingParquetVectorReader.streamRows(
            fileInfo.filePath, col, vectorColumn, hadoopConf
          )
          while (iter.hasNext) {
            val (userId, vec) = iter.next()
            index.add(userId, vec)
            reservoir.offer(userId, vec)
            rowsSeen += 1L
            fileCount += 1L
          }
        case None =>
          // Sequential-id mode: vectorOffset becomes the HNSW internal id.
          val iter = StreamingParquetVectorReader.streamVectors(
            fileInfo.filePath, vectorColumn, hadoopConf
          )
          while (iter.hasNext) {
            val vec = iter.next()
            val seqId = rowsSeen
            index.add(seqId, vec)
            reservoir.offer(seqId, vec)
            rowsSeen += 1L
            fileCount += 1L
          }
      }

      fileEntries += DataFileEntry(
        filePath = fileInfo.filePath,
        numVectors = fileCount,
        vectorOffset = startOffset
      )
    }

    val indexPath = s"$outputPath/local/${group.indexId}.hnsw"
    IndexStorageUtils.saveIndex(index, indexPath, hadoopConf)

    val boundaryNodes = reservoir.result(group.indexId)

    val metadata = LocalIndexMetadata(
      indexId = group.indexId,
      dataFiles = fileEntries.toArray,
      indexPath = indexPath,
      totalVectors = rowsSeen,
      dimension = dimension
    )

    LocalIndexBuildResult(metadata, boundaryNodes)
  }

  /**
   * Get vector dimension by reading the first vector from a Parquet file.
   */
  private def getVectorDimension(
    filePath: String,
    vectorColumn: String,
    hadoopConf: Configuration
  ): Int = {
    val path = new org.apache.hadoop.fs.Path(filePath)
    val inputFile = HadoopInputFile.fromPath(path, hadoopConf)
    val reader = ParquetFileReader.open(inputFile)
    try {
      val schema = reader.getFooter.getFileMetaData.getSchema
      val pages = reader.readNextRowGroup()
      if (pages == null || pages.getRowCount == 0) {
        throw new IllegalArgumentException(s"No vectors found in file: $filePath")
      }
      val columnIO = new org.apache.parquet.io.ColumnIOFactory().getColumnIO(schema)
      val recordReader = columnIO.getRecordReader(
        pages,
        new org.apache.parquet.example.data.simple.convert.GroupRecordConverter(schema)
      )
      val group = recordReader.read()
      val vectorGroup = group.getGroup(vectorColumn, 0)
      vectorGroup.getFieldRepetitionCount("list")
    } finally {
      reader.close()
    }
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
