package com.wayblink.ann.spark.builder

import com.wayblink.ann.core.index.{HNSWConfig, HNSWLibIndex}
import com.wayblink.ann.spark.api.{ANNIndexConfig, ANNIndexMetadata, ANNIndexStatistics}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import java.nio.file.Paths

/**
 * Boundary node selected from a local index for global routing.
 *
 * @param globalId    Unique ID across all indexes (format: "indexId:localId")
 * @param indexId     ID of the source local index
 * @param localId     Vector ID within the local index
 * @param vector      The boundary node vector
 */
@SerialVersionUID(1L)
case class GlobalBoundaryNode(
  globalId: String,
  indexId: String,
  localId: Long,
  vector: Array[Float]
) extends Serializable

/**
 * Builder for constructing complete ANN indexes.
 * Handles distributed local index building, boundary node selection,
 * and global routing index construction.
 */
class ANNIndexBuilder(spark: SparkSession) {

  private val log = LoggerFactory.getLogger(classOf[ANNIndexBuilder])

  /**
   * Build a complete ANN index from a DataFrame.
   *
   * @param df           DataFrame containing vectors
   * @param vectorColumn Name of the column containing vectors
   * @param outputPath   Path to store the built index
   * @param config       Index configuration
   * @return Metadata for the built index
   */
  def build(
    df: DataFrame,
    vectorColumn: String,
    outputPath: String,
    config: ANNIndexConfig = ANNIndexConfig()
  ): ANNIndexMetadata = {
    val startTime = System.currentTimeMillis()

    // Step 1: Write DataFrame to temporary parquet files if not already on disk
    val dataPath = s"$outputPath/data"
    log.info("Saving data to {}", dataPath)
    df.write.mode("overwrite").parquet(dataPath)

    // Step 2: Discover data files (parallelized via footer reading)
    log.info("Discovering data files")
    val dataFiles = FileDiscovery.discoverDataFiles(spark, dataPath, vectorColumn)
    log.info("Found {} data files", dataFiles.length)

    // Step 3: Group files according to strategy
    log.info("Grouping files using {} strategy", config.groupingStrategy)
    val fileGroups = FileGroupingStrategy.groupFiles(
      dataFiles,
      config.groupingStrategy,
      config.targetVectorsPerIndex
    )
    log.info("Created {} file groups", fileGroups.length)

    // Build from file groups
    buildFromFileGroups(fileGroups, vectorColumn, outputPath, config, startTime)
  }

  /**
   * Build a complete ANN index from pre-grouped files.
   *
   * @param fileGroups   Array of file groups to build indexes for
   * @param vectorColumn Name of the column containing vectors
   * @param outputPath   Path to store the built index
   * @param config       Index configuration
   * @return Metadata for the built index
   */
  def buildFromFileGroups(
    fileGroups: Array[FileGroup],
    vectorColumn: String,
    outputPath: String,
    config: ANNIndexConfig = ANNIndexConfig(),
    startTime: Long = System.currentTimeMillis()
  ): ANNIndexMetadata = {

    // Step 1: Build local indexes and co-collect boundary nodes in a single distributed pass
    log.info("Building local indexes (distributed)")
    val hnswConfig = config.toHNSWConfig()
    val buildResults = LocalIndexBuilder.buildFromFileGroupsWithBoundaryNodes(
      spark,
      fileGroups,
      vectorColumn,
      outputPath,
      hnswConfig,
      config.distanceType,
      config.boundaryNodesPerIndex,
      config.pk
    )

    val localMetadata = buildResults.map(_.metadata)
    val boundaryNodes = buildResults.flatMap(_.boundaryNodes)

    if (localMetadata.isEmpty) {
      throw new IllegalArgumentException("No local indexes were built")
    }

    val dimension = localMetadata.head.dimension
    log.info(
      "Built {} local indexes, collected {} boundary nodes",
      localMetadata.length, boundaryNodes.length
    )

    // Step 2: Build global routing index (on driver — boundary nodes are small)
    val globalIndexPath = if (boundaryNodes.nonEmpty && localMetadata.length > 1) {
      log.info("Building global routing index")
      val globalPath = buildGlobalIndex(boundaryNodes, dimension, outputPath, config)
      log.info("Global index built at {}", globalPath)
      Some(globalPath)
    } else {
      log.info("Skipping global index (single local index or no boundary nodes)")
      None
    }

    // Step 3: Calculate statistics
    val totalVectors = localMetadata.map(_.totalVectors).sum
    val totalFiles = localMetadata.map(_.dataFiles.length).sum
    val buildTimeMs = System.currentTimeMillis() - startTime

    val statistics = ANNIndexStatistics(
      totalVectors = totalVectors,
      totalFiles = totalFiles,
      numLocalIndexes = localMetadata.length,
      dimension = dimension,
      buildTimeMs = buildTimeMs
    )

    // Step 4: Create and save metadata
    val metadata = ANNIndexMetadata(
      indexPath = outputPath,
      localIndexes = localMetadata,
      globalIndexPath = globalIndexPath,
      config = config,
      statistics = statistics
    )

    saveMetadata(metadata, outputPath)

    log.info("ANN index built successfully in {}ms", buildTimeMs)
    log.info("{}", statistics)

    metadata
  }

  /**
   * Build the global routing index from boundary nodes.
   * Runs on the driver since boundary nodes are a small dataset.
   */
  private def buildGlobalIndex(
    boundaryNodes: Array[GlobalBoundaryNode],
    dimension: Int,
    outputPath: String,
    config: ANNIndexConfig
  ): String = {

    val globalConfig = HNSWConfig(
      M = config.M,
      efConstruction = config.efConstruction,
      maxElements = boundaryNodes.length * 2
    )

    val globalIndex = HNSWLibIndex(dimension, globalConfig, config.distanceType)

    // Add boundary nodes to global index
    // Use sequential IDs for the global index, store mapping separately
    val vectors = boundaryNodes.zipWithIndex.map { case (node, idx) =>
      (idx.toLong, node.vector)
    }
    globalIndex.addAll(vectors)

    // Save the index
    val globalIndexPath = s"$outputPath/global/global_routing.hnsw"
    val globalDir = new File(globalIndexPath).getParentFile
    if (!globalDir.exists()) {
      globalDir.mkdirs()
    }
    globalIndex.save(globalIndexPath)

    // Save boundary node mapping
    saveBoundaryNodeMapping(boundaryNodes, outputPath)

    globalIndexPath
  }

  /**
   * Save boundary node mapping for reverse lookup. Array index = global
   * routing id (the same id `zipWithIndex` assigned when adding to the
   * global HNSW), value = source local index id + original local id.
   */
  private def saveBoundaryNodeMapping(
    boundaryNodes: Array[GlobalBoundaryNode],
    outputPath: String
  ): Unit = {
    val mappingPath = Paths.get(outputPath, "global", "boundary_mapping.json")
    val entries = boundaryNodes.zipWithIndex.map { case (node, idx) =>
      BoundaryMappingEntry(idx, node.indexId, node.localId)
    }
    MetadataJson.writeBoundaryMapping(entries, mappingPath)
  }

  /**
   * Save index metadata to disk as JSON.
   */
  private def saveMetadata(metadata: ANNIndexMetadata, outputPath: String): Unit = {
    val metadataPath = Paths.get(outputPath, "ann_index.json")
    MetadataJson.writeMetadata(metadata, metadataPath)
  }
}

object ANNIndexBuilder {

  /**
   * Create a new ANNIndexBuilder instance.
   */
  def apply(spark: SparkSession): ANNIndexBuilder = {
    new ANNIndexBuilder(spark)
  }
}
