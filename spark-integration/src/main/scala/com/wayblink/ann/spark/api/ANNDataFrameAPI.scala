package com.wayblink.ann.spark.api

import com.wayblink.ann.bundle.{
  ANNIndexConfig, ANNIndexMetadata, GroupingStrategy
}
import com.wayblink.ann.spark.builder.{ANNIndexBuilder, FileDiscovery, FileGroup, FileGroupingStrategy}
import com.wayblink.ann.spark.search.ANNSearcher
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Implicit class providing ANN search capabilities to DataFrames.
 * Import this to add annSearch and annBatchSearch methods to any DataFrame.
 *
 * Usage:
 * {{{
 * import com.wayblink.ann.spark.api.ANNDataFrameExtensions._
 *
 * val results = df.annSearch(
 *   indexPath = "/path/to/index",
 *   queryVector = Array(0.1f, 0.2f, 0.3f),
 *   k = 10
 * )
 * }}}
 */
object ANNDataFrameExtensions {

  implicit class ANNDataFrameOps(df: DataFrame) {

    /**
     * Search for nearest neighbors using a pre-built ANN index.
     *
     * @param indexPath   Path to the ANN index
     * @param queryVector Query vector
     * @param k           Number of neighbors to return
     * @param nprobe      Number of local indexes to search (default 3)
     * @param ef          Search ef parameter (default 50)
     * @return DataFrame with columns: id, distance, indexId
     */
    def annSearch(
      indexPath: String,
      queryVector: Array[Float],
      k: Int,
      nprobe: Int = 3,
      ef: Int = 50
    ): DataFrame = {
      val searcher = ANNSearcher.load(df.sparkSession, indexPath)
      searcher.search(queryVector, k, nprobe, ef)
    }

    /**
     * Batch search for nearest neighbors of multiple query vectors.
     *
     * @param indexPath         Path to the ANN index
     * @param queries           DataFrame containing query vectors
     * @param queryVectorColumn Name of the column containing query vectors
     * @param k                 Number of neighbors per query
     * @param nprobe            Number of local indexes to search (default 3)
     * @param ef                Search ef parameter (default 50)
     * @return DataFrame with columns: queryIndex, id, distance, indexId
     */
    def annBatchSearch(
      indexPath: String,
      queries: DataFrame,
      queryVectorColumn: String,
      k: Int,
      nprobe: Int = 3,
      ef: Int = 50
    ): DataFrame = {
      val searcher = ANNSearcher.load(df.sparkSession, indexPath)
      searcher.batchSearch(queries, queryVectorColumn, k, nprobe, ef)
    }

    /**
     * Build an ANN index from this DataFrame.
     *
     * @param vectorColumn Name of the column containing vectors
     * @param outputPath   Path to store the built index
     * @param config       Index configuration
     * @return Metadata for the built index
     */
    def buildANNIndex(
      vectorColumn: String,
      outputPath: String,
      config: ANNIndexConfig = ANNIndexConfig()
    ): ANNIndexMetadata = {
      val builder = ANNIndexBuilder(df.sparkSession)
      builder.build(df, vectorColumn, outputPath, config)
    }
  }
}

/**
 * Main API object for building and querying ANN indexes.
 * Provides static methods for common operations.
 *
 * Usage:
 * {{{
 * // Build index
 * val metadata = ANNIndexAPI.buildIndex(
 *   df = vectorsDF,
 *   vectorColumn = "embedding",
 *   outputPath = "/path/to/index"
 * )
 *
 * // Search index
 * val results = ANNIndexAPI.search(
 *   spark = spark,
 *   indexPath = "/path/to/index",
 *   queryVector = Array(0.1f, 0.2f, 0.3f),
 *   k = 10
 * )
 * }}}
 */
object ANNIndexAPI {

  /**
   * Build an ANN index from a DataFrame.
   *
   * @param df           DataFrame containing vectors
   * @param vectorColumn Name of the column containing vectors
   * @param outputPath   Path to store the built index
   * @param config       Index configuration
   * @return Metadata for the built index
   */
  def buildIndex(
    df: DataFrame,
    vectorColumn: String,
    outputPath: String,
    config: ANNIndexConfig = ANNIndexConfig()
  ): ANNIndexMetadata = {
    val builder = ANNIndexBuilder(df.sparkSession)
    builder.build(df, vectorColumn, outputPath, config)
  }

  /**
   * Build an ANN index from file groups.
   * Use this when you want more control over file grouping.
   *
   * @param spark        SparkSession instance
   * @param fileGroups   Array of file groups to build indexes for
   * @param vectorColumn Name of the column containing vectors
   * @param outputPath   Path to store the built index
   * @param config       Index configuration
   * @return Metadata for the built index
   */
  def buildIndexFromFileGroups(
    spark: SparkSession,
    fileGroups: Array[FileGroup],
    vectorColumn: String,
    outputPath: String,
    config: ANNIndexConfig = ANNIndexConfig()
  ): ANNIndexMetadata = {
    val builder = ANNIndexBuilder(spark)
    builder.buildFromFileGroups(fileGroups, vectorColumn, outputPath, config)
  }

  /**
   * Search for nearest neighbors using a pre-built ANN index.
   *
   * @param spark       SparkSession instance
   * @param indexPath   Path to the ANN index
   * @param queryVector Query vector
   * @param k           Number of neighbors to return
   * @param nprobe      Number of local indexes to search (default 3)
   * @param ef          Search ef parameter (default 50)
   * @return DataFrame with columns: id, distance, indexId
   */
  def search(
    spark: SparkSession,
    indexPath: String,
    queryVector: Array[Float],
    k: Int,
    nprobe: Int = 3,
    ef: Int = 50
  ): DataFrame = {
    val searcher = ANNSearcher.load(spark, indexPath)
    searcher.search(queryVector, k, nprobe, ef)
  }

  /**
   * Batch search for nearest neighbors.
   *
   * @param spark             SparkSession instance
   * @param indexPath         Path to the ANN index
   * @param queries           DataFrame containing query vectors
   * @param queryVectorColumn Name of the column containing query vectors
   * @param k                 Number of neighbors per query
   * @param nprobe            Number of local indexes to search (default 3)
   * @param ef                Search ef parameter (default 50)
   * @return DataFrame with columns: queryIndex, id, distance, indexId
   */
  def batchSearch(
    spark: SparkSession,
    indexPath: String,
    queries: DataFrame,
    queryVectorColumn: String,
    k: Int,
    nprobe: Int = 3,
    ef: Int = 50
  ): DataFrame = {
    val searcher = ANNSearcher.load(spark, indexPath)
    searcher.batchSearch(queries, queryVectorColumn, k, nprobe, ef)
  }

  /**
   * Load an ANNSearcher for advanced search operations.
   *
   * @param spark     SparkSession instance
   * @param indexPath Path to the ANN index
   * @return Loaded ANNSearcher instance
   */
  def loadSearcher(spark: SparkSession, indexPath: String): ANNSearcher = {
    ANNSearcher.load(spark, indexPath)
  }

  /**
   * Discover data files in a directory.
   *
   * @param spark        SparkSession instance
   * @param dataPath     Path to the data directory
   * @param vectorColumn Name of the column containing vectors
   * @return Array of discovered data files
   */
  def discoverDataFiles(
    spark: SparkSession,
    dataPath: String,
    vectorColumn: String
  ): Array[com.wayblink.ann.spark.builder.DataFileInfo] = {
    FileDiscovery.discoverDataFiles(spark, dataPath, vectorColumn)
  }

  /**
   * Group data files according to a strategy.
   *
   * @param files                 Array of data files
   * @param strategy              Grouping strategy
   * @param targetVectorsPerIndex Target vectors per index (for MergeSmall)
   * @return Array of file groups
   */
  def groupFiles(
    files: Array[com.wayblink.ann.spark.builder.DataFileInfo],
    strategy: com.wayblink.ann.bundle.GroupingStrategy,
    targetVectorsPerIndex: Long = 500000
  ): Array[FileGroup] = {
    FileGroupingStrategy.groupFiles(files, strategy, targetVectorsPerIndex)
  }
}
