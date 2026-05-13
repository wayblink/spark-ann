package com.wayblink.ann.spark.builder

import com.wayblink.ann.spark.util.SerializableConfiguration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.spark.sql.SparkSession

/**
 * Data file information containing path and vector count.
 *
 * @param filePath   Full path to the data file
 * @param numVectors Number of vectors in the file
 */
case class DataFileInfo(
  filePath: String,
  numVectors: Long
)

/**
 * Utility object for discovering data files in a directory.
 * Scans directories recursively to find all Parquet files and
 * collects metadata about each file.
 */
object FileDiscovery {

  /**
   * Discovers all Parquet data files in the given path and counts vectors in each.
   *
   * @param spark        SparkSession instance
   * @param dataPath     Root path to scan for data files
   * @param vectorColumn Name of the vector column (used for validation)
   * @return Array of DataFileInfo with file paths and vector counts
   */
  def discoverDataFiles(
    spark: SparkSession,
    dataPath: String,
    vectorColumn: String
  ): Array[DataFileInfo] = {

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val basePath = new Path(dataPath)
    val fs = basePath.getFileSystem(hadoopConf)

    // Recursively find all Parquet files
    val parquetFiles = listParquetFiles(fs, basePath)

    if (parquetFiles.isEmpty) {
      throw new IllegalArgumentException(s"No Parquet files found in path: $dataPath")
    }

    // Get vector count for each file using parallel Parquet footer reading.
    // Row counts are stored in Parquet file footers, so no data is scanned.
    val pathStrings = parquetFiles.map(_.toString)
    val bcConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf)
    )
    val bcVectorColumn = spark.sparkContext.broadcast(vectorColumn)

    val fileInfoRDD = spark.sparkContext.parallelize(pathStrings.toSeq).map { pathStr =>
      val conf = bcConf.value.value
      val path = new Path(pathStr)
      val inputFile = HadoopInputFile.fromPath(path, conf)
      val reader = ParquetFileReader.open(inputFile)
      try {
        val numRows = reader.getRecordCount
        // Validate that the vector column exists in the schema
        val schema = reader.getFooter.getFileMetaData.getSchema
        val columnName = bcVectorColumn.value
        if (!schema.containsField(columnName)) {
          throw new IllegalArgumentException(
            s"Vector column '$columnName' not found in file: $pathStr. " +
              s"Available columns: ${schema.getFields.toString}"
          )
        }
        DataFileInfo(
          filePath = pathStr,
          numVectors = numRows
        )
      } finally {
        reader.close()
      }
    }

    fileInfoRDD.collect()
  }

  /**
   * Lists all Parquet files under the given path recursively.
   *
   * @param fs   Hadoop FileSystem instance
   * @param path Root path to search
   * @return Array of Paths to Parquet files
   */
  private[builder] def listParquetFiles(fs: FileSystem, path: Path): Array[Path] = {
    if (!fs.exists(path)) {
      return Array.empty[Path]
    }

    val status = fs.getFileStatus(path)

    if (status.isFile) {
      // Single file - check if it's Parquet
      if (isParquetFile(path)) {
        Array(path)
      } else {
        Array.empty[Path]
      }
    } else {
      // Directory - recurse into children
      fs.listStatus(path).flatMap { childStatus =>
        if (childStatus.isDirectory) {
          // Skip hidden directories (starting with . or _)
          if (isHiddenPath(childStatus.getPath)) {
            Array.empty[Path]
          } else {
            listParquetFiles(fs, childStatus.getPath)
          }
        } else if (isParquetFile(childStatus.getPath)) {
          Array(childStatus.getPath)
        } else {
          Array.empty[Path]
        }
      }
    }
  }

  /**
   * Checks if a path represents a Parquet file.
   */
  private def isParquetFile(path: Path): Boolean = {
    val name = path.getName
    name.endsWith(".parquet") && !name.startsWith("_") && !name.startsWith(".")
  }

  /**
   * Checks if a path is a hidden directory (starts with . or _).
   */
  private def isHiddenPath(path: Path): Boolean = {
    val name = path.getName
    name.startsWith(".") || name.startsWith("_")
  }

  /**
   * Gets the total number of vectors across all files.
   */
  def totalVectors(files: Array[DataFileInfo]): Long = {
    files.map(_.numVectors).sum
  }

  /**
   * Prints a summary of discovered files.
   */
  def printSummary(files: Array[DataFileInfo]): Unit = {
    println(s"Discovered ${files.length} data files:")
    files.sortBy(_.filePath).foreach { f =>
      println(s"  ${f.filePath}: ${f.numVectors} vectors")
    }
    println(s"Total vectors: ${totalVectors(files)}")
  }
}
