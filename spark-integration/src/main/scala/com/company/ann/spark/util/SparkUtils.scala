package com.company.ann.spark.util

import com.company.ann.core.index.HNSWLibIndex
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.parquet.column.page.PageReadStore
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.io.ColumnIOFactory
import org.apache.parquet.schema.MessageType

import java.io.{ObjectInputStream, ObjectOutputStream}
import scala.collection.mutable

/**
 * Serializable wrapper for Hadoop Configuration.
 * Standard Spark pattern for passing Hadoop config to executor closures.
 */
class SerializableConfiguration(@transient var value: Configuration) extends Serializable {

  private def writeObject(out: ObjectOutputStream): Unit = {
    out.defaultWriteObject()
    value.write(out)
  }

  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    value = new Configuration(false)
    value.readFields(in)
  }
}

/**
 * Reads vector columns from Parquet files without SparkSession.
 * Uses the parquet-hadoop low-level API available through Spark's transitive dependencies.
 * Designed for use on Spark executors where SparkSession is not available.
 */
object ParquetVectorReader {

  /**
   * Read all vectors from a Parquet file's specified column.
   *
   * @param filePath     Path to the Parquet file (local, HDFS, or S3)
   * @param vectorColumn Name of the column containing float vectors (stored as repeated float)
   * @param hadoopConf   Hadoop configuration for file system access
   * @return Array of float vectors
   */
  def readVectors(
    filePath: String,
    vectorColumn: String,
    hadoopConf: Configuration
  ): Array[Array[Float]] = {
    val path = new Path(filePath)
    val inputFile = HadoopInputFile.fromPath(path, hadoopConf)
    val reader = ParquetFileReader.open(inputFile)

    val vectors = mutable.ArrayBuffer.empty[Array[Float]]
    try {
      val schema: MessageType = reader.getFooter.getFileMetaData.getSchema
      var pages: PageReadStore = reader.readNextRowGroup()

      while (pages != null) {
        val rows = pages.getRowCount
        val columnIO = new ColumnIOFactory().getColumnIO(schema)
        val recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema))

        var i = 0L
        while (i < rows) {
          val group: Group = recordReader.read()
          // Spark writes Array[Float] as a 3-level LIST structure:
          //   vectorColumn (LIST group) {
          //     repeated group list {
          //       optional float element;
          //     }
          //   }
          val vectorGroup = group.getGroup(vectorColumn, 0)
          val dim = vectorGroup.getFieldRepetitionCount("list")
          val vector = new Array[Float](dim)
          var d = 0
          while (d < dim) {
            vector(d) = vectorGroup.getGroup("list", d).getFloat("element", 0)
            d += 1
          }
          vectors += vector
          i += 1
        }

        pages = reader.readNextRowGroup()
      }
    } finally {
      reader.close()
    }

    vectors.toArray
  }
}

/**
 * Saves an HNSW index to storage, supporting both local and distributed file systems.
 * For distributed FS (HDFS, S3, GCS), writes to a local temp file first, then copies.
 */
object IndexStorageUtils {

  /**
   * Save an HNSW index, handling both local and distributed file systems.
   *
   * @param index      The HNSW index to save
   * @param targetPath Destination path (local, hdfs://, s3a://, gs://)
   * @param hadoopConf Hadoop configuration
   */
  def saveIndex(
    index: HNSWLibIndex,
    targetPath: String,
    hadoopConf: Configuration
  ): Unit = {
    val uri = new java.net.URI(targetPath)
    val scheme = Option(uri.getScheme).getOrElse("file")

    if (scheme == "hdfs" || scheme == "s3a" || scheme == "gs") {
      val tempDir = java.io.File.createTempFile("hnsw_", "_dir")
      tempDir.delete()
      tempDir.mkdirs()
      val tempIndexPath = new java.io.File(tempDir, "index.hnsw").getAbsolutePath
      try {
        index.save(tempIndexPath)
        val fs = FileSystem.get(uri, hadoopConf)
        fs.copyFromLocalFile(true, true,
          new Path(tempIndexPath),
          new Path(targetPath))
        fs.copyFromLocalFile(true, true,
          new Path(tempIndexPath + ".meta"),
          new Path(targetPath + ".meta"))
      } finally {
        new java.io.File(tempIndexPath).delete()
        new java.io.File(tempIndexPath + ".meta").delete()
        tempDir.delete()
      }
    } else {
      val parentDir = new java.io.File(targetPath).getParentFile
      if (parentDir != null && !parentDir.exists()) {
        parentDir.mkdirs()
      }
      index.save(targetPath)
    }
  }
}

/**
 * Per-executor cache for loaded HNSW indexes.
 * Avoids reloading indexes for every partition processed on the same executor.
 * Thread-safe via synchronization.
 */
object ExecutorIndexCache {

  @transient private var cachedLocalIndexes: Map[String, HNSWLibIndex] = Map.empty
  @transient private var cachedLocalIndexPaths: Set[String] = Set.empty
  @transient private var cachedGlobalIndex: Option[HNSWLibIndex] = None
  @transient private var cachedGlobalPath: Option[String] = None

  /**
   * Get or load local indexes from paths. Reloads if the path set has changed.
   */
  def getOrLoadLocal(indexPaths: Map[String, String]): Map[String, HNSWLibIndex] = {
    synchronized {
      val pathSet = indexPaths.values.toSet
      if (cachedLocalIndexes.isEmpty || cachedLocalIndexPaths != pathSet) {
        cachedLocalIndexes = indexPaths.map { case (id, path) =>
          (id, HNSWLibIndex.load(path))
        }
        cachedLocalIndexPaths = pathSet
      }
      cachedLocalIndexes
    }
  }

  /**
   * Get or load the global routing index.
   */
  def getOrLoadGlobal(path: String): HNSWLibIndex = {
    synchronized {
      if (cachedGlobalIndex.isEmpty || cachedGlobalPath != Some(path)) {
        cachedGlobalIndex = Some(HNSWLibIndex.load(path))
        cachedGlobalPath = Some(path)
      }
      cachedGlobalIndex.get
    }
  }

  /**
   * Clear all cached indexes. Useful for testing.
   */
  def clear(): Unit = {
    synchronized {
      cachedLocalIndexes = Map.empty
      cachedLocalIndexPaths = Set.empty
      cachedGlobalIndex = None
      cachedGlobalPath = None
    }
  }
}
