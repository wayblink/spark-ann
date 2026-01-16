package com.company.ann.spark.testutil

import com.company.ann.core.testutil.TestDataGenerator
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

import scala.collection.mutable.WrappedArray

/**
 * Spark integration for test data generation.
 * Provides utilities to generate and persist test data as DataFrames.
 */
object SparkTestData {

  /**
   * Generate test vectors and save as Parquet.
   *
   * @param spark SparkSession
   * @param numVectors Number of vectors to generate
   * @param dimension Dimensionality of vectors
   * @param path Output path for Parquet file
   * @param dataType Type of data distribution: "random", "clustered", or "realistic"
   * @param seed Random seed for reproducibility
   * @return DataFrame with columns (id: Long, vector: Array[Float])
   */
  def generateAndSave(
    spark: SparkSession,
    numVectors: Int,
    dimension: Int,
    path: String,
    dataType: String = "clustered",
    seed: Long = 42
  ): DataFrame = {
    val df = generateDataFrame(spark, numVectors, dimension, dataType, seed)
    df.write.mode("overwrite").parquet(path)
    spark.read.parquet(path)
  }

  /**
   * Load test data from Parquet.
   *
   * @param spark SparkSession
   * @param path Path to Parquet file
   * @return DataFrame with columns (id: Long, vector: Array[Float])
   */
  def loadTestData(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }

  /**
   * Generate test vectors as DataFrame without saving.
   *
   * @param spark SparkSession
   * @param numVectors Number of vectors to generate
   * @param dimension Dimensionality of vectors
   * @param dataType Type of data distribution
   * @param seed Random seed
   * @return DataFrame with columns (id: Long, vector: Array[Float])
   */
  def generateDataFrame(
    spark: SparkSession,
    numVectors: Int,
    dimension: Int,
    dataType: String = "clustered",
    seed: Long = 42
  ): DataFrame = {
    val vectors = generateVectors(numVectors, dimension, dataType, seed)

    // Create schema with ArrayType for proper Spark representation
    val schema = StructType(Array(
      StructField("id", LongType, nullable = false),
      StructField("vector", ArrayType(FloatType, containsNull = false), nullable = false)
    ))

    // Convert to Rows with WrappedArray for proper array handling
    val rows = vectors.map { case (id, vector) =>
      Row(id, WrappedArray.make(vector))
    }

    val rdd = spark.sparkContext.parallelize(rows.toSeq)
    spark.createDataFrame(rdd, schema)
  }

  private def generateVectors(
    numVectors: Int,
    dimension: Int,
    dataType: String,
    seed: Long
  ): Array[(Long, Array[Float])] = {
    dataType.toLowerCase match {
      case "random" =>
        TestDataGenerator.generateRandomVectors(numVectors, dimension, seed)

      case "clustered" =>
        val numClusters = math.max(1, math.sqrt(numVectors).toInt)
        val vectorsPerCluster = math.ceil(numVectors.toDouble / numClusters).toInt
        TestDataGenerator.generateClusteredVectors(
          numClusters = numClusters,
          vectorsPerCluster = vectorsPerCluster,
          dimension = dimension,
          seed = seed
        ).take(numVectors)

      case "realistic" =>
        TestDataGenerator.generateRealisticVectors(numVectors, dimension, seed = seed)

      case _ =>
        throw new IllegalArgumentException(
          s"Unknown data type: $dataType. Supported: random, clustered, realistic"
        )
    }
  }
}
