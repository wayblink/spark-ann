package com.wayblink.ann.core.benchmark

import java.io._
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Path, Paths}

/**
 * SIFT dataset variants for benchmarking.
 */
sealed trait SiftVariant {
  def name: String
  def dirName: String
  def filePrefix: String
  def expectedBaseVectors: Int
  def expectedQueries: Int
}

object SiftVariant {
  /** SIFT1M: 1 million base vectors, 10K queries */
  case object Sift1M extends SiftVariant {
    val name = "SIFT1M"
    val dirName = "sift1m"
    val filePrefix = "sift"
    val expectedBaseVectors = 1000000
    val expectedQueries = 10000
  }

  /** SIFT10K: 10K base vectors, 100 queries (for fast dev testing) */
  case object Sift10K extends SiftVariant {
    val name = "SIFT10K"
    val dirName = "sift10k"
    val filePrefix = "siftsmall"  // File prefix remains siftsmall per original dataset
    val expectedBaseVectors = 10000
    val expectedQueries = 100
  }

  /** Get variant from environment variable or default */
  def fromEnv(default: SiftVariant = Sift10K): SiftVariant = {
    System.getenv("SIFT_DATASET") match {
      case "sift1m" | "SIFT1M" | "1m" => Sift1M
      case "sift10k" | "SIFT10K" | "10k" | "siftsmall" | "small" => Sift10K
      case _ => default
    }
  }
}

/**
 * Unified SIFT dataset loader supporting multiple variants.
 *
 * Dataset source: http://corpus-texmex.irisa.fr/
 */
object SiftDataset {

  /**
   * Load a SIFT dataset variant.
   *
   * @param variant Which dataset variant to load (default: from SIFT_DATASET env var, or Sift10K)
   * @param maxBaseVectors Maximum number of base vectors to load (for smaller tests)
   * @return SiftData containing base vectors, queries, and ground truth
   */
  def load(
    variant: SiftVariant = SiftVariant.fromEnv(),
    maxBaseVectors: Option[Int] = None
  ): SiftData = {
    val dir = findDatasetDir(variant)
    if (dir.isEmpty) {
      throw new FileNotFoundException(
        s"${variant.name} dataset not found. Place it in datasets/${variant.dirName}/"
      )
    }
    loadFromDirectory(dir.get, variant, maxBaseVectors)
  }

  /**
   * Check if a dataset variant is available.
   */
  def isAvailable(variant: SiftVariant): Boolean = {
    findDatasetDir(variant).exists(dir => isValidDataset(dir, variant))
  }

  /**
   * Find the dataset directory for a variant.
   */
  def findDatasetDir(variant: SiftVariant): Option[Path] = {
    val dirName = variant.dirName
    val filePrefix = variant.filePrefix

    // Try relative paths from current directory and parent directories
    val relativePaths = Seq(
      Paths.get("datasets", dirName),
      Paths.get("..", "datasets", dirName),
      Paths.get("..", "..", "datasets", dirName)
    )

    for (relPath <- relativePaths) {
      val absPath = relPath.toAbsolutePath.normalize()
      if (Files.exists(absPath.resolve(s"${filePrefix}_base.fvecs"))) {
        return Some(absPath)
      }
    }

    // Try to find project root by looking for build.sbt
    var dir = Paths.get(".").toAbsolutePath.normalize()
    while (dir != null && dir.getParent != null) {
      if (Files.exists(dir.resolve("build.sbt"))) {
        val datasetPath = dir.resolve("datasets").resolve(dirName)
        if (Files.exists(datasetPath.resolve(s"${filePrefix}_base.fvecs"))) {
          return Some(datasetPath)
        }
      }
      dir = dir.getParent
    }

    None
  }

  /**
   * Load dataset from a specific directory.
   */
  def loadFromDirectory(
    dir: Path,
    variant: SiftVariant,
    maxBaseVectors: Option[Int] = None
  ): SiftData = {
    val prefix = variant.filePrefix
    val baseFile = dir.resolve(s"${prefix}_base.fvecs")
    val queryFile = dir.resolve(s"${prefix}_query.fvecs")
    val groundTruthFile = dir.resolve(s"${prefix}_groundtruth.ivecs")

    println(s"Loading ${variant.name} dataset from $dir ...")

    val baseVectors = readFvecs(baseFile, maxBaseVectors)
    val queryVectors = readFvecs(queryFile, None)
    val groundTruth = readIvecs(groundTruthFile, None)

    println(s"Loaded ${baseVectors.length} base vectors, ${queryVectors.length} queries")

    SiftData(variant, baseVectors, queryVectors, groundTruth)
  }

  /**
   * Check if dataset directory contains valid data files (not LFS pointers).
   */
  def isValidDataset(dir: Path, variant: SiftVariant): Boolean = {
    val prefix = variant.filePrefix
    val baseFile = dir.resolve(s"${prefix}_base.fvecs")
    val queryFile = dir.resolve(s"${prefix}_query.fvecs")
    val groundTruthFile = dir.resolve(s"${prefix}_groundtruth.ivecs")

    if (!Files.exists(baseFile) || !Files.exists(queryFile) || !Files.exists(groundTruthFile)) {
      return false
    }

    // Check file sizes to detect LFS pointers (which are ~130 bytes)
    // Minimum sizes based on expected vector counts
    val minBaseSize = variant.expectedBaseVectors * 128 * 4 / 2  // Half expected size as minimum
    val minQuerySize = variant.expectedQueries * 128 * 4 / 2

    Files.size(baseFile) > minBaseSize && Files.size(queryFile) > minQuerySize
  }

  /**
   * Read vectors from fvecs format.
   */
  def readFvecs(path: Path, maxVectors: Option[Int]): Array[(Long, Array[Float])] = {
    val file = new RandomAccessFile(path.toFile, "r")
    val channel = file.getChannel

    try {
      val vectors = scala.collection.mutable.ArrayBuffer[(Long, Array[Float])]()
      val dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      var id = 0L
      val limit = maxVectors.getOrElse(Int.MaxValue)

      while (channel.position() < channel.size() && vectors.length < limit) {
        dimBuffer.clear()
        channel.read(dimBuffer)
        dimBuffer.flip()
        val dim = dimBuffer.getInt

        val vectorBuffer = ByteBuffer.allocate(dim * 4).order(ByteOrder.LITTLE_ENDIAN)
        channel.read(vectorBuffer)
        vectorBuffer.flip()

        val vector = new Array[Float](dim)
        vectorBuffer.asFloatBuffer().get(vector)

        vectors += ((id, vector))
        id += 1
      }

      vectors.toArray
    } finally {
      channel.close()
      file.close()
    }
  }

  /**
   * Read integer vectors from ivecs format (ground truth indices).
   */
  def readIvecs(path: Path, maxVectors: Option[Int]): Array[Array[Int]] = {
    val file = new RandomAccessFile(path.toFile, "r")
    val channel = file.getChannel

    try {
      val vectors = scala.collection.mutable.ArrayBuffer[Array[Int]]()
      val dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
      val limit = maxVectors.getOrElse(Int.MaxValue)

      while (channel.position() < channel.size() && vectors.length < limit) {
        dimBuffer.clear()
        channel.read(dimBuffer)
        dimBuffer.flip()
        val dim = dimBuffer.getInt

        val vectorBuffer = ByteBuffer.allocate(dim * 4).order(ByteOrder.LITTLE_ENDIAN)
        channel.read(vectorBuffer)
        vectorBuffer.flip()

        val indices = new Array[Int](dim)
        vectorBuffer.asIntBuffer().get(indices)

        vectors += indices
      }

      vectors.toArray
    } finally {
      channel.close()
      file.close()
    }
  }
}

/**
 * SIFT dataset container.
 *
 * @param variant The dataset variant (Sift1M or Sift10K)
 * @param baseVectors Base vectors (id, 128-dim float array)
 * @param queryVectors Query vectors
 * @param groundTruth Ground truth nearest neighbor indices for each query
 */
case class SiftData(
  variant: SiftVariant,
  baseVectors: Array[(Long, Array[Float])],
  queryVectors: Array[(Long, Array[Float])],
  groundTruth: Array[Array[Int]]
) {

  def name: String = variant.name

  def dimension: Int = baseVectors.headOption.map(_._2.length).getOrElse(0)

  def numBaseVectors: Int = baseVectors.length

  def numQueries: Int = queryVectors.length

  /**
   * Calculate recall@k for given search results.
   */
  def calculateRecall(queryIndex: Int, results: Seq[Long], k: Int): Double = {
    val actualK = math.min(k, groundTruth(queryIndex).length)
    val truth = groundTruth(queryIndex).take(actualK).map(_.toLong).toSet
    val found = results.take(actualK).toSet
    found.intersect(truth).size.toDouble / actualK
  }
}
