package com.wayblink.ann.core.testutil

import scala.util.Random

/**
 * Test data generator for ANN index testing.
 * Provides various vector generation strategies for different testing scenarios.
 */
object TestDataGenerator {

  /**
   * Generate random vectors uniformly distributed in [0, 1].
   *
   * @param numVectors Number of vectors to generate
   * @param dimension Dimensionality of each vector
   * @param seed Random seed for reproducibility
   * @return Array of (id, vector) tuples
   */
  def generateRandomVectors(
    numVectors: Int,
    dimension: Int,
    seed: Long = 42
  ): Array[(Long, Array[Float])] = {
    val random = new Random(seed)
    (0L until numVectors).map { id =>
      val vector = Array.fill(dimension)(random.nextFloat())
      (id, vector)
    }.toArray
  }

  /**
   * Generate clustered vectors for testing high recall scenarios.
   * Vectors within the same cluster are close to each other.
   *
   * @param numClusters Number of clusters to generate
   * @param vectorsPerCluster Number of vectors in each cluster
   * @param dimension Dimensionality of each vector
   * @param spread Standard deviation of the Gaussian noise around cluster centers
   * @param seed Random seed for reproducibility
   * @return Array of (id, vector) tuples
   */
  def generateClusteredVectors(
    numClusters: Int,
    vectorsPerCluster: Int,
    dimension: Int,
    spread: Float = 0.1f,
    seed: Long = 42
  ): Array[(Long, Array[Float])] = {
    val random = new Random(seed)

    // Generate cluster centers
    val centers = (0 until numClusters).map { _ =>
      Array.fill(dimension)(random.nextFloat())
    }

    // Generate vectors around each center
    centers.zipWithIndex.flatMap { case (center, clusterId) =>
      (0 until vectorsPerCluster).map { i =>
        val noise = Array.fill(dimension)(random.nextGaussian().toFloat * spread)
        val vector = center.zip(noise).map { case (c, n) => c + n }
        val id = clusterId * vectorsPerCluster + i
        (id.toLong, vector)
      }
    }.toArray
  }

  /**
   * Generate realistic vectors simulating embedding distributions (e.g., Word2Vec, BERT).
   * Features:
   * - Some dimensions are near zero (sparsity)
   * - Values follow a Gaussian distribution
   * - L2 normalized (unit vectors)
   *
   * @param numVectors Number of vectors to generate
   * @param dimension Dimensionality of each vector
   * @param sparsity Fraction of dimensions that are near zero (default 0.3)
   * @param seed Random seed for reproducibility
   * @return Array of (id, vector) tuples with L2 normalized vectors
   */
  def generateRealisticVectors(
    numVectors: Int,
    dimension: Int,
    sparsity: Double = 0.3,
    seed: Long = 42
  ): Array[(Long, Array[Float])] = {
    val random = new Random(seed)

    (0L until numVectors).map { id =>
      val vector = Array.fill(dimension) {
        if (random.nextDouble() < sparsity) {
          random.nextGaussian().toFloat * 0.01f  // Near zero
        } else {
          random.nextGaussian().toFloat * 0.5f   // Normal distribution
        }
      }
      // L2 normalization
      val norm = math.sqrt(vector.map(x => x * x).sum).toFloat
      val normalized = if (norm > 0) vector.map(_ / norm) else vector
      (id, normalized)
    }.toArray
  }

  /**
   * Compute L2 (Euclidean) distance between two vectors.
   */
  def l2Distance(a: Array[Float], b: Array[Float]): Float = {
    require(a.length == b.length, "Vectors must have the same dimension")
    math.sqrt(a.zip(b).map { case (x, y) =>
      val diff = x - y
      diff * diff
    }.sum).toFloat
  }

  /**
   * Compute cosine distance between two vectors.
   * Cosine distance = 1 - cosine similarity
   */
  def cosineDistance(a: Array[Float], b: Array[Float]): Float = {
    require(a.length == b.length, "Vectors must have the same dimension")
    val dotProduct = a.zip(b).map { case (x, y) => x * y }.sum
    val normA = math.sqrt(a.map(x => x * x).sum).toFloat
    val normB = math.sqrt(b.map(x => x * x).sum).toFloat
    if (normA == 0 || normB == 0) 1.0f
    else 1.0f - (dotProduct / (normA * normB))
  }

  /**
   * Perform brute-force k-nearest neighbor search (ground truth).
   *
   * @param query Query vector
   * @param vectors All vectors to search
   * @param k Number of nearest neighbors
   * @param distanceFunc Distance function to use
   * @return Top-k nearest vectors sorted by distance
   */
  def bruteForceKNN(
    query: Array[Float],
    vectors: Array[(Long, Array[Float])],
    k: Int,
    distanceFunc: (Array[Float], Array[Float]) => Float = l2Distance
  ): Array[(Long, Float)] = {
    vectors.map { case (id, vector) =>
      (id, distanceFunc(query, vector))
    }.sortBy(_._2).take(k)
  }

  /**
   * Calculate recall between ANN results and ground truth.
   *
   * @param annResults Results from ANN search (id, distance)
   * @param groundTruth Results from brute-force search (id, distance)
   * @return Recall value between 0 and 1
   */
  def calculateRecall(
    annResults: Seq[(Long, Float)],
    groundTruth: Seq[(Long, Float)]
  ): Double = {
    val resultIds = annResults.map(_._1).toSet
    val truthIds = groundTruth.map(_._1).toSet
    if (truthIds.isEmpty) 1.0
    else resultIds.intersect(truthIds).size.toDouble / truthIds.size
  }
}
