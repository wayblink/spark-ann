package com.wayblink.ann.core.testutil

import org.scalatest.funsuite.AnyFunSuite

class TestDataGeneratorTest extends AnyFunSuite {

  test("generateRandomVectors should produce correct number and dimension") {
    val vectors = TestDataGenerator.generateRandomVectors(1000, 128)

    assert(vectors.length == 1000)
    assert(vectors.head._2.length == 128)
    assert(vectors.forall(_._2.forall(v => v >= 0f && v <= 1f)))
  }

  test("generateRandomVectors should produce unique IDs") {
    val vectors = TestDataGenerator.generateRandomVectors(100, 64)
    val ids = vectors.map(_._1)

    assert(ids.distinct.length == 100)
    assert(ids.min == 0L)
    assert(ids.max == 99L)
  }

  test("generateRandomVectors should be reproducible with same seed") {
    val vectors1 = TestDataGenerator.generateRandomVectors(100, 64, seed = 123)
    val vectors2 = TestDataGenerator.generateRandomVectors(100, 64, seed = 123)

    vectors1.zip(vectors2).foreach { case ((id1, v1), (id2, v2)) =>
      assert(id1 == id2)
      assert(v1.sameElements(v2))
    }
  }

  test("generateRandomVectors should produce different vectors with different seeds") {
    val vectors1 = TestDataGenerator.generateRandomVectors(100, 64, seed = 1)
    val vectors2 = TestDataGenerator.generateRandomVectors(100, 64, seed = 2)

    val firstVector1 = vectors1.head._2
    val firstVector2 = vectors2.head._2
    assert(!firstVector1.sameElements(firstVector2))
  }

  test("generateClusteredVectors should produce correct count") {
    val vectors = TestDataGenerator.generateClusteredVectors(
      numClusters = 5,
      vectorsPerCluster = 20,
      dimension = 64
    )

    assert(vectors.length == 100)
    assert(vectors.head._2.length == 64)
  }

  test("generateClusteredVectors should have good intra-cluster similarity") {
    val vectors = TestDataGenerator.generateClusteredVectors(
      numClusters = 5,
      vectorsPerCluster = 20,
      dimension = 64,
      spread = 0.1f
    )

    // Cluster 0: indices 0-19, Cluster 1: indices 20-39
    val cluster0 = vectors.slice(0, 20).map(_._2)
    val cluster1 = vectors.slice(20, 40).map(_._2)

    val intraDistance = avgPairwiseDistance(cluster0)
    val interDistance = avgCrossDistance(cluster0, cluster1)

    assert(intraDistance < interDistance,
      s"Intra-cluster distance ($intraDistance) should be less than inter-cluster ($interDistance)")
  }

  test("generateRealisticVectors should produce normalized vectors") {
    val vectors = TestDataGenerator.generateRealisticVectors(100, 128, sparsity = 0.3)

    vectors.foreach { case (_, vector) =>
      val norm = math.sqrt(vector.map(x => x * x).sum)
      assert(math.abs(norm - 1.0) < 0.001, s"Vector should be normalized, got norm=$norm")
    }
  }

  test("generateRealisticVectors should have small magnitude sparse values") {
    // Test that sparse dimensions have smaller magnitudes than non-sparse dimensions
    val sparsity = 0.3
    val vectors = TestDataGenerator.generateRealisticVectors(100, 128, sparsity = sparsity)

    // After normalization, sparse values (originally ~0.01) should still be
    // relatively smaller than non-sparse values (originally ~0.5)
    // We verify the distribution has a mix of small and larger magnitudes
    val allValues = vectors.flatMap(_._2).map(math.abs)
    val median = allValues.sorted.apply(allValues.length / 2)
    val smallValues = allValues.count(_ < median * 0.5)
    val smallRatio = smallValues.toDouble / allValues.length

    // Expect at least some proportion of values to be significantly below median
    assert(smallRatio > 0.1 && smallRatio < 0.6,
      s"Expected mix of small and large magnitudes, got small ratio=${smallRatio * 100}%")
  }

  test("l2Distance should compute correct Euclidean distance") {
    val a = Array(0f, 0f, 0f)
    val b = Array(3f, 4f, 0f)

    val distance = TestDataGenerator.l2Distance(a, b)
    assert(math.abs(distance - 5.0f) < 0.001f)
  }

  test("l2Distance should return 0 for identical vectors") {
    val a = Array(1f, 2f, 3f)
    val distance = TestDataGenerator.l2Distance(a, a)
    assert(distance == 0f)
  }

  test("cosineDistance should compute correct distance") {
    val a = Array(1f, 0f)
    val b = Array(0f, 1f)

    // Orthogonal vectors have cosine similarity 0, distance 1
    val distance = TestDataGenerator.cosineDistance(a, b)
    assert(math.abs(distance - 1.0f) < 0.001f)
  }

  test("cosineDistance should return 0 for parallel vectors") {
    val a = Array(1f, 2f, 3f)
    val b = Array(2f, 4f, 6f)

    val distance = TestDataGenerator.cosineDistance(a, b)
    assert(math.abs(distance) < 0.001f)
  }

  test("bruteForceKNN should return correct k results") {
    val vectors = TestDataGenerator.generateRandomVectors(100, 64)
    val query = vectors.head._2

    val results = TestDataGenerator.bruteForceKNN(query, vectors, k = 10)

    assert(results.length == 10)
    assert(results.head._1 == vectors.head._1, "First result should be the query itself")
    assert(results.head._2 < 0.001f, "Distance to self should be ~0")
  }

  test("bruteForceKNN results should be sorted by distance") {
    val vectors = TestDataGenerator.generateRandomVectors(100, 64)
    val query = Array.fill(64)(0.5f)

    val results = TestDataGenerator.bruteForceKNN(query, vectors, k = 20)

    val distances = results.map(_._2)
    assert(distances.sliding(2).forall {
      case Array(a, b) => a <= b
      case _ => true
    }, "Results should be sorted by distance")
  }

  test("calculateRecall should return 1.0 for perfect match") {
    val results = Seq((1L, 0.1f), (2L, 0.2f), (3L, 0.3f))
    val groundTruth = Seq((1L, 0.1f), (2L, 0.2f), (3L, 0.3f))

    val recall = TestDataGenerator.calculateRecall(results, groundTruth)
    assert(recall == 1.0)
  }

  test("calculateRecall should handle partial overlap") {
    val results = Seq((1L, 0.1f), (2L, 0.2f), (4L, 0.4f))
    val groundTruth = Seq((1L, 0.1f), (2L, 0.2f), (3L, 0.3f))

    val recall = TestDataGenerator.calculateRecall(results, groundTruth)
    assert(math.abs(recall - 2.0 / 3.0) < 0.001)
  }

  test("calculateRecall should return 0 for no overlap") {
    val results = Seq((4L, 0.1f), (5L, 0.2f), (6L, 0.3f))
    val groundTruth = Seq((1L, 0.1f), (2L, 0.2f), (3L, 0.3f))

    val recall = TestDataGenerator.calculateRecall(results, groundTruth)
    assert(recall == 0.0)
  }

  // Helper functions
  private def avgPairwiseDistance(vectors: Array[Array[Float]]): Double = {
    if (vectors.length < 2) return 0.0
    val distances = for {
      i <- vectors.indices
      j <- (i + 1) until vectors.length
    } yield TestDataGenerator.l2Distance(vectors(i), vectors(j)).toDouble
    distances.sum / distances.length
  }

  private def avgCrossDistance(v1: Array[Array[Float]], v2: Array[Array[Float]]): Double = {
    val distances = for {
      vec1 <- v1
      vec2 <- v2
    } yield TestDataGenerator.l2Distance(vec1, vec2).toDouble
    distances.sum / distances.length
  }
}
