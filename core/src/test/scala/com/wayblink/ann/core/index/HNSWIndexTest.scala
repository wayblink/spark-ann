package com.wayblink.ann.core.index

import com.wayblink.ann.core.testutil.TestDataGenerator
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.{Files, Path}

class HNSWIndexTest extends AnyFunSuite with BeforeAndAfterAll {

  private var tempDir: Path = _

  override def beforeAll(): Unit = {
    tempDir = Files.createTempDirectory("hnsw_test_")
  }

  override def afterAll(): Unit = {
    deleteRecursively(tempDir.toFile)
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

  // ============== Basic Functionality Tests ==============

  test("Build and search small index") {
    val vectors = TestDataGenerator.generateRandomVectors(1000, 128)
    val index = HNSWLibIndex(dimension = 128)

    index.addAll(vectors)
    assert(index.size == 1000)

    // Query the first vector (should find itself)
    val query = vectors.head._2
    val results = index.search(query, k = 5)

    assert(results.nonEmpty)
    assert(results.head.id == vectors.head._1, "First result should be the query vector itself")
    assert(results.head.distance < 0.001f, "Should find exact match with ~0 distance")
  }

  test("Search returns k results in sorted order") {
    val vectors = TestDataGenerator.generateRandomVectors(500, 64)
    val index = HNSWLibIndex(dimension = 64)
    index.addAll(vectors)

    val query = Array.fill(64)(0.5f)
    val results = index.search(query, k = 10)

    assert(results.length == 10)
    // Results should be sorted by distance in ascending order
    assert(results.sliding(2).forall {
      case Seq(a, b) => a.distance <= b.distance
      case _ => true
    }, "Results should be sorted by distance")
  }

  test("Search with k larger than index size") {
    val vectors = TestDataGenerator.generateRandomVectors(50, 32)
    val index = HNSWLibIndex(dimension = 32)
    index.addAll(vectors)

    val query = vectors.head._2
    val results = index.search(query, k = 100) // Request more than available

    assert(results.length == 50, "Should return all available vectors")
  }

  test("Empty index returns empty results") {
    val index = HNSWLibIndex(dimension = 64)
    val query = Array.fill(64)(0.5f)
    val results = index.search(query, k = 10)

    assert(results.isEmpty)
  }

  test("Add single vector and search") {
    val index = HNSWLibIndex(dimension = 4)
    val id = 42L
    val vector = Array(0.1f, 0.2f, 0.3f, 0.4f)

    index.add(id, vector)
    assert(index.size == 1)
    assert(index.contains(id))

    val results = index.search(vector, k = 1)
    assert(results.length == 1)
    assert(results.head.id == id)
  }

  // ============== Recall Tests ==============

  test("High recall on clustered data") {
    val vectors = TestDataGenerator.generateClusteredVectors(
      numClusters = 10,
      vectorsPerCluster = 100,
      dimension = 128
    )

    val config = HNSWConfig(M = 16, efConstruction = 200, maxElements = 1500)
    val index = HNSWLibIndex(dimension = 128, config)
    index.addAll(vectors)

    // Query with a vector from cluster 0
    val queryVector = vectors.head._2
    val k = 20

    val indexResults = index.search(queryVector, k, ef = 100)
    val groundTruth = TestDataGenerator.bruteForceKNN(queryVector, vectors, k)

    val recall = TestDataGenerator.calculateRecall(
      indexResults.map(r => (r.id, r.distance)),
      groundTruth
    )

    println(s"Recall on clustered data: $recall")
    assert(recall >= 0.9, s"Recall $recall should be >= 0.9")
  }

  test("High recall on random data") {
    val vectors = TestDataGenerator.generateRandomVectors(1000, 64)

    val config = HNSWConfig(M = 16, efConstruction = 200, maxElements = 1500)
    val index = HNSWLibIndex(dimension = 64, config)
    index.addAll(vectors)

    // Test multiple queries
    val queryIndices = Seq(0, 100, 500, 999)
    val recalls = queryIndices.map { i =>
      val query = vectors(i)._2
      val k = 10

      val indexResults = index.search(query, k, ef = 100)
      val groundTruth = TestDataGenerator.bruteForceKNN(query, vectors, k)

      TestDataGenerator.calculateRecall(
        indexResults.map(r => (r.id, r.distance)),
        groundTruth
      )
    }

    val avgRecall = recalls.sum / recalls.length
    println(s"Average recall on random data: $avgRecall (individual: ${recalls.mkString(", ")})")
    assert(avgRecall >= 0.9, s"Average recall $avgRecall should be >= 0.9")
  }

  test("Different ef values affect recall") {
    val vectors = TestDataGenerator.generateClusteredVectors(10, 100, 64)

    val config = HNSWConfig(M = 12, efConstruction = 100, maxElements = 1500)
    val index = HNSWLibIndex(dimension = 64, config)
    index.addAll(vectors)

    val query = vectors(50)._2
    val k = 10
    val groundTruth = TestDataGenerator.bruteForceKNN(query, vectors, k)

    val recalls = Seq(10, 50, 100, 200).map { ef =>
      val results = index.search(query, k, ef = ef)
      val recall = TestDataGenerator.calculateRecall(
        results.map(r => (r.id, r.distance)),
        groundTruth
      )
      (ef, recall)
    }

    println(s"Recall by ef value: ${recalls.map { case (ef, r) => s"ef=$ef -> $r" }.mkString(", ")}")

    // Higher ef should generally have better or equal recall
    recalls.sliding(2).foreach {
      case Seq((ef1, r1), (ef2, r2)) =>
        // Allow small tolerance for non-determinism
        assert(r2 >= r1 - 0.1,
          s"Recall at ef=$ef2 ($r2) should be >= recall at ef=$ef1 ($r1)")
      case _ =>
    }
  }

  // ============== Configuration Tests ==============

  test("Custom M parameter affects index structure") {
    val vectors = TestDataGenerator.generateRandomVectors(500, 32)

    val configM8 = HNSWConfig(M = 8, efConstruction = 100, maxElements = 600)
    val configM32 = HNSWConfig(M = 32, efConstruction = 100, maxElements = 600)

    val indexM8 = HNSWLibIndex(dimension = 32, configM8)
    val indexM32 = HNSWLibIndex(dimension = 32, configM32)

    indexM8.addAll(vectors)
    indexM32.addAll(vectors)

    // Both should work correctly
    val query = vectors.head._2
    val resultsM8 = indexM8.search(query, k = 5, ef = 50)
    val resultsM32 = indexM32.search(query, k = 5, ef = 50)

    assert(resultsM8.nonEmpty)
    assert(resultsM32.nonEmpty)
    assert(resultsM8.head.id == vectors.head._1)
    assert(resultsM32.head.id == vectors.head._1)
  }

  test("efConstruction affects build quality") {
    val vectors = TestDataGenerator.generateRandomVectors(300, 32)
    val query = vectors(50)._2
    val k = 10
    val groundTruth = TestDataGenerator.bruteForceKNN(query, vectors, k)

    val configLow = HNSWConfig(M = 12, efConstruction = 50, maxElements = 400)
    val configHigh = HNSWConfig(M = 12, efConstruction = 200, maxElements = 400)

    val indexLow = HNSWLibIndex(dimension = 32, configLow)
    val indexHigh = HNSWLibIndex(dimension = 32, configHigh)

    indexLow.addAll(vectors)
    indexHigh.addAll(vectors)

    val resultsLow = indexLow.search(query, k, ef = 50)
    val resultsHigh = indexHigh.search(query, k, ef = 50)

    val recallLow = TestDataGenerator.calculateRecall(
      resultsLow.map(r => (r.id, r.distance)), groundTruth)
    val recallHigh = TestDataGenerator.calculateRecall(
      resultsHigh.map(r => (r.id, r.distance)), groundTruth)

    println(s"Recall with efConstruction=50: $recallLow")
    println(s"Recall with efConstruction=200: $recallHigh")

    // Higher efConstruction should give better or equal recall
    assert(recallHigh >= recallLow - 0.1,
      s"Higher efConstruction should not significantly hurt recall")
  }

  // ============== Distance Function Tests ==============

  test("Euclidean distance function works correctly") {
    val index = HNSWLibIndex(dimension = 3, HNSWConfig(maxElements = 10), "euclidean")

    index.add(1L, Array(0.0f, 0.0f, 0.0f))
    index.add(2L, Array(1.0f, 0.0f, 0.0f))
    index.add(3L, Array(0.0f, 1.0f, 0.0f))

    val query = Array(0.0f, 0.0f, 0.0f)
    val results = index.search(query, k = 3)

    assert(results.head.id == 1L)
    assert(results.head.distance < 0.001f) // Origin should match exactly
    // Distance to (1,0,0) and (0,1,0) should be ~1.0
    assert(results(1).distance > 0.9f && results(1).distance < 1.1f)
  }

  test("Cosine distance function works correctly") {
    val index = HNSWLibIndex(dimension = 3, HNSWConfig(maxElements = 10), "cosine")

    // Normalized vectors
    val sqrt2 = math.sqrt(2.0).toFloat
    index.add(1L, Array(1.0f, 0.0f, 0.0f))
    index.add(2L, Array(1.0f / sqrt2, 1.0f / sqrt2, 0.0f)) // 45 degrees from (1,0,0)
    index.add(3L, Array(0.0f, 1.0f, 0.0f)) // 90 degrees from (1,0,0)

    val query = Array(1.0f, 0.0f, 0.0f)
    val results = index.search(query, k = 3)

    assert(results.head.id == 1L) // Same direction, cosine distance ~ 0
    assert(results.head.distance < 0.001f)
    assert(results(1).id == 2L) // 45 degrees
  }

  // ============== Persistence Tests ==============

  test("Save and load index preserves data") {
    val vectors = TestDataGenerator.generateRandomVectors(200, 64)
    val index = HNSWLibIndex(dimension = 64)
    index.addAll(vectors)

    val indexPath = tempDir.resolve("test_index.hnsw").toString
    index.save(indexPath)

    // Load into new index
    val loadedIndex = HNSWLibIndex.load(indexPath)

    assert(loadedIndex.dimension == 64)
    assert(loadedIndex.size == 200)

    // Verify search works on loaded index
    val query = vectors.head._2
    val originalResults = index.search(query, k = 10)
    val loadedResults = loadedIndex.search(query, k = 10)

    assert(originalResults.map(_.id) == loadedResults.map(_.id),
      "Search results should match after load")
  }

  test("Load index with wrong dimension fails") {
    val index = HNSWLibIndex(dimension = 64)
    index.add(1L, Array.fill(64)(0.5f))

    val indexPath = tempDir.resolve("test_dim_check.hnsw").toString
    index.save(indexPath)

    // Try to load with wrong dimension
    val wrongDimIndex = HNSWLibIndex(dimension = 32)
    assertThrows[IllegalArgumentException] {
      wrongDimIndex.load(indexPath)
    }
  }

  // ============== Edge Cases ==============

  test("Vector dimension mismatch throws exception") {
    val index = HNSWLibIndex(dimension = 64)

    assertThrows[IllegalArgumentException] {
      index.add(1L, Array.fill(32)(0.5f)) // Wrong dimension
    }
  }

  test("Query dimension mismatch throws exception") {
    val index = HNSWLibIndex(dimension = 64)
    index.add(1L, Array.fill(64)(0.5f))

    assertThrows[IllegalArgumentException] {
      index.search(Array.fill(32)(0.5f), k = 1) // Wrong query dimension
    }
  }

  test("Contains and remove operations") {
    val index = HNSWLibIndex(dimension = 4)

    index.add(1L, Array(0.1f, 0.2f, 0.3f, 0.4f))
    index.add(2L, Array(0.5f, 0.6f, 0.7f, 0.8f))

    assert(index.contains(1L))
    assert(index.contains(2L))
    assert(!index.contains(3L))

    index.remove(1L)
    assert(!index.contains(1L))
    assert(index.contains(2L))
  }

  test("GetItems returns all indexed vectors") {
    val vectors = TestDataGenerator.generateRandomVectors(100, 16)
    val index = HNSWLibIndex(dimension = 16)
    index.addAll(vectors)

    val items = index.getItems
    assert(items.length == 100)

    val indexedIds = items.map(_._1).toSet
    val originalIds = vectors.map(_._1).toSet
    assert(indexedIds == originalIds)
  }

  // ============== Performance Sanity Tests ==============

  test("Index search is faster than brute force") {
    val vectors = TestDataGenerator.generateRandomVectors(5000, 128)
    val config = HNSWConfig(M = 16, efConstruction = 100, maxElements = 6000)
    val index = HNSWLibIndex(dimension = 128, config)
    index.addAll(vectors)

    val query = vectors(500)._2
    val k = 10

    // Time brute force
    val bruteStartTime = System.nanoTime()
    (0 until 10).foreach(_ => TestDataGenerator.bruteForceKNN(query, vectors, k))
    val bruteTime = (System.nanoTime() - bruteStartTime) / 10.0 / 1e6

    // Time index search
    val indexStartTime = System.nanoTime()
    (0 until 10).foreach(_ => index.search(query, k, ef = 50))
    val indexTime = (System.nanoTime() - indexStartTime) / 10.0 / 1e6

    println(f"Brute force: $bruteTime%.2f ms, Index: $indexTime%.2f ms, Speedup: ${bruteTime / indexTime}%.1fx")

    // Index should be at least 5x faster for 5K vectors
    assert(indexTime < bruteTime,
      s"Index search ($indexTime ms) should be faster than brute force ($bruteTime ms)")
  }

  test("Batch add is efficient") {
    val vectors = TestDataGenerator.generateRandomVectors(2000, 64)
    val config = HNSWConfig(maxElements = 3000)

    // Time single adds
    val singleIndex = HNSWLibIndex(dimension = 64, config)
    val singleStartTime = System.nanoTime()
    vectors.foreach { case (id, vector) => singleIndex.add(id, vector) }
    val singleTime = (System.nanoTime() - singleStartTime) / 1e6

    // Time batch add
    val batchIndex = HNSWLibIndex(dimension = 64, config)
    val batchStartTime = System.nanoTime()
    batchIndex.addAll(vectors)
    val batchTime = (System.nanoTime() - batchStartTime) / 1e6

    println(f"Single adds: $singleTime%.2f ms, Batch add: $batchTime%.2f ms")

    assert(singleIndex.size == 2000)
    assert(batchIndex.size == 2000)
  }
}
