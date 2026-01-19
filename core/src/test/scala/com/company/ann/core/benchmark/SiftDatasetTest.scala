package com.company.ann.core.benchmark

import org.scalatest.funsuite.AnyFunSuite

import java.io.{DataOutputStream, FileOutputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.{Files, Path}

/**
 * Unit tests for SiftDataset fvecs/ivecs parsing.
 */
class SiftDatasetTest extends AnyFunSuite {

  test("readFvecs parses fvecs format correctly") {
    val tempFile = Files.createTempFile("test_vectors", ".fvecs")

    try {
      // Write test vectors in fvecs format
      val vectors = Array(
        Array(0.1f, 0.2f, 0.3f, 0.4f),
        Array(0.5f, 0.6f, 0.7f, 0.8f),
        Array(0.9f, 1.0f, 1.1f, 1.2f)
      )

      writeFvecs(tempFile, vectors)

      // Read back
      val result = SiftDataset.readFvecs(tempFile, None)

      assert(result.length == 3)
      assert(result(0)._1 == 0L)
      assert(result(1)._1 == 1L)
      assert(result(2)._1 == 2L)

      assert(result(0)._2.length == 4)
      assertFloatArrayEquals(result(0)._2, vectors(0))
      assertFloatArrayEquals(result(1)._2, vectors(1))
      assertFloatArrayEquals(result(2)._2, vectors(2))
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("readFvecs respects maxVectors limit") {
    val tempFile = Files.createTempFile("test_vectors_limit", ".fvecs")

    try {
      val vectors = Array(
        Array(1.0f, 2.0f),
        Array(3.0f, 4.0f),
        Array(5.0f, 6.0f),
        Array(7.0f, 8.0f),
        Array(9.0f, 10.0f)
      )

      writeFvecs(tempFile, vectors)

      val result = SiftDataset.readFvecs(tempFile, Some(3))

      assert(result.length == 3)
      assertFloatArrayEquals(result(0)._2, vectors(0))
      assertFloatArrayEquals(result(1)._2, vectors(1))
      assertFloatArrayEquals(result(2)._2, vectors(2))
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("readIvecs parses ivecs format correctly") {
    val tempFile = Files.createTempFile("test_indices", ".ivecs")

    try {
      val indices = Array(
        Array(0, 5, 10, 15),
        Array(1, 6, 11, 16),
        Array(2, 7, 12, 17)
      )

      writeIvecs(tempFile, indices)

      val result = SiftDataset.readIvecs(tempFile, None)

      assert(result.length == 3)
      assert(result(0).length == 4)
      assert(result(0).sameElements(indices(0)))
      assert(result(1).sameElements(indices(1)))
      assert(result(2).sameElements(indices(2)))
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("readIvecs respects maxVectors limit") {
    val tempFile = Files.createTempFile("test_indices_limit", ".ivecs")

    try {
      val indices = Array(
        Array(1, 2, 3),
        Array(4, 5, 6),
        Array(7, 8, 9),
        Array(10, 11, 12)
      )

      writeIvecs(tempFile, indices)

      val result = SiftDataset.readIvecs(tempFile, Some(2))

      assert(result.length == 2)
      assert(result(0).sameElements(indices(0)))
      assert(result(1).sameElements(indices(1)))
    } finally {
      Files.deleteIfExists(tempFile)
    }
  }

  test("SiftData calculateRecall computes correctly") {
    val baseVectors = (0 until 100).map { i =>
      (i.toLong, Array.fill(4)(i.toFloat))
    }.toArray

    val queryVectors = Array((0L, Array(0.0f, 0.0f, 0.0f, 0.0f)))

    // Ground truth: indices 0, 1, 2, ..., 9 are the 10 nearest
    val groundTruth = Array((0 until 100).toArray)

    val data = SiftData(SiftVariant.Sift10K, baseVectors, queryVectors, groundTruth)

    // Perfect recall: results exactly match ground truth
    val perfectRecall = data.calculateRecall(0, Seq(0L, 1L, 2L, 3L, 4L), 5)
    assert(perfectRecall == 1.0)

    // Partial recall: 3 out of 5 match
    val partialRecall = data.calculateRecall(0, Seq(0L, 1L, 2L, 50L, 60L), 5)
    assert(partialRecall == 0.6)

    // Zero recall: no matches
    val zeroRecall = data.calculateRecall(0, Seq(50L, 51L, 52L, 53L, 54L), 5)
    assert(zeroRecall == 0.0)
  }

  test("SiftData dimension and counts are correct") {
    val baseVectors = Array(
      (0L, Array(1.0f, 2.0f, 3.0f)),
      (1L, Array(4.0f, 5.0f, 6.0f))
    )
    val queryVectors = Array(
      (0L, Array(1.0f, 1.0f, 1.0f))
    )
    val groundTruth = Array(Array(0, 1))

    val data = SiftData(SiftVariant.Sift10K, baseVectors, queryVectors, groundTruth)

    assert(data.dimension == 3)
    assert(data.numBaseVectors == 2)
    assert(data.numQueries == 1)
  }

  test("SiftVariant fromEnv returns correct variant") {
    // Default should be Sift10K when no env var is set
    val defaultVariant = SiftVariant.fromEnv(default = SiftVariant.Sift10K)
    assert(defaultVariant == SiftVariant.Sift10K)
  }

  // Helper functions to write test files

  private def writeFvecs(path: Path, vectors: Array[Array[Float]]): Unit = {
    val out = new DataOutputStream(new FileOutputStream(path.toFile))
    try {
      vectors.foreach { vec =>
        // Write dimension (little-endian)
        val dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        dimBuffer.putInt(vec.length)
        out.write(dimBuffer.array())

        // Write floats (little-endian)
        val vecBuffer = ByteBuffer.allocate(vec.length * 4).order(ByteOrder.LITTLE_ENDIAN)
        vec.foreach(vecBuffer.putFloat)
        out.write(vecBuffer.array())
      }
    } finally {
      out.close()
    }
  }

  private def writeIvecs(path: Path, indices: Array[Array[Int]]): Unit = {
    val out = new DataOutputStream(new FileOutputStream(path.toFile))
    try {
      indices.foreach { arr =>
        // Write dimension (little-endian)
        val dimBuffer = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN)
        dimBuffer.putInt(arr.length)
        out.write(dimBuffer.array())

        // Write ints (little-endian)
        val arrBuffer = ByteBuffer.allocate(arr.length * 4).order(ByteOrder.LITTLE_ENDIAN)
        arr.foreach(arrBuffer.putInt)
        out.write(arrBuffer.array())
      }
    } finally {
      out.close()
    }
  }

  private def assertFloatArrayEquals(actual: Array[Float], expected: Array[Float], tolerance: Float = 0.0001f): Unit = {
    assert(actual.length == expected.length, s"Array lengths differ: ${actual.length} vs ${expected.length}")
    actual.zip(expected).zipWithIndex.foreach { case ((a, e), i) =>
      assert(math.abs(a - e) < tolerance, s"Element $i differs: $a vs $e")
    }
  }
}
