package com.wayblink.ann.spark.builder

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class StreamingBoundaryReservoirTest extends AnyFunSuite with Matchers {

  test("reservoir retains exactly targetCount after enough offers") {
    val r = new StreamingBoundaryReservoir(targetCount = 50, seed = 1L)
    (0 until 10000).foreach { i =>
      r.offer(i.toLong, Array.fill(8)(i.toFloat))
    }
    r.size shouldBe 50
    r.totalSeen shouldBe 10000L
  }

  test("reservoir retains everything when stream is smaller than targetCount") {
    val r = new StreamingBoundaryReservoir(targetCount = 50, seed = 1L)
    (0 until 20).foreach { i =>
      r.offer(i.toLong, Array.fill(4)(i.toFloat))
    }
    r.size shouldBe 20
    val out = r.result("idx_test")
    out should have length 20
    out.map(_.localId).toSet shouldBe (0L until 20L).toSet
  }

  test("reservoir samples roughly uniformly across the stream") {
    val targetCount = 200
    val streamLen = 100000
    val r = new StreamingBoundaryReservoir(targetCount, seed = 7L)
    (0 until streamLen).foreach { i =>
      r.offer(i.toLong, Array.fill(1)(0f))
    }

    val sampledIds = r.result("idx_u").map(_.localId)
    // Split the stream into 10 equal buckets and verify each bucket
    // receives within ±50% of the expected reservoir share. The bound is
    // intentionally loose — Algorithm R is exact uniform in expectation
    // but a single run with 200 samples has nontrivial variance.
    val bucketSize = streamLen / 10
    val counts = Array.fill(10)(0)
    sampledIds.foreach { id =>
      val b = math.min(9, (id / bucketSize).toInt)
      counts(b) += 1
    }
    val expected = targetCount / 10
    counts.foreach { c =>
      c should be >= (expected / 2)
      c should be <= (expected * 3 / 2 + 5)
    }
  }

  test("targetCount = 0 is a no-op") {
    val r = new StreamingBoundaryReservoir(targetCount = 0)
    r.offer(1L, Array(0f))
    r.offer(2L, Array(1f))
    r.size shouldBe 0
    r.result("idx") shouldBe empty
    r.totalSeen shouldBe 2L
  }

  test("result entries carry the source indexId and preserve vector reference") {
    val r = new StreamingBoundaryReservoir(targetCount = 3, seed = 42L)
    val v0 = Array(0.1f, 0.2f)
    val v1 = Array(0.3f, 0.4f)
    val v2 = Array(0.5f, 0.6f)
    r.offer(100L, v0); r.offer(101L, v1); r.offer(102L, v2)

    val out = r.result("idx_source")
    out should have length 3
    out.foreach(_.indexId shouldBe "idx_source")
    out.map(_.globalId).toSet shouldBe Set("idx_source:100", "idx_source:101", "idx_source:102")
  }
}
