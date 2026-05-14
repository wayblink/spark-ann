package com.wayblink.ann.bundle

import com.wayblink.ann.core.index.{HNSWConfig, HNSWLibIndex}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class RoutingTest extends AnyFunSuite with Matchers {

  /**
   * Build a tiny global routing index containing one vector per
   * "cluster". The boundary map then maps each vector's id to the
   * source local index name.
   */
  private def buildGlobal(centers: Seq[(Long, Array[Float])]): HNSWLibIndex = {
    val dim = centers.head._2.length
    val cfg = HNSWConfig(M = 8, efConstruction = 50, maxElements = centers.size + 4)
    val idx = HNSWLibIndex(dim, cfg, "euclidean")
    centers.foreach { case (id, v) => idx.add(id, v) }
    idx
  }

  test("with a global routing index, a query routes to the closest cluster") {
    val centers = Seq(
      (0L, Array(0.0f, 0.0f, 0.0f)),
      (1L, Array(1.0f, 1.0f, 1.0f)),
      (2L, Array(5.0f, 5.0f, 5.0f))
    )
    val global = buildGlobal(centers)
    val boundary = Array("idx_zero", "idx_one", "idx_far")
    val all = boundary.toSeq

    Routing.selectTargetIndexes(
      Array(0.05f, 0.05f, 0.05f), nprobe = 1, Some(global), all, boundary
    ).head shouldBe "idx_zero"

    Routing.selectTargetIndexes(
      Array(4.9f, 5.0f, 5.1f), nprobe = 1, Some(global), all, boundary
    ).head shouldBe "idx_far"
  }

  test("falls back to all index ids when no global routing index is given") {
    val all = Seq("idx_a", "idx_b", "idx_c")
    Routing.selectTargetIndexes(
      Array(0.0f), nprobe = 2, None, all, Array.empty[String]
    ) shouldBe all
  }

  test("falls back to all when boundary map is empty") {
    val global = buildGlobal(Seq((0L, Array(1.0f))))
    val all = Seq("idx_a", "idx_b")
    Routing.selectTargetIndexes(
      Array(0.0f), nprobe = 2, Some(global), all, Array.empty[String]
    ) shouldBe all
  }

  test("returns at most nprobe distinct indexIds even when routing repeats") {
    val centers = Seq(
      (0L, Array(0.0f)),
      (1L, Array(0.1f)),
      (2L, Array(0.2f))
    )
    val global = buildGlobal(centers)
    // All three boundary ids point at the same local index.
    val boundary = Array("idx_one", "idx_one", "idx_one")
    val all = Seq("idx_one")
    val out = Routing.selectTargetIndexes(
      Array(0.0f), nprobe = 3, Some(global), all, boundary
    )
    out.distinct shouldBe Seq("idx_one")
  }

  test("ignores out-of-bounds global ids without throwing") {
    val centers = Seq((0L, Array(0.0f)), (99L, Array(0.5f)))
    val global = buildGlobal(centers)
    val boundary = Array("idx_only")  // only globalId 0 is mapped
    val all = Seq("idx_only")

    // The query may hit either id 0 or id 99; either way, only id 0 has
    // a boundary entry, and the function must not blow up on id 99.
    val out = Routing.selectTargetIndexes(
      Array(0.4f), nprobe = 2, Some(global), all, boundary
    )
    out.foreach(_ shouldBe "idx_only")
  }
}
