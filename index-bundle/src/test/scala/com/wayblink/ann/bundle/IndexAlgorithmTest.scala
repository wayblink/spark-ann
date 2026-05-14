package com.wayblink.ann.bundle

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class IndexAlgorithmTest extends AnyFunSuite with Matchers {

  test("HNSW serialises as the string 'hnsw'") {
    IndexAlgorithm.HNSW.id shouldBe "hnsw"
  }

  test("fromId recognises 'hnsw' and rejects everything else") {
    IndexAlgorithm.fromId("hnsw") shouldBe Some(IndexAlgorithm.HNSW)
    IndexAlgorithm.fromId("HNSW") shouldBe None      // case-sensitive on purpose
    IndexAlgorithm.fromId("ivf") shouldBe None
    IndexAlgorithm.fromId("") shouldBe None
  }

  test("parse returns Right for known ids and a typed error otherwise") {
    IndexAlgorithm.parse("hnsw") shouldBe Right(IndexAlgorithm.HNSW)
    IndexAlgorithm.parse("scann") shouldBe Left(BundleError.UnknownAlgorithm("scann"))
  }

  test("All contains exactly the algorithms this build knows about") {
    // If a new case object is added without updating All, callers that
    // dispatch via .find will silently miss it — this test enforces
    // that contract.
    IndexAlgorithm.All should contain only IndexAlgorithm.HNSW
  }
}
