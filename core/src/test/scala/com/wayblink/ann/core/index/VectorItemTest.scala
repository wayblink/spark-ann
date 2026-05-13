package com.wayblink.ann.core.index

import org.scalatest.funsuite.AnyFunSuite

class VectorItemTest extends AnyFunSuite {

  // ============== toString Tests ==============

  test("toString shows all elements for long vectors") {
    val item = VectorItem(42L, Array(0.1f, 0.2f, 0.3f, 0.4f, 0.5f))
    assert(item.toString == "VectorItem(42, [0.1, 0.2, 0.3, 0.4, 0.5])")
  }

  test("toString shows all elements for 3-element vector") {
    val item = VectorItem(1L, Array(1.0f, 2.0f, 3.0f))
    assert(item.toString == "VectorItem(1, [1.0, 2.0, 3.0])")
  }

  test("toString shows all elements for short vectors") {
    val item2 = VectorItem(2L, Array(1.5f, 2.5f))
    assert(item2.toString == "VectorItem(2, [1.5, 2.5])")

    val item1 = VectorItem(3L, Array(9.9f))
    assert(item1.toString == "VectorItem(3, [9.9])")
  }

  test("toString handles empty vector") {
    val item = VectorItem(0L, Array.empty[Float])
    assert(item.toString == "VectorItem(0, [])")
  }

  test("toString handles negative id") {
    val item = VectorItem(-100L, Array(0.1f, 0.2f, 0.3f, 0.4f))
    assert(item.toString == "VectorItem(-100, [0.1, 0.2, 0.3, 0.4])")
  }

  test("toString handles negative float values") {
    val item = VectorItem(5L, Array(-0.5f, -1.0f, -2.5f, 3.0f))
    assert(item.toString == "VectorItem(5, [-0.5, -1.0, -2.5, 3.0])")
  }

  // ============== Basic Accessor Tests ==============

  test("id returns correct value") {
    val item = VectorItem(12345L, Array(0.1f, 0.2f))
    assert(item.id() == 12345L)
  }

  test("vector returns correct array") {
    val vec = Array(0.1f, 0.2f, 0.3f)
    val item = VectorItem(1L, vec)
    assert(item.vector() sameElements vec)
  }

  test("dimensions returns vector length") {
    val item = VectorItem(1L, Array(0.1f, 0.2f, 0.3f, 0.4f, 0.5f))
    assert(item.dimensions() == 5)
  }

  test("dimensions is 0 for empty vector") {
    val item = VectorItem(1L, Array.empty[Float])
    assert(item.dimensions() == 0)
  }

  // ============== Equality Tests ==============

  test("equals returns true for identical items") {
    val item1 = VectorItem(1L, Array(0.1f, 0.2f, 0.3f))
    val item2 = VectorItem(1L, Array(0.1f, 0.2f, 0.3f))
    assert(item1 == item2)
    assert(item1.equals(item2))
  }

  test("equals returns false for different ids") {
    val item1 = VectorItem(1L, Array(0.1f, 0.2f, 0.3f))
    val item2 = VectorItem(2L, Array(0.1f, 0.2f, 0.3f))
    assert(item1 != item2)
  }

  test("equals returns false for different vectors") {
    val item1 = VectorItem(1L, Array(0.1f, 0.2f, 0.3f))
    val item2 = VectorItem(1L, Array(0.1f, 0.2f, 0.4f))
    assert(item1 != item2)
  }

  test("equals returns false for different vector lengths") {
    val item1 = VectorItem(1L, Array(0.1f, 0.2f, 0.3f))
    val item2 = VectorItem(1L, Array(0.1f, 0.2f))
    assert(item1 != item2)
  }

  test("equals returns false for non-VectorItem") {
    val item = VectorItem(1L, Array(0.1f, 0.2f))
    assert(item != "not a VectorItem")
    assert(item != null)
    assert(item != 42)
  }

  // ============== HashCode Tests ==============

  test("hashCode is consistent for equal items") {
    val item1 = VectorItem(1L, Array(0.1f, 0.2f, 0.3f))
    val item2 = VectorItem(1L, Array(0.1f, 0.2f, 0.3f))
    assert(item1.hashCode() == item2.hashCode())
  }

  test("hashCode differs for different items") {
    val item1 = VectorItem(1L, Array(0.1f, 0.2f, 0.3f))
    val item2 = VectorItem(2L, Array(0.1f, 0.2f, 0.3f))
    val item3 = VectorItem(1L, Array(0.4f, 0.5f, 0.6f))
    // Hash codes should generally be different (though collisions are possible)
    assert(item1.hashCode() != item2.hashCode() || item1.hashCode() != item3.hashCode())
  }

  // ============== Companion Object Tests ==============

  test("apply creates VectorItem correctly") {
    val item = VectorItem(99L, Array(1.0f, 2.0f))
    assert(item.id() == 99L)
    assert(item.vector() sameElements Array(1.0f, 2.0f))
  }
}
