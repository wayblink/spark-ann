package com.wayblink.ann.core.index

import com.github.jelmerk.knn.Item
import java.util.{Arrays => JArrays}

/**
 * Vector item implementation for hnswlib-core integration.
 * Wraps a Long ID and Float array vector.
 *
 * @param id The unique identifier for this vector
 * @param vector The vector data as Float array
 */
class VectorItem(
  private val itemId: Long,
  private val itemVector: Array[Float]
) extends Item[Long, Array[Float]] with Serializable {

  override def id(): Long = itemId

  override def vector(): Array[Float] = itemVector

  override def dimensions(): Int = itemVector.length

  override def equals(obj: Any): Boolean = obj match {
    case other: VectorItem =>
      itemId == other.itemId && JArrays.equals(itemVector, other.itemVector)
    case _ => false
  }

  override def hashCode(): Int = {
    31 * itemId.hashCode() + JArrays.hashCode(itemVector)
  }

  override def toString: String = s"VectorItem($itemId, [${itemVector.mkString(", ")}])"
}

object VectorItem {
  def apply(id: Long, vector: Array[Float]): VectorItem = new VectorItem(id, vector)
}
