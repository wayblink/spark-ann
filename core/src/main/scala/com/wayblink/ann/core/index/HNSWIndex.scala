package com.wayblink.ann.core.index

/**
 * Core HNSW Index interface.
 * This defines the contract for all HNSW implementations.
 */
trait HNSWIndex {
  def dimension: Int
  def size: Int

  def add(id: Long, vector: Array[Float]): Unit
  def addAll(vectors: Seq[(Long, Array[Float])]): Unit

  def search(query: Array[Float], k: Int, ef: Int = 50): Seq[SearchResult]

  def save(path: String): Unit
  def load(path: String): Unit
}

/**
 * Search result from an ANN query.
 *
 * @param id The ID of the matching vector
 * @param distance The distance from the query vector
 */
case class SearchResult(
  id: Long,
  distance: Float
)

/**
 * Configuration for HNSW index construction.
 *
 * @param M Number of connections per element (default 16)
 * @param efConstruction Size of dynamic candidate list during construction (default 200)
 * @param maxElements Maximum number of elements the index can hold
 * @param randomSeed Random seed for reproducibility
 */
case class HNSWConfig(
  M: Int = 16,
  efConstruction: Int = 200,
  maxElements: Int = 1000000,
  randomSeed: Long = 42
)
