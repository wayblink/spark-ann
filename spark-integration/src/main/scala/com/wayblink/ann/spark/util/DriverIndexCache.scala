package com.wayblink.ann.spark.util

import com.wayblink.ann.core.index.HNSWLibIndex

import java.util
import scala.collection.JavaConverters._

/**
 * Driver-side LRU cache for lazily-loaded HNSW indexes.
 *
 * Sibling to [[ExecutorIndexCache]], but with bounded capacity. The
 * executor cache is unbounded because each executor's working set is the
 * partition's subset of indexes; the driver, in contrast, may be queried
 * against an entire index set far larger than its heap. The bounded LRU
 * caps resident state and evicts least-recently-used entries.
 *
 * Reuses [[HNSWLibIndex.load]] for actual deserialization. Thread-safe via
 * coarse-grained synchronization. The single-query driver `search` path
 * uses this; `batchSearch` continues to use [[ExecutorIndexCache]] on
 * executors.
 */
object DriverIndexCache {

  private val DefaultMaxLoaded = 10

  private var maxLoaded: Int = DefaultMaxLoaded
  // access-order LRU; eldest entry evicted on overflow.
  private val cache: util.LinkedHashMap[String, HNSWLibIndex] =
    new util.LinkedHashMap[String, HNSWLibIndex](16, 0.75f, true) {
      override def removeEldestEntry(eldest: util.Map.Entry[String, HNSWLibIndex]): Boolean =
        size() > maxLoaded
    }
  private var globalIndex: Option[HNSWLibIndex] = None
  private var globalPath: Option[String] = None

  /** Configure the maximum number of locally cached indexes. Default 10. */
  def setMaxLoaded(n: Int): Unit = synchronized {
    require(n > 0, s"maxLoaded must be positive, got $n")
    maxLoaded = n
    while (cache.size() > maxLoaded) {
      val it = cache.entrySet().iterator()
      it.next(); it.remove()
    }
  }

  def maxLoadedCapacity: Int = synchronized(maxLoaded)

  /**
   * Load (or fetch from cache) a single local index by id + path. The
   * mapping is path-keyed internally so the same id pointing to a new path
   * forces a reload.
   */
  def getOrLoadLocal(indexId: String, indexPath: String): HNSWLibIndex = synchronized {
    val key = cacheKey(indexId, indexPath)
    val cached = cache.get(key)
    if (cached != null) cached
    else {
      val loaded = HNSWLibIndex.load(indexPath)
      cache.put(key, loaded)
      loaded
    }
  }

  /**
   * Load (or fetch from cache) the global routing index. Only one global
   * index is held at a time per driver; reassigning a different path
   * evicts the previous one.
   */
  def getOrLoadGlobal(path: String): HNSWLibIndex = synchronized {
    if (globalIndex.isEmpty || !globalPath.contains(path)) {
      globalIndex = Some(HNSWLibIndex.load(path))
      globalPath = Some(path)
    }
    globalIndex.get
  }

  /**
   * Snapshot of currently cached local index keys, useful for tests and
   * diagnostics.
   */
  def loadedKeys: Set[String] = synchronized {
    cache.keySet().asScala.toSet
  }

  /** Number of local indexes currently held by the cache. */
  def loadedCount: Int = synchronized(cache.size())

  /** Whether a global routing index is currently loaded. */
  def hasGlobal: Boolean = synchronized(globalIndex.isDefined)

  /** Clear everything; primarily for tests. */
  def clear(): Unit = synchronized {
    cache.clear()
    globalIndex = None
    globalPath = None
  }

  private def cacheKey(indexId: String, indexPath: String): String =
    s"$indexId@$indexPath"
}
