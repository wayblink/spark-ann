package com.wayblink.ann.api.service

import com.wayblink.ann.api.error.ApiError
import com.wayblink.ann.bundle.{ANNIndexMetadata, BundleError, BundleReader}
import com.wayblink.ann.core.index.{HNSWConfig, HNSWLibIndex}

import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Metadata about a single flat HNSW index loaded into memory. Backwards-
 * compatible payload for the legacy `POST /indexes` endpoint that
 * accepts a single `.hnsw` file path or an inline vector list.
 */
case class LoadedIndexInfo(
  indexId: String,
  index: HNSWLibIndex,
  indexPath: Option[String],
  distanceType: String,
  loadedAt: Long = System.currentTimeMillis()
)

/**
 * Metadata about a loaded bundle (pattern B). All local indexes are
 * eagerly loaded — the api-server is the canonical pattern-B online
 * server and is expected to hold its full bundle resident.
 */
case class LoadedBundleInfo(
  indexId: String,
  bundlePath: String,
  metadata: ANNIndexMetadata,
  localIndexes: Map[String, HNSWLibIndex],
  globalIndex: Option[HNSWLibIndex],
  boundaryMap: Array[String],
  loadedAt: Long = System.currentTimeMillis()
) {
  def distanceType: String = metadata.config.distanceType
  def algorithmId: String  = metadata.config.algorithm.id
  def dimension: Int       = metadata.dimension
  def totalVectors: Long   = metadata.totalVectors
}

/**
 * Lookup result spanning both index modes, so route handlers can
 * dispatch with a single `match` instead of probing two maps.
 */
sealed trait IndexEntry
object IndexEntry {
  final case class Flat(info: LoadedIndexInfo) extends IndexEntry
  final case class Bundle(info: LoadedBundleInfo) extends IndexEntry
}

/**
 * Thread-safe manager for in-memory indexes.
 *
 * Two storage backends sit side-by-side:
 *  - `flatIndexes`   — single-file `.hnsw` indexes (legacy; create-from-vectors)
 *  - `bundleIndexes` — multi-local-index bundles (pattern B; loaded from disk)
 *
 * Identifiers are unique across both backends; loading a flat with id
 * "x" then a bundle with id "x" returns IndexAlreadyExists. The
 * lookup APIs (`getEntry`, `listEntries`) probe both backends.
 *
 * Concurrency: every state-change uses ConcurrentHashMap.putIfAbsent
 * so the historical TOCTOU window between containsKey and put is
 * gone (todo.md #11).
 */
class IndexManager {

  private val flatIndexes   = new ConcurrentHashMap[String, LoadedIndexInfo]()
  private val bundleIndexes = new ConcurrentHashMap[String, LoadedBundleInfo]()

  // ── Flat-index API (legacy) ──────────────────────────────────────────

  def loadIndex(indexId: String, indexPath: String): Either[ApiError, LoadedIndexInfo] = {
    if (bundleIndexes.containsKey(indexId)) {
      return Left(ApiError.IndexAlreadyExists(indexId))
    }
    try {
      val index = HNSWLibIndex.load(indexPath)
      // distanceType from the .hnsw.meta sidecar would be ideal, but
      // HNSWLibIndex.load doesn't currently expose it post-load.
      // TODO(spark-ann/#future): plumb distanceType through
      //   HNSWLibIndex so this default goes away.
      val info = LoadedIndexInfo(
        indexId = indexId,
        index = index,
        indexPath = Some(indexPath),
        distanceType = "euclidean"
      )
      val existing = flatIndexes.putIfAbsent(indexId, info)
      if (existing != null) Left(ApiError.IndexAlreadyExists(indexId))
      else Right(info)
    } catch {
      case e: Exception =>
        Left(ApiError.InternalFailure(s"Failed to load index from '$indexPath': ${e.getMessage}"))
    }
  }

  def createIndex(
    indexId: String,
    dimension: Int,
    vectors: Seq[(Long, Array[Float])],
    config: HNSWConfig = HNSWConfig(),
    distanceType: String = "euclidean"
  ): Either[ApiError, LoadedIndexInfo] = {
    if (bundleIndexes.containsKey(indexId)) {
      return Left(ApiError.IndexAlreadyExists(indexId))
    }
    try {
      val maxElements = math.max(config.maxElements, vectors.size)
      val adjustedConfig = config.copy(maxElements = maxElements)
      val index = HNSWLibIndex(dimension, adjustedConfig, distanceType)
      index.addAll(vectors)

      val info = LoadedIndexInfo(
        indexId = indexId,
        index = index,
        indexPath = None,
        distanceType = distanceType
      )
      val existing = flatIndexes.putIfAbsent(indexId, info)
      if (existing != null) Left(ApiError.IndexAlreadyExists(indexId))
      else Right(info)
    } catch {
      case e: Exception =>
        Left(ApiError.InternalFailure(s"Failed to create index: ${e.getMessage}"))
    }
  }

  def getIndex(indexId: String): Option[LoadedIndexInfo] =
    Option(flatIndexes.get(indexId))

  def listIndexes(): Seq[LoadedIndexInfo] =
    flatIndexes.values().asScala.toSeq

  def unloadIndex(indexId: String): Boolean =
    flatIndexes.remove(indexId) != null || bundleIndexes.remove(indexId) != null

  def saveIndex(indexId: String, path: String): Either[ApiError, Unit] = {
    Option(flatIndexes.get(indexId)) match {
      case Some(info) =>
        try {
          info.index.save(path)
          val updated = info.copy(indexPath = Some(path))
          // Race-tolerant: only persist the path update if the entry
          // hasn't been swapped out from under us in the meantime.
          flatIndexes.replace(indexId, info, updated)
          Right(())
        } catch {
          case e: Exception => Left(ApiError.InternalFailure(s"Failed to save index: ${e.getMessage}"))
        }
      case None => Left(ApiError.IndexNotFound(indexId))
    }
  }

  def addVectors(indexId: String, vectors: Seq[(Long, Array[Float])]): Either[ApiError, LoadedIndexInfo] = {
    Option(flatIndexes.get(indexId)) match {
      case Some(info) =>
        try {
          info.index.addAll(vectors)
          Right(info)
        } catch {
          case e: Exception => Left(ApiError.InternalFailure(s"Failed to add vectors: ${e.getMessage}"))
        }
      case None => Left(ApiError.IndexNotFound(indexId))
    }
  }

  // ── Bundle API (pattern B) ────────────────────────────────────────────

  /**
   * Load a bundle from disk. Detects bundle-vs-flat by structure: the
   * given path must be a directory containing `ann_index.json`. Loads
   * the metadata, every local HNSW, the optional global routing HNSW,
   * and the boundary map.
   *
   * Errors:
   *  - IndexAlreadyExists when the indexId is taken on either side
   *  - BundleNotFound / InvalidBundle for structural problems
   *  - InternalFailure for I/O surprises
   */
  def loadBundle(indexId: String, bundlePath: String): Either[ApiError, LoadedBundleInfo] = {
    if (flatIndexes.containsKey(indexId) || bundleIndexes.containsKey(indexId)) {
      return Left(ApiError.IndexAlreadyExists(indexId))
    }
    val path = Paths.get(bundlePath)
    if (!BundleReader.isBundle(path)) {
      return Left(ApiError.BundleNotFound(bundlePath))
    }
    BundleReader.loadMetadata(path) match {
      case Left(BundleError.BundleNotFound(p))     => Left(ApiError.BundleNotFound(p))
      case Left(BundleError.InvalidBundle(p, why)) => Left(ApiError.InvalidBundle(p, why))
      case Left(BundleError.UnknownVersion(f, s))  =>
        Left(ApiError.InvalidBundle(bundlePath, s"unknown version $f (max supported $s)"))
      case Left(BundleError.UnknownAlgorithm(id))  =>
        Left(ApiError.InvalidBundle(bundlePath, s"unknown algorithm '$id'"))
      case Left(BundleError.IoFailure(p, m))       =>
        Left(ApiError.InternalFailure(s"I/O failure reading $p: $m"))
      case Right(metadata) =>
        try {
          val locals  = BundleReader.loadAllLocalIndexes(metadata)
          val global  = BundleReader.loadGlobalIndex(metadata)
          val mapping = BundleReader.loadBoundaryMap(path, metadata)
          val info = LoadedBundleInfo(
            indexId      = indexId,
            bundlePath   = bundlePath,
            metadata     = metadata,
            localIndexes = locals,
            globalIndex  = global,
            boundaryMap  = mapping
          )
          val existing = bundleIndexes.putIfAbsent(indexId, info)
          if (existing != null) Left(ApiError.IndexAlreadyExists(indexId))
          else Right(info)
        } catch {
          case e: Exception =>
            Left(ApiError.InternalFailure(s"Failed to load bundle: ${e.getMessage}"))
        }
    }
  }

  def getBundle(indexId: String): Option[LoadedBundleInfo] =
    Option(bundleIndexes.get(indexId))

  def listBundles(): Seq[LoadedBundleInfo] =
    bundleIndexes.values().asScala.toSeq

  // ── Unified lookup ────────────────────────────────────────────────────

  def getEntry(indexId: String): Option[IndexEntry] = {
    val flat = Option(flatIndexes.get(indexId)).map(IndexEntry.Flat)
    flat.orElse(Option(bundleIndexes.get(indexId)).map(IndexEntry.Bundle))
  }

  def listEntries(): Seq[IndexEntry] =
    listIndexes().map(IndexEntry.Flat) ++ listBundles().map(IndexEntry.Bundle)

  // ── Aggregates ───────────────────────────────────────────────────────

  def indexCount: Int = flatIndexes.size() + bundleIndexes.size()

  def totalVectors: Long = {
    val flatTotal: Long = flatIndexes.values().asScala.map(_.index.size.toLong).sum
    val bundleTotal: Long = bundleIndexes.values().asScala.map(_.totalVectors).sum
    flatTotal + bundleTotal
  }

  def exists(indexId: String): Boolean =
    flatIndexes.containsKey(indexId) || bundleIndexes.containsKey(indexId)
}

object IndexManager {
  def apply(): IndexManager = new IndexManager()
}
