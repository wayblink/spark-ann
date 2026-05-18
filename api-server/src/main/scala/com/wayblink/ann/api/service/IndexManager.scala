package com.wayblink.ann.api.service

import com.wayblink.ann.api.error.ApiError
import com.wayblink.ann.bundle.{ANNIndexMetadata, BundleError, BundleReader}

import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * Metadata about a loaded bundle (pattern B). All local indexes are
 * eagerly loaded — the api-server is the canonical pattern-B online
 * server and is expected to hold its full bundle resident.
 */
case class LoadedBundleInfo(
  indexId: String,
  bundlePath: String,
  metadata: ANNIndexMetadata,
  localIndexes: Map[String, com.wayblink.ann.core.index.HNSWLibIndex],
  globalIndex: Option[com.wayblink.ann.core.index.HNSWLibIndex],
  boundaryMap: Array[String],
  loadedAt: Long = System.currentTimeMillis()
) {
  def distanceType: String = metadata.config.distanceType
  def algorithmId: String  = metadata.config.algorithm.id
  def dimension: Int       = metadata.dimension
  def totalVectors: Long   = metadata.totalVectors
}

/**
 * Bundle-only manager for in-memory indexes.
 *
 * @param maxLoadedIndexes Hard cap on concurrently loaded bundles. Once
 *   reached, further loadBundle calls return [[ApiError.CapacityExceeded]]
 *   until something is unloaded. Wired from `ann-service.index.max-loaded-indexes`
 *   in application.conf.
 */
class IndexManager(val maxLoadedIndexes: Int) {

  require(maxLoadedIndexes > 0, s"maxLoadedIndexes must be positive, got $maxLoadedIndexes")

  private val bundleIndexes = new ConcurrentHashMap[String, LoadedBundleInfo]()

  def loadBundle(indexId: String, bundlePath: String): Either[ApiError, LoadedBundleInfo] = {
    if (bundleIndexes.containsKey(indexId)) {
      return Left(ApiError.IndexAlreadyExists(indexId))
    }
    if (bundleIndexes.size() >= maxLoadedIndexes) {
      return Left(ApiError.CapacityExceeded(bundleIndexes.size(), maxLoadedIndexes))
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
          // Re-check capacity under the atomic put. Two concurrent callers
          // could both have passed the early check above; only one wins the
          // race for the last slot.
          if (bundleIndexes.size() >= maxLoadedIndexes) {
            Left(ApiError.CapacityExceeded(bundleIndexes.size(), maxLoadedIndexes))
          } else {
            val existing = bundleIndexes.putIfAbsent(indexId, info)
            if (existing != null) Left(ApiError.IndexAlreadyExists(indexId))
            else Right(info)
          }
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

  def indexCount: Int = bundleIndexes.size()

  def totalVectors: Long = bundleIndexes.values().asScala.map(_.totalVectors).sum

  def exists(indexId: String): Boolean = bundleIndexes.containsKey(indexId)

  def unloadIndex(indexId: String): Boolean = bundleIndexes.remove(indexId) != null
}

object IndexManager {
  /** Fallback when no config value is supplied. Matches application.conf. */
  val DefaultMaxLoadedIndexes: Int = 10

  def apply(): IndexManager = new IndexManager(DefaultMaxLoadedIndexes)

  def apply(maxLoadedIndexes: Int): IndexManager = new IndexManager(maxLoadedIndexes)
}
