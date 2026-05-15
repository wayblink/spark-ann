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
 */
class IndexManager {

  private val bundleIndexes = new ConcurrentHashMap[String, LoadedBundleInfo]()

  def loadBundle(indexId: String, bundlePath: String): Either[ApiError, LoadedBundleInfo] = {
    if (bundleIndexes.containsKey(indexId)) {
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

  def indexCount: Int = bundleIndexes.size()

  def totalVectors: Long = bundleIndexes.values().asScala.map(_.totalVectors).sum

  def exists(indexId: String): Boolean = bundleIndexes.containsKey(indexId)

  def unloadIndex(indexId: String): Boolean = bundleIndexes.remove(indexId) != null
}

object IndexManager {
  def apply(): IndexManager = new IndexManager()
}
