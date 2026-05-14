package com.wayblink.ann.bundle

import com.wayblink.ann.core.index.HNSWLibIndex

import java.nio.file.{Files, Path, Paths}

/**
 * Runtime-agnostic bundle reader. Hosts everything an online server
 * (or any non-Spark consumer) needs to load and query a bundle:
 *  - detect whether a path is a bundle directory
 *  - parse ann_index.json into ANNIndexMetadata
 *  - parse boundary_mapping.json into a positional Array[String]
 *  - eagerly load all local HNSW indexes referenced by the metadata
 *  - eagerly load the optional global routing HNSW
 *
 * Lazy-loading and caching are runtime-specific concerns — Spark's
 * per-executor cache and the api-server's bundle-resident eager load
 * each layer their own policy on top of these primitive operations.
 */
object BundleReader {

  /** Filename of the bundle's top-level metadata document. */
  val MetadataFileName: String = "ann_index.json"

  /** Subdirectory containing the routing index and boundary map. */
  val GlobalDirName: String = "global"

  /** Filename of the global routing HNSW index. */
  val GlobalIndexFileName: String = "global_routing.hnsw"

  /** Filename of the boundary-id → local-indexId mapping document. */
  val BoundaryMappingFileName: String = "boundary_mapping.json"

  /**
   * Quick structural check: is this directory a bundle?
   *
   * True iff the path is a directory and contains an ann_index.json
   * file at its root. Does NOT validate the JSON content — use
   * loadMetadata for that.
   */
  def isBundle(path: Path): Boolean = {
    Files.isDirectory(path) && Files.exists(path.resolve(MetadataFileName))
  }

  /** Convenience overload accepting a String path. */
  def isBundle(path: String): Boolean = isBundle(Paths.get(path))

  /**
   * Load and validate the top-level metadata document.
   *
   * Returns Left when the bundle root is missing, when ann_index.json
   * is absent, or when the envelope/version checks inside MetadataJson
   * raise.
   */
  def loadMetadata(bundlePath: Path): Either[BundleError, ANNIndexMetadata] = {
    if (!Files.isDirectory(bundlePath)) {
      return Left(BundleError.BundleNotFound(bundlePath.toString))
    }
    val metadataPath = bundlePath.resolve(MetadataFileName)
    if (!Files.exists(metadataPath)) {
      return Left(BundleError.InvalidBundle(
        bundlePath.toString,
        s"missing $MetadataFileName at bundle root"
      ))
    }
    try {
      Right(MetadataJson.readMetadata(metadataPath))
    } catch {
      case e: IllegalStateException =>
        // MetadataJson raises IllegalStateException for envelope
        // version / type mismatches — surface them as typed bundle
        // errors rather than letting the exception escape.
        Left(BundleError.InvalidBundle(bundlePath.toString, e.getMessage))
      case e: Throwable =>
        Left(BundleError.IoFailure(metadataPath.toString, e.getMessage))
    }
  }

  /**
   * Load the boundary-id → indexId positional array. Returns an empty
   * array when the bundle has no global routing index (single-local
   * bundles skip the global step at build time).
   */
  def loadBoundaryMap(
    bundlePath: Path,
    metadata: ANNIndexMetadata
  ): Array[String] = {
    if (metadata.globalIndexPath.isEmpty) return Array.empty[String]

    val mappingPath = bundlePath.resolve(GlobalDirName).resolve(BoundaryMappingFileName)
    if (!Files.exists(mappingPath)) return Array.empty[String]

    val entries = MetadataJson.readBoundaryMapping(mappingPath)
    val arr = new Array[String](entries.length)
    var i = 0
    while (i < entries.length) {
      val e = entries(i)
      arr(e.globalId) = e.indexId
      i += 1
    }
    arr
  }

  /**
   * Eagerly load every local HNSW index referenced by the metadata.
   * Used by the api-server pattern-B path where keeping all local
   * indexes resident is the whole point of online serving.
   *
   * For the offline Spark path, prefer the executor-side
   * ExecutorIndexCache instead — this method would pull every local
   * onto a single JVM, which defeats distribution.
   */
  def loadAllLocalIndexes(metadata: ANNIndexMetadata): Map[String, HNSWLibIndex] = {
    metadata.localIndexes.map { lm =>
      lm.indexId -> HNSWLibIndex.load(lm.indexPath)
    }.toMap
  }

  /** Load the optional global routing index. */
  def loadGlobalIndex(metadata: ANNIndexMetadata): Option[HNSWLibIndex] =
    metadata.globalIndexPath.map(HNSWLibIndex.load)
}
