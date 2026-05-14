package com.wayblink.ann.bundle

import com.wayblink.ann.core.index.HNSWConfig

/**
 * Pure data model for the on-disk ANN Index Bundle layout. None of
 * these classes touch Spark; they only describe what's written to the
 * `ann_index.json` / `boundary_mapping.json` files.
 *
 * Lives in the bundle module so api-server (and future C++ / Rust /
 * other-runtime readers) can consume bundles without dragging Spark or
 * spark-integration onto their classpath.
 */

// ─── Build grouping strategy ──────────────────────────────────────────
// These belong with the bundle metadata because they're referenced from
// ANNIndexConfig and persisted in ann_index.json. The runtime grouping
// implementation (FileGroupingStrategy object) stays in spark-integration
// — the trait + two case objects are pure data.

/**
 * Strategy used at build time to group input data files into local
 * indexes. Persisted in ANNIndexConfig.groupingStrategy.
 */
sealed trait GroupingStrategy extends Serializable

/** Each data file gets its own local index (1:1 mapping). */
case object SingleFile extends GroupingStrategy

/** Merge small files until each group reaches the target vector count. */
case object MergeSmall extends GroupingStrategy

// ─── Envelope and routing entry ──────────────────────────────────────

/**
 * Envelope wrapped around every metadata JSON file on disk. `version`
 * guards future schema migrations; readers reject unknown major versions.
 */
case class MetadataEnvelope[T](
  version: Int,
  `type`: String,
  payload: T
)

/**
 * Mapping entry persisted in boundary_mapping.json. Position in the JSON
 * array equals the global routing id, so lookups are O(1) once loaded.
 */
case class BoundaryMappingEntry(
  globalId: Int,
  indexId: String,
  localId: Long
)

// ─── Boundary node carried in-memory during build ────────────────────

/**
 * Boundary node selected from a local index for global routing.
 *
 * `globalId` is the combined "indexId:localId" string. Build-time only;
 * once persisted to boundary_mapping.json the `BoundaryMappingEntry`
 * shape (positional array) takes over.
 *
 * @param globalId    Unique id across all indexes (format: "indexId:localId")
 * @param indexId     ID of the source local index
 * @param localId     Vector ID within the local index
 * @param vector      The boundary node vector
 */
@SerialVersionUID(1L)
case class GlobalBoundaryNode(
  globalId: String,
  indexId: String,
  localId: Long,
  vector: Array[Float]
) extends Serializable

// ─── Data file entries ───────────────────────────────────────────────

/**
 * Entry describing a single data file inside a local index.
 *
 * @param filePath     Full path to the source parquet file
 * @param numVectors   Number of vectors contributed by this file
 * @param vectorOffset Starting HNSW internal id for vectors from this file
 *                     (sequential-id mode); when pk passthrough is enabled
 *                     this field is informational only.
 */
@SerialVersionUID(1L)
case class DataFileEntry(
  filePath: String,
  numVectors: Long,
  vectorOffset: Long
) extends Serializable

// ─── Local index metadata ────────────────────────────────────────────

/**
 * Metadata for a single local HNSW index inside a bundle.
 *
 * @param indexId      Unique identifier for this local index (also the
 *                     basename of its .hnsw file)
 * @param dataFiles    Source parquet files that produced this local index
 * @param indexPath    Absolute path to the .hnsw file
 * @param totalVectors Number of vectors indexed
 * @param dimension    Vector dimensionality
 */
@SerialVersionUID(1L)
case class LocalIndexMetadata(
  indexId: String,
  dataFiles: Array[DataFileEntry],
  indexPath: String,
  totalVectors: Long,
  dimension: Int
) extends Serializable {
  override def toString: String =
    s"LocalIndexMetadata($indexId, ${dataFiles.length} files, $totalVectors vectors, dim=$dimension)"
}

/**
 * Result of building one local index. Carries the metadata plus the
 * sampled boundary nodes that feed the global routing build.
 */
@SerialVersionUID(1L)
case class LocalIndexBuildResult(
  metadata: LocalIndexMetadata,
  boundaryNodes: Array[GlobalBoundaryNode]
) extends Serializable

// ─── Top-level config and metadata ───────────────────────────────────

/**
 * Configuration for building ANN indexes. Persisted as
 * ann_index.json/config; consumed by both the offline Spark builder
 * and online readers (the latter only care about distanceType, pk,
 * algorithm — the build-time knobs are informational at read time).
 *
 * @param M                      HNSW M parameter (connections per node)
 * @param efConstruction         HNSW efConstruction parameter
 * @param groupingStrategy       How to group data files at build time
 * @param targetVectorsPerIndex  Target per-local-index vector count for
 *                               MergeSmall grouping
 * @param boundaryNodesPerIndex  Routing samples per local index
 * @param distanceType           "euclidean" | "cosine"
 * @param pk                     Optional INT32/INT64 primary-key column.
 *                               When set, HNSW internal ids carry user pks.
 * @param algorithm              Index algorithm. Today only HNSW is
 *                               implemented; the field reserves the
 *                               extension point in the on-disk contract.
 */
@SerialVersionUID(4L)
case class ANNIndexConfig(
  M: Int = 16,
  efConstruction: Int = 200,
  groupingStrategy: GroupingStrategy = SingleFile,
  targetVectorsPerIndex: Long = 500000,
  boundaryNodesPerIndex: Int = 50,
  distanceType: String = "euclidean",
  pk: Option[String] = None,
  algorithm: IndexAlgorithm = IndexAlgorithm.HNSW
) extends Serializable {

  /**
   * Convert to the lower-level core HNSWConfig used by HNSWLibIndex.
   * Callers that know the exact element count should pass a tight
   * maxElements rather than the default; the default exists for
   * compatibility with the previous API signature.
   */
  def toHNSWConfig(maxElements: Int = 1000000): HNSWConfig =
    HNSWConfig(M = M, efConstruction = efConstruction, maxElements = maxElements)
}

/**
 * Build statistics captured at the end of a Spark build run.
 */
@SerialVersionUID(1L)
case class ANNIndexStatistics(
  totalVectors: Long,
  totalFiles: Int,
  numLocalIndexes: Int,
  dimension: Int,
  buildTimeMs: Long
) extends Serializable {
  override def toString: String =
    s"""ANNIndexStatistics(
       |  totalVectors: $totalVectors,
       |  totalFiles: $totalFiles,
       |  numLocalIndexes: $numLocalIndexes,
       |  dimension: $dimension,
       |  buildTimeMs: $buildTimeMs
       |)""".stripMargin
}

/**
 * Top-level metadata for a bundle: which local indexes exist, where the
 * (optional) global routing index lives, what config produced it, and
 * what was measured during the build.
 *
 * @param indexPath       Bundle root path
 * @param localIndexes    Per-local-index metadata
 * @param globalIndexPath Optional path to global routing .hnsw file
 * @param config          ANNIndexConfig the bundle was built with
 * @param statistics      Build statistics
 * @param createdAt       Build timestamp (epoch ms)
 */
@SerialVersionUID(1L)
case class ANNIndexMetadata(
  indexPath: String,
  localIndexes: Array[LocalIndexMetadata],
  globalIndexPath: Option[String],
  config: ANNIndexConfig,
  statistics: ANNIndexStatistics,
  createdAt: Long = System.currentTimeMillis()
) extends Serializable {

  def totalVectors: Long = statistics.totalVectors
  def dimension: Int = statistics.dimension

  override def toString: String =
    s"ANNIndexMetadata($indexPath, ${localIndexes.length} local indexes, ${statistics.totalVectors} vectors)"
}
