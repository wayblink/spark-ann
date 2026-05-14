package com.wayblink.ann.spark

/**
 * Backward-compat re-exports for callers that imported builder-package
 * types that have moved into com.wayblink.ann.bundle. Scheduled for
 * removal in 0.4.0.
 */
package object builder {

  // Grouping strategy (sealed trait + case objects) — persisted in
  // metadata, hence the move.
  @deprecated("Moved to com.wayblink.ann.bundle.GroupingStrategy", "0.3.0")
  type GroupingStrategy = com.wayblink.ann.bundle.GroupingStrategy
  @deprecated("Moved to com.wayblink.ann.bundle.SingleFile", "0.3.0")
  val SingleFile = com.wayblink.ann.bundle.SingleFile
  @deprecated("Moved to com.wayblink.ann.bundle.MergeSmall", "0.3.0")
  val MergeSmall = com.wayblink.ann.bundle.MergeSmall

  // Build-result and per-file data classes.
  @deprecated("Moved to com.wayblink.ann.bundle.DataFileEntry", "0.3.0")
  type DataFileEntry = com.wayblink.ann.bundle.DataFileEntry
  @deprecated("Moved to com.wayblink.ann.bundle.DataFileEntry", "0.3.0")
  val DataFileEntry = com.wayblink.ann.bundle.DataFileEntry

  @deprecated("Moved to com.wayblink.ann.bundle.LocalIndexMetadata", "0.3.0")
  type LocalIndexMetadata = com.wayblink.ann.bundle.LocalIndexMetadata
  @deprecated("Moved to com.wayblink.ann.bundle.LocalIndexMetadata", "0.3.0")
  val LocalIndexMetadata = com.wayblink.ann.bundle.LocalIndexMetadata

  @deprecated("Moved to com.wayblink.ann.bundle.LocalIndexBuildResult", "0.3.0")
  type LocalIndexBuildResult = com.wayblink.ann.bundle.LocalIndexBuildResult
  @deprecated("Moved to com.wayblink.ann.bundle.LocalIndexBuildResult", "0.3.0")
  val LocalIndexBuildResult = com.wayblink.ann.bundle.LocalIndexBuildResult

  @deprecated("Moved to com.wayblink.ann.bundle.GlobalBoundaryNode", "0.3.0")
  type GlobalBoundaryNode = com.wayblink.ann.bundle.GlobalBoundaryNode
  @deprecated("Moved to com.wayblink.ann.bundle.GlobalBoundaryNode", "0.3.0")
  val GlobalBoundaryNode = com.wayblink.ann.bundle.GlobalBoundaryNode

  // JSON helpers and envelope types.
  @deprecated("Moved to com.wayblink.ann.bundle.MetadataJson", "0.3.0")
  val MetadataJson = com.wayblink.ann.bundle.MetadataJson

  @deprecated("Moved to com.wayblink.ann.bundle.MetadataEnvelope", "0.3.0")
  type MetadataEnvelope[T] = com.wayblink.ann.bundle.MetadataEnvelope[T]
  @deprecated("Moved to com.wayblink.ann.bundle.MetadataEnvelope", "0.3.0")
  val MetadataEnvelope = com.wayblink.ann.bundle.MetadataEnvelope

  @deprecated("Moved to com.wayblink.ann.bundle.BoundaryMappingEntry", "0.3.0")
  type BoundaryMappingEntry = com.wayblink.ann.bundle.BoundaryMappingEntry
  @deprecated("Moved to com.wayblink.ann.bundle.BoundaryMappingEntry", "0.3.0")
  val BoundaryMappingEntry = com.wayblink.ann.bundle.BoundaryMappingEntry
}
