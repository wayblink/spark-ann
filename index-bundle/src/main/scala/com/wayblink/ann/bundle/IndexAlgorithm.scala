package com.wayblink.ann.bundle

/**
 * Index algorithm identifiers carried in bundle metadata.
 *
 * Today HNSW is the only implementation. The sealed trait + case-object
 * shape reserves the extension point: future algorithms (IVF, ScaNN,
 * HNSW+PQ, ...) become additional case objects, and the on-disk
 * `algorithm` field gains additional string values. Readers MUST reject
 * unknown ids — see BUNDLE_SPEC.md.
 *
 * The `id` string is what gets persisted, not the Scala class name, so
 * cross-language readers (a future C++ / Rust server) only need to
 * recognise the string set, not the Scala ADT.
 */
sealed trait IndexAlgorithm {
  def id: String
}

object IndexAlgorithm {

  case object HNSW extends IndexAlgorithm {
    val id: String = "hnsw"
  }

  /** All algorithms known to this build. */
  val All: Seq[IndexAlgorithm] = Seq(HNSW)

  /** Lookup by on-disk id string. None for unknown ids. */
  def fromId(idValue: String): Option[IndexAlgorithm] =
    All.find(_.id == idValue)

  /**
   * Parse with a typed error for unknown ids, suitable for use by
   * readers that want to raise rather than silently fall back.
   */
  def parse(idValue: String): Either[BundleError.UnknownAlgorithm, IndexAlgorithm] =
    fromId(idValue).toRight(BundleError.UnknownAlgorithm(idValue))
}
