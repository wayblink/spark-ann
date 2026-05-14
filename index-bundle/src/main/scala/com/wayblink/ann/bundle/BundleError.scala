package com.wayblink.ann.bundle

/**
 * Typed errors surfaced by the bundle-layer reader and validators.
 *
 * Kept as a sealed trait so callers (api-server in particular) can
 * pattern-match exhaustively. Mapping to HTTP status codes lives in
 * the api-server layer, not here, because the bundle module must
 * stay HTTP-agnostic.
 */
sealed trait BundleError

object BundleError {

  /** The given path is not a directory or has no ann_index.json. */
  final case class BundleNotFound(path: String) extends BundleError

  /** Bundle exists but contents are structurally broken. */
  final case class InvalidBundle(path: String, reason: String) extends BundleError

  /** Bundle metadata declares a JSON envelope version newer than supported. */
  final case class UnknownVersion(found: Int, supported: Int) extends BundleError

  /** Bundle metadata names an algorithm this build does not implement. */
  final case class UnknownAlgorithm(id: String) extends BundleError

  /** Underlying I/O failure while reading bundle artefacts. */
  final case class IoFailure(path: String, message: String) extends BundleError
}
