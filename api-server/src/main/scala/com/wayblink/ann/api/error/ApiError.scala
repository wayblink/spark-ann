package com.wayblink.ann.api.error

import akka.http.scaladsl.model.{StatusCode, StatusCodes}

/**
 * Typed error hierarchy for api-server service layers. Replaces the
 * legacy `Left[String]` pattern, where HTTP-status routing was done by
 * string-substring inspection (todo.md #6) — error messages that
 * happened to share a fragment with `not found` or `dimension` would
 * silently route the same way.
 *
 * The route layer maps these to HTTP status codes via
 * [[ApiError.toHttpStatus]]. The service layer never imports Akka.
 */
sealed trait ApiError {

  /** A short machine-readable code that ends up in ErrorResponse.error. */
  def code: String

  /** A human-readable explanation of what went wrong. */
  def message: String
}

object ApiError {

  /** No index/bundle with the given identifier is currently loaded. */
  final case class IndexNotFound(id: String) extends ApiError {
    val code = "index_not_found"
    val message = s"Index '$id' not found"
  }

  /** The bundle path does not point at a valid bundle directory. */
  final case class BundleNotFound(path: String) extends ApiError {
    val code = "bundle_not_found"
    val message = s"Bundle not found at '$path'"
  }

  /** The bundle exists but its contents are structurally broken. */
  final case class InvalidBundle(path: String, reason: String) extends ApiError {
    val code = "invalid_bundle"
    val message = s"Invalid bundle at '$path': $reason"
  }

  /** Query vector dimension does not match the index dimension. */
  final case class DimensionMismatch(expected: Int, actual: Int) extends ApiError {
    val code = "dimension_mismatch"
    val message = s"Query dimension $actual doesn't match index dimension $expected"
  }

  /** The request body failed schema or value validation. */
  final case class InvalidRequest(message: String) extends ApiError {
    val code = "invalid_request"
  }

  /** Multi-search was requested but no indexes are loaded. */
  case object NoIndexesAvailable extends ApiError {
    val code = "no_indexes_available"
    val message = "No indexes are currently loaded"
  }

  /** Caller attempted to load an indexId that is already taken. */
  final case class IndexAlreadyExists(id: String) extends ApiError {
    val code = "index_already_exists"
    val message = s"Index '$id' already exists"
  }

  /** Server has reached its configured maximum number of loaded bundles. */
  final case class CapacityExceeded(loaded: Int, max: Int) extends ApiError {
    val code = "capacity_exceeded"
    val message =
      s"Server already holds $loaded loaded indexes (max=$max); unload one before loading another"
  }

  /** Underlying search call raised an exception. */
  final case class SearchFailed(detail: String) extends ApiError {
    val code = "search_failed"
    val message = detail
  }

  /** Underlying I/O / library call raised an exception. */
  final case class InternalFailure(detail: String) extends ApiError {
    val code = "internal_failure"
    val message = detail
  }

  /**
   * HTTP status code for each error variant. Lives next to the ADT so
   * adding a new variant fails the route layer's compile if the
   * mapping isn't extended — pattern match exhaustiveness flags it.
   */
  def toHttpStatus(err: ApiError): StatusCode = err match {
    case _: IndexNotFound      => StatusCodes.NotFound
    case _: BundleNotFound     => StatusCodes.NotFound
    case _: InvalidBundle      => StatusCodes.BadRequest
    case _: DimensionMismatch  => StatusCodes.UnprocessableEntity
    case _: InvalidRequest     => StatusCodes.BadRequest
    case NoIndexesAvailable    => StatusCodes.BadRequest
    case _: IndexAlreadyExists => StatusCodes.Conflict
    case _: CapacityExceeded   => StatusCodes.ServiceUnavailable
    case _: SearchFailed       => StatusCodes.InternalServerError
    case _: InternalFailure    => StatusCodes.InternalServerError
  }
}
