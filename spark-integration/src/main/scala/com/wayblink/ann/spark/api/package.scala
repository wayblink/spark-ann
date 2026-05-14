package com.wayblink.ann.spark

/**
 * Backward-compat re-exports for callers that imported
 * `com.wayblink.ann.spark.api.ANNIndexConfig` etc. before these types
 * moved to com.wayblink.ann.bundle. Scheduled for removal in 0.4.0.
 */
package object api {

  @deprecated("Moved to com.wayblink.ann.bundle.ANNIndexConfig", "0.3.0")
  type ANNIndexConfig = com.wayblink.ann.bundle.ANNIndexConfig
  @deprecated("Moved to com.wayblink.ann.bundle.ANNIndexConfig", "0.3.0")
  val ANNIndexConfig = com.wayblink.ann.bundle.ANNIndexConfig

  @deprecated("Moved to com.wayblink.ann.bundle.ANNIndexMetadata", "0.3.0")
  type ANNIndexMetadata = com.wayblink.ann.bundle.ANNIndexMetadata
  @deprecated("Moved to com.wayblink.ann.bundle.ANNIndexMetadata", "0.3.0")
  val ANNIndexMetadata = com.wayblink.ann.bundle.ANNIndexMetadata

  @deprecated("Moved to com.wayblink.ann.bundle.ANNIndexStatistics", "0.3.0")
  type ANNIndexStatistics = com.wayblink.ann.bundle.ANNIndexStatistics
  @deprecated("Moved to com.wayblink.ann.bundle.ANNIndexStatistics", "0.3.0")
  val ANNIndexStatistics = com.wayblink.ann.bundle.ANNIndexStatistics
}
