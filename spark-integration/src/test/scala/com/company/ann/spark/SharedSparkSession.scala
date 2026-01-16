package com.company.ann.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite}

import java.io.File

/**
 * Trait providing a shared SparkSession for tests.
 * Mix this into test classes to get access to a single SparkSession
 * that is shared across all tests and properly cleaned up.
 */
trait SharedSparkSession extends BeforeAndAfterAll { self: Suite =>

  @transient private var _spark: SparkSession = _

  protected def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (_spark == null) {
      _spark = SparkSession.builder()
        .master("local[4]")
        .appName(getClass.getSimpleName)
        .config("spark.driver.memory", "2g")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    }
  }

  override def afterAll(): Unit = {
    try {
      if (_spark != null) {
        _spark.stop()
        _spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  protected def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      Option(file.listFiles()).foreach(_.foreach(deleteRecursively))
    }
    file.delete()
  }
}
