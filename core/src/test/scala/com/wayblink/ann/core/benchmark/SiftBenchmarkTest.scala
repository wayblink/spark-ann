package com.wayblink.ann.core.benchmark

import com.wayblink.ann.core.index.{HNSWConfig, HNSWLibIndex}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Tag}

/**
 * Tag for benchmark tests that require datasets.
 * Run with: sbt "testOnly *SiftBenchmark* -- -n Benchmark"
 * Skip with: sbt "testOnly *SiftBenchmark* -- -l Benchmark"
 */
object Benchmark extends Tag("Benchmark")

/**
 * SIFT benchmark tests for HNSW index.
 *
 * Supports both SIFT1M (1M vectors) and SIFTSmall (10K vectors) datasets.
 * Default: SIFTSmall for fast dev testing.
 *
 * Configure dataset via environment variable:
 *   SIFT_DATASET=siftsmall sbt "core/testOnly *SiftBenchmarkTest"  (default)
 *   SIFT_DATASET=sift1m sbt "core/testOnly *SiftBenchmarkTest"     (full benchmark)
 *
 * Skip benchmarks:
 *   SKIP_BENCHMARK=true sbt "core/test"
 */
class SiftBenchmarkTest extends AnyFunSuite with BeforeAndAfterAll {

  private var dataset: SiftData = _
  private var datasetLoaded: Boolean = false
  private var variant: SiftVariant = _

  override def beforeAll(): Unit = {
    if (System.getenv("SKIP_BENCHMARK") == "true") {
      println("SKIP_BENCHMARK=true, skipping SIFT benchmark")
      return
    }

    // Get variant from environment (default: Sift10K for fast testing)
    variant = SiftVariant.fromEnv(default = SiftVariant.Sift10K)

    try {
      if (SiftDataset.isAvailable(variant)) {
        println(s"Loading ${variant.name} dataset...")
        dataset = SiftDataset.load(variant)
        datasetLoaded = true
      } else {
        println(s"${variant.name} dataset not found.")
        println(s"  Expected location: datasets/${variant.dirName}/")
        println(s"  Set SIFT_DATASET=siftsmall or SIFT_DATASET=sift1m")
        println(s"  Or set SKIP_BENCHMARK=true to skip")
      }
    } catch {
      case e: Exception =>
        println(s"Warning: Could not load dataset: ${e.getMessage}")
    }
  }

  private def requireDataset(): Unit = {
    if (!datasetLoaded) {
      cancel(
        s"""${variant.name} dataset not available.
           |Place dataset files in: datasets/${variant.dirName}/
           |Or set SKIP_BENCHMARK=true to skip.""".stripMargin
      )
    }
  }

  // ============== Recall Benchmarks ==============

  test("SIFT recall@1 should be >= 0.95 with high ef", Benchmark) {
    requireDataset()

    val config = HNSWConfig(M = 16, efConstruction = 200, maxElements = dataset.numBaseVectors + 1000)
    val index = HNSWLibIndex(dataset.dimension, config)

    println(s"Building index with ${dataset.numBaseVectors} vectors...")
    val buildStart = System.currentTimeMillis()
    index.addAll(dataset.baseVectors)
    val buildTime = System.currentTimeMillis() - buildStart
    println(f"Index built in ${buildTime / 1000.0}%.2f seconds")

    val numQueries = math.min(dataset.numQueries, 1000)
    val ef = 200
    val k = 1

    val recalls = (0 until numQueries).map { i =>
      val query = dataset.queryVectors(i)._2
      val results = index.search(query, k, ef)
      dataset.calculateRecall(i, results.map(_.id), k)
    }

    val avgRecall = recalls.sum / recalls.length
    println(f"${dataset.name} Recall@$k with ef=$ef: $avgRecall%.4f (tested $numQueries queries)")

    assert(avgRecall >= 0.95, s"Recall@1 should be >= 0.95, got $avgRecall")
  }

  test("SIFT recall@10 should be >= 0.90 with default ef", Benchmark) {
    requireDataset()

    val config = HNSWConfig(M = 16, efConstruction = 200, maxElements = dataset.numBaseVectors + 1000)
    val index = HNSWLibIndex(dataset.dimension, config)

    println(s"Building index with ${dataset.numBaseVectors} vectors...")
    index.addAll(dataset.baseVectors)

    val numQueries = math.min(dataset.numQueries, 1000)
    val ef = 50
    val k = 10

    val recalls = (0 until numQueries).map { i =>
      val query = dataset.queryVectors(i)._2
      val results = index.search(query, k, ef)
      dataset.calculateRecall(i, results.map(_.id), k)
    }

    val avgRecall = recalls.sum / recalls.length
    println(f"${dataset.name} Recall@$k with ef=$ef: $avgRecall%.4f (tested $numQueries queries)")

    assert(avgRecall >= 0.90, s"Recall@10 should be >= 0.90, got $avgRecall")
  }

  test("SIFT recall@100 should be >= 0.85", Benchmark) {
    requireDataset()

    val config = HNSWConfig(M = 16, efConstruction = 200, maxElements = dataset.numBaseVectors + 1000)
    val index = HNSWLibIndex(dataset.dimension, config)

    println(s"Building index with ${dataset.numBaseVectors} vectors...")
    index.addAll(dataset.baseVectors)

    val numQueries = math.min(dataset.numQueries, 500)
    val ef = 200
    val k = 100

    val recalls = (0 until numQueries).map { i =>
      val query = dataset.queryVectors(i)._2
      val results = index.search(query, k, ef)
      dataset.calculateRecall(i, results.map(_.id), k)
    }

    val avgRecall = recalls.sum / recalls.length
    println(f"${dataset.name} Recall@$k with ef=$ef: $avgRecall%.4f (tested $numQueries queries)")

    assert(avgRecall >= 0.85, s"Recall@100 should be >= 0.85, got $avgRecall")
  }

  // ============== Performance Benchmarks ==============

  test("SIFT query latency benchmark", Benchmark) {
    requireDataset()

    val config = HNSWConfig(M = 16, efConstruction = 200, maxElements = dataset.numBaseVectors + 1000)
    val index = HNSWLibIndex(dataset.dimension, config)

    println(s"Building index with ${dataset.numBaseVectors} vectors...")
    index.addAll(dataset.baseVectors)

    val numQueries = math.min(dataset.numQueries, 1000)
    val ef = 50
    val k = 10

    // Warmup
    (0 until math.min(100, numQueries)).foreach { i =>
      index.search(dataset.queryVectors(i)._2, k, ef)
    }

    // Benchmark
    val startTime = System.nanoTime()
    (0 until numQueries).foreach { i =>
      index.search(dataset.queryVectors(i % dataset.numQueries)._2, k, ef)
    }
    val totalTime = System.nanoTime() - startTime
    val avgLatencyMs = totalTime / numQueries / 1e6

    println(f"${dataset.name} Query latency: $avgLatencyMs%.3f ms/query (k=$k, ef=$ef)")
    println(f"Throughput: ${1000 / avgLatencyMs}%.0f queries/second")

    // Latency target depends on dataset size
    val maxLatency = if (dataset.numBaseVectors > 100000) 1.0 else 0.5
    assert(avgLatencyMs < maxLatency, s"Query latency should be < ${maxLatency}ms, got $avgLatencyMs ms")
  }

  test("SIFT index build throughput", Benchmark) {
    requireDataset()

    val config = HNSWConfig(M = 16, efConstruction = 100, maxElements = dataset.numBaseVectors + 1000)
    val index = HNSWLibIndex(dataset.dimension, config)

    val startTime = System.currentTimeMillis()
    index.addAll(dataset.baseVectors)
    val buildTime = System.currentTimeMillis() - startTime

    val vectorsPerSecond = dataset.numBaseVectors.toDouble / (buildTime / 1000.0)
    println(f"${dataset.name} Build time: ${buildTime / 1000.0}%.2f seconds")
    println(f"Build throughput: $vectorsPerSecond%.0f vectors/second")

    assert(vectorsPerSecond >= 5000, s"Build throughput should be >= 5K vectors/sec, got $vectorsPerSecond")
  }

  // ============== Parameter Sensitivity ==============

  test("SIFT recall vs ef tradeoff", Benchmark) {
    requireDataset()

    val config = HNSWConfig(M = 16, efConstruction = 200, maxElements = dataset.numBaseVectors + 1000)
    val index = HNSWLibIndex(dataset.dimension, config)

    println(s"Building index with ${dataset.numBaseVectors} vectors...")
    index.addAll(dataset.baseVectors)

    val numQueries = math.min(dataset.numQueries, 500)
    val k = 10
    val efValues = Seq(10, 20, 50, 100, 200)

    println(s"\n${dataset.name} Recall vs ef tradeoff:")
    println("-" * 50)

    efValues.foreach { ef =>
      val startTime = System.nanoTime()
      val recalls = (0 until numQueries).map { i =>
        val query = dataset.queryVectors(i)._2
        val results = index.search(query, k, ef)
        dataset.calculateRecall(i, results.map(_.id), k)
      }
      val totalTime = System.nanoTime() - startTime

      val avgRecall = recalls.sum / recalls.length
      val avgLatencyMs = totalTime / numQueries / 1e6

      println(f"ef=$ef%4d: Recall@$k = $avgRecall%.4f, Latency = $avgLatencyMs%.3f ms")
    }
  }

  private def euclideanDistance(a: Array[Float], b: Array[Float]): Float = {
    var sum = 0.0f
    var i = 0
    while (i < a.length) {
      val diff = a(i) - b(i)
      sum += diff * diff
      i += 1
    }
    math.sqrt(sum).toFloat
  }
}
