# Spark ANN Index

A distributed Approximate Nearest Neighbor (ANN) index system built on Apache Spark.

## Prerequisites

- Java 8 or 11 (JDK)
- sbt 1.9.x
- Apache Spark 3.5.0 (for running tests)

### macOS Installation

```bash
# Install Java
brew install openjdk@11

# Install sbt
brew install sbt
```

## Project Structure

```
spark-ann/
├── build.sbt                    # Build configuration
├── core/                        # Core algorithms (no Spark dependency)
│   └── src/main/scala/
│       ├── index/               # HNSW implementation
│       ├── storage/             # Storage layer
│       ├── metadata/            # Metadata management
│       └── testutil/            # Test data generators
├── spark-integration/           # Spark integration
│   └── src/main/scala/
│       ├── api/                 # DataFrame API
│       ├── rdd/                 # RDD operations
│       ├── builder/             # Index construction
│       └── testutil/            # Spark test utilities
├── spark-sql-extension/         # SQL extension (Phase 2)
│   └── src/main/scala/
│       ├── optimizer/           # Optimizer rules
│       ├── execution/           # Physical operators
│       └── expressions/         # Built-in functions
└── native/                      # Native acceleration (Phase 3)
    ├── cpp/
    └── jni/
```

## Building

```bash
# Compile all modules
sbt compile

# Run tests
sbt test

# Run only core module tests
sbt core/test

# Run only spark-integration tests
sbt sparkIntegration/test

# Package JAR files
sbt package
```

## Benchmarks

### SIFT Benchmarks

The project includes benchmarks using SIFT datasets (128-dimensional vectors).

#### Supported Datasets

| Dataset | Base Vectors | Queries | Size | Use Case |
|---------|-------------|---------|------|----------|
| **SIFT10K** | 10K | 100 | ~5 MB | Fast dev testing (default) |
| **SIFT1M** | 1M | 10K | ~500 MB | Full benchmark |

#### Setup

Place dataset files in `datasets/<variant>/`:
```
datasets/
├── sift10k/
│   ├── sift10k_base.fvecs
│   ├── sift10k_query.fvecs
│   └── sift10k_groundtruth.ivecs
└── sift1m/
    ├── sift_base.fvecs
    ├── sift_query.fvecs
    └── sift_groundtruth.ivecs
```

Dataset source: http://corpus-texmex.irisa.fr/

#### Running Benchmarks

```bash
# Run with SIFT10K (default, fast)
sbt "core/testOnly *SiftBenchmarkTest"

# Run with SIFT1M (full benchmark)
SIFT_DATASET=sift1m sbt "core/testOnly *SiftBenchmarkTest"

# Run only dataset parser tests (no dataset required)
sbt "core/testOnly *SiftDatasetTest"

# Skip benchmarks (e.g., in CI)
SKIP_BENCHMARK=true sbt "core/test"
```

#### Benchmark Tests

| Test | Description | Target |
|------|-------------|--------|
| recall@1 | Top-1 accuracy with ef=200 | >= 95% |
| recall@10 | Top-10 accuracy with ef=50 | >= 90% |
| recall@100 | Top-100 accuracy with ef=200 | >= 85% |
| query latency | Average query time | < 1ms (1M) / < 0.5ms (10K) |
| build throughput | Index construction speed | >= 5K vectors/sec |
| recall vs ef | Tradeoff analysis | - |

## Quick Start

```scala
import com.company.ann.spark.api._
import com.company.ann.spark.testutil.SparkTestData

val spark = SparkSession.builder()
  .appName("ANN Quick Start")
  .master("local[4]")
  .getOrCreate()

// Generate test data
val vectors = SparkTestData.generateAndSave(
  spark,
  numVectors = 10000,
  dimension = 128,
  path = "/tmp/vectors",
  dataType = "clustered"
)

// Build index (coming in Week 2+)
// val metadata = ANNIndexAPI.buildIndex(
//   df = vectors,
//   vectorColumn = "vector",
//   outputPath = "/tmp/ann_index"
// )

// Query (coming in Week 2+)
// val results = vectors.annSearch(
//   indexPath = "/tmp/ann_index",
//   queryVector = queryVector,
//   k = 10
// )
```

## Implementation Progress

### Phase 1: Core Library (Week 1-4)
- [x] Day 1: Project structure and test data generators
- [x] Day 2-3: HNSW algorithm wrapper (HNSWLibIndex with hnswlib-core)
- [ ] Day 4-5: Spark Local Index builder
- [ ] Day 6: Boundary node selection
- [ ] Day 7-8: DataFrame API
- [ ] Week 4: Metadata and end-to-end integration

### Phase 2: SQL Extension (Week 5-8)
- [ ] Custom optimizer rules
- [ ] Query plan optimization
- [ ] Iceberg integration

### Phase 3: Native Acceleration (Week 9-12, Optional)
- [ ] JNI interface
- [ ] SIMD optimization
- [ ] Zero-copy transfer

## License

Proprietary - Internal Use Only
