# Spark ANN Index

A distributed Approximate Nearest Neighbor (ANN) index system built on Apache Spark, using the HNSW algorithm for efficient vector similarity search.

> **新用户 / New users**: read [`docs/GETSTART.md`](docs/GETSTART.md) first — 10-minute walkthrough covering Scala, PySpark, and Docker paths.

## Overview

Spark ANN provides scalable vector search with:
- **Hierarchical HNSW Indexing**: Local indexes + global routing for distributed search
- **DataFrame API**: Native Spark integration with implicit extensions
- **REST API Server**: Production-ready HTTP endpoints for index management and search
- **Web UI**: React-based dashboard for index management and search operations
- **Docker Support**: Ready-to-use containerized deployment
- **File-Based Building**: Efficiently handles large datasets across distributed file systems

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    REST API Server                          │
│           (Akka HTTP - Search & Index Management)           │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│                  Spark Integration Layer                    │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  DataFrame API: buildANNIndex(), annSearch()          │  │
│  └───────────────────────────────────────────────────────┘  │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Builder: FileDiscovery → FileGrouping → IndexBuilder │  │
│  │  Search: ANNSearcher (routing + multi-index merge)    │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              Core Module (No Spark Dependency)              │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  HNSWLibIndex (hnswlib-core wrapper)                  │  │
│  │  → C++ HNSW via JNI for high performance              │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Prerequisites

### For Development
- Java 11 or 17 (JDK)
- sbt 1.9.x
- Apache Spark 3.5.0 (for running tests)
- Node.js 20+ (for Web UI development)

### For Docker Deployment
- Docker 20.10+
- Docker Compose v2+

## Quick Start

### Building an Index

```scala
import com.wayblink.ann.spark.api._
import com.wayblink.ann.spark.api.ANNDataFrameExtensions._

val spark = SparkSession.builder()
  .appName("ANN Quick Start")
  .master("local[4]")
  .getOrCreate()

import spark.implicits._

// Create sample data
val vectors = Seq(
  (1L, Seq(0.1f, 0.2f, 0.3f, 0.4f)),
  (2L, Seq(0.2f, 0.3f, 0.4f, 0.5f)),
  (3L, Seq(0.3f, 0.4f, 0.5f, 0.6f))
).toDF("id", "vector")

// Build index using DataFrame extension
val metadata = vectors.buildANNIndex(
  vectorColumn = "vector",
  outputPath = "/tmp/ann_index"
)
println(s"Built index with ${metadata.totalVectors} vectors")
```

### Searching

```scala
// Single query search
val queryVector = Array(0.15f, 0.25f, 0.35f, 0.45f)
val results = vectors.annSearch(
  indexPath = "/tmp/ann_index",
  queryVector = queryVector,
  k = 3
)
results.show()
// +---+----------+--------+
// | id|  distance| indexId|
// +---+----------+--------+
// |  1|     0.012| local_0|
// |  2|     0.023| local_0|
// |  3|     0.056| local_0|
// +---+----------+--------+

// Batch search
val queriesDF = Seq(
  (0, Seq(0.1f, 0.2f, 0.3f, 0.4f)),
  (1, Seq(0.5f, 0.6f, 0.7f, 0.8f))
).toDF("queryId", "vector")

val batchResults = ANNIndexAPI.batchSearch(
  spark = spark,
  indexPath = "/tmp/ann_index",
  queries = queriesDF,
  queryVectorColumn = "vector",
  k = 5
)
```

## Project Structure

```
spark-ann/
├── build.sbt                          # Build configuration
├── docker-compose.yml                 # Docker orchestration
├── core/                              # Core algorithms (no Spark dependency)
│   └── src/main/scala/
│       └── com/company/ann/core/
│           ├── index/                 # HNSW implementation
│           │   ├── HNSWIndex.scala    # Abstract trait
│           │   ├── HNSWLibIndex.scala # hnswlib-core wrapper
│           │   └── HNSWConfig.scala   # Configuration
│           └── testutil/              # Test data generators
├── spark-integration/                 # Spark integration
│   └── src/main/scala/
│       └── com/company/ann/spark/
│           ├── api/                   # DataFrame API
│           │   ├── ANNDataFrameAPI.scala    # Extensions + static API
│           │   └── ANNIndexConfig.scala     # Config & metadata classes
│           ├── builder/               # Index construction
│           │   ├── ANNIndexBuilder.scala    # Main orchestrator
│           │   ├── LocalIndexBuilder.scala  # Per-partition builder
│           │   ├── FileDiscovery.scala      # Parquet file discovery
│           │   └── FileGroupingStrategy.scala # File grouping strategies
│           ├── search/                # Query execution
│           │   └── ANNSearcher.scala        # Routing + result merging
│           └── examples/              # Example code
│               └── QuickStart.scala
├── api-server/                        # REST API server
│   ├── Dockerfile                     # Docker build for API server
│   └── src/main/scala/
│       └── com/company/ann/api/
│           ├── AnnApiServer.scala     # Main entry point
│           ├── model/                 # Request/response DTOs
│           ├── routes/                # HTTP route handlers
│           └── service/               # Business logic & Swagger
├── web-ui/                            # React Web UI
│   ├── Dockerfile                     # Docker build for UI
│   ├── nginx.conf                     # Nginx config for SPA + API proxy
│   └── src/
│       ├── api/                       # API client & React Query hooks
│       ├── components/                # React components
│       ├── pages/                     # Page components
│       └── types/                     # TypeScript types
├── spark-sql-extension/               # SQL extension (Phase 2)
└── native/                            # Native acceleration (Phase 3)
```

## Building

```bash
# Compile all modules
sbt compile

# Run tests
sbt test                    # All tests
sbt core/test               # Core module only
sbt sparkIntegration/test   # Spark integration only

# Package JAR files
sbt package
```

This creates:
```
core/target/scala-2.12/spark-ann-core_2.12-0.1.0-SNAPSHOT.jar
spark-integration/target/scala-2.12/spark-ann-integration_2.12-0.1.0-SNAPSHOT.jar
```

### Building Fat JARs

Build fat JARs with all dependencies bundled:

```bash
# Spark integration (for use with spark-submit/spark-shell)
sbt sparkIntegration/assembly
# Output: spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar

# API server (standalone executable)
sbt apiServer/assembly
# Output: api-server/target/scala-2.12/spark-ann-api-server-assembly.jar
```

## Using with Spark

### spark-shell

```bash
# Using fat JAR (recommended)
spark-shell --jars spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar

# Or using individual JARs
spark-shell \
  --jars core/target/scala-2.12/spark-ann-core_2.12-0.1.0-SNAPSHOT.jar,spark-integration/target/scala-2.12/spark-ann-integration_2.12-0.1.0-SNAPSHOT.jar
```

Then in the shell:
```scala
import com.wayblink.ann.spark.api._
import com.wayblink.ann.spark.api.ANNDataFrameExtensions._

val vectors = Seq(
  (1L, Seq(0.1f, 0.2f, 0.3f, 0.4f)),
  (2L, Seq(0.2f, 0.3f, 0.4f, 0.5f))
).toDF("id", "vector")

// Build index
val metadata = vectors.buildANNIndex("vector", "/tmp/ann_index")

// Search
val results = vectors.annSearch("/tmp/ann_index", Array(0.15f, 0.25f, 0.35f, 0.45f), k = 2)
results.show()
```

### spark-submit

```bash
# Using fat JAR
spark-submit \
  --class com.wayblink.ann.spark.examples.QuickStart \
  --master local[4] \
  spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar
```

### Cluster Deployment (YARN/Kubernetes)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class your.app.MainClass \
  spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar
```

### As a Project Dependency

Publish locally first:
```bash
sbt publishLocal
```

Then add to your `build.sbt`:
```scala
libraryDependencies ++= Seq(
  "com.wayblink" %% "spark-ann-core" % "0.1.0-SNAPSHOT",
  "com.wayblink" %% "spark-ann-integration" % "0.1.0-SNAPSHOT"
)
```

## DataFrame API Reference

### Implicit Extensions

Import to add ANN methods directly to DataFrames:

```scala
import com.wayblink.ann.spark.api.ANNDataFrameExtensions._

df.buildANNIndex(vectorColumn, outputPath)
df.annSearch(indexPath, queryVector, k)
df.annBatchSearch(indexPath, queries, queryVectorColumn, k)
```

### Static API

Use `ANNIndexAPI` for explicit operations:

```scala
import com.wayblink.ann.spark.api.ANNIndexAPI

ANNIndexAPI.buildIndex(df, vectorColumn, outputPath, config)
ANNIndexAPI.search(spark, indexPath, queryVector, k)
ANNIndexAPI.batchSearch(spark, indexPath, queries, queryVectorColumn, k)
ANNIndexAPI.loadSearcher(spark, indexPath)
ANNIndexAPI.discoverDataFiles(spark, dataPath, vectorColumn)
ANNIndexAPI.groupFiles(files, strategy)
```

### Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `M` | Int | 16 | HNSW connections per node (higher = better recall, more memory) |
| `efConstruction` | Int | 200 | Build-time accuracy (higher = better quality, slower build) |
| `distanceType` | String | "euclidean" | Distance metric: "euclidean" or "cosine" |
| `targetVectorsPerIndex` | Long | 500000 | Target vectors per local index |
| `boundaryNodesPerIndex` | Int | 50 | Boundary nodes for global routing |
| `groupingStrategy` | Strategy | SingleFile | File grouping: `SingleFile` or `MergeSmall` |

### Search Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `k` | Int | required | Number of neighbors to return |
| `ef` | Int | 50 | Search accuracy (higher = more accurate, slower) |
| `nprobe` | Int | 3 | Number of local indexes to probe |

### File Grouping Strategies

| Strategy | Description | Use Case |
|----------|-------------|----------|
| `SingleFile` | One index per data file | When files are naturally partitioned |
| `MergeSmall` | Merge small files to target size | Mixed file sizes |

### Advanced: File-Based Index Building

For large datasets with more control over file grouping:

```scala
import com.wayblink.ann.spark.builder.{FileDiscovery, FileGroupingStrategy, MergeSmall}

// Discover data files
val dataFiles = FileDiscovery.discoverDataFiles(spark, "/data/vectors", "vector")

// Group files (merge small files to ~500K vectors each)
val fileGroups = FileGroupingStrategy.groupFiles(dataFiles, MergeSmall, 500000)

// Build index from file groups
val metadata = ANNIndexAPI.buildIndexFromFileGroups(
  spark = spark,
  fileGroups = fileGroups,
  vectorColumn = "vector",
  outputPath = "/tmp/ann_index"
)
```

## PySpark

A Python wrapper is available as the `pyspark-ann` PyPI package. It is a
thin Py4J bridge over the Scala API — no logic duplicated in Python, all
distributed work runs on the JVM.

### Install

```bash
pip install pyspark-ann
```

The JVM JAR is **not** bundled in the wheel — pass it to Spark via
`--jars` or `--packages`:

```bash
pyspark --jars spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar
# or once published:
pyspark --packages com.wayblink:spark-ann-integration_2.12:0.1.0
```

### Functional API

```python
import spark_ann
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[4]").getOrCreate()

df = spark.createDataFrame(
    [(i, [0.1 * i] * 128) for i in range(10_000)],
    ["id", "vector"],
)

meta = spark_ann.build_ann_index(
    df, vector_column="vector", output_path="/tmp/idx",
    config={"M": 16, "ef_construction": 200, "distance_type": "cosine"},
)

results = spark_ann.ann_search(spark, "/tmp/idx", [0.5] * 128, k=10)
results.show()
```

### DataFrame methods (P2)

```python
import spark_ann  # installs methods on pyspark.sql.DataFrame

# Flat:
df.build_ann_index("vector", "/tmp/idx")
df.ann_search("/tmp/idx", [0.5] * 128, k=10)

# Accessor namespace:
df.ann.build_index("vector", "/tmp/idx")
df.ann.search("/tmp/idx", [0.5] * 128, k=10)
```

See [python/README.md](python/README.md) for full documentation and
configuration reference.

## REST API Server

> **Pattern-B online serving** is now supported: the api-server can
> load bundle directories produced by the offline Spark builder
> directly, no Spark needed at serve time. The on-disk contract is
> documented in [`docs/BUNDLE_SPEC.md`](docs/BUNDLE_SPEC.md).
> Section [Online Serving (Pattern B)](#online-serving-pattern-b)
> below has a runnable walkthrough.

### Starting the Server

```bash
# Using sbt
sbt apiServer/run

# Using fat JAR
java -jar api-server/target/scala-2.12/spark-ann-api-server-assembly.jar

# With custom port
ANN_SERVER_PORT=9090 java -jar api-server/target/scala-2.12/spark-ann-api-server-assembly.jar
```

The server starts at `http://localhost:8080` by default.

### Configuration

| Environment Variable | Description | Default |
|---------------------|-------------|---------|
| `ANN_SERVER_HOST` | Bind host | `0.0.0.0` |
| `ANN_SERVER_PORT` | Bind port | `8080` |
| `ANN_INDEX_PATH` | Index file base path | `/data/indexes` |

### API Endpoints

#### Health & Status

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Health status with statistics |
| GET | `/api/v1/health/ready` | Readiness probe |
| GET | `/api/v1/health/live` | Liveness probe |

#### Search Operations

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/indexes/{indexId}/search` | Search single index |
| POST | `/api/v1/search` | Multi-index search with merging |
| POST | `/api/v1/search/batch` | Batch search |

#### Index Management

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/indexes` | List loaded bundles |
| GET | `/api/v1/indexes/{indexId}` | Get bundle details |
| POST | `/api/v1/indexes/bundle` | Load a bundle from disk |
| DELETE | `/api/v1/indexes/{indexId}` | Unload bundle |

### Usage Examples

```bash
# Health check
curl http://localhost:8080/api/v1/health

# Create an index
curl -X POST http://localhost:8080/api/v1/indexes/bundle \
  -H "Content-Type: application/json" \
  -d '{
    "indexId": "products",
    "bundlePath": "/data/bundles/products-2026-05-14"
  }'

# Search for nearest neighbors
curl -X POST http://localhost:8080/api/v1/indexes/products/search \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.15, 0.25, 0.35, 0.45], "k": 5, "ef": 100}'

# Batch search
curl -X POST http://localhost:8080/api/v1/search/batch \
  -H "Content-Type: application/json" \
  -d '{
    "indexId": "products",
    "queries": [
      {"vector": [0.1, 0.2, 0.3, 0.4], "k": 5},
      {"vector": [0.5, 0.6, 0.7, 0.8], "k": 3}
    ]
  }'
```

### Online Serving (Pattern B)

The api-server can serve bundles produced by the offline Spark builder
without re-importing or re-indexing data. This is the "pattern B"
deployment shape: shared on-disk contract, independent runtimes.

**Step 1 — build a bundle offline:**

```scala
import com.wayblink.ann.spark.api._
import com.wayblink.ann.bundle.ANNIndexConfig

val metadata = vectors.buildANNIndex(
  vectorColumn = "embedding",
  outputPath   = "/data/bundles/products-2026-05-14",
  config       = ANNIndexConfig(pk = Some("product_id"))
)
```

The output directory matches the layout in `docs/BUNDLE_SPEC.md` —
`ann_index.json`, `local/*.hnsw`, `global/global_routing.hnsw`,
`boundary_mapping.json`.

**Step 2 — load the bundle into a running api-server:**

```bash
curl -X POST http://localhost:8080/api/v1/indexes/bundle \
  -H "Content-Type: application/json" \
  -d '{
    "indexId": "products",
    "bundlePath": "/data/bundles/products-2026-05-14"
  }'
```

The server eagerly loads every local HNSW + the global routing index,
exposes the bundle as the only supported server-side index model, and
becomes ready for queries.

**Step 3 — search through the bundle:**

```bash
curl -X POST http://localhost:8080/api/v1/indexes/products/search \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.15, 0.25, ...], "k": 10}'
```

The api-server runs the same routing logic the offline Spark
batchSearch uses (via the shared `index-bundle` module). When `pk` was
set at build time, the `id` field in each result IS the user's product
id — no offset translation needed.

**List loaded bundles:**

```bash
curl http://localhost:8080/api/v1/indexes
```

Each entry carries a `kind` field (`"bundle"`) so clients can branch cleanly.

**Implementation pointers**

- Reference reader: `index-bundle/src/main/scala/com/wayblink/ann/bundle/BundleReader.scala`
- Routing function: `index-bundle/src/main/scala/com/wayblink/ann/bundle/Routing.scala`
- Spec contract: [`docs/BUNDLE_SPEC.md`](docs/BUNDLE_SPEC.md)
- Any future C++ / Rust / GPU server can replace this api-server by
  implementing the same contract.

## Docker Deployment

The project includes Docker support for easy deployment of both the API server and Web UI.

### Quick Start with Docker Compose

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

This starts:
- **API Server**: http://localhost:8080
- **Web UI**: http://localhost:80

### Services

| Service | Port | Description |
|---------|------|-------------|
| `api-server` | 8080 | REST API server for vector search operations |
| `web-ui` | 80 | React-based web dashboard |

### Building Individual Images

```bash
# Build API server image
docker build -t spark-ann-api-server -f api-server/Dockerfile .

# Build Web UI image
docker build -t spark-ann-web-ui ./web-ui
```

### Running API Server Standalone

```bash
docker run -d \
  --name spark-ann-api \
  -p 8080:8080 \
  -v index-data:/data/indexes \
  -e JAVA_OPTS="-Xmx2g -Xms512m" \
  spark-ann-api-server
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `JAVA_OPTS` | `-Xmx2g -Xms512m` | JVM options |
| `ANN_HOST` | `0.0.0.0` | API server bind host |
| `ANN_PORT` | `8080` | API server bind port |

### Persistent Storage

The `index-data` volume stores index files. Mount a host directory for persistence:

```bash
docker run -d \
  -v /path/to/indexes:/data/indexes \
  spark-ann-api-server
```

## Web UI

A React-based web interface for managing indexes and performing searches.

### Features

- **Dashboard**: Overview of loaded indexes and system health
- **Index Management**: Create, load, delete, and inspect indexes
- **Search Interface**: Single, multi-index, and batch search operations
- **Theme Support**: Light and dark mode

### Development

```bash
cd web-ui

# Install dependencies
npm install

# Start development server (connects to API at localhost:8080)
npm run dev

# Build for production
npm run build
```

### Tech Stack

- React 18 + TypeScript
- Vite (build tool)
- Tailwind CSS + shadcn/ui (styling)
- TanStack Query (API state management)
- React Router v6 (routing)

See [web-ui/README.md](web-ui/README.md) for detailed documentation.

## OpenAPI Documentation

The API server provides OpenAPI/Swagger documentation with an interactive UI.

### Accessing API Docs

Once the server is running:
- **Swagger UI**: http://localhost:8080/api/v1/swagger
- **OpenAPI JSON**: http://localhost:8080/api-docs/swagger.json

The Swagger UI provides an interactive interface to explore and test all API endpoints.

### API Categories

| Tag | Description |
|-----|-------------|
| Health | Service health and readiness endpoints |
| Index | Index management operations (create, load, save, delete) |
| Search | Vector search operations (single, multi-index, batch) |

## Benchmarks

### SIFT Datasets

| Dataset | Base Vectors | Queries | Size | Use Case |
|---------|-------------|---------|------|----------|
| **SIFT10K** | 10K | 100 | ~5 MB | Fast dev testing (default) |
| **SIFT1M** | 1M | 10K | ~500 MB | Full benchmark |

### Setup

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

### Running Benchmarks

```bash
# SIFT10K (default, fast)
sbt "core/testOnly *SiftBenchmarkTest"

# SIFT1M (full benchmark)
SIFT_DATASET=sift1m sbt "core/testOnly *SiftBenchmarkTest"

# Skip benchmarks in CI
SKIP_BENCHMARK=true sbt "core/test"
```

### Performance Targets

| Test | Target |
|------|--------|
| recall@1 (ef=200) | >= 95% |
| recall@10 (ef=50) | >= 90% |
| recall@100 (ef=200) | >= 85% |
| Query latency (1M) | < 1ms |
| Build throughput | >= 5K vectors/sec |

## Implementation Progress

### Phase 1: Core Library (Complete)
- [x] Project structure and test data generators
- [x] HNSW algorithm wrapper (HNSWLibIndex with hnswlib-core)
- [x] Spark Local Index builder
- [x] Boundary node selection and global routing index
- [x] DataFrame API (ANNDataFrameExtensions, ANNIndexAPI)
- [x] File discovery and grouping strategies
- [x] ANNSearcher with multi-index routing

### REST API Server (MVP Complete)
- [x] Health & status endpoints
- [x] Single/multi-index search
- [x] Batch search
- [x] Index management (create, load, unload, save)
- [x] OpenAPI/Swagger documentation
- [ ] Prometheus metrics
- [ ] API key authentication
- [ ] TLS/HTTPS support

### Web UI (Complete)
- [x] Dashboard with health stats and index overview
- [x] Index management (create, load, delete, inspect)
- [x] Single index search
- [x] Multi-index search with result merging
- [x] Batch search
- [x] Demo data generation for testing
- [x] Light/dark theme support

### Docker Support (Complete)
- [x] API server Dockerfile (multi-stage build)
- [x] Web UI Dockerfile (nginx + SPA)
- [x] Docker Compose orchestration
- [x] Health checks and service dependencies
- [x] Persistent volume for index storage

### Phase 2: SQL Extension (Planned)
- [ ] Custom optimizer rules
- [ ] Query plan optimization
- [ ] Iceberg integration

### Phase 3: Native Acceleration (Optional)
- [ ] JNI interface
- [ ] SIMD optimization
- [ ] Zero-copy transfer

## License

Proprietary - Internal Use Only
