# Spark-ANN Quick Start Guide

## Prerequisites

- Java 11 or 17 (JDK)
- sbt 1.9.x
- Apache Spark 3.5.0 (included as dependency for local mode)
- Node.js 20+ (only if developing the Web UI)
- Docker 20.10+ and Docker Compose v2+ (only for containerized deployment)

## 1. Build the Project

```bash
# Clone and build
cd spark-ann
sbt compile

# Run tests to verify
sbt test

# Build fat JARs
sbt sparkIntegration/assembly   # For use with spark-submit/spark-shell
sbt apiServer/assembly          # Standalone API server
```

## 2. Spark DataFrame API

### Minimal Example

```scala
import com.company.ann.spark.api._
import com.company.ann.spark.api.ANNDataFrameExtensions._
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("ANN Example")
  .master("local[4]")
  .getOrCreate()

import spark.implicits._

// Create vector data
val vectors = Seq(
  (1L, Seq(0.1f, 0.2f, 0.3f, 0.4f)),
  (2L, Seq(0.2f, 0.3f, 0.4f, 0.5f)),
  (3L, Seq(0.3f, 0.4f, 0.5f, 0.6f)),
  (4L, Seq(0.4f, 0.5f, 0.6f, 0.7f)),
  (5L, Seq(0.5f, 0.6f, 0.7f, 0.8f))
).toDF("id", "vector")

// Build an index
val metadata = vectors.buildANNIndex(
  vectorColumn = "vector",
  outputPath = "/tmp/ann_index"
)

// Search
val results = vectors.annSearch(
  indexPath = "/tmp/ann_index",
  queryVector = Array(0.15f, 0.25f, 0.35f, 0.45f),
  k = 3
)
results.show()
```

### Static API (Alternative)

Use `ANNIndexAPI` when you prefer explicit method calls over implicit DataFrame extensions:

```scala
import com.company.ann.spark.api.ANNIndexAPI

// Build
val metadata = ANNIndexAPI.buildIndex(df, "vector", "/tmp/ann_index")

// Single search
val results = ANNIndexAPI.search(spark, "/tmp/ann_index", queryVector, k = 10)

// Batch search
val batchResults = ANNIndexAPI.batchSearch(
  spark, "/tmp/ann_index", queriesDF, "vector", k = 5
)
```

### Configuration

Pass `ANNIndexConfig` to control index construction:

```scala
import com.company.ann.spark.api.ANNIndexConfig
import com.company.ann.spark.builder.MergeSmall

val config = ANNIndexConfig(
  M = 16,                       // Connections per node (default: 16)
  efConstruction = 200,         // Build quality (default: 200)
  distanceType = "cosine",      // "euclidean" or "cosine" (default: "euclidean")
  groupingStrategy = MergeSmall, // SingleFile or MergeSmall (default: SingleFile)
  targetVectorsPerIndex = 500000, // Target index size for MergeSmall
  boundaryNodesPerIndex = 50     // Routing nodes per index (default: 50)
)

val metadata = vectors.buildANNIndex("vector", "/tmp/ann_index", config)
```

### Search Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `k`       | required | Number of nearest neighbors to return |
| `ef`      | 50       | Search accuracy. Higher = better recall, slower |
| `nprobe`  | 3        | Number of local indexes to probe in multi-index setup |

```scala
val results = vectors.annSearch(
  indexPath = "/tmp/ann_index",
  queryVector = queryVector,
  k = 10,
  nprobe = 5,  // search more indexes for better recall
  ef = 200     // higher accuracy
)
```

### Advanced: File-Based Pipeline

For large datasets, control each stage independently:

```scala
import com.company.ann.spark.builder._

// 1. Discover parquet files
val files = FileDiscovery.discoverDataFiles(spark, "/data/vectors", "vector")

// 2. Group files (merge small ones up to ~500K vectors per index)
val groups = FileGroupingStrategy.groupFiles(files, MergeSmall, 500000)

// 3. Build index from groups
val metadata = ANNIndexAPI.buildIndexFromFileGroups(
  spark, groups, "vector", "/tmp/ann_index", config
)

// 4. Load searcher for repeated queries (avoids re-loading per search)
val searcher = ANNIndexAPI.loadSearcher(spark, "/tmp/ann_index")
val results1 = searcher.search(queryVector1, k = 10)
val results2 = searcher.search(queryVector2, k = 10)
```

## 3. Using with spark-shell / spark-submit

### spark-shell

```bash
spark-shell --jars spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar
```

Then use the API as shown above.

### spark-submit

```bash
spark-submit \
  --class com.company.ann.spark.examples.QuickStart \
  --master local[4] \
  spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar
```

### Cluster mode (YARN / Kubernetes)

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --class your.app.MainClass \
  spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar
```

## 4. REST API Server

### Start the server

```bash
# Via sbt
sbt apiServer/run

# Via fat JAR
java -jar api-server/target/scala-2.12/spark-ann-api-server-assembly.jar

# Custom port
ANN_SERVER_PORT=9090 java -jar api-server/target/scala-2.12/spark-ann-api-server-assembly.jar
```

Server starts at `http://localhost:8080`. Swagger UI is at `http://localhost:8080/api/v1/swagger`.

### Create an index

```bash
curl -X POST http://localhost:8080/api/v1/indexes \
  -H "Content-Type: application/json" \
  -d '{
    "indexId": "demo",
    "vectors": [
      {"id": 1, "vector": [0.1, 0.2, 0.3, 0.4]},
      {"id": 2, "vector": [0.5, 0.6, 0.7, 0.8]},
      {"id": 3, "vector": [0.2, 0.3, 0.4, 0.5]}
    ],
    "config": {"m": 16, "efConstruction": 200, "distanceType": "euclidean"}
  }'
```

### Search

```bash
# Single index search
curl -X POST http://localhost:8080/api/v1/indexes/demo/search \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.15, 0.25, 0.35, 0.45], "k": 3}'

# Multi-index search (searches all loaded indexes)
curl -X POST http://localhost:8080/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.15, 0.25, 0.35, 0.45], "k": 5}'

# Batch search
curl -X POST http://localhost:8080/api/v1/search/batch \
  -H "Content-Type: application/json" \
  -d '{
    "indexId": "demo",
    "queries": [
      {"vector": [0.1, 0.2, 0.3, 0.4], "k": 3},
      {"vector": [0.5, 0.6, 0.7, 0.8], "k": 3}
    ]
  }'
```

### Other index operations

```bash
# List indexes
curl http://localhost:8080/api/v1/indexes

# Get index details
curl http://localhost:8080/api/v1/indexes/demo

# Add vectors to existing index
curl -X POST http://localhost:8080/api/v1/indexes/demo/vectors \
  -H "Content-Type: application/json" \
  -d '{"vectors": [{"id": 4, "vector": [0.9, 0.8, 0.7, 0.6]}]}'

# Save index to disk
curl -X POST http://localhost:8080/api/v1/indexes/demo/save \
  -H "Content-Type: application/json" \
  -d '{"path": "/data/indexes/demo.hnsw"}'

# Load index from disk
curl -X POST http://localhost:8080/api/v1/indexes \
  -H "Content-Type: application/json" \
  -d '{"indexId": "restored", "indexPath": "/data/indexes/demo.hnsw"}'

# Delete (unload) an index
curl -X DELETE http://localhost:8080/api/v1/indexes/demo

# Health check
curl http://localhost:8080/api/v1/health
```

### Server configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `ANN_SERVER_HOST`   | `0.0.0.0` | Bind address |
| `ANN_SERVER_PORT`   | `8080`    | Bind port |
| `ANN_INDEX_PATH`    | `/data/indexes` | Base path for index storage |

## 5. Docker Deployment

### Start everything with Docker Compose

```bash
docker-compose up -d
```

This starts:
- **API Server** at http://localhost:8080
- **Web UI** at http://localhost:80

```bash
# View logs
docker-compose logs -f

# Stop
docker-compose down
```

### Run API server standalone

```bash
docker build -t spark-ann-api -f api-server/Dockerfile .

docker run -d \
  --name spark-ann-api \
  -p 8080:8080 \
  -v /path/to/indexes:/data/indexes \
  -e JAVA_OPTS="-Xmx2g -Xms512m" \
  spark-ann-api
```

### Web UI development

```bash
cd web-ui
npm install
npm run dev     # Dev server at http://localhost:5173, proxies API to :8080
npm run build   # Production build
```

## 6. Key Concepts

### Architecture

```
DataFrame → Parquet files → File groups → Local HNSW indexes → Global routing index
```

1. **File Discovery**: Finds Parquet files and reads row counts from footers (no data scan)
2. **File Grouping**: Groups files into index-sized chunks (`SingleFile` or `MergeSmall`)
3. **Distributed Index Build**: Each file group is built into an HNSW index on a separate Spark executor
4. **Boundary Node Selection**: Representative vectors are sampled during build for query routing
5. **Global Routing Index**: Built from boundary nodes to route queries to the right local indexes

### Search Flow

1. Query vector goes to the **global routing index** to identify the most relevant local indexes
2. The top `nprobe` local indexes are searched
3. Results are merged and sorted by distance, returning the top `k`

### Distance Metrics

- **`euclidean`** (default): L2 distance. Lower = more similar.
- **`cosine`**: Cosine distance (1 - cosine_similarity). Lower = more similar. Best for normalized embeddings.

### Tuning

| Goal | Action |
|------|--------|
| Better recall | Increase `ef` (search) and `efConstruction` (build) |
| Faster search | Decrease `ef` and `nprobe` |
| Less memory per index | Decrease `M` (but recall drops) |
| Better routing | Increase `boundaryNodesPerIndex` |
| Fewer indexes | Increase `targetVectorsPerIndex` or use `MergeSmall` |
