# Spark-ANN REST API Design Document

## 1. Overview

### 1.1 Purpose

This document describes the REST API design for deploying Spark-ANN's vector search capabilities as a service. The API exposes Approximate Nearest Neighbor (ANN) search functionality using HNSW (Hierarchical Navigable Small World) algorithm.

### 1.2 Goals

- Provide RESTful endpoints for vector similarity search
- Support multi-index management and querying
- Enable index lifecycle operations (create, load, unload, delete)
- Maintain high performance for real-time search requirements
- Offer flexible search parameters for accuracy/speed trade-offs

### 1.3 Scope

| In Scope | Out of Scope |
|----------|--------------|
| Single-index search | Distributed search coordination |
| Multi-index search with result merging | Authentication/Authorization (Phase 2) |
| Index management (CRUD) | Rate limiting (Phase 2) |
| Health monitoring | Index replication |
| Batch vector operations | Real-time index updates at scale |

---

## 2. Architecture

### 2.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        API Gateway                               │
│                    (Future: Auth, Rate Limit)                    │
└─────────────────────────────────┬───────────────────────────────┘
                                  │
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│                      REST API Server                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Search       │  │ Index        │  │ Health       │          │
│  │ Controller   │  │ Controller   │  │ Controller   │          │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘          │
└─────────┼─────────────────┼─────────────────┼───────────────────┘
          │                 │                 │
          ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                       Service Layer                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ SearchService│  │ IndexService │  │ MetricsService│         │
│  └──────┬───────┘  └──────┬───────┘  └──────────────┘          │
└─────────┼─────────────────┼─────────────────────────────────────┘
          │                 │
          ▼                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Index Manager                                │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │                  In-Memory Index Registry                │    │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐    │    │
│  │  │ Index A │  │ Index B │  │ Index C │  │   ...   │    │    │
│  │  │ (HNSW)  │  │ (HNSW)  │  │ (HNSW)  │  │         │    │    │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘    │    │
│  └─────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Storage Layer                                 │
│  ┌──────────────────────┐  ┌────────────────────────────────┐   │
│  │  Index Files (.hnsw) │  │  Metadata Files (.meta)        │   │
│  │  - Binary HNSW data  │  │  - dimension, distanceType     │   │
│  │  - Optimized for     │  │  - vectorCount, sourceFiles    │   │
│  │    memory mapping    │  │                                │   │
│  └──────────────────────┘  └────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### 2.2 Technology Stack

| Component | Technology | Rationale |
|-----------|------------|-----------|
| HTTP Server | Akka HTTP | Spray JSON already in use; reactive, async |
| JSON Serialization | Spray JSON | Existing implementation in ApiModels |
| Index Algorithm | hnswlib-core 1.1.0 | Already integrated, proven performance |
| Build Tool | SBT | Existing project setup |
| Runtime | Scala 2.12 / JVM 11+ | Consistency with Spark integration |

---

## 3. API Specification

### 3.1 Base URL

```
http://{host}:{port}/api/v1
```

### 3.2 Common Headers

| Header | Required | Description |
|--------|----------|-------------|
| `Content-Type` | Yes | `application/json` for POST/PUT |
| `Accept` | No | `application/json` (default) |
| `X-Request-ID` | No | Client-provided request tracking ID |

### 3.3 Common Response Codes

| Code | Meaning |
|------|---------|
| 200 | Success |
| 201 | Created |
| 400 | Bad Request (invalid input) |
| 404 | Not Found (index doesn't exist) |
| 409 | Conflict (index already exists) |
| 422 | Unprocessable Entity (dimension mismatch, etc.) |
| 500 | Internal Server Error |
| 503 | Service Unavailable |

---

## 4. Endpoints

### 4.1 Health & Status

#### `GET /health`

Check service health and basic statistics.

**Response:**
```json
{
  "status": "healthy",
  "version": "1.0.0",
  "indexCount": 3,
  "totalVectors": 1500000
}
```

#### `GET /health/ready`

Readiness probe for orchestration systems.

**Response:**
```json
{
  "ready": true
}
```

#### `GET /health/live`

Liveness probe.

**Response:**
```json
{
  "alive": true
}
```

---

### 4.2 Search Operations

#### `POST /indexes/{indexId}/search`

Search for nearest neighbors in a specific index.

**Path Parameters:**
| Parameter | Type | Description |
|-----------|------|-------------|
| indexId | string | Target index identifier |

**Request Body:**
```json
{
  "vector": [0.1, 0.2, 0.3, ...],
  "k": 10,
  "ef": 100
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| vector | float[] | Yes | - | Query vector |
| k | int | Yes | - | Number of neighbors (1-1000) |
| ef | int | No | 50 | Search ef parameter (higher = more accurate, slower) |

**Response (200):**
```json
{
  "indexId": "products-v1",
  "results": [
    {"id": 12345, "distance": 0.023},
    {"id": 67890, "distance": 0.045},
    {"id": 11111, "distance": 0.067}
  ],
  "queryTimeMs": 2
}
```

**Error Response (404):**
```json
{
  "error": "IndexNotFound",
  "message": "Index 'products-v1' is not loaded"
}
```

**Error Response (422):**
```json
{
  "error": "DimensionMismatch",
  "message": "Query vector dimension (128) does not match index dimension (256)"
}
```

---

#### `POST /search`

Search across multiple indexes with result merging.

**Request Body:**
```json
{
  "vector": [0.1, 0.2, 0.3, ...],
  "k": 10,
  "ef": 100,
  "indexIds": ["index-1", "index-2"]
}
```

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| vector | float[] | Yes | - | Query vector |
| k | int | Yes | - | Number of neighbors per index |
| ef | int | No | 50 | Search ef parameter |
| indexIds | string[] | No | all | Specific indexes to search |

**Response (200):**
```json
{
  "results": {
    "index-1": [
      {"id": 100, "distance": 0.01},
      {"id": 101, "distance": 0.02}
    ],
    "index-2": [
      {"id": 200, "distance": 0.015},
      {"id": 201, "distance": 0.03}
    ]
  },
  "merged": [
    {"id": 100, "distance": 0.01, "indexId": "index-1"},
    {"id": 200, "distance": 0.015, "indexId": "index-2"},
    {"id": 101, "distance": 0.02, "indexId": "index-1"},
    {"id": 201, "distance": 0.03, "indexId": "index-2"}
  ],
  "totalTimeMs": 5
}
```

---

#### `POST /search/batch`

Batch search with multiple query vectors.

**Request Body:**
```json
{
  "queries": [
    {"vector": [0.1, 0.2, ...], "k": 5},
    {"vector": [0.3, 0.4, ...], "k": 10}
  ],
  "indexId": "products-v1",
  "ef": 100
}
```

**Response (200):**
```json
{
  "results": [
    {
      "queryIndex": 0,
      "results": [
        {"id": 123, "distance": 0.01}
      ]
    },
    {
      "queryIndex": 1,
      "results": [
        {"id": 456, "distance": 0.02}
      ]
    }
  ],
  "totalTimeMs": 8
}
```

---

### 4.3 Index Management

#### `GET /indexes`

List all loaded indexes.

**Response (200):**
```json
{
  "indexes": [
    {
      "indexId": "products-v1",
      "dimension": 256,
      "size": 500000,
      "indexPath": "/data/indexes/products-v1.hnsw",
      "distanceType": "cosine",
      "memoryUsageMb": 512
    },
    {
      "indexId": "users-v2",
      "dimension": 128,
      "size": 100000,
      "indexPath": "/data/indexes/users-v2.hnsw",
      "distanceType": "euclidean",
      "memoryUsageMb": 128
    }
  ],
  "totalIndexes": 2,
  "totalVectors": 600000
}
```

---

#### `GET /indexes/{indexId}`

Get details of a specific index.

**Response (200):**
```json
{
  "indexId": "products-v1",
  "dimension": 256,
  "size": 500000,
  "indexPath": "/data/indexes/products-v1.hnsw",
  "distanceType": "cosine",
  "memoryUsageMb": 512,
  "config": {
    "m": 16,
    "efConstruction": 200
  },
  "createdAt": "2024-01-15T10:30:00Z",
  "loadedAt": "2024-01-16T08:00:00Z"
}
```

---

#### `POST /indexes`

Load an existing index from disk or create a new one.

**Request Body (Load from disk):**
```json
{
  "indexId": "products-v1",
  "indexPath": "/data/indexes/products-v1.hnsw"
}
```

**Request Body (Create new with vectors):**
```json
{
  "indexId": "temp-index",
  "vectors": [
    {"id": 1, "vector": [0.1, 0.2, 0.3]},
    {"id": 2, "vector": [0.4, 0.5, 0.6]}
  ],
  "config": {
    "m": 16,
    "efConstruction": 200,
    "distanceType": "cosine"
  }
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| indexId | string | Yes | Unique index identifier |
| indexPath | string | No* | Path to existing index file |
| vectors | VectorData[] | No* | Vectors to build index from |
| config.m | int | No | HNSW M parameter (default: 16) |
| config.efConstruction | int | No | HNSW construction ef (default: 200) |
| config.distanceType | string | No | "euclidean" or "cosine" (default: "euclidean") |

*Either `indexPath` or `vectors` must be provided.

**Response (201):**
```json
{
  "success": true,
  "message": "Index 'products-v1' loaded successfully",
  "index": {
    "indexId": "products-v1",
    "dimension": 256,
    "size": 500000,
    "indexPath": "/data/indexes/products-v1.hnsw"
  }
}
```

---

#### `POST /indexes/{indexId}/vectors`

Add vectors to an existing index.

**Request Body:**
```json
{
  "vectors": [
    {"id": 1001, "vector": [0.1, 0.2, 0.3]},
    {"id": 1002, "vector": [0.4, 0.5, 0.6]}
  ]
}
```

**Response (200):**
```json
{
  "success": true,
  "message": "Added 2 vectors to index 'products-v1'",
  "index": {
    "indexId": "products-v1",
    "dimension": 256,
    "size": 500002
  }
}
```

---

#### `DELETE /indexes/{indexId}`

Unload an index from memory.

**Query Parameters:**
| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| deleteFile | boolean | false | Also delete the index file from disk |

**Response (200):**
```json
{
  "success": true,
  "message": "Index 'products-v1' unloaded"
}
```

---

#### `POST /indexes/{indexId}/save`

Save an in-memory index to disk.

**Request Body:**
```json
{
  "path": "/data/indexes/products-v1.hnsw"
}
```

**Response (200):**
```json
{
  "success": true,
  "message": "Index saved to /data/indexes/products-v1.hnsw"
}
```

---

## 5. Data Models

### 5.1 Core Models

```scala
// Search
case class SearchRequest(
  vector: Array[Float],
  k: Int,
  ef: Option[Int] = None
)

case class SearchResultItem(
  id: Long,
  distance: Float
)

case class SearchResponse(
  indexId: String,
  results: Seq[SearchResultItem],
  queryTimeMs: Long
)

// Multi-search
case class MultiSearchRequest(
  vector: Array[Float],
  k: Int,
  ef: Option[Int] = None,
  indexIds: Option[Seq[String]] = None
)

case class MergedSearchResultItem(
  id: Long,
  distance: Float,
  indexId: String
)

case class MultiSearchResponse(
  results: Map[String, Seq[SearchResultItem]],
  merged: Seq[MergedSearchResultItem],
  totalTimeMs: Long
)

// Index management
case class LoadIndexRequest(
  indexId: String,
  indexPath: String
)

case class CreateIndexRequest(
  indexId: String,
  vectors: Option[Seq[VectorData]] = None,
  indexPath: Option[String] = None,
  config: Option[IndexConfig] = None
)

case class IndexConfig(
  m: Option[Int] = None,
  efConstruction: Option[Int] = None,
  distanceType: Option[String] = None
)

case class VectorData(
  id: Long,
  vector: Array[Float]
)

case class IndexInfo(
  indexId: String,
  dimension: Int,
  size: Int,
  indexPath: Option[String],
  distanceType: Option[String] = None,
  memoryUsageMb: Option[Long] = None
)

// Responses
case class IndexOperationResponse(
  success: Boolean,
  message: String,
  index: Option[IndexInfo] = None
)

case class ErrorResponse(
  error: String,
  message: String,
  details: Option[Map[String, String]] = None
)
```

---

## 6. Error Handling

### 6.1 Error Response Format

All errors return a consistent JSON structure:

```json
{
  "error": "ErrorCode",
  "message": "Human-readable description",
  "details": {
    "field": "Additional context"
  }
}
```

### 6.2 Error Codes

| Error Code | HTTP Status | Description |
|------------|-------------|-------------|
| `InvalidRequest` | 400 | Malformed JSON or missing required fields |
| `InvalidVector` | 400 | Vector format invalid (empty, non-numeric) |
| `InvalidParameter` | 400 | Parameter out of valid range |
| `IndexNotFound` | 404 | Requested index not loaded |
| `IndexAlreadyExists` | 409 | Index ID already in use |
| `DimensionMismatch` | 422 | Vector dimension doesn't match index |
| `IndexFull` | 422 | Index has reached maximum capacity |
| `FileNotFound` | 422 | Index file path doesn't exist |
| `InternalError` | 500 | Unexpected server error |
| `ServiceUnavailable` | 503 | Service not ready |

---

## 7. Implementation Plan

### Phase 1: MVP (Current Scope)

1. **HTTP Server Setup**
   - Akka HTTP server with routing DSL
   - JSON marshalling/unmarshalling with Spray JSON
   - Basic error handling

2. **Index Manager**
   - In-memory registry of loaded indexes
   - Thread-safe concurrent access
   - Load/unload operations

3. **Core Endpoints**
   - `GET /health`
   - `POST /indexes/{indexId}/search`
   - `POST /search` (multi-index)
   - `GET /indexes`
   - `POST /indexes` (load)
   - `DELETE /indexes/{indexId}`

### Phase 2: Production Readiness

1. **Observability**
   - Prometheus metrics endpoint
   - Request logging with correlation IDs
   - Latency histograms per endpoint

2. **Reliability**
   - Graceful shutdown
   - Health check with dependency status
   - Request timeout configuration

3. **Security**
   - API key authentication
   - TLS/HTTPS support
   - Request validation and sanitization

### Phase 3: Advanced Features

1. **Performance**
   - Batch search optimization
   - Connection pooling
   - Index preloading on startup

2. **Operations**
   - Index hot reload
   - Dynamic configuration
   - Admin endpoints

---

## 8. Configuration

### 8.1 Application Configuration

```hocon
ann-service {
  server {
    host = "0.0.0.0"
    port = 8080
    request-timeout = 30s
  }

  index {
    base-path = "/data/indexes"
    preload = ["products-v1", "users-v1"]
    max-loaded-indexes = 10
  }

  search {
    default-ef = 50
    max-k = 1000
    timeout = 10s
  }

  hnsw {
    default-m = 16
    default-ef-construction = 200
    default-distance-type = "euclidean"
  }
}
```

---

## 9. Example Usage

### 9.1 Load Index and Search

```bash
# Load an index
curl -X POST http://localhost:8080/api/v1/indexes \
  -H "Content-Type: application/json" \
  -d '{
    "indexId": "products",
    "indexPath": "/data/indexes/products.hnsw"
  }'

# Search for similar vectors
curl -X POST http://localhost:8080/api/v1/indexes/products/search \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, 0.4, 0.5],
    "k": 5,
    "ef": 100
  }'
```

### 9.2 Multi-Index Search

```bash
curl -X POST http://localhost:8080/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{
    "vector": [0.1, 0.2, 0.3, 0.4, 0.5],
    "k": 10,
    "indexIds": ["products", "recommendations"]
  }'
```

---

## 10. Appendix

### A. Performance Considerations

| Metric | Target | Notes |
|--------|--------|-------|
| P50 latency (single search) | < 5ms | For index size < 1M vectors |
| P99 latency (single search) | < 20ms | Depends on ef parameter |
| Throughput | > 1000 QPS | Per index, single node |
| Index load time | < 30s | For 1M vectors |

### B. Capacity Planning

| Vectors | Dimension | Memory (approx) |
|---------|-----------|-----------------|
| 100K | 128 | ~100 MB |
| 1M | 128 | ~1 GB |
| 1M | 256 | ~2 GB |
| 10M | 128 | ~10 GB |

### C. Related Documents

- [HNSW Algorithm Paper](https://arxiv.org/abs/1603.09320)
- [hnswlib Documentation](https://github.com/nmslib/hnswlib)
- Spark-ANN Local Index Building: `spark-integration/LocalIndexBuilder.scala`
