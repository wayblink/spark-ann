# 第4章：元数据格式规范

## 4.1 元数据设计原则

### 4.1.1 自描述性
- 元数据应包含索引的完整描述
- 无需额外文档即可理解索引结构
- 支持工具自动解析和验证

### 4.1.2 版本化
- 支持元数据格式的向后兼容演进
- 索引版本与元数据版本独立管理
- 明确的升级路径

### 4.1.3 可扩展性
- 预留扩展字段
- 支持自定义属性
- 不破坏现有解析逻辑

## 4.2 顶层索引元数据

### 4.2.1 JSON Schema定义

**文件名**: `index_metadata.json`

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "ANN Index Metadata",
  "type": "object",
  "required": ["format_version", "index_version", "index_type", "vector_config", "global_index", "local_indices"],
  
  "properties": {
    "format_version": {
      "type": "string",
      "description": "元数据格式版本",
      "pattern": "^[0-9]+\\.[0-9]+$",
      "example": "1.0"
    },
    
    "index_version": {
      "type": "string",
      "description": "索引版本标识",
      "example": "v1"
    },
    
    "index_type": {
      "type": "string",
      "enum": ["hnsw", "ivf", "lsh", "flat"],
      "description": "索引类型"
    },
    
    "vector_config": {
      "type": "object",
      "required": ["dimension", "distance_metric"],
      "properties": {
        "dimension": {
          "type": "integer",
          "minimum": 1,
          "description": "向量维度"
        },
        "distance_metric": {
          "type": "string",
          "enum": ["l2", "cosine", "inner_product"],
          "description": "距离度量"
        },
        "normalize": {
          "type": "boolean",
          "default": false,
          "description": "是否归一化向量"
        }
      }
    },
    
    "data_source": {
      "type": "object",
      "required": ["base_path", "vector_column"],
      "properties": {
        "base_path": {
          "type": "string",
          "description": "数据文件根路径"
        },
        "vector_column": {
          "type": "string",
          "description": "向量列名"
        },
        "id_column": {
          "type": "string",
          "description": "ID列名（可选）"
        },
        "file_format": {
          "type": "string",
          "enum": ["parquet", "orc", "avro"],
          "default": "parquet"
        }
      }
    },
    
    "global_index": {
      "$ref": "#/$defs/global_index_config"
    },
    
    "local_indices": {
      "type": "array",
      "items": {
        "$ref": "#/$defs/local_index_config"
      }
    },
    
    "build_info": {
      "type": "object",
      "properties": {
        "build_timestamp": {
          "type": "string",
          "format": "date-time"
        },
        "builder_version": {
          "type": "string"
        },
        "build_duration_seconds": {
          "type": "number"
        },
        "num_vectors": {
          "type": "integer"
        }
      }
    },
    
    "statistics": {
      "type": "object",
      "properties": {
        "total_vectors": {
          "type": "integer"
        },
        "num_files": {
          "type": "integer"
        },
        "total_index_size_bytes": {
          "type": "integer"
        },
        "avg_vectors_per_file": {
          "type": "number"
        }
      }
    },
    
    "extensions": {
      "type": "object",
      "description": "扩展字段，用于自定义属性"
    }
  },
  
  "$defs": {
    "global_index_config": {
      "type": "object",
      "required": ["enabled", "index_path"],
      "properties": {
        "enabled": {
          "type": "boolean",
          "description": "是否启用全局索引"
        },
        "index_path": {
          "type": "string",
          "description": "全局索引存储路径"
        },
        "entry_point": {
          "type": "string",
          "description": "全局入口点NodeId",
          "pattern": "^[^:]+:[0-9]+$"
        },
        "max_layer": {
          "type": "integer",
          "minimum": 0
        },
        "num_boundary_nodes": {
          "type": "integer"
        },
        "hnsw_config": {
          "type": "object",
          "properties": {
            "M": {
              "type": "integer",
              "default": 32,
              "description": "全局HNSW的M参数"
            },
            "ef_construction": {
              "type": "integer",
              "default": 200
            }
          }
        },
        "storage": {
          "type": "object",
          "properties": {
            "top_layers_file": {
              "type": "string",
              "description": "高层图文件路径"
            },
            "layer0_file": {
              "type": "string",
              "description": "底层图文件路径"
            },
            "routing_file": {
              "type": "string",
              "description": "路由表文件路径"
            }
          }
        }
      }
    },
    
    "local_index_config": {
      "type": "object",
      "required": ["file_id", "data_file", "index_file"],
      "properties": {
        "file_id": {
          "type": "string",
          "description": "文件唯一标识"
        },
        "data_file": {
          "type": "string",
          "description": "数据文件路径"
        },
        "index_file": {
          "type": "string",
          "description": "索引文件路径"
        },
        "num_vectors": {
          "type": "integer"
        },
        "entry_point": {
          "type": "integer",
          "description": "本地入口点向量ID"
        },
        "max_layer": {
          "type": "integer"
        },
        "boundary_nodes": {
          "type": "array",
          "items": {
            "type": "integer"
          },
          "description": "boundary节点的本地ID列表"
        },
        "hnsw_config": {
          "type": "object",
          "properties": {
            "M": {
              "type": "integer",
              "default": 16
            },
            "M_max_0": {
              "type": "integer",
              "default": 32
            },
            "ef_construction": {
              "type": "integer",
              "default": 200
            }
          }
        },
        "checksum": {
          "type": "string",
          "description": "索引文件校验和（MD5/SHA256）"
        },
        "build_timestamp": {
          "type": "string",
          "format": "date-time"
        }
      }
    }
  }
}
```

### 4.2.2 示例元数据文件

```json
{
  "format_version": "1.0",
  "index_version": "v1",
  "index_type": "hnsw",
  
  "vector_config": {
    "dimension": 768,
    "distance_metric": "l2",
    "normalize": false
  },
  
  "data_source": {
    "base_path": "s3://my-bucket/vectors/",
    "vector_column": "embedding",
    "id_column": "vector_id",
    "file_format": "parquet"
  },
  
  "global_index": {
    "enabled": true,
    "index_path": "s3://my-bucket/indices/global/v1/",
    "entry_point": "file_001:42",
    "max_layer": 5,
    "num_boundary_nodes": 10000,
    "hnsw_config": {
      "M": 32,
      "ef_construction": 200
    },
    "storage": {
      "top_layers_file": "s3://my-bucket/indices/global/v1/top_layers.parquet",
      "layer0_file": "s3://my-bucket/indices/global/v1/layer0.parquet",
      "routing_file": "s3://my-bucket/indices/global/v1/routing.parquet"
    }
  },
  
  "local_indices": [
    {
      "file_id": "file_001",
      "data_file": "s3://my-bucket/vectors/part-00001.parquet",
      "index_file": "s3://my-bucket/indices/local/v1/part-00001.hnsw",
      "num_vectors": 1000000,
      "entry_point": 42,
      "max_layer": 4,
      "boundary_nodes": [42, 100, 256, 1337, 9999],
      "hnsw_config": {
        "M": 16,
        "M_max_0": 32,
        "ef_construction": 200
      },
      "checksum": "a1b2c3d4e5f6...",
      "build_timestamp": "2025-01-15T10:30:00Z"
    },
    {
      "file_id": "file_002",
      "data_file": "s3://my-bucket/vectors/part-00002.parquet",
      "index_file": "s3://my-bucket/indices/local/v1/part-00002.hnsw",
      "num_vectors": 950000,
      "entry_point": 88,
      "max_layer": 4,
      "boundary_nodes": [88, 200, 512, 2048, 8888],
      "hnsw_config": {
        "M": 16,
        "M_max_0": 32,
        "ef_construction": 200
      },
      "checksum": "b2c3d4e5f6g7...",
      "build_timestamp": "2025-01-15T10:35:00Z"
    }
  ],
  
  "build_info": {
    "build_timestamp": "2025-01-15T10:00:00Z",
    "builder_version": "1.0.0",
    "build_duration_seconds": 3600,
    "num_vectors": 10000000
  },
  
  "statistics": {
    "total_vectors": 10000000,
    "num_files": 10,
    "total_index_size_bytes": 12884901888,
    "avg_vectors_per_file": 1000000
  }
}
```

## 4.3 Global HNSW存储格式

### 4.3.1 高层图 (Top Layers) - Parquet格式

**文件**: `top_layers.parquet`

**Schema**:
```sql
CREATE TABLE top_layers (
  node_id STRING,                           -- 节点ID "file_id:local_vector_id"
  file_id STRING,                           -- 所属文件
  local_vector_id BIGINT,                   -- 本地向量ID
  max_layer INT,                            -- 节点最高层级
  vector ARRAY<FLOAT>,                      -- 向量值（可选，用于缓存）
  
  -- 每一层的邻居信息
  layer_neighbors ARRAY<STRUCT<
    layer: INT,                             -- 层级
    neighbors: ARRAY<STRUCT<
      neighbor_id: STRING,                  -- 邻居节点ID
      neighbor_file: STRING,                -- 邻居文件ID
      distance: FLOAT                       -- 预计算距离（可选）
    >>
  >>
)
PARTITIONED BY (max_layer);                 -- 按最高层级分区
```

**示例数据**:
```json
{
  "node_id": "file_001:42",
  "file_id": "file_001",
  "local_vector_id": 42,
  "max_layer": 4,
  "vector": [0.123, 0.456, ...],
  "layer_neighbors": [
    {
      "layer": 4,
      "neighbors": [
        {"neighbor_id": "file_003:88", "neighbor_file": "file_003", "distance": 0.234},
        {"neighbor_id": "file_005:100", "neighbor_file": "file_005", "distance": 0.312}
      ]
    },
    {
      "layer": 3,
      "neighbors": [
        {"neighbor_id": "file_001:100", "neighbor_file": "file_001", "distance": 0.189},
        {"neighbor_id": "file_002:200", "neighbor_file": "file_002", "distance": 0.245},
        ...
      ]
    }
  ]
}
```

**优化建议**:
- 按`max_layer`分区，只加载高层数据
- 启用Snappy或ZSTD压缩
- 对`layer_neighbors`使用dictionary encoding

### 4.3.2 底层图 (Layer 0) - Parquet格式

**文件**: `layer0.parquet`（可选，如果Layer 0也存储在Global中）

**Schema**:
```sql
CREATE TABLE layer0 (
  node_id STRING,
  file_id STRING,
  local_vector_id BIGINT,
  neighbors ARRAY<STRUCT<
    neighbor_id: STRING,
    neighbor_file: STRING,
    distance: FLOAT
  >>
)
PARTITIONED BY (file_id);                   -- 按文件分区便于局部查询
```

### 4.3.3 路由表 - Parquet格式

**文件**: `routing.parquet`

**Schema**:
```sql
CREATE TABLE routing (
  node_id STRING PRIMARY KEY,
  file_id STRING,
  local_vector_id BIGINT,
  data_file_path STRING,                    -- 完整数据文件路径
  index_file_path STRING,                   -- 完整索引文件路径
  max_layer INT,
  is_boundary_node BOOLEAN,
  vector ARRAY<FLOAT>                       -- 可选，缓存向量
)
PARTITIONED BY (file_id);
```

**示例查询**:
```sql
-- 查找节点所属文件
SELECT file_id, data_file_path 
FROM routing 
WHERE node_id = 'file_001:42';

-- 列出某文件的所有boundary nodes
SELECT node_id, local_vector_id 
FROM routing 
WHERE file_id = 'file_001' AND is_boundary_node = true;
```

## 4.4 Local HNSW存储格式

### 4.4.1 二进制格式（推荐）

**文件**: `part-xxxxx.hnsw`

**格式定义**:
```
[Header]
- magic_number: 4 bytes (0x484E5357 = "HNSW")
- version: 4 bytes
- dimension: 4 bytes
- num_vectors: 8 bytes
- max_layer: 4 bytes
- M: 4 bytes
- entry_point: 8 bytes
- distance_metric: 1 byte (0=L2, 1=Cosine, 2=IP)

[Vector Data Section]
for each vector (num_vectors):
  - vector_id: 8 bytes
  - max_layer: 4 bytes
  - vector: dimension * 4 bytes (float32)

[Graph Section]
for each vector (num_vectors):
  for each layer (0 to max_layer):
    - num_neighbors: 4 bytes
    - neighbor_ids: num_neighbors * 8 bytes
    - distances: num_neighbors * 4 bytes (optional)
```

**优点**:
- 紧凑，节省存储空间
- 加载速度快
- 便于mmap

**缺点**:
- 不易调试
- 工具支持少

### 4.4.2 Parquet格式（备选）

如果希望更好的互操作性，也可以用Parquet存储Local HNSW:

**文件**: `part-xxxxx_index.parquet`

**Schema**:
```sql
CREATE TABLE local_hnsw_index (
  vector_id BIGINT,
  max_layer INT,
  vector ARRAY<FLOAT>,
  
  -- 图结构
  graph ARRAY<STRUCT<
    layer: INT,
    neighbors: ARRAY<STRUCT<
      neighbor_id: BIGINT,
      distance: FLOAT
    >>
  >>
)
SORTED BY (vector_id);
```

## 4.5 版本管理

### 4.5.1 索引版本命名

```
索引版本格式: v{major}.{minor}
- major: 重大变更（不兼容）
- minor: 增量更新（兼容）

示例:
- v1.0: 初始版本
- v1.1: 增量添加向量
- v2.0: 重建索引（参数变更）
```

### 4.5.2 元数据版本演进

```json
{
  "format_version": "1.0",
  "supported_by": ["reader-1.0", "reader-1.1", "reader-2.0"],
  
  "deprecated_fields": {
    "old_field_name": {
      "deprecated_in": "1.1",
      "removed_in": "2.0",
      "replacement": "new_field_name"
    }
  }
}
```

### 4.5.3 多版本共存

```
indices/
├── metadata/
│   ├── index_v1.json
│   ├── index_v1.1.json
│   └── index_v2.json
├── global/
│   ├── v1/
│   ├── v1.1/
│   └── v2/
└── local/
    ├── v1/
    ├── v1.1/
    └── v2/
```

**查询时版本选择**:
```scala
def selectIndexVersion(
  availableVersions: Seq[String],
  preference: Option[String] = None
): String = {
  preference match {
    case Some(v) if availableVersions.contains(v) => v
    case _ => availableVersions.max  // 默认使用最新版本
  }
}
```

## 4.6 元数据验证

### 4.6.1 完整性检查

```scala
def validateMetadata(metadata: IndexMetadata): Validation = {
  val errors = mutable.ArrayBuffer.empty[String]
  
  // 1. 检查文件存在性
  if (!fileExists(metadata.global_index.index_path)) {
    errors += s"Global index path not found: ${metadata.global_index.index_path}"
  }
  
  metadata.local_indices.foreach { local =>
    if (!fileExists(local.data_file)) {
      errors += s"Data file not found: ${local.data_file}"
    }
    if (!fileExists(local.index_file)) {
      errors += s"Index file not found: ${local.index_file}"
    }
  }
  
  // 2. 检查配置一致性
  metadata.local_indices.foreach { local =>
    if (local.hnsw_config.M > 64) {
      errors += s"M too large in ${local.file_id}: ${local.hnsw_config.M}"
    }
  }
  
  // 3. 检查boundary nodes一致性
  val globalBoundaryNodes = loadGlobalBoundaryNodes(metadata.global_index)
  metadata.local_indices.foreach { local =>
    val localNodes = local.boundary_nodes.map(id => s"${local.file_id}:$id").toSet
    val missing = localNodes.diff(globalBoundaryNodes)
    if (missing.nonEmpty) {
      errors += s"Boundary nodes missing in global index: $missing"
    }
  }
  
  if (errors.isEmpty) Valid else Invalid(errors.toSeq)
}
```

### 4.6.2 校验和验证

```scala
def verifyChecksums(metadata: IndexMetadata): Boolean = {
  metadata.local_indices.forall { local =>
    val actualChecksum = computeChecksum(local.index_file)
    actualChecksum == local.checksum
  }
}

def computeChecksum(filePath: String): String = {
  val md = MessageDigest.getInstance("SHA-256")
  val bytes = readFileBytes(filePath)
  md.digest(bytes).map("%02x".format(_)).mkString
}
```

---

**下一章预告**：第5章将详细介绍索引构建的完整流程，包括分布式构建策略和容错机制。
