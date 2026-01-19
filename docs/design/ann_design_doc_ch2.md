# 第2章：整体架构设计

## 2.1 架构概览

系统采用**三层架构**设计，从上到下分别是：

```
┌─────────────────────────────────────────────────────┐
│            查询协调层 (Query Coordinator)            │
│  - 查询解析与优化                                     │
│  - 索引选择与路由                                     │
│  - 结果聚合与排序                                     │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│           全局索引层 (Global Index Layer)            │
│  - Global HNSW图                                     │
│  - 跨文件路由信息                                     │
│  - Boundary节点管理                                   │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│           本地索引层 (Local Index Layer)             │
│  - 每个数据文件的Local HNSW                          │
│  - 向量数据读取                                       │
│  - 精确距离计算                                       │
└─────────────────────────────────────────────────────┘
```

## 2.2 组件详细说明

### 2.2.1 查询协调层

**职责**
- 接收用户查询请求
- 解析查询参数（k、ef_search、nprobe等）
- 选择合适的索引版本
- 协调全局和本地搜索
- 聚合并返回最终结果

**核心组件**
```scala
trait QueryCoordinator {
  def search(
    queryVector: Array[Float],
    k: Int,
    indexVersion: Option[String] = None,
    params: SearchParams = SearchParams.default
  ): Array[SearchResult]
}

case class SearchParams(
  efSearch: Int = 50,           // HNSW搜索参数
  nprobe: Int = 3,              // 搜索的候选文件数
  useIndex: Boolean = true,     // 是否使用索引
  rerank: Boolean = true        // 是否精确重排序
)
```

**执行模式**
1. **索引模式**：利用两层HNSW加速搜索
2. **混合模式**：索引粗选 + 精确计算
3. **暴力模式**：无索引时的fallback

### 2.2.2 全局索引层

**职责**
- 维护跨文件的连接关系
- 提供从入口点到候选文件的快速路由
- 管理boundary节点信息

**数据结构**
```scala
case class GlobalIndex(
  entryPoint: NodeId,                      // 全局入口点
  topLayers: Map[NodeId, LayerNeighbors], // 高层图（常驻内存）
  nodeToFile: Map[NodeId, FileId],        // 节点到文件的映射
  boundaryNodes: Map[FileId, Set[NodeId]] // 每个文件的边界节点
)

case class NodeId(fileId: String, localVectorId: Long) {
  override def toString: String = s"$fileId:$localVectorId"
}

case class LayerNeighbors(
  maxLayer: Int,
  layers: Map[Int, Array[Neighbor]]
)

case class Neighbor(
  nodeId: NodeId,
  distance: Option[Float] = None  // 预计算距离（可选）
)
```

**存储方式**
- 高层图（Layer ≥ 2）：全量加载到Driver内存，broadcast到Executors
- 低层图（Layer 0-1）：按需从Parquet读取
- 路由表：独立的Parquet文件，支持快速查找

### 2.2.3 本地索引层

**职责**
- 为一个或多个数据文件维护独立的HNSW索引
- 执行文件组内的向量搜索
- 提供向量数据访问接口

**数据结构**
```scala
trait LocalIndex {
  def search(
    queryVector: Array[Float],
    k: Int,
    efSearch: Int
  ): Array[LocalSearchResult]

  def getVector(vectorId: Long): Array[Float]

  def getVectors(vectorIds: Array[Long]): Array[Array[Float]]

  // 获取该索引覆盖的数据文件列表
  def getDataFiles(): Array[String]
}

case class LocalSearchResult(
  vectorId: Long,
  distance: Float,
  vector: Option[Array[Float]] = None,
  sourceFile: String  // 来源数据文件
)
```

**索引文件组织（基于文件）**

每个Local Index可以覆盖一个或多个数据文件：
```
# 单文件模式 - 一个数据文件对应一个索引
data_file: s3://bucket/data/file_001.parquet
index_file: s3://bucket/indices/local/v1/file_001.hnsw

# 多文件模式 - 多个小文件合并为一个索引（减少索引碎片）
data_files: [s3://bucket/data/file_001.parquet,
             s3://bucket/data/file_002.parquet,
             s3://bucket/data/file_003.parquet]
index_file: s3://bucket/indices/local/v1/group_001.hnsw
```

**文件分组策略**
- 默认：每个数据文件一个索引（1:1映射）
- 可选：按文件大小分组，将小文件合并到同一索引（减少索引数量）
- 阈值建议：单个索引覆盖的向量数控制在10万~100万之间

## 2.3 数据流图

### 2.3.1 索引构建流程

```
原始数据文件列表
    ↓
┌─────────────────────┐
│ 扫描数据文件         │
│ (列出所有数据文件)    │
└─────────────────────┘
    ↓
┌─────────────────────┐
│ 文件分组策略         │
│ (单文件/多文件合并)   │
└─────────────────────┘
    ↓
┌─────────────────────┐
│ 构建Local HNSW      │
│ (每个文件组独立并行)  │
└─────────────────────┘
    ↓
┌─────────────────────┐
│ 选择Boundary节点    │
│ (提取高层节点)       │
└─────────────────────┘
    ↓
┌─────────────────────┐
│ 收集所有Boundary节点│
│ (Driver聚合)         │
└─────────────────────┘
    ↓
┌─────────────────────┐
│ 构建Global HNSW     │
│ (跨文件连接)         │
└─────────────────────┘
    ↓
┌─────────────────────┐
│ 生成元数据          │
│ (记录文件路径列表)    │
└─────────────────────┘
```

### 2.3.2 查询执行流程

```
查询向量
    ↓
┌──────────────────────┐
│ 加载全局索引元数据    │
│ (读取metadata.json)   │
└──────────────────────┘
    ↓
┌──────────────────────┐
│ Global Layer导航      │
│ (从入口点贪心搜索)    │
└──────────────────────┘
    ↓
┌──────────────────────┐
│ 确定候选文件集合      │
│ (nprobe个文件)        │
└──────────────────────┘
    ↓
┌──────────────────────┐
│ 并行Local搜索         │
│ (每个文件返回top-k')  │
└──────────────────────┘
    ↓
┌──────────────────────┐
│ 结果聚合与重排序      │
│ (精确计算top-k)       │
└──────────────────────┘
    ↓
最终结果
```

## 2.4 存储架构

### 2.4.1 目录结构

```
table_root/
├── data/                          # 原始数据文件
│   ├── year=2025/month=01/        # 可能的分区目录
│   │   ├── file_001.parquet
│   │   ├── file_002.parquet
│   │   └── ...
│   └── year=2025/month=02/
│       └── ...
│
├── indices/                       # 索引文件目录
│   ├── metadata/                  # 索引元数据
│   │   ├── index_v1.json         # 索引版本1（包含文件路径列表）
│   │   └── index_v2.json         # 索引版本2
│   │
│   ├── global/                    # 全局索引
│   │   ├── v1/
│   │   │   ├── top_layers.parquet      # 高层图
│   │   │   ├── layer0.parquet          # 底层图
│   │   │   └── routing.parquet         # 路由表
│   │   └── v2/
│   │       └── ...
│   │
│   └── local/                     # 本地索引（基于文件）
│       ├── v1/
│       │   ├── file_001.hnsw      # 单文件索引
│       │   ├── file_002.hnsw
│       │   ├── group_001.hnsw     # 多文件合并索引
│       │   └── ...
│       └── v2/
│           └── ...
│
└── metadata/                      # Iceberg元数据（未来）
    └── ...
```

**索引与数据文件的映射关系**
- 每个Local Index在元数据中记录其覆盖的数据文件路径列表
- 支持1:1映射（一个数据文件一个索引）和N:1映射（多个数据文件一个索引）
- 索引文件名可以基于数据文件名或自定义的组ID

### 2.4.2 存储容量估算

假设：
- 向量维度：768
- 向量数量：10亿
- HNSW参数：M=16, max_layer=5

**数据文件大小**
- 原始向量：10亿 × 768 × 4字节 = 2.86 TB
- Parquet压缩后：约 1.5 TB

**索引文件大小**
- Local HNSW：每个向量约 (M × 2 × max_layer) × 8字节 = 1280字节
- 总计：10亿 × 1280字节 = 1.19 TB

**Global HNSW**
- Boundary节点数：约100万（0.1%）
- Global索引：100万 × 32 × 8字节 × 5层 = 1.2 GB

**总存储开销**
- 数据：1.5 TB
- 索引：1.19 TB
- 索引开销比：79%

## 2.5 关键设计决策

### 2.5.1 为什么选择两层架构？

**单层架构的问题**
- 全局单一HNSW图：内存无法容纳PB级数据的完整图
- 纯分区方案：跨分区边界查询召回率低

**两层架构的优势**
- 分而治之：每个文件独立索引，可并行构建
- 全局导航：高层图快速定位相关文件
- 内存友好：只需加载高层图和当前搜索的文件索引

### 2.5.2 为什么使用Parquet存储图结构？

**替代方案对比**

| 方案 | 优点 | 缺点 |
|------|------|------|
| 自定义二进制 | 紧凑、快速 | 不易调试、工具支持少 |
| Parquet | 压缩、工具支持好 | 略大 |
| JSON | 可读性好 | 体积大、性能差 |

**选择Parquet的理由**
- Spark原生支持，无需额外依赖
- 列式存储，支持选择性读取
- 支持复杂嵌套结构（array, struct）
- 预测下推，分区裁剪

### 2.5.3 为什么索引与数据分离？

**核心考虑**
1. **灵活性**：数据不动，索引可独立演进
2. **兼容性**：不影响现有数据处理流程
3. **可维护性**：索引损坏不影响原始数据
4. **多版本**：支持索引A/B测试

## 2.6 系统边界与接口

### 2.6.1 输入接口

```scala
// 构建索引
IndexBuilder.build(
  dataPath: String,              // 数据文件路径
  vectorColumn: String,          // 向量列名
  indexPath: String,             // 索引输出路径
  indexType: String = "hnsw",    // 索引类型
  params: IndexParams            // 索引参数
): IndexMetadata

// 执行查询
IndexSearcher.search(
  indexPath: String,             // 索引路径
  queryVector: Array[Float],     // 查询向量
  k: Int,                        // 返回结果数
  params: SearchParams           // 查询参数
): Array[SearchResult]
```

### 2.6.2 输出格式

```scala
case class SearchResult(
  fileId: String,           // 所属文件
  vectorId: Long,           // 向量ID
  distance: Float,          // 距离
  vector: Array[Float],     // 向量值（可选）
  metadata: Map[String, Any] // 附加元数据（可选）
)

case class IndexMetadata(
  version: String,
  indexType: String,
  vectorDim: Int,
  numVectors: Long,
  indexPath: String,
  buildTimestamp: Long,
  buildConfig: Map[String, Any]
)
```

---

**下一章预告**：第3章将详细介绍两层HNSW索引的算法设计与实现细节。
