# 第6章：查询执行流程

## 6.1 查询接口设计

```scala
trait ANNSearcher {
  def search(
    query: Array[Float],
    k: Int,
    params: SearchParams = SearchParams.default
  ): Array[SearchResult]
  
  def batchSearch(
    queries: Array[Array[Float]],
    k: Int,
    params: SearchParams = SearchParams.default
  ): Array[Array[SearchResult]]
}

case class SearchParams(
  indexVersion: Option[String] = None,    // 使用特定索引版本
  efSearch: Int = 50,                     // HNSW搜索参数
  nprobe: Int = 3,                        // 搜索文件数
  useIndex: Boolean = true,               // 是否使用索引
  rerank: Boolean = true,                 // 是否精确重排序
  maxDistance: Option[Float] = None,      // 距离阈值
  returnVectors: Boolean = false          // 是否返回向量值
)

case class SearchResult(
  fileId: String,
  vectorId: Long,
  distance: Float,
  vector: Option[Array[Float]] = None,
  metadata: Map[String, Any] = Map.empty
)
```

## 6.2 查询执行引擎

```scala
class SparkANNSearcher(
  spark: SparkSession,
  indexMetadata: IndexMetadata
) extends ANNSearcher {
  
  // 预加载Global Index到内存
  private val globalIndex: GlobalHNSW = loadGlobalIndex(indexMetadata.globalIndex)
  
  // Broadcast Global Index到所有Executors
  private val broadcastGlobalIndex = spark.sparkContext.broadcast(globalIndex)
  
  override def search(
    query: Array[Float],
    k: Int,
    params: SearchParams
  ): Array[SearchResult] = {
    
    if (!params.useIndex) {
      // Fallback: 暴力搜索
      return bruteForceSearch(query, k)
    }
    
    // Phase 1: Global navigation
    val candidateFiles = navigateGlobalLayer(
      query,
      globalIndex,
      params.nprobe
    )
    
    // Phase 2: Parallel local search
    val localResults = searchLocalIndices(
      query,
      candidateFiles,
      k * 2,  // 取更多候选以保证精度
      params.efSearch
    )
    
    // Phase 3: Global reranking
    val finalResults = if (params.rerank) {
      rerankResults(localResults, query, k)
    } else {
      localResults.sortBy(_.distance).take(k)
    }
    
    // 应用距离阈值
    params.maxDistance match {
      case Some(threshold) => finalResults.filter(_.distance <= threshold)
      case None => finalResults
    }
  }
  
  private def searchLocalIndices(
    query: Array[Float],
    candidateFiles: Set[String],
    k: Int,
    efSearch: Int
  ): Array[SearchResult] = {
    
    val sc = spark.sparkContext
    val queryBC = sc.broadcast(query)
    val filesRDD = sc.parallelize(candidateFiles.toSeq)
    
    val results = filesRDD.flatMap { fileId =>
      val localMeta = indexMetadata.localIndices.find(_.fileId == fileId).get
      val localIndex = loadLocalIndex(localMeta.indexFile)
      
      val localResults = localIndex.search(
        queryBC.value,
        k,
        efSearch
      )
      
      localResults.map { r =>
        SearchResult(
          fileId = fileId,
          vectorId = r.vectorId,
          distance = r.distance,
          vector = if (returnVectors) Some(r.vector) else None
        )
      }
    }.collect()
    
    queryBC.unpersist()
    results
  }
  
  private def rerankResults(
    candidates: Array[SearchResult],
    query: Array[Float],
    k: Int
  ): Array[SearchResult] = {
    
    // 精确计算距离并重新排序
    candidates
      .map { result =>
        val vector = if (result.vector.isDefined) {
          result.vector.get
        } else {
          loadVector(result.fileId, result.vectorId)
        }
        val exactDistance = computeDistance(query, vector)
        result.copy(distance = exactDistance)
      }
      .sortBy(_.distance)
      .take(k)
  }
  
  private def bruteForceSearch(
    query: Array[Float],
    k: Int
  ): Array[SearchResult] = {
    
    val dataFiles = indexMetadata.localIndices.map(_.dataFile)
    val sc = spark.sparkContext
    val queryBC = sc.broadcast(query)
    
    val results = sc.parallelize(dataFiles).flatMap { dataFile =>
      val vectors = loadVectorsFromDataFile(dataFile)
      vectors.map { case (id, vector) =>
        val distance = computeDistance(queryBC.value, vector)
        SearchResult(
          fileId = extractFileId(dataFile),
          vectorId = id,
          distance = distance
        )
      }
    }
    .takeOrdered(k)(Ordering.by(_.distance))
    
    queryBC.unpersist()
    results
  }
}
```

## 6.3 批量查询优化

```scala
override def batchSearch(
  queries: Array[Array[Float]],
  k: Int,
  params: SearchParams
): Array[Array[SearchResult]] = {
  
  val sc = spark.sparkContext
  val globalIndexBC = broadcastGlobalIndex
  
  // 为每个查询确定候选文件
  val queryCandidates = queries.map { query =>
    navigateGlobalLayer(query, globalIndexBC.value, params.nprobe)
  }
  
  // 按文件分组查询，减少重复加载
  val fileToQueries = queries.zipWithIndex
    .flatMap { case (query, queryIdx) =>
      queryCandidates(queryIdx).map(fileId => (fileId, queryIdx, query))
    }
    .groupBy(_._1)
  
  // 批量搜索每个文件
  val fileResults = sc.parallelize(fileToQueries.toSeq).flatMap { case (fileId, queryList) =>
    val localMeta = indexMetadata.localIndices.find(_.fileId == fileId).get
    val localIndex = loadLocalIndex(localMeta.indexFile)
    
    queryList.flatMap { case (_, queryIdx, query) =>
      val results = localIndex.search(query, k * 2, params.efSearch)
      results.map(r => (queryIdx, SearchResult(fileId, r.vectorId, r.distance)))
    }
  }
  .collect()
  .groupBy(_._1)
  .mapValues(_.map(_._2).sortBy(_.distance).take(k))
  
  // 确保所有查询都有结果
  queries.indices.map { i =>
    fileResults.getOrElse(i, Array.empty)
  }.toArray
}
```

---

# 第7章：Iceberg集成方案

## 7.1 扩展Iceberg Metadata

### 7.1.1 Metadata JSON扩展

在Iceberg的metadata文件中添加索引信息：

```json
{
  "format-version": 2,
  "table-uuid": "...",
  "location": "s3://bucket/table",
  
  // Iceberg原有字段
  "last-sequence-number": 1,
  "last-updated-ms": 1234567890,
  "schemas": [...],
  "partition-spec": [...],
  "properties": {...},
  
  // 新增: ANN索引信息
  "ann-indices": {
    "indices": [
      {
        "index-id": 1,
        "index-name": "embedding_hnsw_v1",
        "status": "active",
        "vector-column": "embedding",
        "index-type": "hnsw",
        "index-metadata-location": "s3://bucket/indices/metadata/index_v1.json",
        "snapshot-id": 123456,
        "created-at": "2025-01-15T10:00:00Z"
      }
    ],
    "current-index-id": 1
  }
}
```

### 7.1.2 索引与Snapshot关联

```scala
case class IcebergIndexMetadata(
  indexId: Long,
  indexName: String,
  status: String,              // "active", "building", "deprecated"
  vectorColumn: String,
  indexType: String,
  indexMetadataLocation: String,
  snapshotId: Long,           // 关联的数据snapshot
  createdAt: String,
  properties: Map[String, String] = Map.empty
)

def associateIndexWithSnapshot(
  table: Table,
  indexMetadata: IndexMetadata,
  snapshotId: Long
): Unit = {
  
  val icebergIndex = IcebergIndexMetadata(
    indexId = generateIndexId(),
    indexName = s"${indexMetadata.dataSource.vectorColumn}_hnsw_${indexMetadata.indexVersion}",
    status = "active",
    vectorColumn = indexMetadata.dataSource.vectorColumn,
    indexType = indexMetadata.indexType,
    indexMetadataLocation = s"${indexMetadata.dataSource.basePath}/indices/metadata/index_${indexMetadata.indexVersion}.json",
    snapshotId = snapshotId,
    createdAt = indexMetadata.buildInfo.buildTimestamp
  )
  
  // 更新Iceberg表属性
  table.updateProperties()
    .set(s"ann-index.${icebergIndex.indexId}.metadata", JsonSerializer.toJson(icebergIndex))
    .set("ann-index.current-index-id", icebergIndex.indexId.toString)
    .commit()
}
```

## 7.2 查询计划集成

### 7.2.1 ANN Scan节点

扩展Spark的逻辑计划，添加ANN索引扫描：

```scala
case class ANNIndexScan(
  relation: LogicalRelation,
  indexMetadata: IndexMetadata,
  queryVector: Expression,
  k: Int,
  searchParams: SearchParams,
  output: Seq[Attribute]
) extends LeafNode {
  
  override def computeStats(): Statistics = {
    // 估算返回k行
    Statistics(sizeInBytes = BigInt(k * output.map(_.dataType.defaultSize).sum))
  }
}
```

### 7.2.2 查询优化规则

```scala
object ANNIndexOptimization extends Rule[LogicalPlan] {
  
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    
    // 识别模式: SELECT * FROM table ORDER BY distance(vector, query) LIMIT k
    case limit @ Limit(IntegerLiteral(k),
         sort @ Sort(sortOrders, true,
           filter @ Filter(condition,
             relation @ LogicalRelation(_, _, table, _)))) =>
      
      // 检查是否有可用的ANN索引
      val annIndex = findApplicableIndex(table, sortOrders)
      
      annIndex match {
        case Some(indexMeta) =>
          // 替换为ANN索引扫描
          val queryVector = extractQueryVector(sortOrders)
          ANNIndexScan(
            relation = relation,
            indexMetadata = indexMeta,
            queryVector = queryVector,
            k = k,
            searchParams = SearchParams.default,
            output = relation.output
          )
        
        case None =>
          // 保持原计划
          limit
      }
  }
  
  private def findApplicableIndex(
    table: CatalogTable,
    sortOrders: Seq[SortOrder]
  ): Option[IndexMetadata] = {
    // 从Iceberg表属性中查找索引
    val currentIndexId = table.properties.get("ann-index.current-index-id")
    currentIndexId.flatMap { id =>
      val indexJson = table.properties.get(s"ann-index.$id.metadata")
      indexJson.map { json =>
        val icebergIndex = JsonSerializer.fromJson[IcebergIndexMetadata](json)
        loadIndexMetadata(icebergIndex.indexMetadataLocation)
      }
    }
  }
}

// 注册优化规则
spark.experimental.extraOptimizations = Seq(ANNIndexOptimization)
```

### 7.2.3 执行计划

```scala
case class ANNIndexScanExec(
  indexMetadata: IndexMetadata,
  queryVector: Array[Float],
  k: Int,
  searchParams: SearchParams,
  output: Seq[Attribute]
) extends LeafExecNode {
  
  override protected def doExecute(): RDD[InternalRow] = {
    val searcher = new SparkANNSearcher(sparkContext.sparkSession, indexMetadata)
    
    val results = searcher.search(queryVector, k, searchParams)
    
    // 转换为InternalRow
    val rows = results.map { result =>
      // 从数据文件加载完整行
      loadFullRow(result.fileId, result.vectorId, output)
    }
    
    sparkContext.parallelize(rows)
  }
}
```

## 7.3 索引维护

### 7.3.1 增量更新策略

```scala
def updateIndexIncremental(
  table: Table,
  newSnapshot: Snapshot,
  oldSnapshot: Snapshot,
  indexMetadata: IndexMetadata
): IndexMetadata = {
  
  // 识别新增的数据文件
  val newFiles = newSnapshot.addedDataFiles(oldSnapshot)
  
  if (newFiles.isEmpty) {
    return indexMetadata  // 无需更新
  }
  
  // 为新文件构建Local Index
  val newLocalIndices = newFiles.map { file =>
    buildLocalIndex(file, indexMetadata.vectorConfig)
  }
  
  // 重建Global Index (包含新的boundary nodes)
  val updatedGlobalIndex = rebuildGlobalIndex(
    oldBoundaryNodes = collectBoundaryNodes(indexMetadata.localIndices),
    newBoundaryNodes = collectBoundaryNodes(newLocalIndices)
  )
  
  // 生成新版本元数据
  val newVersion = incrementVersion(indexMetadata.indexVersion)
  indexMetadata.copy(
    indexVersion = newVersion,
    localIndices = indexMetadata.localIndices ++ newLocalIndices,
    globalIndex = updatedGlobalIndex,
    buildInfo = indexMetadata.buildInfo.copy(
      buildTimestamp = Instant.now().toString
    )
  )
}
```

### 7.3.2 完全重建触发条件

```scala
def shouldRebuildIndex(
  currentIndex: IndexMetadata,
  table: Table
): Boolean = {
  
  val currentVectors = currentIndex.statistics.totalVectors
  val tableVectors = table.currentSnapshot().summary().get("total-records").toLong
  
  val growth = (tableVectors - currentVectors).toDouble / currentVectors
  
  // 触发条件
  growth > 0.5 ||                                    // 数据增长超过50%
  currentIndex.localIndices.length > 1000 ||        // 文件过多
  indexAge(currentIndex) > Duration.ofDays(30)      // 索引过旧
}
```

---

# 第8章：性能优化策略

## 8.1 查询性能优化

### 8.1.1 自适应nprobe

```scala
class AdaptiveSearchParams {
  
  def adjustNprobe(
    initialNprobe: Int,
    recall: Double,
    targetRecall: Double = 0.95
  ): Int = {
    
    if (recall >= targetRecall) {
      // 召回率足够，可以减少nprobe
      Math.max(1, (initialNprobe * 0.8).toInt)
    } else {
      // 召回率不足，增加nprobe
      Math.min(initialNprobe * 2, 20)
    }
  }
  
  def estimateRecall(
    annResults: Array[SearchResult],
    bruteForceResults: Array[SearchResult],
    k: Int
  ): Double = {
    
    val annTopK = annResults.take(k).map(_.vectorId).toSet
    val bfTopK = bruteForceResults.take(k).map(_.vectorId).toSet
    
    annTopK.intersect(bfTopK).size.toDouble / k
  }
}
```

### 8.1.2 结果缓存

```scala
class ANNResultCache(maxSize: Int = 10000) {
  
  private val cache = new ConcurrentLRUCache[CacheKey, Array[SearchResult]](maxSize)
  
  case class CacheKey(
    queryHash: Int,
    k: Int,
    params: SearchParams
  )
  
  def get(query: Array[Float], k: Int, params: SearchParams): Option[Array[SearchResult]] = {
    val key = CacheKey(
      queryHash = java.util.Arrays.hashCode(query),
      k = k,
      params = params
    )
    cache.get(key)
  }
  
  def put(query: Array[Float], k: Int, params: SearchParams, results: Array[SearchResult]): Unit = {
    val key = CacheKey(
      queryHash = java.util.Arrays.hashCode(query),
      k = k,
      params = params
    )
    cache.put(key, results)
  }
}
```

### 8.1.3 向量压缩

```scala
// Product Quantization压缩
class PQCompressor(
  dimension: Int,
  numSubvectors: Int = 8,
  bitsPerSubvector: Int = 8
) {
  
  private val subvectorDim = dimension / numSubvectors
  private val codebooks: Array[Array[Array[Float]]] = trainCodebooks()
  
  def compress(vector: Array[Float]): Array[Byte] = {
    val codes = new Array[Byte](numSubvectors)
    
    for (i <- 0 until numSubvectors) {
      val subvector = vector.slice(i * subvectorDim, (i + 1) * subvectorDim)
      codes(i) = findNearestCode(subvector, codebooks(i)).toByte
    }
    
    codes
  }
  
  def decompress(codes: Array[Byte]): Array[Float] = {
    val vector = new Array[Float](dimension)
    
    for (i <- 0 until numSubvectors) {
      val code = codes(i) & 0xFF
      val subvector = codebooks(i)(code)
      Array.copy(subvector, 0, vector, i * subvectorDim, subvectorDim)
    }
    
    vector
  }
  
  def asymmetricDistance(query: Array[Float], codes: Array[Byte]): Float = {
    var distance = 0.0f
    
    for (i <- 0 until numSubvectors) {
      val querySubvector = query.slice(i * subvectorDim, (i + 1) * subvectorDim)
      val code = codes(i) & 0xFF
      val dbSubvector = codebooks(i)(code)
      
      distance += l2Distance(querySubvector, dbSubvector)
    }
    
    distance
  }
}
```

## 8.2 构建性能优化

### 8.2.1 并行度调优

```scala
def optimizeParallelism(
  totalVectors: Long,
  vectorDimension: Int,
  availableMemoryGB: Int,
  numExecutors: Int
): ParallelismConfig = {
  
  // 每个executor可处理的向量数
  val memoryPerExecutorGB = availableMemoryGB / numExecutors
  val vectorSizeBytes = vectorDimension * 4 + 1024  // 向量 + 索引开销
  val vectorsPerExecutor = (memoryPerExecutorGB * 1e9 / vectorSizeBytes * 0.7).toLong
  
  // 计算分区数
  val numPartitions = Math.ceil(totalVectors.toDouble / vectorsPerExecutor).toInt
  
  ParallelismConfig(
    numPartitions = numPartitions,
    vectorsPerPartition = vectorsPerExecutor,
    executorMemory = s"${memoryPerExecutorGB}g"
  )
}
```

### 8.2.2 Checkpoint优化

```scala
def buildWithSmartCheckpoint(
  spark: SparkSession,
  config: IndexConfig
): IndexMetadata = {
  
  val checkpointInterval = 10  // 每10个文件checkpoint一次
  var processedFiles = 0
  
  val allFiles = listDataFiles(config.dataPath)
  val batches = allFiles.grouped(checkpointInterval).toArray
  
  val allLocalIndices = batches.flatMap { batch =>
    val batchPath = s"${config.outputPath}/checkpoints/batch_$processedFiles"
    
    if (checkpointExists(batchPath)) {
      println(s"Loading checkpoint for files $processedFiles to ${processedFiles + batch.length}")
      loadCheckpoint(spark, batchPath)
    } else {
      val indices = buildLocalIndicesForBatch(spark, batch, config)
      saveCheckpoint(indices, batchPath)
      indices
    }
    
    processedFiles += batch.length
  }
  
  buildGlobalIndexAndMetadata(spark, allLocalIndices, config)
}
```

---

# 第9章：实施路线图

## 9.1 阶段划分

### Phase 1: MVP (8周)

**目标**: 验证核心技术可行性

**交付物**:
1. Local HNSW构建 (基于hnswlib Java binding)
2. Global HNSW构建 (简化版，无跨文件边优化)
3. 基本查询接口
4. 元数据格式v1.0
5. 单元测试和集成测试

**里程碑**:
- Week 2: Local HNSW构建完成
- Week 4: Global HNSW构建完成
- Week 6: 查询接口完成
- Week 8: MVP发布，召回率 > 90%

### Phase 2: 生产就绪 (8周)

**目标**: 性能优化和稳定性增强

**交付物**:
1. 容错机制 (checkpoint, retry)
2. 性能优化 (并行度调优, 缓存)
3. 监控和日志
4. 完整文档
5. Benchmark测试套件

**里程碑**:
- Week 10: 容错机制完成
- Week 12: 性能优化完成，QPS提升2x
- Week 14: 监控和文档完成
- Week 16: 生产发布，召回率 > 95%

### Phase 3: Iceberg集成 (6周)

**目标**: 与数据湖深度集成

**交付物**:
1. Iceberg元数据扩展
2. 查询优化器集成
3. 增量索引更新
4. 索引版本管理

**里程碑**:
- Week 18: Metadata扩展完成
- Week 20: 查询优化器集成完成
- Week 22: 增量更新完成，正式发布

### Phase 4: 高级特性 (持续)

**功能列表**:
1. 向量压缩 (PQ/OPQ)
2. GPU加速
3. 过滤条件下的ANN
4. 多模态向量支持
5. 实时索引更新

## 9.2 技术选型建议

### 9.2.1 HNSW实现

**选项1: hnswlib** (推荐MVP)
- 优点: 成熟稳定，性能好
- 缺点: C++库，需要JNI binding
- 建议: 用于快速验证

**选项2: 纯Scala实现**
- 优点: 易于定制和调试
- 缺点: 性能可能不如C++
- 建议: Phase 2考虑重写关键路径

### 9.2.2 存储格式

**Local Index**: 二进制格式 (紧凑，快速)
**Global Index**: Parquet (便于Spark处理)
**Metadata**: JSON (人类可读)

## 9.3 资源需求

### 9.3.1 人力

- 后端工程师 × 2 (Scala/Spark专家)
- 算法工程师 × 1 (ANN算法专家)
- 测试工程师 × 1

### 9.3.2 计算资源

**开发环境**:
- Spark集群: 10节点，每节点32GB内存
- 存储: 10TB S3

**生产环境**:
- Spark集群: 100节点，每节点64GB内存
- 存储: 100TB S3

## 9.4 风险与缓解

| 风险 | 影响 | 概率 | 缓解措施 |
|------|------|------|----------|
| HNSW性能不达标 | 高 | 中 | 提前做POC验证 |
| 内存不足 | 高 | 中 | 实现向量压缩 |
| 召回率低 | 高 | 低 | 调整M和ef参数，增加nprobe |
| Iceberg兼容性问题 | 中 | 中 | 与Iceberg社区沟通 |
| 增量更新复杂度高 | 中 | 高 | Phase 3再实现，先支持批量重建 |

## 9.5 成功指标

**性能指标**:
- 查询延迟: P95 < 1s (百万级数据)
- 召回率: Recall@10 > 95%
- 构建速度: 100万向量/小时

**质量指标**:
- 单元测试覆盖率 > 80%
- 文档完整度 > 90%
- Bug密度 < 1 / KLOC

**业务指标**:
- 用户采用率 > 50% (6个月内)
- 查询满意度 > 4.5/5
- 相比暴力搜索加速 > 100x

---

**文档结束**

以上是完整的Spark分布式ANN索引系统设计文档。如需进一步细化某个章节或添加特定内容，请告知。
