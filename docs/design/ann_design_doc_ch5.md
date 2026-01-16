# 第5章：索引构建流程

## 5.1 构建流程概览

索引构建分为三个主要阶段：

```
Phase 1: Local Index Construction (并行)
  ├── 读取数据文件
  ├── 构建Local HNSW
  ├── 持久化索引文件
  └── 选择Boundary Nodes

Phase 2: Global Index Construction (串行)
  ├── 收集所有Boundary Nodes
  ├── 加载Boundary Nodes的向量
  ├── 构建Global HNSW
  └── 持久化Global Index

Phase 3: Metadata Generation (串行)
  ├── 生成索引元数据
  ├── 验证完整性
  └── 发布索引版本
```

## 5.2 Phase 1: 本地索引构建

### 5.2.1 Spark作业设计

```scala
object LocalIndexBuilder {
  
  def buildLocalIndices(
    spark: SparkSession,
    dataPath: String,
    vectorColumn: String,
    outputPath: String,
    indexConfig: HNSWConfig
  ): Dataset[LocalIndexMetadata] = {
    
    import spark.implicits._
    
    // Step 1: 读取数据
    val vectorData = spark.read.parquet(dataPath)
      .select($"${vectorColumn}", monotonically_increasing_id().as("row_id"))
    
    // Step 2: 为每个分区构建索引
    val localIndices = vectorData
      .repartition(indexConfig.numPartitions)  // 控制分区数
      .mapPartitions { partition =>
        buildIndexForPartition(partition, indexConfig)
      }
    
    localIndices.cache()
    localIndices
  }
  
  def buildIndexForPartition(
    partition: Iterator[Row],
    config: HNSWConfig
  ): Iterator[LocalIndexMetadata] = {
    
    val partitionId = TaskContext.getPartitionId()
    val fileId = f"file_$partitionId%05d"
    
    // 收集分区内的所有向量
    val vectors = partition.map { row =>
      val vectorArray = row.getAs[mutable.WrappedArray[Float]](0).toArray
      val rowId = row.getLong(1)
      (rowId, vectorArray)
    }.toArray
    
    if (vectors.isEmpty) {
      return Iterator.empty
    }
    
    // 构建Local HNSW
    val localIndex = buildHNSW(vectors, config)
    
    // 选择Boundary Nodes
    val boundaryNodes = selectBoundaryNodes(
      localIndex, 
      targetCount = Math.sqrt(vectors.length).toInt
    )
    
    // 持久化索引
    val indexFilePath = s"${config.outputPath}/local/v${config.version}/$fileId.hnsw"
    saveLocalIndex(localIndex, indexFilePath)
    
    // 返回元数据
    Iterator(LocalIndexMetadata(
      fileId = fileId,
      dataFile = config.dataPath,  // 原始数据文件路径
      indexFile = indexFilePath,
      numVectors = vectors.length,
      entryPoint = localIndex.entryPoint,
      maxLayer = localIndex.maxLayer,
      boundaryNodes = boundaryNodes.map(_._1).toArray,
      boundaryVectors = boundaryNodes.map(_._2).toArray,
      hnswConfig = config,
      checksum = computeChecksum(indexFilePath)
    ))
  }
  
  def buildHNSW(
    vectors: Array[(Long, Array[Float])],
    config: HNSWConfig
  ): LocalHNSW = {
    
    val dimension = vectors.head._2.length
    val numVectors = vectors.length
    
    // 初始化图结构
    val graph = new mutable.HashMap[Long, Array[mutable.Set[Long]]]()
    val vectorMap = vectors.toMap
    
    // 为每个向量分配层级
    val nodeLayers = vectors.map { case (id, _) =>
      (id, selectLayer(config.mL))
    }
    
    val maxLayer = nodeLayers.map(_._2).max
    val entryPoint = nodeLayers.maxBy(_._2)._1
    
    // 初始化图
    nodeLayers.foreach { case (id, layer) =>
      graph(id) = Array.fill(layer + 1)(mutable.Set.empty[Long])
    }
    
    // 插入节点
    vectors.indices.foreach { i =>
      val (nodeId, vector) = vectors(i)
      val nodeLayer = nodeLayers(i)._2
      
      if (nodeId != entryPoint) {
        insertNode(
          graph = graph,
          nodeId = nodeId,
          vector = vector,
          maxLayer = nodeLayer,
          entryPoint = entryPoint,
          vectorMap = vectorMap,
          config = config
        )
      }
      
      // 进度反馈
      if (i % 10000 == 0) {
        println(s"[Partition ${TaskContext.getPartitionId()}] Inserted $i / $numVectors vectors")
      }
    }
    
    LocalHNSW(
      fileId = s"file_${TaskContext.getPartitionId()}",
      graph = graph.toMap,
      vectors = vectorMap,
      entryPoint = entryPoint,
      maxLayer = maxLayer,
      dimension = dimension
    )
  }
  
  def selectBoundaryNodes(
    localIndex: LocalHNSW,
    targetCount: Int
  ): Array[(Long, Array[Float])] = {
    
    // 策略: 优先选择高层节点
    val highLayerNodes = localIndex.graph
      .filter { case (_, layers) => layers.length >= 2 }
      .keys
      .toArray
    
    val selected = if (highLayerNodes.length >= targetCount) {
      Random.shuffle(highLayerNodes.toSeq).take(targetCount)
    } else {
      // 不够则补充Layer 1节点
      val layer1Nodes = localIndex.graph
        .filter { case (_, layers) => layers.length == 2 }
        .keys
        .toArray
      
      val additional = Random.shuffle(layer1Nodes.toSeq)
        .take(targetCount - highLayerNodes.length)
      
      highLayerNodes ++ additional
    }
    
    selected.map(id => (id, localIndex.vectors(id))).toArray
  }
}
```

### 5.2.2 内存管理

```scala
case class MemoryConfig(
  maxVectorsPerPartition: Int = 1000000,  // 每分区最大向量数
  vectorCacheSizeMB: Int = 512,           // 向量缓存大小
  graphCacheSizeMB: Int = 1024            // 图结构缓存大小
)

def estimateMemoryUsage(
  numVectors: Int,
  dimension: Int,
  M: Int,
  maxLayer: Int
): Long = {
  // 向量数据
  val vectorMemory = numVectors.toLong * dimension * 4
  
  // 图结构 (每层每个节点M个邻居)
  val avgEdges = M * maxLayer
  val graphMemory = numVectors.toLong * avgEdges * 8
  
  // 额外开销 (HashMap等数据结构)
  val overhead = (vectorMemory + graphMemory) * 0.3
  
  (vectorMemory + graphMemory + overhead).toLong
}

def adjustPartitioning(
  totalVectors: Long,
  dimension: Int,
  availableMemoryPerExecutor: Long,
  config: HNSWConfig
): Int = {
  
  val memoryPerVector = estimateMemoryUsage(
    numVectors = 1,
    dimension = dimension,
    M = config.M,
    maxLayer = 5  // 假设最大层级
  )
  
  val vectorsPerPartition = (availableMemoryPerExecutor * 0.8 / memoryPerVector).toInt
  val numPartitions = Math.ceil(totalVectors.toDouble / vectorsPerPartition).toInt
  
  println(s"Estimated memory per vector: ${memoryPerVector} bytes")
  println(s"Vectors per partition: $vectorsPerPartition")
  println(s"Recommended partitions: $numPartitions")
  
  numPartitions
}
```

### 5.2.3 容错机制

```scala
def buildWithRetry(
  spark: SparkSession,
  config: IndexConfig,
  maxRetries: Int = 3
): Dataset[LocalIndexMetadata] = {
  
  var attempt = 0
  var lastException: Option[Throwable] = None
  
  while (attempt < maxRetries) {
    try {
      println(s"Building local indices (attempt ${attempt + 1}/$maxRetries)...")
      
      val result = LocalIndexBuilder.buildLocalIndices(
        spark, 
        config.dataPath,
        config.vectorColumn,
        config.outputPath,
        config.hnswConfig
      )
      
      // 验证结果
      val count = result.count()
      println(s"Successfully built $count local indices")
      
      return result
      
    } catch {
      case e: Exception =>
        lastException = Some(e)
        attempt += 1
        
        if (attempt < maxRetries) {
          println(s"Build failed: ${e.getMessage}. Retrying...")
          Thread.sleep(5000 * attempt)  // 指数退避
        }
    }
  }
  
  throw new RuntimeException(
    s"Failed to build indices after $maxRetries attempts",
    lastException.orNull
  )
}

// Checkpoint机制
def buildWithCheckpoint(
  spark: SparkSession,
  config: IndexConfig,
  checkpointDir: String
): Dataset[LocalIndexMetadata] = {
  
  val checkpointPath = s"$checkpointDir/local_indices_checkpoint"
  
  // 检查是否有checkpoint
  if (directoryExists(checkpointPath)) {
    println(s"Resuming from checkpoint: $checkpointPath")
    spark.read.parquet(checkpointPath).as[LocalIndexMetadata]
  } else {
    val result = LocalIndexBuilder.buildLocalIndices(
      spark,
      config.dataPath,
      config.vectorColumn,
      config.outputPath,
      config.hnswConfig
    )
    
    // 保存checkpoint
    result.write.mode("overwrite").parquet(checkpointPath)
    result
  }
}
```

## 5.3 Phase 2: 全局索引构建

### 5.3.1 Boundary Nodes收集

```scala
object GlobalIndexBuilder {
  
  def buildGlobalIndex(
    spark: SparkSession,
    localIndices: Dataset[LocalIndexMetadata],
    config: GlobalIndexConfig
  ): GlobalIndexMetadata = {
    
    import spark.implicits._
    
    // Step 1: 收集所有Boundary Nodes
    val boundaryNodes = localIndices.flatMap { meta =>
      meta.boundaryNodes.zip(meta.boundaryVectors).map { case (nodeId, vector) =>
        BoundaryNode(
          nodeId = s"${meta.fileId}:$nodeId",
          fileId = meta.fileId,
          localVectorId = nodeId,
          vector = vector,
          maxLayer = estimateLayer(vector, config.mL)
        )
      }
    }.collect()
    
    println(s"Collected ${boundaryNodes.length} boundary nodes from ${localIndices.count()} files")
    
    // Step 2: 构建Global HNSW (在Driver上执行)
    val globalHNSW = buildGlobalHNSW(boundaryNodes, config)
    
    // Step 3: 持久化Global Index
    val globalPath = s"${config.outputPath}/global/v${config.version}"
    saveGlobalIndex(spark, globalHNSW, globalPath)
    
    // Step 4: 生成Global Index元数据
    GlobalIndexMetadata(
      enabled = true,
      indexPath = globalPath,
      entryPoint = globalHNSW.entryPoint,
      maxLayer = globalHNSW.maxLayer,
      numBoundaryNodes = boundaryNodes.length,
      hnswConfig = config.hnswConfig,
      storage = StorageConfig(
        topLayersFile = s"$globalPath/top_layers.parquet",
        layer0File = s"$globalPath/layer0.parquet",
        routingFile = s"$globalPath/routing.parquet"
      )
    )
  }
  
  def buildGlobalHNSW(
    boundaryNodes: Array[BoundaryNode],
    config: GlobalIndexConfig
  ): GlobalHNSW = {
    
    println(s"Building Global HNSW with ${boundaryNodes.length} nodes...")
    
    val graph = new mutable.HashMap[String, Array[mutable.Set[String]]]()
    val nodeToFile = new mutable.HashMap[String, String]()
    val vectors = boundaryNodes.map(n => n.nodeId -> n.vector).toMap
    
    // 确定入口点
    val entryPoint = boundaryNodes.maxBy(_.maxLayer).nodeId
    val maxLayer = boundaryNodes.map(_.maxLayer).max
    
    println(s"Entry point: $entryPoint, Max layer: $maxLayer")
    
    // 初始化图
    boundaryNodes.foreach { node =>
      graph(node.nodeId) = Array.fill(node.maxLayer + 1)(mutable.Set.empty[String])
      nodeToFile(node.nodeId) = node.fileId
    }
    
    // 插入节点
    boundaryNodes.zipWithIndex.foreach { case (node, i) =>
      if (node.nodeId != entryPoint) {
        insertNodeGlobal(
          graph = graph,
          nodeId = node.nodeId,
          vector = node.vector,
          maxLayer = node.maxLayer,
          entryPoint = entryPoint,
          vectors = vectors,
          config = config.hnswConfig
        )
      }
      
      if (i % 1000 == 0) {
        println(s"Inserted $i / ${boundaryNodes.length} boundary nodes into global index")
      }
    }
    
    // 增强跨文件边
    println("Enhancing cross-file edges...")
    enhanceCrossFileEdges(graph, nodeToFile)
    
    GlobalHNSW(
      entryPoint = entryPoint,
      graph = graph.map { case (k, v) => k -> v.map(_.toSet) }.toMap,
      nodeToFile = nodeToFile.toMap,
      maxLayer = maxLayer,
      vectors = vectors
    )
  }
  
  def enhanceCrossFileEdges(
    graph: mutable.HashMap[String, Array[mutable.Set[String]]],
    nodeToFile: mutable.HashMap[String, String]
  ): Unit = {
    
    val targetCrossFileRatio = 0.4  // 目标跨文件边比例
    var edgesAdded = 0
    
    graph.foreach { case (nodeId, layers) =>
      layers.zipWithIndex.foreach { case (neighbors, layer) =>
        val sameFile = neighbors.count(n => nodeToFile(n) == nodeToFile(nodeId))
        val crossFile = neighbors.size - sameFile
        val currentRatio = if (neighbors.nonEmpty) crossFile.toDouble / neighbors.size else 0.0
        
        if (currentRatio < targetCrossFileRatio && neighbors.size < 64) {
          // 从其他文件找最近的节点
          val otherFileNodes = graph.keys
            .filter(n => nodeToFile(n) != nodeToFile(nodeId))
            .filter(n => graph(n).length > layer)
            .take(100)  // 限制候选数量
          
          // 简化: 随机选择一些添加
          val toAdd = Random.shuffle(otherFileNodes.toSeq).take(2)
          toAdd.foreach { n =>
            neighbors.add(n)
            graph(n)(layer).add(nodeId)
            edgesAdded += 1
          }
        }
      }
    }
    
    println(s"Added $edgesAdded cross-file edges")
  }
}
```

### 5.3.2 Global Index持久化

```scala
def saveGlobalIndex(
  spark: SparkSession,
  globalHNSW: GlobalHNSW,
  outputPath: String
): Unit = {
  
  import spark.implicits._
  
  // 1. 保存高层图 (Layer >= 2)
  val topLayersData = globalHNSW.graph
    .filter { case (_, layers) => layers.length >= 3 }
    .map { case (nodeId, layers) =>
      val parts = nodeId.split(":")
      TopLayerNode(
        nodeId = nodeId,
        fileId = parts(0),
        localVectorId = parts(1).toLong,
        maxLayer = layers.length - 1,
        vector = globalHNSW.vectors.get(nodeId).map(_.toSeq),
        layerNeighbors = layers.zipWithIndex.map { case (neighbors, layer) =>
          LayerNeighbors(
            layer = layer,
            neighbors = neighbors.map { neighborId =>
              Neighbor(
                neighborId = neighborId,
                neighborFile = globalHNSW.nodeToFile(neighborId),
                distance = None  // 可选: 预计算距离
              )
            }.toSeq
          )
        }.toSeq
      )
    }
    .toSeq
  
  spark.createDataset(topLayersData)
    .repartition($"maxLayer")
    .write
    .mode("overwrite")
    .partitionBy("maxLayer")
    .parquet(s"$outputPath/top_layers.parquet")
  
  // 2. 保存路由表
  val routingData = globalHNSW.nodeToFile.map { case (nodeId, fileId) =>
    val parts = nodeId.split(":")
    RoutingInfo(
      nodeId = nodeId,
      fileId = fileId,
      localVectorId = parts(1).toLong,
      maxLayer = globalHNSW.graph(nodeId).length - 1,
      isBoundaryNode = true,
      vector = globalHNSW.vectors.get(nodeId).map(_.toSeq)
    )
  }.toSeq
  
  spark.createDataset(routingData)
    .repartition($"fileId")
    .write
    .mode("overwrite")
    .partitionBy("fileId")
    .parquet(s"$outputPath/routing.parquet")
  
  println(s"Global index saved to $outputPath")
}
```

## 5.4 Phase 3: 元数据生成

```scala
def generateMetadata(
  spark: SparkSession,
  config: IndexConfig,
  localIndices: Dataset[LocalIndexMetadata],
  globalIndex: GlobalIndexMetadata
): IndexMetadata = {
  
  val localMetas = localIndices.collect()
  
  // 计算统计信息
  val totalVectors = localMetas.map(_.numVectors).sum
  val totalIndexSize = localMetas.map { meta =>
    getFileSize(meta.indexFile)
  }.sum + getFileSize(globalIndex.indexPath)
  
  val metadata = IndexMetadata(
    formatVersion = "1.0",
    indexVersion = config.version,
    indexType = "hnsw",
    vectorConfig = VectorConfig(
      dimension = config.dimension,
      distanceMetric = config.distanceMetric,
      normalize = config.normalize
    ),
    dataSource = DataSource(
      basePath = config.dataPath,
      vectorColumn = config.vectorColumn,
      idColumn = config.idColumn,
      fileFormat = "parquet"
    ),
    globalIndex = globalIndex,
    localIndices = localMetas,
    buildInfo = BuildInfo(
      buildTimestamp = Instant.now().toString,
      builderVersion = "1.0.0",
      buildDurationSeconds = computeBuildDuration(),
      numVectors = totalVectors
    ),
    statistics = Statistics(
      totalVectors = totalVectors,
      numFiles = localMetas.length,
      totalIndexSizeBytes = totalIndexSize,
      avgVectorsPerFile = totalVectors.toDouble / localMetas.length
    )
  )
  
  // 验证元数据
  val validation = validateMetadata(metadata)
  validation match {
    case Valid => println("Metadata validation passed")
    case Invalid(errors) =>
      throw new RuntimeException(s"Metadata validation failed: ${errors.mkString(", ")}")
  }
  
  // 保存元数据
  val metadataPath = s"${config.outputPath}/metadata/index_${config.version}.json"
  saveMetadata(metadata, metadataPath)
  
  println(s"Metadata saved to $metadataPath")
  metadata
}

def saveMetadata(metadata: IndexMetadata, path: String): Unit = {
  val json = JsonSerializer.toJson(metadata, pretty = true)
  writeFile(path, json)
}
```

## 5.5 完整构建流程示例

```scala
object ANNIndexBuilder {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ANN Index Builder")
      .getOrCreate()
    
    val config = IndexConfig(
      dataPath = "s3://bucket/vectors/",
      vectorColumn = "embedding",
      outputPath = "s3://bucket/indices/",
      version = "v1",
      dimension = 768,
      distanceMetric = "l2",
      hnswConfig = HNSWConfig(M = 16, efConstruction = 200),
      numPartitions = 100
    )
    
    try {
      println("=" * 80)
      println("Phase 1: Building Local Indices")
      println("=" * 80)
      
      val localIndices = buildWithCheckpoint(
        spark,
        config,
        checkpointDir = config.outputPath + "/checkpoints"
      )
      
      println("\n" + "=" * 80)
      println("Phase 2: Building Global Index")
      println("=" * 80)
      
      val globalIndex = GlobalIndexBuilder.buildGlobalIndex(
        spark,
        localIndices,
        GlobalIndexConfig(
          outputPath = config.outputPath,
          version = config.version,
          mL = 1.0 / Math.log(2),
          hnswConfig = HNSWConfig(M = 32, efConstruction = 200)
        )
      )
      
      println("\n" + "=" * 80)
      println("Phase 3: Generating Metadata")
      println("=" * 80)
      
      val metadata = generateMetadata(
        spark,
        config,
        localIndices,
        globalIndex
      )
      
      println("\n" + "=" * 80)
      println("Index Build Complete!")
      println("=" * 80)
      println(s"Total vectors indexed: ${metadata.statistics.totalVectors}")
      println(s"Number of files: ${metadata.statistics.numFiles}")
      println(s"Index size: ${formatBytes(metadata.statistics.totalIndexSizeBytes)}")
      println(s"Metadata path: ${config.outputPath}/metadata/index_${config.version}.json")
      
    } finally {
      spark.stop()
    }
  }
}
```

---

**下一章预告**：第6章将介绍查询执行流程，包括查询优化和结果聚合策略。
