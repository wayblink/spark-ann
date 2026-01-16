# 第3章：两层HNSW索引详细设计

## 3.1 HNSW算法回顾

### 3.1.1 基本原理

HNSW（Hierarchical Navigable Small World）是一种基于图的ANN算法，其核心思想是：

**分层结构**
- 构建多层图，上层稀疏，下层稠密
- 上层用于快速跳跃，下层用于精确搜索
- 类似于跳表（Skip List）的思想

**小世界网络**
- 每个节点与其最近的M个邻居连接
- 保证图的连通性和短路径特性
- 搜索复杂度：O(log N)

**贪心搜索**
- 从入口点开始，贪心地向查询向量靠近
- 在每一层找到局部最优，然后下降到下一层
- 在第0层进行精细搜索

### 3.1.2 标准HNSW参数

```python
# 构建参数
M = 16              # 每层每个节点的最大连接数
M_max_0 = 32        # 第0层的最大连接数
ef_construction = 200  # 构建时的搜索宽度
m_L = 1/ln(2)       # 层级分配参数

# 查询参数
ef_search = 50      # 查询时的搜索宽度
```

## 3.2 两层HNSW架构设计

### 3.2.1 整体结构

```
Global HNSW (跨文件)
┌────────────────────────────────────────────────┐
│ Layer 4: [f1:v42] ←→ [f3:v88]                 │  
│ Layer 3: [f1:v42] ←→ [f3:v88] ←→ [f5:v100]    │  Entry Point
│ Layer 2: [boundary nodes only]                 │  ↓
│ Layer 1: [boundary nodes + cross-file edges]   │  
└────────────────────────────────────────────────┘
              ↓           ↓           ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ Local HNSW 1 │  │ Local HNSW 2 │  │ Local HNSW 3 │
│ (File 1)     │  │ (File 2)     │  │ (File 3)     │
│ Layer 2-0    │  │ Layer 2-0    │  │ Layer 2-0    │
│ [all vectors]│  │ [all vectors]│  │ [all vectors]│
└──────────────┘  └──────────────┘  └──────────────┘
```

### 3.2.2 关键概念：Boundary Nodes

**定义**
Boundary Nodes是Local HNSW中被选中参与Global HNSW构建的特殊节点，它们充当文件间导航的"桥梁"。

**选择策略**

**策略1：高层节点选择**
```scala
def selectBoundaryNodesByLayer(localIndex: LocalHNSW, minLayer: Int = 2): Set[NodeId] = {
  localIndex.nodes
    .filter(_.maxLayer >= minLayer)
    .map(_.nodeId)
    .toSet
}
```
- 优点：自然分布，高层节点已经是"中心"节点
- 缺点：数量不可控

**策略2：固定数量选择**
```scala
def selectBoundaryNodesByCount(
  localIndex: LocalHNSW, 
  count: Int = 1000
): Set[NodeId] = {
  // 方法1: 随机采样高层节点
  val highLayerNodes = localIndex.nodes.filter(_.maxLayer >= 1)
  Random.shuffle(highLayerNodes).take(count).map(_.nodeId).toSet
  
  // 方法2: 基于中心性选择
  // 选择连接度最高的节点
  localIndex.nodes
    .sortBy(n => -n.neighbors.map(_.size).sum)
    .take(count)
    .map(_.nodeId)
    .toSet
}
```
- 优点：可控制Global HNSW的大小
- 推荐：每个文件选择 √N 个boundary节点（N为文件向量数）

**策略3：聚类中心选择**
```scala
def selectBoundaryNodesByClustering(
  localIndex: LocalHNSW,
  numClusters: Int = 100
): Set[NodeId] = {
  val vectors = localIndex.getAllVectors()
  val clusters = KMeans.fit(vectors, k = numClusters)
  
  // 选择每个cluster最靠近中心的点作为boundary node
  clusters.clusterCenters.map { center =>
    findNearestNode(localIndex, center)
  }.toSet
}
```
- 优点：更好的覆盖性
- 缺点：额外计算开销

**推荐方案**：策略2（固定数量）+ 策略1（高层节点优先）的组合
```scala
def selectBoundaryNodes(localIndex: LocalHNSW, targetCount: Int): Set[NodeId] = {
  // 先选所有Layer >= 2的节点
  val highLayerNodes = localIndex.nodes.filter(_.maxLayer >= 2)
  
  if (highLayerNodes.size >= targetCount) {
    // 如果足够，随机采样
    Random.shuffle(highLayerNodes).take(targetCount).map(_.nodeId).toSet
  } else {
    // 不够则补充Layer 1的节点
    val layer1Nodes = localIndex.nodes.filter(_.maxLayer == 1)
    val additional = Random.shuffle(layer1Nodes)
      .take(targetCount - highLayerNodes.size)
    
    (highLayerNodes ++ additional).map(_.nodeId).toSet
  }
}
```

### 3.2.3 Global HNSW构建算法

**输入**
- 所有文件的boundary nodes及其向量
- 每个boundary node的原始层级信息

**输出**
- Global HNSW图结构
- 节点到文件的映射

**算法流程**

```scala
def buildGlobalHNSW(
  boundaryNodes: Map[FileId, Set[(NodeId, Array[Float], Int)]], // (nodeId, vector, maxLayer)
  M: Int = 32,
  efConstruction: Int = 200
): GlobalHNSW = {
  
  val allNodes = boundaryNodes.values.flatten.toArray
  
  // Step 1: 确定全局入口点（选择最高层的节点）
  val entryPoint = allNodes.maxBy(_._3)._1
  val globalMaxLayer = allNodes.map(_._3).max
  
  // Step 2: 初始化图结构
  val graph = new mutable.HashMap[NodeId, Array[mutable.Set[NodeId]]]()
  allNodes.foreach { case (nodeId, _, maxLayer) =>
    graph(nodeId) = Array.fill(maxLayer + 1)(mutable.Set.empty[NodeId])
  }
  
  // Step 3: 逐个插入节点（标准HNSW插入算法）
  allNodes.foreach { case (nodeId, vector, maxLayer) =>
    if (nodeId != entryPoint) {
      insertNode(graph, nodeId, vector, maxLayer, entryPoint, M, efConstruction)
    }
  }
  
  // Step 4: 添加跨文件边的权重提升
  // 优先保留连接不同文件的边，增强全局连通性
  enhanceCrossFileEdges(graph, boundaryNodes)
  
  GlobalHNSW(
    entryPoint = entryPoint,
    graph = graph.toMap,
    nodeToFile = allNodes.map { case (nodeId, _, _) => 
      nodeId -> getFileId(nodeId)
    }.toMap,
    maxLayer = globalMaxLayer
  )
}

// 标准HNSW插入算法
def insertNode(
  graph: mutable.HashMap[NodeId, Array[mutable.Set[NodeId]]],
  nodeId: NodeId,
  vector: Array[Float],
  maxLayer: Int,
  entryPoint: NodeId,
  M: Int,
  efConstruction: Int
): Unit = {
  var ep = entryPoint
  
  // 从顶层下降到目标节点的maxLayer
  for (layer <- graph(entryPoint).length - 1 to maxLayer + 1 by -1) {
    ep = searchLayer(graph, vector, ep, 1, layer)(0)
  }
  
  // 在maxLayer到0层插入连接
  for (layer <- maxLayer to 0 by -1) {
    val candidates = searchLayer(graph, vector, ep, efConstruction, layer)
    val M_layer = if (layer == 0) 2 * M else M
    
    // 选择最近的M个邻居
    val neighbors = selectNeighbors(candidates, M_layer, vector)
    
    // 添加双向边
    neighbors.foreach { neighbor =>
      graph(nodeId)(layer).add(neighbor)
      graph(neighbor)(layer).add(nodeId)
      
      // 修剪邻居的连接以保持M约束
      pruneConnections(graph, neighbor, layer, M_layer)
    }
    
    ep = candidates(0)
  }
}
```

**跨文件边增强策略**
```scala
def enhanceCrossFileEdges(
  graph: mutable.HashMap[NodeId, Array[mutable.Set[NodeId]]],
  boundaryNodes: Map[FileId, Set[(NodeId, Array[Float], Int)]]
): Unit = {
  
  graph.foreach { case (nodeId, layers) =>
    layers.zipWithIndex.foreach { case (neighbors, layer) =>
      val sameFileEdges = neighbors.filter(n => getFileId(n) == getFileId(nodeId))
      val crossFileEdges = neighbors.filter(n => getFileId(n) != getFileId(nodeId))
      
      // 如果跨文件边太少，主动添加
      val targetCrossFileRatio = 0.3
      val currentRatio = crossFileEdges.size.toDouble / neighbors.size
      
      if (currentRatio < targetCrossFileRatio && neighbors.size < M) {
        // 从其他文件的boundary nodes中寻找候选
        val otherFiles = boundaryNodes.keys.filter(_ != getFileId(nodeId))
        // ... 添加跨文件连接的逻辑
      }
    }
  }
}
```

### 3.2.4 Local HNSW构建算法

**特点**
- 标准HNSW构建算法
- 完全独立，可并行构建
- 保存完整的图结构（所有层）

**实现**
```scala
def buildLocalHNSW(
  vectors: Array[Array[Float]],
  M: Int = 16,
  efConstruction: Int = 200
): LocalHNSW = {
  
  val nodes = vectors.zipWithIndex.map { case (vector, id) =>
    val layer = selectLayer(mL = 1.0 / Math.log(2))
    Node(id, vector, layer)
  }
  
  val graph = new mutable.HashMap[Long, Array[mutable.Set[Long]]]()
  val maxLayer = nodes.map(_.maxLayer).max
  val entryPoint = nodes.maxBy(_.maxLayer).id
  
  // 初始化图
  nodes.foreach { node =>
    graph(node.id) = Array.fill(node.maxLayer + 1)(mutable.Set.empty[Long])
  }
  
  // 逐个插入节点
  nodes.foreach { node =>
    if (node.id != entryPoint) {
      insertNodeLocal(graph, node, nodes, entryPoint, M, efConstruction)
    }
  }
  
  LocalHNSW(
    fileId = fileId,
    graph = graph.toMap,
    vectors = vectors,
    entryPoint = entryPoint,
    maxLayer = maxLayer
  )
}

def selectLayer(mL: Double): Int = {
  (-Math.log(Random.nextDouble()) * mL).toInt
}
```

## 3.3 查询算法

### 3.3.1 两阶段查询流程

```scala
def search(
  query: Array[Float],
  k: Int,
  globalIndex: GlobalHNSW,
  localIndices: Map[FileId, LocalHNSW],
  efSearch: Int = 50,
  nprobe: Int = 3
): Array[SearchResult] = {
  
  // Phase 1: Global navigation
  val candidateFiles = navigateGlobalLayer(
    query, 
    globalIndex, 
    nprobe
  )
  
  // Phase 2: Local search (parallel)
  val localResults = candidateFiles.par.flatMap { fileId =>
    val localIndex = localIndices(fileId)
    searchLocalLayer(query, localIndex, k * 2, efSearch)
      .map(r => (fileId, r))
  }
  
  // Phase 3: Global re-ranking
  localResults
    .sortBy(_._2.distance)
    .take(k)
    .map { case (fileId, result) =>
      SearchResult(fileId, result.vectorId, result.distance)
    }
    .toArray
}
```

### 3.3.2 Global Layer导航

**目标**：找到最可能包含近邻的nprobe个文件

```scala
def navigateGlobalLayer(
  query: Array[Float],
  globalIndex: GlobalHNSW,
  nprobe: Int
): Set[FileId] = {
  
  var currentNodes = Set(globalIndex.entryPoint)
  var visited = Set.empty[NodeId]
  
  // 从顶层下降到Layer 1
  for (layer <- globalIndex.maxLayer to 1 by -1) {
    val (newCurrent, newVisited) = searchLayerGlobal(
      query,
      globalIndex,
      currentNodes,
      visited,
      layer,
      efSearch = if (layer > 1) 1 else nprobe * 3
    )
    currentNodes = newCurrent
    visited = newVisited
  }
  
  // 收集最近的nprobe个节点对应的文件
  val candidateNodes = currentNodes.toArray
    .sortBy(nodeId => distance(query, getVector(nodeId)))
    .take(nprobe * 5)
  
  // 返回这些节点所属的文件，去重后取前nprobe个
  candidateNodes
    .map(globalIndex.nodeToFile(_))
    .distinct
    .take(nprobe)
    .toSet
}

def searchLayerGlobal(
  query: Array[Float],
  globalIndex: GlobalHNSW,
  entryPoints: Set[NodeId],
  visited: Set[NodeId],
  layer: Int,
  efSearch: Int
): (Set[NodeId], Set[NodeId]) = {
  
  var candidates = mutable.PriorityQueue.empty[(NodeId, Float)](
    Ordering.by(-_._2)  // Max heap
  )
  var w = mutable.PriorityQueue.empty[(NodeId, Float)](
    Ordering.by(_._2)   // Min heap
  )
  var v = visited
  
  entryPoints.foreach { ep =>
    val dist = distance(query, getVector(ep))
    candidates.enqueue((ep, dist))
    w.enqueue((ep, dist))
    v += ep
  }
  
  while (candidates.nonEmpty) {
    val (c, cDist) = candidates.dequeue()
    val (f, fDist) = w.head
    
    if (cDist > fDist) {
      // 所有候选都已探索完
      break
    }
    
    // 探索c的邻居
    globalIndex.graph(c)(layer).foreach { neighbor =>
      if (!v.contains(neighbor)) {
        v += neighbor
        val nDist = distance(query, getVector(neighbor))
        
        if (nDist < fDist || w.size < efSearch) {
          candidates.enqueue((neighbor, nDist))
          w.enqueue((neighbor, nDist))
          
          if (w.size > efSearch) {
            w.dequeue()  // 移除最远的
          }
        }
      }
    }
  }
  
  (w.map(_._1).toSet, v)
}
```

### 3.3.3 Local Layer搜索

**标准HNSW查询**
```scala
def searchLocalLayer(
  query: Array[Float],
  localIndex: LocalHNSW,
  k: Int,
  efSearch: Int
): Array[LocalSearchResult] = {
  
  var ep = localIndex.entryPoint
  
  // 从顶层贪心下降
  for (layer <- localIndex.maxLayer to 1 by -1) {
    ep = searchLayerLocal(localIndex, query, ep, 1, layer)(0)
  }
  
  // 在Layer 0进行宽度搜索
  val results = searchLayerLocal(localIndex, query, ep, efSearch, 0)
  
  results
    .sortBy(_.distance)
    .take(k)
}

def searchLayerLocal(
  localIndex: LocalHNSW,
  query: Array[Float],
  entryPoint: Long,
  efSearch: Int,
  layer: Int
): Array[LocalSearchResult] = {
  // 与searchLayerGlobal类似，但操作Local graph
  // ... 实现细节省略
}
```

## 3.4 性能优化技巧

### 3.4.1 向量缓存

```scala
class VectorCache(maxSize: Int = 10000) {
  private val cache = new mutable.LRUCache[NodeId, Array[Float]](maxSize)
  
  def getVector(nodeId: NodeId, loader: NodeId => Array[Float]): Array[Float] = {
    cache.getOrElseUpdate(nodeId, loader(nodeId))
  }
}
```

### 3.4.2 距离计算优化

```scala
// 使用SIMD加速（通过JNI调用原生代码）
def distanceL2SIMD(a: Array[Float], b: Array[Float]): Float = {
  NativeLib.l2Distance(a, b)
}

// 或使用Java Vector API (JDK 16+)
import jdk.incubator.vector._

def distanceL2Vector(a: Array[Float], b: Array[Float]): Float = {
  val species = FloatVector.SPECIES_PREFERRED
  var sum = 0.0f
  var i = 0
  
  while (i < a.length) {
    val va = FloatVector.fromArray(species, a, i)
    val vb = FloatVector.fromArray(species, b, i)
    val diff = va.sub(vb)
    sum += diff.mul(diff).reduceLanes(VectorOperators.ADD)
    i += species.length()
  }
  
  Math.sqrt(sum).toFloat
}
```

### 3.4.3 并行搜索优化

```scala
// 使用Spark的并行计算
def searchParallel(
  query: Array[Float],
  candidateFiles: Set[FileId],
  k: Int
): Array[SearchResult] = {
  
  val sc = SparkContext.getOrCreate()
  val candidateFilesRDD = sc.parallelize(candidateFiles.toSeq)
  
  val results = candidateFilesRDD.flatMap { fileId =>
    val localIndex = loadLocalIndex(fileId)  // 每个executor加载
    searchLocalLayer(query, localIndex, k * 2, efSearch = 50)
      .map(r => (fileId, r))
  }
  
  results
    .takeOrdered(k)(Ordering.by(_._2.distance))
    .map { case (fileId, result) =>
      SearchResult(fileId, result.vectorId, result.distance)
    }
}
```

---

**下一章预告**：第4章将详细定义元数据格式规范，包括JSON schema和Parquet表结构。
