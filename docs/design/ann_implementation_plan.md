# ANN索引系统完整实现计划

## 总体策略：三阶段渐进式演进

```
Phase 1: Core Library (Week 1-4)
  ├── 核心HNSW算法
  ├── 两层索引架构
  └── DataFrame API集成

Phase 2: SQL Extension (Week 5-8)
  ├── 优化器规则注入
  ├── 自动查询识别
  └── Iceberg集成

Phase 3: Native Acceleration (Week 9-12, 可选)
  ├── JNI接口
  ├── SIMD优化
  └── 零拷贝传输
```

---

## Phase 1: Core Library (Week 1-4)

### Week 1: 基础设施与核心封装

#### Step 1.1: 项目框架搭建 (Day 1)

**目标**: 建立多模块项目结构

**项目结构**:
```
spark-ann-index/
├── build.sbt
├── core/                          # 核心算法（不依赖Spark）
│   └── src/main/scala/
│       ├── index/                 # HNSW实现
│       ├── storage/               # 存储层
│       └── metadata/              # 元数据管理
├── spark-integration/             # Spark集成
│   └── src/main/scala/
│       ├── api/                   # DataFrame API
│       ├── rdd/                   # RDD操作
│       └── builder/               # 索引构建
├── spark-sql-extension/           # SQL扩展（Phase 2）
│   └── src/main/scala/
│       ├── optimizer/             # 优化规则
│       ├── execution/             # 物理算子
│       └── expressions/           # 内置函数
└── native/                        # Native加速（Phase 3）
    ├── cpp/
    └── jni/
```

**build.sbt配置**:
```scala
lazy val root = (project in file("."))
  .aggregate(core, sparkIntegration, sparkSqlExtension)

lazy val core = (project in file("core"))
  .settings(
    name := "spark-ann-core",
    libraryDependencies ++= Seq(
      "com.github.jelmerk" % "hnswlib-core" % "1.1.0",
      "org.json4s" %% "json4s-jackson" % "4.0.6",
      "org.scalatest" %% "scalatest" % "3.2.15" % Test
    )
  )

lazy val sparkIntegration = (project in file("spark-integration"))
  .dependsOn(core)
  .settings(
    name := "spark-ann-integration",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.0" % Provided,
      "org.apache.spark" %% "spark-core" % "3.5.0" % Provided
    )
  )

lazy val sparkSqlExtension = (project in file("spark-sql-extension"))
  .dependsOn(sparkIntegration)
  .settings(
    name := "spark-ann-sql-extension"
  )
```

**验证测试**:
```scala
// core/src/test/scala/SparkEnvironmentTest.scala
class SparkEnvironmentTest extends AnyFunSuite with BeforeAndAfterAll {
  
  var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[4]")
      .appName("ANNIndexTest")
      .config("spark.driver.memory", "2g")
      .config("spark.sql.adaptive.enabled", "false")  // 简化测试
      .getOrCreate()
  }
  
  test("Spark session should be created") {
    assert(spark != null)
    assert(spark.version.startsWith("3."))
  }
  
  test("Can read and write Parquet") {
    import spark.implicits._
    
    val data = Seq((1, Array(0.1f, 0.2f, 0.3f)), (2, Array(0.4f, 0.5f, 0.6f)))
    val df = data.toDF("id", "vector")
    
    val path = "/tmp/test_vectors.parquet"
    df.write.mode("overwrite").parquet(path)
    
    val readDf = spark.read.parquet(path)
    assert(readDf.count() == 2)
    assert(readDf.columns.contains("vector"))
  }
  
  test("Project structure is correct") {
    // 验证模块依赖
    val coreClass = Class.forName("com.company.ann.core.index.HNSWIndex")
    assert(coreClass != null)
  }
  
  override def afterAll(): Unit = {
    spark.stop()
  }
}
```

**产出**: 
- ✅ 多模块项目结构
- ✅ 构建系统配置
- ✅ 基础测试通过

---

#### Step 1.2: 测试数据生成器 (Day 1)

**目标**: 生成多种场景的测试数据

**实现** (core模块):
```scala
// core/src/main/scala/testutil/TestDataGenerator.scala
package com.company.ann.core.testutil

object TestDataGenerator {
  
  /**
   * 生成随机向量数据
   */
  def generateRandomVectors(
    numVectors: Int,
    dimension: Int,
    seed: Long = 42
  ): Array[(Long, Array[Float])] = {
    val random = new Random(seed)
    (0L until numVectors).map { id =>
      val vector = Array.fill(dimension)(random.nextFloat())
      (id, vector)
    }.toArray
  }
  
  /**
   * 生成聚类向量数据（高召回率测试）
   */
  def generateClusteredVectors(
    numClusters: Int,
    vectorsPerCluster: Int,
    dimension: Int,
    spread: Float = 0.1f,
    seed: Long = 42
  ): Array[(Long, Array[Float])] = {
    val random = new Random(seed)
    
    // 生成聚类中心
    val centers = (0 until numClusters).map { _ =>
      Array.fill(dimension)(random.nextFloat())
    }
    
    // 在每个中心周围生成向量
    centers.zipWithIndex.flatMap { case (center, clusterId) =>
      (0 until vectorsPerCluster).map { i =>
        val noise = Array.fill(dimension)(random.nextGaussian().toFloat * spread)
        val vector = center.zip(noise).map { case (c, n) => c + n }
        val id = clusterId * vectorsPerCluster + i
        (id.toLong, vector)
      }
    }.toArray
  }
  
  /**
   * 生成真实分布的向量（模拟Word2Vec/BERT embeddings）
   */
  def generateRealisticVectors(
    numVectors: Int,
    dimension: Int,
    sparsity: Double = 0.3  // 30%的维度接近0
  ): Array[(Long, Array[Float])] = {
    val random = new Random(42)
    
    (0L until numVectors).map { id =>
      val vector = Array.fill(dimension) {
        if (random.nextDouble() < sparsity) {
          random.nextGaussian().toFloat * 0.01f  // 接近0
        } else {
          random.nextGaussian().toFloat * 0.5f   // 正态分布
        }
      }
      // L2归一化（模拟BERT）
      val norm = math.sqrt(vector.map(x => x * x).sum).toFloat
      val normalized = vector.map(_ / norm)
      (id, normalized)
    }.toArray
  }
}
```

**Spark集成** (spark-integration模块):
```scala
// spark-integration/src/main/scala/testutil/SparkTestData.scala
package com.company.ann.spark.testutil

import com.company.ann.core.testutil.TestDataGenerator
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkTestData {
  
  def generateAndSave(
    spark: SparkSession,
    numVectors: Int,
    dimension: Int,
    path: String,
    dataType: String = "clustered"
  ): DataFrame = {
    import spark.implicits._
    
    val vectors = dataType match {
      case "random" => 
        TestDataGenerator.generateRandomVectors(numVectors, dimension)
      case "clustered" => 
        TestDataGenerator.generateClusteredVectors(
          numClusters = math.sqrt(numVectors).toInt,
          vectorsPerCluster = math.sqrt(numVectors).toInt,
          dimension = dimension
        )
      case "realistic" =>
        TestDataGenerator.generateRealisticVectors(numVectors, dimension)
    }
    
    val df = vectors.toSeq.toDF("id", "vector")
    df.write.mode("overwrite").parquet(path)
    
    spark.read.parquet(path)
  }
  
  def loadTestData(spark: SparkSession, path: String): DataFrame = {
    spark.read.parquet(path)
  }
}
```

**测试用例**:
```scala
class TestDataGeneratorTest extends AnyFunSuite {
  
  test("Generate random vectors") {
    val vectors = TestDataGenerator.generateRandomVectors(1000, 128)
    
    assert(vectors.length == 1000)
    assert(vectors.head._2.length == 128)
    assert(vectors.forall(_._2.forall(v => v >= 0f && v <= 1f)))
    
    // 验证随机性
    val firstVector = vectors.head._2
    val secondVector = vectors(1)._2
    assert(!firstVector.sameElements(secondVector))
  }
  
  test("Generate clustered vectors with good intra-cluster similarity") {
    val vectors = TestDataGenerator.generateClusteredVectors(
      numClusters = 5,
      vectorsPerCluster = 20,
      dimension = 64
    )
    
    assert(vectors.length == 100)
    
    // 验证聚类特性
    val cluster0 = vectors.take(20).map(_._2)
    val cluster1 = vectors.slice(20, 40).map(_._2)
    
    val intraDistance = avgPairwiseDistance(cluster0)
    val interDistance = avgCrossDistance(cluster0, cluster1)
    
    assert(intraDistance < interDistance, 
      s"Intra-cluster distance ($intraDistance) should be less than inter-cluster ($interDistance)")
  }
  
  test("Generate realistic vectors with sparsity") {
    val vectors = TestDataGenerator.generateRealisticVectors(100, 128, sparsity = 0.3)
    
    vectors.foreach { case (_, vector) =>
      // 验证L2归一化
      val norm = math.sqrt(vector.map(x => x * x).sum)
      assert(math.abs(norm - 1.0) < 0.001, s"Vector should be normalized, got norm=$norm")
      
      // 验证稀疏性
      val nearZeroCount = vector.count(math.abs(_) < 0.05)
      val sparsityRatio = nearZeroCount.toDouble / vector.length
      assert(sparsityRatio > 0.2, s"Expected ~30% sparsity, got $sparsityRatio")
    }
  }
  
  // 辅助函数
  def avgPairwiseDistance(vectors: Array[Array[Float]]): Double = {
    val distances = for {
      i <- vectors.indices
      j <- (i + 1) until vectors.length
    } yield l2Distance(vectors(i), vectors(j))
    distances.sum / distances.length
  }
  
  def avgCrossDistance(v1: Array[Array[Float]], v2: Array[Array[Float]]): Double = {
    val distances = for {
      vec1 <- v1
      vec2 <- v2
    } yield l2Distance(vec1, vec2)
    distances.sum / distances.length
  }
  
  def l2Distance(a: Array[Float], b: Array[Float]): Float = {
    math.sqrt(a.zip(b).map { case (x, y) => 
      val diff = x - y
      diff * diff
    }.sum).toFloat
  }
}
```

**产出**:
- ✅ 3种测试数据生成器
- ✅ Spark集成的数据工具
- ✅ 完整的验证测试

---

### Week 2: Local HNSW核心实现

#### Step 2.1: HNSW算法封装 (Day 2-3)

**目标**: 封装hnswlib，提供统一接口

**核心接口设计** (core模块):
```scala
// core/src/main/scala/index/HNSWIndex.scala
package com.company.ann.core.index

trait HNSWIndex {
  def dimension: Int
  def size: Int
  
  def add(id: Long, vector: Array[Float]): Unit
  def addAll(vectors: Seq[(Long, Array[Float])]): Unit
  
  def search(query: Array[Float], k: Int, ef: Int = 50): Seq[SearchResult]
  
  def save(path: String): Unit
  def load(path: String): Unit
}

case class SearchResult(
  id: Long,
  distance: Float
)

case class HNSWConfig(
  M: Int = 16,
  efConstruction: Int = 200,
  maxElements: Int = 1000000,
  randomSeed: Long = 42
)
```

**基于hnswlib的实现**:
```scala
// core/src/main/scala/index/HNSWLibIndex.scala
package com.company.ann.core.index

import com.github.jelmerk.knn.hnsw._
import scala.collection.JavaConverters._

class HNSWLibIndex(
  val dimension: Int,
  config: HNSWConfig
) extends HNSWIndex {
  
  private val index = HnswIndex.newBuilder(
    dimension,
    DistanceFunctions.FLOAT_EUCLIDEAN_DISTANCE,
    config.maxElements
  )
    .withM(config.M)
    .withEfConstruction(config.efConstruction)
    .withRandomSeed(config.randomSeed)
    .build()
  
  override def size: Int = index.size()
  
  override def add(id: Long, vector: Array[Float]): Unit = {
    require(vector.length == dimension, 
      s"Vector dimension ${vector.length} doesn't match index dimension $dimension")
    index.add(new FloatVectorItem(id.toString, vector))
  }
  
  override def addAll(vectors: Seq[(Long, Array[Float])]): Unit = {
    vectors.foreach { case (id, vector) => add(id, vector) }
  }
  
  override def search(query: Array[Float], k: Int, ef: Int = 50): Seq[SearchResult] = {
    require(query.length == dimension,
      s"Query dimension ${query.length} doesn't match index dimension $dimension")
    
    index.setEf(ef)
    val results = index.findNearest(query, k)
    
    results.asScala.map { r =>
      SearchResult(
        id = r.item().id().toLong,
        distance = r.distance()
      )
    }.toSeq
  }
  
  override def save(path: String): Unit = {
    index.save(new java.io.File(path))
  }
  
  override def load(path: String): Unit = {
    // hnswlib需要重新构建index来加载
    // 这里简化处理，实际应该序列化整个索引状态
    throw new UnsupportedOperationException("Load not yet implemented")
  }
}

object HNSWLibIndex {
  def apply(dimension: Int, config: HNSWConfig = HNSWConfig()): HNSWLibIndex = {
    new HNSWLibIndex(dimension, config)
  }
}
```

**测试用例**:
```scala
// core/src/test/scala/index/HNSWIndexTest.scala
class HNSWIndexTest extends AnyFunSuite {
  
  test("Build and search small index") {
    val vectors = TestDataGenerator.generateRandomVectors(1000, 128)
    val index = HNSWLibIndex(dimension = 128)
    
    index.addAll(vectors)
    assert(index.size == 1000)
    
    // 查询第一个向量（应该找到自己）
    val query = vectors.head._2
    val results = index.search(query, k = 5)
    
    assert(results.nonEmpty)
    assert(results.head.id == vectors.head._1)
    assert(results.head.distance < 0.001f, "Should find exact match")
  }
  
  test("Search returns k results") {
    val vectors = TestDataGenerator.generateRandomVectors(500, 64)
    val index = HNSWLibIndex(dimension = 64)
    index.addAll(vectors)
    
    val query = Array.fill(64)(0.5f)
    val results = index.search(query, k = 10)
    
    assert(results.length == 10)
    assert(results.map(_.distance).sliding(2).forall {
      case Seq(a, b) => a <= b  // 结果应该按距离排序
    })
  }
  
  test("High recall on clustered data") {
    val vectors = TestDataGenerator.generateClusteredVectors(
      numClusters = 10,
      vectorsPerCluster = 100,
      dimension = 128
    )
    
    val index = HNSWLibIndex(dimension = 128)
    index.addAll(vectors)
    
    // 用聚类中心查询
    val cluster0Vectors = vectors.take(100)
    val queryVector = cluster0Vectors.head._2
    val k = 20
    
    val indexResults = index.search(queryVector, k)
    val groundTruth = bruteForceKNN(queryVector, vectors, k)
    
    val recall = calculateRecall(indexResults, groundTruth)
    assert(recall > 0.9, s"Recall $recall is too low")
  }
  
  test("Different ef values affect recall") {
    val vectors = TestDataGenerator.generateClusteredVectors(10, 100, 64)
    val index = HNSWLibIndex(dimension = 64)
    index.addAll(vectors)
    
    val query = vectors(50)._2
    val k = 10
    
    val results_ef10 = index.search(query, k, ef = 10)
    val results_ef50 = index.search(query, k, ef = 50)
    val results_ef100 = index.search(query, k, ef = 100)
    
    val groundTruth = bruteForceKNN(query, vectors, k)
    
    val recall10 = calculateRecall(results_ef10, groundTruth)
    val recall50 = calculateRecall(results_ef50, groundTruth)
    val recall100 = calculateRecall(results_ef100, groundTruth)
    
    println(s"Recall@ef=10: $recall10")
    println(s"Recall@ef=50: $recall50")
    println(s"Recall@ef=100: $recall100")
    
    assert(recall50 >= recall10, "Higher ef should have better or equal recall")
    assert(recall100 >= recall50)
  }
  
  // 辅助函数
  def bruteForceKNN(
    query: Array[Float],
    vectors: Array[(Long, Array[Float])],
    k: Int
  ): Seq[SearchResult] = {
    vectors.map { case (id, vector) =>
      SearchResult(id, l2Distance(query, vector))
    }.sortBy(_.distance).take(k)
  }
  
  def calculateRecall(
    results: Seq[SearchResult],
    groundTruth: Seq[SearchResult]
  ): Double = {
    val resultIds = results.map(_.id).toSet
    val truthIds = groundTruth.map(_.id).toSet
    resultIds.intersect(truthIds).size.toDouble / groundTruth.length
  }
  
  def l2Distance(a: Array[Float], b: Array[Float]): Float = {
    math.sqrt(a.zip(b).map { case (x, y) =>
      val diff = x - y
      diff * diff
    }.sum).toFloat
  }
}
```

**产出**:
- ✅ 统一的HNSW接口
- ✅ hnswlib实现
- ✅ 召回率 > 90%

---

#### Step 2.2: Spark集成 - Local Index构建 (Day 4-5)

**目标**: 在Spark中并行构建Local索引

**实现** (spark-integration模块):
```scala
// spark-integration/src/main/scala/builder/LocalIndexBuilder.scala
package com.company.ann.spark.builder

import com.company.ann.core.index.{HNSWLibIndex, HNSWConfig}
import org.apache.spark.TaskContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object LocalIndexBuilder {
  
  /**
   * 为DataFrame的每个分区构建Local HNSW索引
   */
  def buildFromDataFrame(
    df: DataFrame,
    vectorColumn: String,
    indexOutputPath: String,
    config: HNSWConfig = HNSWConfig()
  ): Array[LocalIndexMetadata] = {
    
    import df.sparkSession.implicits._
    
    // 获取向量维度
    val firstRow = df.first()
    val dimension = firstRow.getAs[Seq[Float]](vectorColumn).length
    
    println(s"Building local indices with dimension=$dimension")
    
    val metadata = df.select($"id", col(vectorColumn).as("vector"))
      .rdd
      .mapPartitionsWithIndex { (partitionId, rows) =>
        buildIndexForPartition(
          partitionId,
          rows,
          dimension,
          indexOutputPath,
          config
        )
      }
      .collect()
    
    println(s"Built ${metadata.length} local indices")
    metadata
  }
  
  private def buildIndexForPartition(
    partitionId: Int,
    rows: Iterator[org.apache.spark.sql.Row],
    dimension: Int,
    outputPath: String,
    config: HNSWConfig
  ): Iterator[LocalIndexMetadata] = {
    
    val vectors = rows.map { row =>
      val id = row.getLong(0)
      val vector = row.getSeq[Float](1).toArray
      (id, vector)
    }.toArray
    
    if (vectors.isEmpty) {
      println(s"[Partition $partitionId] Empty partition, skipping")
      return Iterator.empty
    }
    
    println(s"[Partition $partitionId] Building index for ${vectors.length} vectors")
    
    // 构建HNSW索引
    val index = HNSWLibIndex(dimension, config.copy(maxElements = vectors.length * 2))
    index.addAll(vectors)
    
    // 保存索引
    val indexPath = s"$outputPath/local/partition_${partitionId}.hnsw"
    new java.io.File(indexPath).getParentFile.mkdirs()
    index.save(indexPath)
    
    println(s"[Partition $partitionId] Index saved to $indexPath")
    
    Iterator(LocalIndexMetadata(
      partitionId = partitionId,
      indexPath = indexPath,
      numVectors = vectors.length,
      dimension = dimension
    ))
  }
}

case class LocalIndexMetadata(
  partitionId: Int,
  indexPath: String,
  numVectors: Int,
  dimension: Int
)
```

**测试用例**:
```scala
// spark-integration/src/test/scala/builder/LocalIndexBuilderTest.scala
class LocalIndexBuilderTest extends AnyFunSuite with BeforeAndAfterAll {
  
  var spark: SparkSession = _
  
  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .master("local[4]")
      .appName("LocalIndexBuilderTest")
      .getOrCreate()
  }
  
  test("Build indices for single partition") {
    val df = SparkTestData.generateAndSave(
      spark,
      numVectors = 1000,
      dimension = 128,
      path = "/tmp/test_single_partition",
      dataType = "random"
    ).repartition(1)
    
    val metadata = LocalIndexBuilder.buildFromDataFrame(
      df,
      vectorColumn = "vector",
      indexOutputPath = "/tmp/test_index_single"
    )
    
    assert(metadata.length == 1)
    assert(metadata.head.numVectors == 1000)
    assert(new java.io.File(metadata.head.indexPath).exists())
  }
  
  test("Build indices for multiple partitions") {
    val df = SparkTestData.generateAndSave(
      spark,
      numVectors = 5000,
      dimension = 64,
      path = "/tmp/test_multi_partition",
      dataType = "clustered"
    ).repartition(5)
    
    val metadata = LocalIndexBuilder.buildFromDataFrame(
      df,
      vectorColumn = "vector",
      indexOutputPath = "/tmp/test_index_multi"
    )
    
    assert(metadata.length == 5)
    assert(metadata.map(_.numVectors).sum == 5000)
    
    // 验证所有索引文件存在
    metadata.foreach { meta =>
      val file = new java.io.File(meta.indexPath)
      assert(file.exists(), s"Index file ${meta.indexPath} should exist")
      assert(file.length() > 0, "Index file should not be empty")
    }
  }
  
  test("Query built local index") {
    val vectors = TestDataGenerator.generateClusteredVectors(5, 200, 128)
    import spark.implicits._
    val df = vectors.toSeq.toDF("id", "vector").repartition(1)
    
    val metadata = LocalIndexBuilder.buildFromDataFrame(
      df,
      vectorColumn = "vector",
      indexOutputPath = "/tmp/test_query_local"
    ).head
    
    // 加载索引并查询
    val index = HNSWLibIndex(128)
    index.load(metadata.indexPath)
    
    val query = vectors.head._2
    val results = index.search(query, k = 10)
    
    assert(results.nonEmpty)
    assert(results.head.id == vectors.head._1)
  }
  
  test("Handle empty partitions gracefully") {
    val df = SparkTestData.generateAndSave(
      spark,
      numVectors = 100,
      dimension = 64,
      path = "/tmp/test_empty_partitions",
      dataType = "random"
    ).repartition(20)  // 很多空分区
    
    val metadata = LocalIndexBuilder.buildFromDataFrame(
      df,
      vectorColumn = "vector",
      indexOutputPath = "/tmp/test_index_empty"
    )
    
    assert(metadata.length < 20, "Should skip empty partitions")
    assert(metadata.length > 0, "Should have some non-empty partitions")
    assert(metadata.map(_.numVectors).sum == 100)
  }
  
  override def afterAll(): Unit = {
    spark.stop()
  }
}
```

**产出**:
- ✅ Spark并行Local Index构建
- ✅ 支持多分区
- ✅ 空分区处理

---

### Week 3: Global Index和DataFrame API

#### Step 3.1: Boundary Nodes选择 (Day 6)

**实现** (core模块):
```scala
// core/src/main/scala/index/BoundaryNodeSelector.scala
package com.company.ann.core.index

import scala.util.Random

object BoundaryNodeSelector {
  
  /**
   * 从向量集合中选择boundary nodes
   * 策略：随机采样 + 可选的聚类优化
   */
  def selectBoundaryNodes(
    vectors: Array[(Long, Array[Float])],
    targetCount: Int,
    strategy: String = "random"  // "random", "kmeans", "high_degree"
  ): Array[BoundaryNode] = {
    
    val actualCount = math.min(targetCount, vectors.length)
    
    strategy match {
      case "random" =>
        Random.shuffle(vectors.toSeq)
          .take(actualCount)
          .map { case (id, vector) => BoundaryNode(id, vector) }
          .toArray
      
      case "distributed" =>
        // 均匀分布采样
        val step = vectors.length.toDouble / actualCount
        (0 until actualCount).map { i =>
          val idx = (i * step).toInt
          val (id, vector) = vectors(idx)
          BoundaryNode(id, vector)
        }.toArray
      
      case _ =>
        throw new IllegalArgumentException(s"Unknown strategy: $strategy")
    }
  }
}

case class BoundaryNode(
  localId: Long,
  vector: Array[Float]
)
```

**Spark集成**:
```scala
// spark-integration/src/main/scala/builder/BoundaryNodesCollector.scala
package com.company.ann.spark.builder

import com.company.ann.core.index.{BoundaryNode, BoundaryNodeSelector}
import org.apache.spark.sql.{DataFrame, SparkSession}

object BoundaryNodesCollector {
  
  /**
   * 从每个分区收集boundary nodes
   */
  def collectFromDataFrame(
    df: DataFrame,
    vectorColumn: String,
    nodesPerPartition: Int
  ): Array[GlobalBoundaryNode] = {
    
    import df.sparkSession.implicits._
    
    val boundaryNodes = df.select($"id", col(vectorColumn).as("vector"))
      .rdd
      .mapPartitionsWithIndex { (partitionId, rows) =>
        val vectors = rows.map { row =>
          (row.getLong(0), row.getSeq[Float](1).toArray)
        }.toArray
        
        if (vectors.isEmpty) {
          Iterator.empty
        } else {
          val selected = BoundaryNodeSelector.selectBoundaryNodes(
            vectors,
            nodesPerPartition,
            strategy = "distributed"
          )
          
          selected.map { node =>
            GlobalBoundaryNode(
              globalId = s"part_$partitionId:${node.localId}",
              partitionId = partitionId,
              localId = node.localId,
              vector = node.vector
            )
          }.iterator
        }
      }
      .collect()
    
    println(s"Collected ${boundaryNodes.length} boundary nodes from ${df.rdd.getNumPartitions} partitions")
    boundaryNodes
  }
}

case class GlobalBoundaryNode(
  globalId: String,
  partitionId: Int,
  localId: Long,
  vector: Array[Float]
)
```

**产出**:
- ✅ Boundary nodes选择算法
- ✅ 跨分区收集

---

#### Step 3.2: DataFrame API实现 (Day 7-8)

**目标**: 提供用户友好的API

**实现**:
```scala
// spark-integration/src/main/scala/api/ANNDataFrameAPI.scala
package com.company.ann.spark.api

import org.apache.spark.sql.DataFrame
import com.company.ann.spark.search.ANNSearcher

/**
 * DataFrame扩展：提供annSearch方法
 */
implicit class ANNDataFrameExtensions(df: DataFrame) {
  
  /**
   * 使用ANN索引进行相似性搜索
   * 
   * @param indexPath 索引路径
   * @param queryVector 查询向量
   * @param k 返回结果数
   * @param nprobe 搜索的分区数（默认3）
   * @return 包含查询结果的DataFrame
   */
  def annSearch(
    indexPath: String,
    queryVector: Array[Float],
    k: Int,
    nprobe: Int = 3
  ): DataFrame = {
    
    val searcher = ANNSearcher.load(df.sparkSession, indexPath)
    searcher.search(queryVector, k, nprobe)
  }
  
  /**
   * 批量ANN搜索
   */
  def annBatchSearch(
    indexPath: String,
    queries: DataFrame,
    queryVectorColumn: String,
    k: Int,
    nprobe: Int = 3
  ): DataFrame = {
    
    val searcher = ANNSearcher.load(df.sparkSession, indexPath)
    searcher.batchSearch(queries, queryVectorColumn, k, nprobe)
  }
}

/**
 * 构建ANN索引的API
 */
object ANNIndexAPI {
  
  /**
   * 为DataFrame构建ANN索引
   */
  def buildIndex(
    df: DataFrame,
    vectorColumn: String,
    outputPath: String,
    config: ANNIndexConfig = ANNIndexConfig()
  ): ANNIndexMetadata = {
    
    val builder = new ANNIndexBuilder(df.sparkSession)
    builder.build(df, vectorColumn, outputPath, config)
  }
}

case class ANNIndexConfig(
  M: Int = 16,
  efConstruction: Int = 200,
  numPartitions: Option[Int] = None,  // None表示使用DataFrame现有分区
  boundaryNodesPerPartition: Int = 50
)
```

**完整示例**:
```scala
// spark-integration/src/main/scala/examples/QuickStart.scala
object QuickStart {
  
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ANN Quick Start")
      .master("local[4]")
      .getOrCreate()
    
    import com.company.ann.spark.api._
    
    // 1. 生成测试数据
    val vectors = SparkTestData.generateAndSave(
      spark,
      numVectors = 10000,
      dimension = 128,
      path = "/tmp/vectors",
      dataType = "clustered"
    )
    
    // 2. 构建索引
    println("Building ANN index...")
    val metadata = ANNIndexAPI.buildIndex(
      df = vectors,
      vectorColumn = "vector",
      outputPath = "/tmp/ann_index",
      config = ANNIndexConfig(M = 16, efConstruction = 200)
    )
    
    println(s"Index built: ${metadata.statistics.totalVectors} vectors indexed")
    
    // 3. 查询
    println("\nQuerying index...")
    val queryVector = Array.fill(128)(scala.util.Random.nextFloat())
    
    val results = vectors.annSearch(
      indexPath = "/tmp/ann_index",
      queryVector = queryVector,
      k = 10,
      nprobe = 3
    )
    
    results.show()
    
    spark.stop()
  }
}
```

**产出**:
- ✅ DataFrame API
- ✅ 简单易用的接口
- ✅ 完整示例

---

### Week 4: 元数据和端到端集成

*(继续按原计划执行Step 4.1-4.3)*

---

## Phase 2: SQL Extension (Week 5-8)

### Week 5: 优化器规则

#### Step 5.1: 自定义LogicalPlan节点 (Day 13)

**实现** (spark-sql-extension模块):
```scala
// spark-sql-extension/src/main/scala/logical/ANNLogicalPlan.scala
package com.company.ann.spark.sql.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * 逻辑计划节点：ANN索引扫描
 */
case class ANNIndexScan(
  table: CatalogTable,
  queryVector: Expression,  // 查询向量表达式
  k: Int,                   // top-k
  vectorColumn: String,     // 向量列名
  indexVersion: Option[String] = None,
  searchParams: Map[String, String] = Map.empty,
  output: Seq[Attribute]
) extends LeafNode {
  
  override lazy val resolved: Boolean = {
    table != null && queryVector.resolved && output.nonEmpty
  }
  
  override def computeStats(): org.apache.spark.sql.catalyst.plans.logical.Statistics = {
    // 估算只返回k行
    val sizeInBytes = BigInt(k) * output.map(_.dataType.defaultSize).sum
    org.apache.spark.sql.catalyst.plans.logical.Statistics(
      sizeInBytes = sizeInBytes,
      rowCount = Some(BigInt(k))
    )
  }
}
```

---

#### Step 5.2: 优化规则实现 (Day 14-15)

**实现**:
```scala
// spark-sql-extension/src/main/scala/optimizer/ANNOptimizationRule.scala
package com.company.ann.spark.sql.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

case class ANNOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {
  
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    
    // 模式: SELECT * FROM table ORDER BY distance(vector, query) LIMIT k
    case Limit(Literal(k: Int, _),
         Sort(Seq(SortOrder(distanceExpr, Ascending, _, _)), true,
           Project(projectList,
             relation @ LogicalRelation(_, _, Some(table), _))))
      if isANNEligible(distanceExpr, table) =>
      
      val queryVector = extractQueryVector(distanceExpr)
      val vectorColumn = extractVectorColumn(distanceExpr)
      
      ANNIndexScan(
        table = table,
        queryVector = queryVector,
        k = k,
        vectorColumn = vectorColumn,
        output = projectList.map(_.toAttribute)
      )
  }
  
  private def isANNEligible(expr: Expression, table: CatalogTable): Boolean = {
    // 检查是否是distance函数
    val isDistanceFunc = expr match {
      case ScalaUDF(_, _, _, _, _, _, _) => true
      case _ => false
    }
    
    // 检查表是否有ANN索引
    val hasIndex = table.properties.contains("ann-index.current-index-id")
    
    isDistanceFunc && hasIndex
  }
  
  private def extractQueryVector(expr: Expression): Expression = {
    // 从distance(vector, query)中提取query
    expr match {
      case ScalaUDF(_, _, Seq(_, queryVector), _, _, _, _) => queryVector
      case _ => throw new IllegalArgumentException("Cannot extract query vector")
    }
  }
  
  private def extractVectorColumn(expr: Expression): String = {
    expr match {
      case ScalaUDF(_, _, Seq(AttributeReference(name, _, _, _), _), _, _, _, _) => name
      case _ => throw new IllegalArgumentException("Cannot extract vector column")
    }
  }
}
```

**注册扩展**:
```scala
// spark-sql-extension/src/main/scala/ANNSparkExtension.scala
package com.company.ann.spark.sql

import org.apache.spark.sql.SparkSessionExtensions
import com.company.ann.spark.sql.optimizer.ANNOptimizationRule
import com.company.ann.spark.sql.strategy.ANNIndexScanStrategy

class ANNSparkExtension extends (SparkSessionExtensions => Unit) {
  
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // 注入优化规则
    extensions.injectOptimizerRule { session =>
      ANNOptimizationRule(session)
    }
    
    // 注入执行策略
    extensions.injectPlannerStrategy { session =>
      ANNIndexScanStrategy(session)
    }
    
    // 注册内置函数
    extensions.injectFunction(
      ("l2_distance", new ExpressionInfo(
        "com.company.ann.spark.sql.expressions.L2Distance",
        "l2_distance",
        "l2_distance(vector1, vector2) - Calculate L2 distance"
      ),
      builder => new L2Distance(builder.head, builder(1)))
    )
  }
}
```

**使用方式**:
```scala
// spark-defaults.conf
spark.sql.extensions=com.company.ann.spark.sql.ANNSparkExtension

// 或代码中注册
val spark = SparkSession.builder()
  .config("spark.sql.extensions", "com.company.ann.spark.sql.ANNSparkExtension")
  .getOrCreate()

// 自动优化
spark.sql("""
  SELECT * FROM vectors
  ORDER BY l2_distance(embedding, array(0.1, 0.2, ...))
  LIMIT 10
""")
```

**产出**:
- ✅ 自动识别ANN查询
- ✅ 查询计划优化
- ✅ SQL透明使用

---

### Week 6-8: Iceberg集成和测试

*(按照设计文档第7章实施)*

---

## Phase 3: Native Acceleration (Week 9-12, 可选)

### 评估标准

**只有满足以下条件才启动Phase 3**:
1. ✅ Phase 1召回率 > 90%
2. ✅ Phase 2查询优化生效
3. ❌ 查询延迟仍是瓶颈（P95 > 500ms for 1M vectors）
4. ❌ 距离计算占用 > 60%的执行时间
5. ✅ 有专门的C++工程师资源

### 如果启动Phase 3

**Week 9: JNI接口设计**
- Native HNSW接口定义
- Java/Scala binding
- 内存管理策略

**Week 10-11: Native实现**
- C++ HNSW实现（或集成hnswlib）
- SIMD优化距离计算
- 多线程并行搜索

**Week 12: 集成测试**
- 性能对比
- 稳定性测试
- 生产验证

---

## 关键里程碑检查点

### ✅ Milestone 1: Core Library (Week 4结束)
- [ ] Local HNSW召回率 > 90%
- [ ] Global Index构建成功
- [ ] DataFrame API可用
- [ ] 端到端测试通过

### ✅ Milestone 2: SQL Extension (Week 8结束)
- [ ] 自动识别ANN查询
- [ ] 查询计划优化生效
- [ ] Iceberg集成完成
- [ ] 文档完整

### 🚀 Milestone 3: Native Acceleration (Week 12结束, 可选)
- [ ] 查询延迟降低50%+
- [ ] 吞吐提升2x+
- [ ] 生产环境稳定运行

---

## 开发最佳实践

### 1. 测试驱动开发
```scala
// 先写测试
test("should find top-k nearest neighbors") {
  // Given
  val vectors = generateTestVectors(1000, 128)
  
  // When
  val results = index.search(query, k=10)
  
  // Then
  assert(results.length == 10)
  assert(calculateRecall(results, groundTruth) > 0.9)
}

// 再写实现
def search(query: Array[Float], k: Int): Seq[Result] = {
  // Implementation
}
```

### 2. 渐进式数据规模
```scala
// 开发：100向量（秒级反馈）
val devData = generateVectors(100, 64)

// 验证：10K向量（分钟级）
val testData = generateVectors(10000, 128)

// 压测：100K+向量（小时级）
val prodData = generateVectors(100000, 256)
```

### 3. 性能基准对比
```scala
// 始终与baseline对比
val bruteForceTime = measureTime { bruteForceSearch() }
val indexTime = measureTime { indexSearch() }

assert(indexTime < bruteForceTime * 0.1,  // 至少10x加速
  s"Index search ($indexTime ms) should be faster than brute force ($bruteForceTime ms)")
```

### 4. 日志和监控
```scala
// 关键路径添加日志
logger.info(s"Building index for ${vectors.length} vectors")
logger.info(s"Selected ${candidatePartitions.size} partitions: $candidatePartitions")
logger.info(s"Query completed in ${duration}ms, recall=$recall")

// 性能指标
metrics.recordLatency("ann.search", duration)
metrics.recordRecall("ann.search", recall)
```

---

## 风险缓解策略

| 风险 | 检测时间 | 缓解措施 |
|------|----------|----------|
| hnswlib不稳定 | Week 2 | 准备纯Scala fallback实现 |
| 召回率不达标 | Week 3 | 调整M/ef参数，增加nprobe |
| 内存溢出 | Week 3 | 减小分区大小，启用溢出到磁盘 |
| Extension API不兼容 | Week 5 | 降级到UDF方式 |
| 性能不达标 | Week 8 | 启动Phase 3 Native加速 |

---

## 总结

### 核心原则
1. **Phase 1优先** - 快速验证核心价值
2. **渐进演进** - 不过早优化
3. **测试驱动** - 每步都可验证
4. **性能基准** - 数据说话

### 下一步行动
1. ✅ 创建项目结构（Day 1上午）
2. ✅ 搭建测试框架（Day 1下午）
3. ✅ 实现HNSW封装（Day 2-3）
4. → 开始Spark集成（Day 4起）

祝开发顺利！🚀
