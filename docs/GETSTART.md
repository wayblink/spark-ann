# 入门指南 (Getting Started)

> 本文档帮你 **10 分钟内** 完成第一次 spark-ann 体验：离线建一个向量索引、用 Spark 查询、把它部署成在线 HTTP 服务、用 curl 查询。
>
> 已经熟悉概念？直接看 [`README.md`](../README.md) 的 "PySpark" 和 "Online Serving (Pattern B)" 章节，或读 [`BUNDLE_SPEC.md`](BUNDLE_SPEC.md) 了解 on-disk 契约。

---

## 0. 准备 (Prerequisites)

| 必装 | 版本要求 | 用途 |
|---|---|---|
| **Java** | 11 或 17 | JVM runtime |
| **sbt** | 1.9.x | 构建 |
| **curl** | 任意 | 调 HTTP API |
| **jq** | 任意 | 看 JSON 响应 |

可选：

- **Python 3.9+** + `pip` —— 走 PySpark 路径需要
- **Docker 20.10+** + `docker compose` —— 走 Docker 路径需要

检查一下：

```bash
java -version    # 需要 11+
sbt -version
curl --version
jq --version
```

第一次跑前 **构建 fat JAR**（约 30 秒）：

```bash
cd <repo-root>
sbt 'sparkIntegration/assembly' 'apiServer/assembly'
```

产物：

```
spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar   # ~14M
api-server/target/scala-2.12/spark-ann-api-server-assembly.jar           # ~47M
```

---

## 1. 核心概念速览

| 术语 | 一句话解释 |
|---|---|
| **bundle** | 一个目录里的所有索引文件（`ann_index.json` + `local/*.hnsw` + `global/*` + `boundary_mapping.json`），完整契约见 [BUNDLE_SPEC.md](BUNDLE_SPEC.md) |
| **local index** | 一个 HNSW 索引文件，对应一组 parquet 文件 |
| **global routing index** | 小型路由 HNSW（仅边界节点），用来路由查询到正确的 local index |
| **pk** | 用户主键列 (INT32/INT64)。设置后，搜索结果的 `id` 就是你的业务 id，不再是 HNSW 内部行号 |
| **Pattern B** | 离线 Spark 建 bundle → 在线 api-server 直接读 bundle，serve 端不需要 Spark |

数据流：

```
原始 parquet → Spark 离线建索引 → bundle 目录（HDFS/S3/本地）
                                       ↓
                          ┌──────────────┴──────────────┐
                          ↓                              ↓
                离线 batchSearch（Spark）          api-server（无 Spark）
                                                         ↓
                                                    HTTP 查询
```

---

## 2. 路径 A：Scala / Spark 用户

> 适合：用 Scala 后端、Spark Job、希望走 sbt 工具链的工程师。

### 2.1 离线建索引

最小例子（在 `spark-shell` 或者 sbt 项目里）：

```scala
import org.apache.spark.sql.SparkSession
import com.wayblink.ann.bundle.ANNIndexConfig
import com.wayblink.ann.spark.api.ANNIndexAPI

val spark = SparkSession.builder().master("local[4]").appName("hello").getOrCreate()
import spark.implicits._

// 准备数据。pk 必须是 Long/Int，vector 是 Array[Float] 或 Seq[Float]
val rows = (0 until 1000).map { i =>
  val pk = 10000000L + i.toLong * 7L
  val vec = Array.fill(32)(scala.util.Random.nextFloat()).toSeq
  (pk, vec)
}
val df = rows.toDF("pk", "vector")

// 配置：pk 列要传字段名，HNSW 会用它做内部 id
val cfg = ANNIndexConfig(
  M = 16,
  efConstruction = 100,
  distanceType = "euclidean",
  pk = Some("pk")
)

val meta = ANNIndexAPI.buildIndex(df, vectorColumn = "vector",
  outputPath = "/tmp/my-bundle", config = cfg)

println(s"built ${meta.totalVectors} vectors across ${meta.localIndexes.length} local indexes")
```

执行后 `/tmp/my-bundle/` 下生成符合 BUNDLE_SPEC.md 的目录结构。

### 2.2 离线查询（batchSearch）

```scala
import com.wayblink.ann.spark.api.ANNIndexAPI

val queries = Seq(
  (0, Array.fill(32)(0.5f).toSeq),
  (1, Array.fill(32)(0.3f).toSeq)
).toDF("query_id", "vector")

val results = ANNIndexAPI.batchSearch(
  spark, indexPath = "/tmp/my-bundle",
  queries = queries, queryVectorColumn = "vector",
  k = 5, nprobe = 3
)
results.show(false)
// +----------+--------+----------+--------+
// |queryIndex|      id|  distance| indexId|
// +----------+--------+----------+--------+
// |         0|10000007|  1.456...|idx_part|
// |         0|10004921|  1.487...|idx_part|
// ...
```

**注意 `id` 列**：因为我们建索引时设置了 `pk = Some("pk")`，这里返回的 id 就是我们插入的业务 id（10000000 + i*7 形态），不是 HNSW 内部行号。

### 2.3 用 spark-submit 提交到集群

> ⚠️ **本节需要本地装 Apache Spark 3.5**（提供 `spark-submit` 命令）。如果你只想本机跑通而没装 Spark，跳到 §2.4，用 `sbt sparkIntegration/Test/runMain ...` 等效入口。

把上面的 Scala 代码打成你自己的 jar，或直接复用现成的 `OfflineSmoke` 例子：

```bash
spark-submit \
  --class com.wayblink.ann.spark.examples.OfflineSmoke \
  --jars spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar \
  --master "local[4]" \
  spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar \
  /tmp/my-bundle 1000 32
```

参数：`<output-path> <num-vectors> <dim>`。

集群模式（YARN/K8s）：

```bash
spark-submit \
  --master yarn --deploy-mode cluster \
  --jars spark-ann-integration-assembly.jar \
  --class your.app.MainClass \
  spark-ann-integration-assembly.jar
```

### 2.4 没装 Spark 时的本地等效入口

如果你只想在开发机上跑通离线建索引（无 `spark-submit`），用 sbt 的测试-scope runMain：

```bash
sbt 'sparkIntegration/Test/runMain com.wayblink.ann.spark.examples.OfflineSmoke /tmp/my-bundle 1000 32'
```

为什么是 `Test/runMain` 而不是 `runMain`？— Spark 在 `build.sbt` 里是 `% Provided`，所以 main scope 的 classpath 没有 Spark；Test scope 把 Spark 完整带回来。这是开发友好路径，**生产仍走 §2.3 的 spark-submit**。

### 2.5 常见配置项

| 参数 | 默认 | 说明 |
|---|---|---|
| `M` | 16 | HNSW 每节点连边数。越大召回越高、内存越多 |
| `efConstruction` | 200 | 建索引精度。越大越准、越慢 |
| `groupingStrategy` | `SingleFile` | `SingleFile`（一文件一索引）或 `MergeSmall`（合并小文件） |
| `boundaryNodesPerIndex` | 50 | 每个 local index 抽多少边界节点喂给路由索引 |
| `distanceType` | `"euclidean"` | `"euclidean"` 或 `"cosine"` |
| `pk` | `None` | 主键列名。如果是 `None`，结果的 `id` 是 parquet 行号；如果是 `Some("xxx")`，结果的 `id` 就是该列的值（要求 INT32/INT64） |

---

## 3. 路径 B：PySpark 用户

> 适合：用 Python 数据栈、Jupyter、ML 工程师。
>
> ⚠️ **Python 版本兼容性**：PySpark 3.5 官方支持 Python 3.8 - 3.11。Python 3.13 / 3.14 上 PySpark 仍可装，但部分 worker-side 行为（特别是 cloudpickle 相关）未经充分测试。推荐先在 **3.9 ~ 3.11** 上跑通；3.12+ 视为实验性。我们 CI 验过 8/8 PyTest 通过在 3.8。

### 3.1 安装

```bash
# 一次性
cd <repo-root>
pip install -e python
```

这会装 `pyspark-ann` 包。它本身很小（~10KB），**不打包 JVM JAR** —— JAR 通过 `--jars` 传给 Spark。

### 3.2 第一个查询

```python
import os
import random
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, ArrayType, FloatType
import spark_ann   # 导入时自动给 DataFrame 注入方法

# JAR 必须在 Spark classpath 上
JAR = os.path.abspath("spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar")
spark = (SparkSession.builder
         .master("local[2]")
         .config("spark.jars", JAR)
         .getOrCreate())

# 准备数据 —— pk 列声明为 LongType
schema = StructType([
    StructField("pk", LongType(), False),
    StructField("vector", ArrayType(FloatType()), False),
])
rng = random.Random(13)
rows = [(10000000 + i * 7, [rng.random() for _ in range(32)]) for i in range(1000)]
df = spark.createDataFrame(rows, schema=schema)

# 函数式 API
import spark_ann
meta = spark_ann.build_ann_index(
    df, vector_column="vector", output_path="/tmp/my-bundle",
    config={"M": 16, "ef_construction": 100, "pk": "pk"},
)
print(f"built {int(meta.totalVectors())} vectors")

# 单查询
hits = spark_ann.ann_search(spark, "/tmp/my-bundle",
                             query_vector=[0.5] * 32, k=5)
hits.show()
```

### 3.3 DataFrame 方法风格

`import spark_ann` 之后 DataFrame 上多出两套调用方式（同一段 JVM 逻辑）：

```python
# 风格 1：扁平方法
df.build_ann_index("vector", "/tmp/my-bundle", {"pk": "pk"})
df.ann_search("/tmp/my-bundle", [0.5] * 32, k=5)
queries_df.ann_batch_search("/tmp/my-bundle", "vector", k=5)

# 风格 2：accessor 命名空间
df.ann.build_index("vector", "/tmp/my-bundle", {"pk": "pk"})
df.ann.search("/tmp/my-bundle", [0.5] * 32, k=5)
queries_df.ann.batch_search("/tmp/my-bundle", "vector", k=5)
```

完整配置 key 见 [`python/README.md`](../python/README.md)。

### 3.4 在 spark-submit 里跑 Python 脚本

```bash
spark-submit \
  --jars spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar \
  --master "local[4]" \
  my_script.py
```

---

## 4. 路径 C：上线 — 部署 api-server 作为常驻服务

> 适合：已经有 bundle 文件，想以 HTTP 服务的形式查询。**不需要 Spark**。

### 4.1 本地最小启动

```bash
ANN_SERVER_PORT=18080 \
java -jar api-server/target/scala-2.12/spark-ann-api-server-assembly.jar
```

启动日志大约长这样（关键字 `bound to`）：

```
INFO  AnnApiServer$ - ANN API Server started at http://0.0.0.0:18080
```

健康检查：

```bash
curl http://localhost:18080/api/v1/health/ready
# {"ready":true}
```

### 4.2 加载一个 bundle

假设你刚在路径 A 或 B 中生成了 `/tmp/my-bundle`：

```bash
curl -X POST http://localhost:18080/api/v1/indexes/bundle \
  -H "Content-Type: application/json" \
  -d '{
    "indexId": "products",
    "bundlePath": "/tmp/my-bundle"
  }'
# {"message":"Bundle 'products' loaded from /tmp/my-bundle","success":true}
```

查看已加载的所有索引：

```bash
curl http://localhost:18080/api/v1/indexes | jq
# {
#   "indexes": [{
#     "kind": "bundle",
#     "indexId": "products",
#     "algorithm": "hnsw",
#     "distanceType": "euclidean",
#     "dimension": 32,
#     "size": 1000,
#     "hasGlobalIndex": true,
#     "numLocalIndexes": 2,
#     ...
#   }],
#   "totalIndexes": 1,
#   "totalVectors": 1000
# }
```

> `kind` 字段告诉你这是 `"bundle"`。

### 4.3 在线查询

```bash
# 注意 vector 数组长度必须等于 bundle 的 dimension
VECTOR='[0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5,0.5]'
curl -X POST http://localhost:18080/api/v1/indexes/products/search \
  -H "Content-Type: application/json" \
  -d "{\"vector\":$VECTOR,\"k\":5}" | jq
# {
#   "indexId": "products",
#   "queryTimeMs": 5,
#   "results": [
#     {"id": 10000896, "distance": 1.5629},
#     {"id": 10005474, "distance": 1.5807},
#     ...
#   ]
# }
```

`id` 字段直接就是你之前 `pk` 列的值（10000000 + i*7 形态）。

### 4.4 Docker Compose 一键起

适合演示和 CI。`docker-compose.yml` 已经写好：

```bash
docker compose up -d
```

会起两个服务：

| 服务 | 端口 | 描述 |
|---|---|---|
| `api-server` | 8080 | REST API |
| `web-ui`     | 80   | React 仪表盘（连接到 api-server） |

挂卷 `index-data` 到 `/data/indexes`，把你的 bundle 拷进去：

```bash
docker cp /tmp/my-bundle spark-ann-api:/data/indexes/my-bundle
curl -X POST http://localhost:8080/api/v1/indexes/bundle \
  -H "Content-Type: application/json" \
  -d '{"indexId":"products","bundlePath":"/data/indexes/my-bundle"}'
```

停服务：`docker compose down`。

---

## 5. 我应该选哪条路径？

| 你的场景 | 推荐路径 |
|---|---|
| 已有 Spark 集群、用 Scala | 路径 A |
| 已有 Spark 集群、用 Python | 路径 B |
| 演示 / hackathon / 单机 | 路径 A + C（spark local + api-server） |
| 真生产，离线大量数据 | 路径 A or B（建索引）+ 路径 C 部署 api-server 副本 |
| 没数据，只想看接口 | `docker compose up -d` 然后调 `/api/v1` |

### 一键跑通 A + C 的脚本

如果你只是想**看一眼整条流水线**到底什么样，跑这个：

```bash
examples/shell/run_e2e_smoke.sh
```

它会自动：preflight → 构 JAR（如缺）→ sbt 调 `OfflineSmoke` 建 bundle → 启 api-server → POST 加载 → POST 查询 → 验证 pk passthrough → 退出清理。约 30 秒完成（首次构 JAR 时长一些）。读它的源码是了解端到端流程最快的方式。

---

## 6. 常见错误 (Troubleshooting)

api-server 把内部错误归一化成下面这种 JSON：

```json
{ "error": "<error_code>", "message": "<human-readable>" }
```

各错误码及对应 HTTP 状态码在 §6 各子节里列出。完整 ADT 见 `api-server/src/main/scala/com/wayblink/ann/api/error/ApiError.scala`。

### `error: bundle_not_found` → HTTP 404

```bash
curl -X POST /api/v1/indexes/bundle -d '{"indexId":"foo","bundlePath":"/no/such/dir"}'
# {"error":"bundle_not_found","message":"Bundle not found at '/no/such/dir'"}
```

bundle 路径不对，或者目录里没有 `ann_index.json`。检查：

```bash
ls -la /your/bundle/path
# 应该看到 ann_index.json + local/ + global/
```

### `error: invalid_bundle` → HTTP 400（含 "newer than supported"）

bundle 是用更新版本的 spark-ann 建的，envelope `version` 超过当前 reader 已知最大。升级 api-server 这边的 spark-ann 版本，或重建 bundle。版本规则见 [`BUNDLE_SPEC.md`](BUNDLE_SPEC.md) §8。

### `error: dimension_mismatch` → HTTP 422

```bash
# 查询向量长度 != bundle 的 dimension
# {"error":"dimension_mismatch","message":"Query dimension 5 doesn't match index dimension 32"}
```

看 `GET /api/v1/indexes/<id>` 的 `dimension` 字段，对照查询向量长度。

### `error: invalid_request` → HTTP 400

最常见触发：

- 空向量 (`vector: []`)
- `k <= 0` 或 `k > 1000`
- `ef <= 0`
- 创建/加载时 `indexId` 或 `vectors`/`bundlePath` 为空

错误消息会指出具体原因。

### `error: index_already_exists` → HTTP 409

同名 indexId 已经加载。先 `DELETE /api/v1/indexes/<id>` 卸载再重新加载。

### `Pk column 'xxx' must be INT32 or INT64, got BINARY`（构建时）

这是**离线建索引**阶段的错误（不是 api-server 错误码）：你的 pk 列是 String/UUID。当前版本只支持整型 pk。可以：

1. 把字符串 pk hash 成 Long（`hash(col) & Long.MaxValue`）
2. 不传 `pk`，让 spark-ann 用顺序行号；查询结果是 parquet 行号，需要你自己反查 parquet 取业务 id

字符串 pk 通过外部 mapping table 的方案在 todo list 上，未实现。

### api-server 启动后立刻退出

看日志（默认 stdout / `$workdir/server.log`），最常见原因：

- 端口被占用：换 `ANN_SERVER_PORT=...`
- Java 版本太低：要 11 或 17

### `JavaPackage object is not callable` (PySpark)

`--jars` 没传 spark-ann-integration-assembly.jar，或路径不对。确认：

```bash
echo $PYSPARK_SUBMIT_ARGS
# 应该含 --jars /abs/path/to/spark-ann-integration-assembly.jar
```

### `ClassNotFoundException: scala.reflect.api.TypeCreator` （sbt runMain）

`sbt sparkIntegration/runMain` 失败，因为 Spark 是 `Provided` scope。改用 `sbt sparkIntegration/Test/runMain ...`，或用 `spark-submit`。

---

## 7. 下一步

- **理解契约**：[`BUNDLE_SPEC.md`](BUNDLE_SPEC.md) —— 详细的 on-disk 格式规范
- **API 全面参考**：根目录 [`README.md`](../README.md) 里的 DataFrame API + REST API 章节
- **代码示例**：[`examples/`](../examples/) 目录下 5 个 Python + 2 个 Scala 例子
- **Web UI**：起 docker compose 后访问 `http://localhost:80`
- **Swagger**：api-server 启动后访问 `http://localhost:18080/api/v1/swagger`

有问题在 issues 上提。
