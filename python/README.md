# pyspark-ann

PySpark bindings for [`spark-ann`](../README.md) — a distributed HNSW vector
index built on Apache Spark.

The package is a thin Py4J wrapper over the Scala/JVM API. All heavy
lifting (parquet streaming, distributed build, lazy index loading,
boundary-node sampling) happens on the JVM; this layer just translates
Python types and DataFrame handles.

## Install

```bash
pip install pyspark-ann
```

The JVM JAR is **not** bundled in the wheel. Provide it to Spark explicitly:

```bash
# From a local build:
pyspark --jars /path/to/spark-ann-integration-assembly.jar

# From Maven (once published):
pyspark --packages com.wayblink:spark-ann-integration_2.12:0.1.0
```

For `spark-submit`:

```bash
spark-submit --jars spark-ann-integration-assembly.jar your_script.py
```

For programmatic SparkSession creation, set `spark.jars` or
`spark.driver.extraClassPath` + `spark.executor.extraClassPath` in your
session builder. Without the JAR on the classpath, any `spark_ann` call
raises with a hint.

## Usage

### Functional API

```python
import spark_ann
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[4]")\
    .config("spark.jars", "/path/to/spark-ann-integration-assembly.jar")\
    .getOrCreate()

# Build an index from a DataFrame
df = spark.createDataFrame(
    [(i, [0.1 * i] * 128) for i in range(10_000)],
    ["id", "vector"],
)
meta = spark_ann.build_ann_index(
    df,
    vector_column="vector",
    output_path="hdfs://ns/indexes/demo",
    config={"M": 16, "ef_construction": 200, "distance_type": "cosine"},
)

# Single-query search
results = spark_ann.ann_search(
    spark,
    "hdfs://ns/indexes/demo",
    query_vector=[0.5] * 128,
    k=10,
)
results.show()

# Distributed batch search
queries = spark.createDataFrame(
    [(0, [0.1] * 128), (1, [0.9] * 128)],
    ["qid", "vector"],
)
batch = spark_ann.ann_batch_search(
    spark,
    "hdfs://ns/indexes/demo",
    queries,
    query_vector_column="vector",
    k=5,
)
batch.show()
```

### DataFrame method injection

Importing `spark_ann` installs methods directly on
`pyspark.sql.DataFrame`. Two styles are available; pick whichever fits
your house style:

```python
import spark_ann  # installs both styles

# Flat methods
df.build_ann_index("vector", "/tmp/idx", {"M": 32})
results = df.ann_search("/tmp/idx", [0.5] * 128, k=10)
batch = queries.ann_batch_search("/tmp/idx", "vector", k=5)

# Accessor namespace
df.ann.build_index("vector", "/tmp/idx", {"M": 32})
results = df.ann.search("/tmp/idx", [0.5] * 128, k=10)
batch = queries.ann.batch_search("/tmp/idx", "vector", k=5)
```

Both call the same JVM code.

## Configuration

Keys accept either snake_case (Pythonic) or camelCase (matching the Scala
`ANNIndexConfig`):

| Key | Default | Description |
|---|---|---|
| `M` | 16 | HNSW connections per node |
| `ef_construction` / `efConstruction` | 200 | Build-time accuracy |
| `grouping_strategy` / `groupingStrategy` | `"SingleFile"` | `"SingleFile"` or `"MergeSmall"` |
| `target_vectors_per_index` / `targetVectorsPerIndex` | 500000 | Vectors per local index |
| `boundary_nodes_per_index` / `boundaryNodesPerIndex` | 50 | Routing samples per local index |
| `distance_type` / `distanceType` | `"euclidean"` | `"euclidean"` or `"cosine"` |

## Development

```bash
# 1. Build the JAR
cd ..
sbt sparkIntegration/assembly

# 2. Install Python package in editable mode
cd python
pip install -e ".[dev]"

# 3. Run tests (point at the built JAR)
PYSPARK_PYTHON=$(which python) \
PYSPARK_DRIVER_PYTHON=$(which python) \
pytest tests/ -v
```

Override the JAR location with `SPARK_ANN_JAR=/abs/path/to/jar`.

## Notes

- Python `float` lists serialise as `array<double>` in Spark DataFrames.
  The JVM build path now accepts both `FLOAT` and `DOUBLE` element types
  and coerces internally to `float32`.
- `pyspark-ann` requires Python ≥ 3.8 and PySpark 3.4+.
