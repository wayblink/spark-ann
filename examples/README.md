# spark-ann Usage Examples

End-to-end runnable examples for the spark-ann library. Each script is
self-contained and exits with a non-zero status on assertion failure, so
you can wire them into smoke-test scripts.

## Prerequisites

Build the assembly JAR once:

```bash
sbt sparkIntegration/assembly
# produces: spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar
```

The JAR path used by the examples is:

```
SPARK_ANN_JAR=$(pwd)/spark-integration/target/scala-2.12/spark-ann-integration-assembly.jar
```

## Python (pyspark-ann)

Install the Python wrapper once:

```bash
pip install -e python
```

Then run each example with `python`, passing the JAR via
`PYSPARK_SUBMIT_ARGS` so it's added to both the driver and executor
classpath:

```bash
export PYSPARK_PYTHON=$(which python)
export PYSPARK_SUBMIT_ARGS="--jars $SPARK_ANN_JAR pyspark-shell"

python examples/python/01_build_and_search.py
python examples/python/02_batch_search.py
python examples/python/03_dataframe_methods.py
python examples/python/04_clustered_routing.py
```

Files:

| Script | What it shows |
|---|---|
| `python/01_build_and_search.py` | Minimal end-to-end: random vectors → build → single-query search |
| `python/02_batch_search.py` | Distributed batch search over a query DataFrame |
| `python/03_dataframe_methods.py` | P2 surface: `df.build_ann_index`, `df.ann.search`, etc. |
| `python/04_clustered_routing.py` | Verifies routing accuracy on linearly-separable clusters |
| `python/05_id_column_preservation.py` | Sets `id_column` so search results carry the user's id instead of HNSW's internal offset |

## Scala / spark-shell

The same scenarios in Scala. Launch `spark-shell` with the JAR:

```bash
spark-shell --jars $SPARK_ANN_JAR -i examples/scala/01_build_and_search.scala
spark-shell --jars $SPARK_ANN_JAR -i examples/scala/02_batch_search.scala
```

## spark-submit

For the cluster-style run:

```bash
spark-submit \
  --master "local[4]" \
  --jars $SPARK_ANN_JAR \
  --class com.wayblink.ann.spark.examples.QuickStart \
  $SPARK_ANN_JAR
```

(The `QuickStart` class is already bundled inside the assembly.)

## Preserving your `id` column through search results

By default, HNSW assigns sequential internal ids (0, 1, 2, ...) in parquet
row order. The `id` column in search results is that internal id, not the
user-supplied id column. If you want your own id back, opt in with
`id_column`:

```python
spark_ann.build_ann_index(
    df,
    vector_column="vector",
    output_path="/tmp/idx",
    config={"id_column": "id"},   # column must be parquet INT32 or INT64
)
```

The value of that column becomes the HNSW internal id, so search results
carry it back as the `id` column unchanged. See
`examples/python/05_id_column_preservation.py` for a runnable demo.

### Limitations

- `id_column` must be INT32 or INT64. String / UUID id columns will raise
  a clear error at build time; mapping-table support for arbitrary id
  types is planned but not yet implemented.
- Values must be **unique within each local index** (HNSW rejects duplicate
  ids inside one index). If you build with `MergeSmall` or split files in
  a way that could put two rows with the same user id into the same local
  index, deduplicate upstream first.

When `id_column` is unset, the previous sequential-offset behavior is
preserved exactly, so existing pipelines keep working.
