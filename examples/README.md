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
  --class com.company.ann.spark.examples.QuickStart \
  $SPARK_ANN_JAR
```

(The `QuickStart` class is already bundled inside the assembly.)

## Known limitation: the returned `id` is not your `id` column

The HNSW indexes assign internal ids sequentially in parquet row order
(see `LocalIndexBuilder.buildIndexForFileGroup`'s `vectorOffset`
counter). When you feed a DataFrame whose own `id` column is `42L`, the
search results refer to that vector by its build-time position, not by
`42L`.

If you need user-visible ids you currently have two options:

1. **Stable parquet ordering**: write your data with `coalesce(1)` and
   ensure row order matches user ids; then the HNSW internal id equals
   the user id by construction.
2. **Round-trip via the `indexId` + offset**: each `LocalIndexMetadata`
   records `dataFiles[*].vectorOffset` and `numVectors`, so a result
   `(id=37, indexId='idx_foo')` corresponds to row 37 within the files
   listed under `idx_foo` — which you can re-read from parquet.

A future change to preserve the user-supplied `id` column through to
the search result is a planned follow-up.
