"""PySpark bindings for spark-ann.

Importing this package patches ``pyspark.sql.DataFrame`` with both flat
methods (``df.build_ann_index``, ``df.ann_search``, ``df.ann_batch_search``)
and an accessor namespace (``df.ann.build_index``, ``df.ann.search``,
``df.ann.batch_search``). The functional API is also available at module
level for callers who prefer it.

The JVM JAR is NOT bundled in this wheel. Pass it explicitly when starting
Spark:

    pyspark --jars /path/to/spark-ann-integration-assembly.jar
    # or
    pyspark --packages com.wayblink:spark-ann-integration_2.12:0.1.0
"""

from __future__ import annotations

from .api import (
    ann_batch_search,
    ann_search,
    build_ann_index,
    discover_data_files,
    load_searcher,
)
from ._dataframe import install as _install_dataframe_extensions

_install_dataframe_extensions()

__all__ = [
    "build_ann_index",
    "ann_search",
    "ann_batch_search",
    "load_searcher",
    "discover_data_files",
]

__version__ = "0.1.0"
