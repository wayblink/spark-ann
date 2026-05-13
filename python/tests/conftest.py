"""Shared pytest fixtures for the spark-ann PySpark tests.

The SparkSession is started once per test session with the spark-ann
assembly JAR on the classpath. The JAR path defaults to the location
``sbt sparkIntegration/assembly`` writes to inside this repo; override via
the ``SPARK_ANN_JAR`` environment variable for CI or alternate builds.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession

_REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
_DEFAULT_JAR = (
    _REPO_ROOT
    / "spark-integration"
    / "target"
    / "scala-2.12"
    / "spark-ann-integration-assembly.jar"
)


def _resolve_jar() -> str:
    explicit = os.environ.get("SPARK_ANN_JAR")
    if explicit:
        if not os.path.exists(explicit):
            pytest.exit(f"SPARK_ANN_JAR is set to {explicit} but the file does not exist", 2)
        return explicit
    if _DEFAULT_JAR.exists():
        return str(_DEFAULT_JAR)
    pytest.exit(
        f"spark-ann assembly JAR not found at {_DEFAULT_JAR}. "
        "Build it with `sbt sparkIntegration/assembly` or set SPARK_ANN_JAR.",
        2,
    )


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    jar = _resolve_jar()
    # spark.jars instructs Spark to ship the JAR to executors via the
    # BlockManager. Combined with explicit bindAddress=host=127.0.0.1 so
    # the executor's fetch URL is reachable in local-mode networking
    # configurations that don't resolve the machine's primary IP.
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pyspark-ann-tests")
        .config("spark.jars", jar)
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.blockManager.port", "0")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def tmp_index_dir():
    path = tempfile.mkdtemp(prefix="pyspark-ann-")
    yield path
    shutil.rmtree(path, ignore_errors=True)
