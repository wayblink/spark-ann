"""DataFrame method injection (P2).

Two flavors of ergonomic API on top of the functional P1 surface:

1. Flat monkey-patch: ``df.build_ann_index(...)``, ``df.ann_search(...)``,
   ``df.ann_batch_search(...)``. Discoverable via tab-completion after
   importing ``spark_ann``.

2. Accessor namespace: ``df.ann.build_index(...)``, ``df.ann.search(...)``,
   ``df.ann.batch_search(...)``. Mirrors the koalas / pyspark-pandas style
   for callers who prefer a grouped namespace.

Both flavors share the same call sites — neither holds any state of its
own — so users can mix and match without surprises.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Optional

from pyspark.sql import DataFrame

from .api import (
    ann_batch_search as _ann_batch_search,
    ann_search as _ann_search,
    build_ann_index as _build_ann_index,
)


class _ANNAccessor:
    """Accessor object exposed as ``df.ann``."""

    def __init__(self, df: DataFrame):
        self._df = df

    def build_index(
        self,
        vector_column: str,
        output_path: str,
        config: Optional[Dict[str, Any]] = None,
    ) -> Any:
        return _build_ann_index(self._df, vector_column, output_path, config)

    def search(
        self,
        index_path: str,
        query_vector: Iterable[float],
        k: int,
        nprobe: int = 3,
        ef: int = 50,
    ) -> DataFrame:
        return _ann_search(
            self._df.sparkSession, index_path, query_vector, k, nprobe, ef
        )

    def batch_search(
        self,
        index_path: str,
        query_vector_column: str,
        k: int,
        nprobe: int = 3,
        ef: int = 50,
    ) -> DataFrame:
        return _ann_batch_search(
            self._df.sparkSession,
            index_path,
            self._df,
            query_vector_column,
            k,
            nprobe,
            ef,
        )


def _df_build_ann_index(
    self,
    vector_column: str,
    output_path: str,
    config: Optional[Dict[str, Any]] = None,
) -> Any:
    return _build_ann_index(self, vector_column, output_path, config)


def _df_ann_search(
    self,
    index_path: str,
    query_vector: Iterable[float],
    k: int,
    nprobe: int = 3,
    ef: int = 50,
) -> DataFrame:
    return _ann_search(self.sparkSession, index_path, query_vector, k, nprobe, ef)


def _df_ann_batch_search(
    self,
    index_path: str,
    query_vector_column: str,
    k: int,
    nprobe: int = 3,
    ef: int = 50,
) -> DataFrame:
    return _ann_batch_search(
        self.sparkSession, index_path, self, query_vector_column, k, nprobe, ef
    )


def install() -> None:
    """Idempotently attach spark-ann methods to ``pyspark.sql.DataFrame``."""
    if getattr(DataFrame, "_spark_ann_installed", False):
        return
    DataFrame.build_ann_index = _df_build_ann_index
    DataFrame.ann_search = _df_ann_search
    DataFrame.ann_batch_search = _df_ann_batch_search
    DataFrame.ann = property(lambda self: _ANNAccessor(self))
    DataFrame._spark_ann_installed = True
