"""Dagster IO manager: every asset is a Polars DataFrame stored as Parquet,
one file per partition: data/warehouse/<asset>/<partition>.parquet"""
from __future__ import annotations

from pathlib import Path

import polars as pl
from dagster import ConfigurableIOManager, InputContext, OutputContext


class PolarsParquetIOManager(ConfigurableIOManager):
    base_dir: str

    def _path(self, context) -> Path:
        name = "/".join(context.asset_key.path)
        part = context.asset_partition_key if context.has_asset_partitions else "all"
        return Path(self.base_dir) / name / f"{part}.parquet"

    def handle_output(self, context: OutputContext, obj: pl.DataFrame) -> None:
        p = self._path(context)
        p.parent.mkdir(parents=True, exist_ok=True)
        obj.write_parquet(p)
        context.add_output_metadata({"rows": obj.height, "path": str(p)})

    def load_input(self, context: InputContext) -> pl.DataFrame:
        return pl.read_parquet(self._path(context))
