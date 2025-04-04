import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
    ResourceDependency,
)
from dagster_pyspark.resources import LazyPySparkResource
from dagster_shared.check.functions import failed
from pyspark.sql import DataFrame as PySparkDataFrame


class PartitionedParquetIOManager(ConfigurableIOManager):  # type: ignore
    """This IOManager will take in a pandas or pyspark dataframe and store it in parquet at the
    specified path.

    It stores outputs for different partitions in different filepaths.

    Downstream ops can either load this dataframe into a spark session or simply retrieve a path
    to where the data is stored.
    """

    # Based on: https://github.com/dagster-io/dagster/blob/master/examples/project_fully_featured/project_fully_featured/resources/parquet_io_manager.py#L17

    _base_path: Optional[str]
    pyspark: ResourceDependency[LazyPySparkResource]
    _storage_options: Dict[str, Any]

    @property
    def storage_options(self):
        raise NotImplementedError()

    def handle_output(
        self, context: OutputContext, obj: Union[pd.DataFrame, PySparkDataFrame]
    ):
        path = self._get_path(context)
        context.log.debug(
            f"partitions: {context.has_asset_partitions}, type: {context.dagster_type.typing_type} ***"
        )
        if self._base_path is not None and "://" not in self._base_path:
            os.makedirs(os.path.dirname(path), exist_ok=True)

        if isinstance(obj, pd.DataFrame):
            row_count = len(obj)
            context.log.info(f"Row count: {row_count}")
            obj.to_parquet(
                path=path,
                index=False,
                compression="gzip",
                storage_options=self._storage_options,
            )
        elif isinstance(obj, PySparkDataFrame):
            obj.write.parquet(path=path, mode="overwrite", compression="gzip")
            row_count = self.pyspark.spark_session.read.parquet(path).count()
        else:
            raise Exception(f"Outputs of type {type(obj)} not supported.")

        context.add_output_metadata({"row_count": row_count, "path": path})

    def load_input(self, context) -> Union[PySparkDataFrame, pd.DataFrame, str]:
        path = self._get_path(context)
        context.log.debug(
            f"Trying to load input from {path} for type: {context.dagster_type.typing_type}"
        )
        if context.dagster_type.typing_type == PySparkDataFrame:
            return self.pyspark.spark_session.read.parquet(path)
        if context.dagster_type.typing_type == pd.DataFrame:
            return pd.read_parquet(path, storage_options=self._storage_options)
        if context.dagster_type.typing_type is str:
            return path
        return failed(
            f"Inputs of type {context.dagster_type} not supported. Please specify a valid type "
            "for this input either on the argument of the @asset-decorated function."
        )

    def _get_path(self, context: Union[InputContext, OutputContext]):
        key = context.asset_key.path[-1]

        if context.has_asset_partitions:
            start, end = context.asset_partitions_time_window
            dt_format = "%Y%m%d%H%M%S"
            # TODO potentially use hive-style partitioning for better spark support (if needed)
            partition_str = start.strftime(dt_format) + "_" + end.strftime(dt_format)
            if self._base_path is not None:
                return os.path.join(self._base_path, key, f"{partition_str}.parquet")
            else:
                return os.path.join(key, f"{partition_str}.parquet")
        else:
            if self._base_path is not None:
                return os.path.join(self._base_path, f"{key}.parquet")
            else:
                return f"{key}.parquet"


class LocalPartitionedParquetIOManager(PartitionedParquetIOManager):  # type: ignore
    base_key: Optional[str]

    @property  # type: ignore
    def _base_path(self):
        out_path = Path("z_state") / "object_warehouse"
        out_path.mkdir(parents=True, exist_ok=True)

        if self.base_key:
            return os.path.join(str(out_path), self.base_key)
        else:
            return str(out_path)

    @property  # type: ignore
    def _storage_options(self):
        return {}


class S3PartitionedParquetIOManager(PartitionedParquetIOManager):  # type: ignore
    s3_bucket: str
    access_key_id: str
    access_key_secret: str
    endpoint_url: Optional[str]

    @property  # type: ignore
    def _base_path(self):
        return "s3a://" + self.s3_bucket

    @property  # type: ignore
    def _storage_options(self):
        # Prep branch deployments:
        # listen to environment variables like:
        # DAGSTER_CLOUD_DEPLOYMENT_NAME for branch name and another one for boolean
        # provide configuration to overwrite - manually set branch (for testing)
        # cleanup: use S3 with short retention period
        # figure out how to generalize. Review also: https://docs.lakefs.io/
        # they will need to provide a cleanup hook
        so: Dict[str, Any] = {
            "key": self.access_key_id,
            "secret": self.access_key_secret,
        }
        if self.endpoint_url:
            so["client_kwargs"] = {}
            so["client_kwargs"]["endpoint_url"] = self.endpoint_url
        return so
