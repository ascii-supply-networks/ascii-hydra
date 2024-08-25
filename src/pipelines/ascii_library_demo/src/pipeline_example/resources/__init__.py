import os

from ascii_library.orchestration.pipes.spark_pipes import (
    Engine,
    ExecutionMode,
    SparkPipesResource,
)
from ascii_library.orchestration.resources.parquet_io_manager import (
    LocalPartitionedParquetIOManager,
)
from ascii_library.orchestration.resources.spark import get_pyspark_config
from dagster import ResourceDefinition

configured_pyspark = get_pyspark_config()

RESOURCES_LOCAL = {
    "pyspark": configured_pyspark,
    # none uses pyspark local
    "step_launcher": ResourceDefinition.none_resource(),
    "spark_pipes_client": SparkPipesResource(
        engine=Engine(os.environ.get("SPARK_PIPES_ENGINE", "pyspark")),
        # engine=Engine(Engine.Databricks),
        # engine=Engine(Engine.EMR),
        # execution_mode=ExecutionMode.SmallDevSampleS3,
        execution_mode=ExecutionMode(
            os.environ.get("SPARK_EXECUTION_MODE", "small_dev_sample_local")
        ),
    ),
    "parquet_io_manager": LocalPartitionedParquetIOManager(pyspark=configured_pyspark),
    # "parquet_io_manager": S3PartitionedParquetIOManager(
    #     pyspark=configured_pyspark,
    #     s3_bucket="ascii-supply-chain-research-results/foo",
    #     access_key_id=EnvVar("ASCII_AWS_ACCESS_KEY_ID"),
    #     access_key_secret=EnvVar("ASCII_AWS_SECRET_ACCESS_KEY"),
    # )
}
