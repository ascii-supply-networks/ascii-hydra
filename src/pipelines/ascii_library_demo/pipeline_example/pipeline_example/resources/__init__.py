import os

from ascii_library.orchestration.pipes.spark_pipes import (
    Engine,
    ExecutionMode,
    SparkPipesResource,
)
from ascii_library.orchestration.resources.spark import get_pyspark_config
from dagster import ResourceDefinition

configured_pyspark = get_pyspark_config()

RESOURCES_DEV_LOCAL = {
    "pyspark": configured_pyspark,
    "step_launcher": ResourceDefinition.none_resource(),
    "spark_pipes_client": SparkPipesResource(
        engine=Engine(os.environ.get("SPARK_PIPES_ENGINE", "pyspark")),
        # Keep in mind certain environment variables are mandatory
        # - some need to be set in the execution environment of dagster
        # - some in the remote one where spark runs
        # engine=Engine(Engine.Databricks),
        # engine=Engine(Engine.EMR),
        # execution_mode=ExecutionMode.SmallDevSampleS3,
        execution_mode=ExecutionMode(
            os.environ.get("SPARK_EXECUTION_MODE", "small_dev_sample_local")
        ),
    ),
}

RESOURCES_PROD = {
    "pyspark": configured_pyspark,
    "spark_pipes_client": SparkPipesResource(
        # engine=Engine.Databricks,
        engine=Engine.EMR,
        execution_mode=ExecutionMode.Full,
    ),
}


resource_defs_by_deployment_name = {
    "dev": RESOURCES_DEV_LOCAL,
    "production": RESOURCES_PROD,
}
