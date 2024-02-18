import os

from ascii_library.orchestration.pipes import Engine, ExecutionMode
from dagster import ConfigurableResource


class SparkPipesResource(ConfigurableResource):  # type: ignore
    """
    Generic configurable spark-pipes resource which executes either in:

    - local mode for quick local development
    - databricks mode for scalable execution

    Additionally, pipelines may apply a sampling function to avoid waiting until PBs of data are processed for quick E2E results.

    In the case of databricks execution mode the following environment variables have to be set in order to authenticate with DB

    - `DATABRICKS_HOST`
    - `DATABRICKS_TOKEN`

    For EMR mode:

    - `ASCII_AWS_ACCESS_KEY_ID`
    - `ASCII_AWS_SECRET_ACCESS_KEY`
    """

    engine: Engine
    execution_mode: ExecutionMode

    def get_spark_pipes_client(self, override_default_engine):
        if override_default_engine is not None:
            engine_to_use = override_default_engine
        else:
            engine_to_use = self.engine
        if engine_to_use == Engine.Local:
            from dagster import PipesSubprocessClient

            return PipesSubprocessClient()
        elif engine_to_use == Engine.Databricks:
            from ascii_library.orchestration.pipes.databricks import (
                PipesDatabricksEnhancedClient,
            )
            from databricks.sdk import WorkspaceClient

            workspace_client = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST", "dummy"),
                token=os.environ.get("DATABRICKS_TOKEN", "dummy"),
            )
            return PipesDatabricksEnhancedClient(workspace_client)
        elif engine_to_use == Engine.EMR:
            import boto3
            from ascii_library.orchestration.pipes.emr import PipesEmrEnhancedClient

            aws_access_key_id = os.environ.get("ASCII_AWS_ACCESS_KEY_ID", "dummy")
            aws_secret_access_key = os.environ.get(
                "ASCII_AWS_SECRET_ACCESS_KEY", "dummy"
            )
            emrClient = boto3.client(
                "emr",
                region_name="us-east-1",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            s3Client = boto3.client(
                "s3",
                region_name="us-east-1",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            priceClient = boto3.client(
                "pricing",
                region_name="us-east-1",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
            )
            return PipesEmrEnhancedClient(
                emr_client=emrClient,
                s3_client=s3Client,
                bucket="ascii-supply-chain-research-pipeline",
                price_client=priceClient,
            )
        else:
            raise ValueError(f"Unsupported engine mode: {engine_to_use}")
