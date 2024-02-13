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

    def get_spark_pipes_client(self):
        if self.engine == Engine.Local:
            from dagster import PipesSubprocessClient

            return PipesSubprocessClient()
        elif self.engine == Engine.Databricks:
            from ascii_library.orchestration.pipes.databricks import (
                PipesDatabricksEnhancedClient,
            )
            from databricks.sdk import WorkspaceClient

            workspace_client = WorkspaceClient(
                host=os.environ["DATABRICKS_HOST"], token=os.environ["DATABRICKS_TOKEN"]
            )
            return PipesDatabricksEnhancedClient(workspace_client)
        elif self.engine == Engine.EMR:
            import boto3
            from ascii_library.orchestration.pipes.emr import PipesEmrEnhancedClient

            aws_access_key_id = os.environ["ASCII_AWS_ACCESS_KEY_ID"]
            aws_secret_access_key = os.environ["ASCII_AWS_SECRET_ACCESS_KEY"]
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
            raise ValueError(f"Unsupported engine mode: {self.engine}")
