import os

from botocore.config import Config
from dagster import ConfigurableResource

from ascii_library.orchestration.pipes import Engine, ExecutionMode


class SparkPipesResource(ConfigurableResource):  # type: ignore
    r"""
    Generic configurable spark-pipes resource.

    Executes jobs in one of several modes: ``local`` for quick development,
    or on scalable cloud backends like ``databricks`` and ``emr``.
    Pipelines may optionally apply sampling to speed up end-to-end runs.

    **Databricks authentication (environment variables)**

    .. code-block:: text

       DATABRICKS_HOST
       DATABRICKS_CLIENT_ID
       DATABRICKS_CLIENT_SECRET

    **EMR credentials (environment variables)**

    .. code-block:: text

       ASCII_AWS_ACCESS_KEY_ID
       ASCII_AWS_SECRET_ACCESS_KEY

    :ivar engine: The default execution engine to use.
    :vartype engine: Engine
    :ivar execution_mode: The execution mode for the pipeline (e.g. ``debug``, ``prod``).
    :vartype execution_mode: ExecutionMode
    """

    engine: Engine
    execution_mode: ExecutionMode

    def get_spark_pipes_client(self, override_default_engine):
        aws_access_key_id = os.environ.get("ASCII_AWS_ACCESS_KEY_ID", "dummy")
        aws_secret_access_key = os.environ.get("ASCII_AWS_SECRET_ACCESS_KEY", "dummy")
        if override_default_engine is not None:
            engine_to_use = override_default_engine
        else:
            engine_to_use = self.engine
        if engine_to_use == Engine.Local:
            from dagster import PipesSubprocessClient

            return PipesSubprocessClient()
        elif engine_to_use == Engine.Databricks:
            import boto3
            from databricks.sdk import WorkspaceClient

            from ascii_library.orchestration.pipes.databricks import (
                PipesDatabricksEnhancedClient,
            )

            workspace_client = WorkspaceClient(
                host=os.environ.get("DATABRICKS_HOST", "dummy"),
                client_id=os.environ.get("DATABRICKS_CLIENT_ID", "dummy"),
                client_secret=os.environ.get("DATABRICKS_CLIENT_SECRET", "dummy"),
            )
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name="us-east-1",
            )
            tagging_client = boto3.client(
                "resourcegroupstaggingapi",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name="us-east-1",
            )
            return PipesDatabricksEnhancedClient(
                client=workspace_client,
                tagging_client=tagging_client,
                s3_client=s3_client,
            )
        elif engine_to_use == Engine.EMR:
            import boto3

            from ascii_library.orchestration.pipes.emr import PipesEmrEnhancedClient

            emrClient = boto3.client(
                "emr",
                region_name="us-east-1",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                config=Config(
                    retries={"max_attempts": 10, "mode": "adaptive"},
                ),
            )
            s3Client = boto3.client(
                "s3",
                region_name="us-east-1",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                config=Config(
                    retries={"max_attempts": 10, "mode": "adaptive"},
                ),
            )
            priceClient = boto3.client(
                "pricing",
                region_name="us-east-1",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                config=Config(
                    retries={"max_attempts": 10, "mode": "adaptive"},
                ),
            )
            return PipesEmrEnhancedClient(
                emr_client=emrClient,
                s3_client=s3Client,
                bucket="ascii-supply-chain-research-pipeline",
                price_client=priceClient,
            )
        else:
            raise ValueError(f"Unsupported engine mode: {engine_to_use}")
