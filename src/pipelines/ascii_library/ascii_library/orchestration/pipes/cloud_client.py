import base64
import time
from typing import List, Optional, Union

import boto3
import dagster._check as check
from ascii_library.orchestration.pipes.emr_constants import pipeline_bucket
from botocore.exceptions import ClientError, NoCredentialsError
from dagster import file_relative_path, get_dagster_logger
from dagster._core.errors import DagsterPipesExecutionError
from dagster._core.pipes.client import (
    PipesClient,
    PipesContextInjector,
    PipesMessageReader,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from .utils import library_to_cloud_paths, package_library


class _PipesBaseCloudClient(PipesClient):
    """
    Base class for Pipes clients, containing common methods and attributes.
    """

    def __init__(
        self,
        main_client: Union[boto3.client, WorkspaceClient],
        context_injector: Optional[PipesContextInjector] = None,
        message_reader: Optional[PipesMessageReader] = None,
        poll_interval_seconds: float = 5,
        **kwargs,
    ):
        self.poll_interval_seconds = check.numeric_param(
            poll_interval_seconds, "poll_interval_seconds"
        )
        self.message_reader = check.opt_inst_param(
            message_reader,
            "message_reader",
            PipesMessageReader,
        )
        self.main_client = main_client
        if not hasattr(main_client, "dbfs"):
            self._s3_client = kwargs.get("s3_client")
            self.filesystem = ""
        else:
            self.filesystem = "dbfs"

    def _handle_emr_polling(self, cluster_id):
        description = self.main_client.describe_cluster(ClusterId=cluster_id)
        state = description["Cluster"]["Status"]["State"]
        get_dagster_logger().debug(f"EMR cluster id {cluster_id} status: {state}")
        if state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
            return self._handle_terminated_state_emr(
                job_flow=cluster_id, description=description, state=state
            )
        else:
            return True

    def _handle_dbr_polling(self, run_id):
        last_observed_state = None
        run = self.main_client.jobs.get_run(run_id)
        if run.state.life_cycle_state != last_observed_state:
            get_dagster_logger().debug(
                f"[pipes] Databricks run {run_id} observed state transition to {run.state.life_cycle_state}"
            )
            last_observed_state = run.state.life_cycle_state
        if run.state.life_cycle_state in (
            jobs.RunLifeCycleState.TERMINATED,
            jobs.RunLifeCycleState.SKIPPED,
            jobs.RunLifeCycleState.INTERNAL_ERROR,
        ):
            return self._handle_terminated_state_dbr(run=run)
        else:
            return True

    def _poll_till_success(self, **kwargs):
        cont = True
        while cont:
            if not hasattr(self.main_client, "dbfs"):
                cont = self._handle_emr_polling(kwargs.get("cluster_id"))
            elif hasattr(self.main_client, "dbfs"):
                cont = self._handle_dbr_polling(kwargs.get("run_id"))
            time.sleep(self.poll_interval_seconds)

    def _handle_terminated_state_emr(self, job_flow, description, state):
        message = description["Cluster"]["Status"]["StateChangeReason"]["Message"]
        if "error" in message.lower() or state == "TERMINATED_WITH_ERRORS":
            raise DagsterPipesExecutionError(f"Error running EMR job flow: {job_flow}")
        elif state == "TERMINATING" or state == "TERMINATED":
            return False
        else:
            return True

    def _handle_terminated_state_dbr(self, run):
        if run.state.result_state == jobs.RunResultState.SUCCESS:
            return False
        else:
            raise DagsterPipesExecutionError(
                f"Error running Databricks job: {run.state.state_message}"
            )

    def _ensure_library_on_cloud(
        self,
        libraries_to_build_and_upload: Optional[List[str]],
        **kwargs,
    ):
        """
        Ensure that the specified library is available on S3.

        Args:
            dbfs_path (str): The S3 path where the library should reside.
            local_library_path (str): The local file system path to the library.
        """
        bucket = kwargs.get("bucket", pipeline_bucket)

        if libraries_to_build_and_upload is not None:
            for library in libraries_to_build_and_upload:
                path = library_to_cloud_paths(
                    lib_name=library, filesystem=self.filesystem
                )
                file_relative_path(__file__, f"../../../../{library}")
                to_upload = package_library(
                    file_relative_path(__file__, f"../../../../{library}")
                )[0]
                self._upload_file_to_cloud(
                    local_file_path=to_upload, cloud_path=path, bucket=bucket
                )

    def _upload_file_to_cloud(self, local_file_path: str, cloud_path: str, **kwargs):
        try:
            if not hasattr(self.main_client, "dbfs"):
                self._upload_file_to_s3(local_file_path, cloud_path, **kwargs)
            elif hasattr(self.main_client, "dbfs"):
                self._upload_file_to_dbfs(local_file_path, cloud_path)
        except Exception as e:
            self.handle_exep(e)

    def handle_exep(self, e):
        if isinstance(e, FileNotFoundError):
            get_dagster_logger().error("The file was not found")
            raise
        elif isinstance(e, NoCredentialsError):
            get_dagster_logger().error("Credentials not available")
            raise
        elif isinstance(e, ClientError):
            get_dagster_logger().error("Client error while uploading")
            raise

    def _upload_file_to_s3(self, local_file_path: str, cloud_path: str, **kwargs):
        bucket = kwargs.get("bucket")
        get_dagster_logger().debug(f"uploading: {cloud_path} into bucket: {bucket}")
        if self._s3_client is not None:
            self._s3_client.upload_file(local_file_path, bucket, cloud_path)
        else:  # noqa: E722
            get_dagster_logger().debug("fail to upload to S3")

    def _upload_file_to_dbfs(self, local_file_path: str, dbfs_path: str):
        with open(local_file_path, "rb") as file:
            encoded_string = base64.b64encode(file.read()).decode("utf-8")
        self.main_client.dbfs.put(
            path=dbfs_path, contents=encoded_string, overwrite=True
        )
        get_dagster_logger().debug(
            f"uploading: {local_file_path} to DBFS at: {dbfs_path}"
        )
