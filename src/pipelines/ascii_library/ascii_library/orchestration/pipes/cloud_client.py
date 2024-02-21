import base64
import select
import socket
import time
from typing import List, Optional, Union

import boto3
import dagster._check as check
from ascii_library.orchestration.pipes.emr_constants import pipeline_bucket
from botocore.exceptions import (
    ClientError,
    ConnectionError,
    ConnectTimeoutError,
    NoCredentialsError,
    ReadTimeoutError,
    ResponseStreamingError,
)
from dagster import file_relative_path, get_dagster_logger
from dagster._core.errors import DagsterPipesExecutionError
from dagster._core.pipes.client import (
    PipesClient,
    PipesContextInjector,
    PipesMessageReader,
)
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import jobs
from paramiko import AutoAddPolicy, PKey, SSHClient
from paramiko.transport import Transport
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
)

from .utils import library_to_cloud_paths, package_library


def after_retry(retry_state: RetryCallState):
    """Function to log after each retry attempt."""
    if retry_state.next_action is not None:
        sleep_time = retry_state.next_action.sleep
    else:
        sleep_time = 0.0
    get_dagster_logger().debug(
        (
            f"Retry attempt: {retry_state.attempt_number}. Waiting {sleep_time} seconds before next try."
        )
    )


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
        self.last_observed_state = None
        if not hasattr(main_client, "dbfs"):
            self._s3_client = kwargs.get("s3_client")
            self.filesystem = ""
        else:
            self.filesystem = "dbfs"
            self._tagging_client = kwargs.get("tagging_client")

    @retry(
        stop=stop_after_delay(20) | stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, max=60),
        after=after_retry,
        retry=retry_if_exception_type(
            (
                ConnectTimeoutError,
                ReadTimeoutError,
                ResponseStreamingError,
                ConnectionError,
            )
        ),
    )
    def _retrieve_state_emr(self, cluster_id):
        return self.main_client.describe_cluster(ClusterId=cluster_id)

    def _handle_emr_polling(self, cluster_id):
        description = self._retrieve_state_emr(cluster_id)
        state = description["Cluster"]["Status"]["State"]
        dns = None
        if description["Cluster"].get("MasterPublicDnsName") and dns is not None:
            dns = description["Cluster"]["MasterPublicDnsName"]
            get_dagster_logger().debug(f"dns: {dns}")
        if state != self.last_observed_state:
            get_dagster_logger().debug(
                f"[pipes] EMR cluster id {cluster_id} observed state transition to {state}"
            )
            self.last_observed_state = state
        if state in ["TERMINATED", "TERMINATED_WITH_ERRORS"]:
            return self._handle_terminated_state_emr(
                job_flow=cluster_id, description=description, state=state
            )
        else:
            return True

    @retry(
        stop=stop_after_delay(20) | stop_after_attempt(10),
        wait=wait_exponential(multiplier=1, max=60),
        after=after_retry,
        retry=retry_if_exception_type((DatabricksError)),
    )
    def _retrieve_state_dbr(self, run_id):
        return self.main_client.jobs.get_run(run_id)

    def _handle_dbr_polling(self, run_id):
        run = self._retrieve_state_dbr(run_id)
        if run.state.life_cycle_state != self.last_observed_state:
            get_dagster_logger().debug(
                f"[pipes] Databricks run {run_id} observed state transition to {run.state.life_cycle_state}"
            )
            self.last_observed_state = run.state.life_cycle_state
        if run.state.life_cycle_state in (
            jobs.RunLifeCycleState.TERMINATED,
            jobs.RunLifeCycleState.SKIPPED,
            jobs.RunLifeCycleState.INTERNAL_ERROR,
        ):
            return self._handle_terminated_state_dbr(run=run)
        else:
            return True

    def _poll_till_success(self, **kwargs):
        self._tagging_client = kwargs.get("tagging_client")
        cont = True
        if kwargs.get("extras") is not None:
            self.engine = kwargs.get("extras")["engine"]
            self.executionMode = kwargs.get("extras")["execution_mode"]
        while cont:
            if not hasattr(self.main_client, "dbfs"):
                cont = self._handle_emr_polling(kwargs.get("cluster_id"))
            elif hasattr(self.main_client, "dbfs"):
                cont = self._handle_dbr_polling(run_id=kwargs.get("run_id"))
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

        resources = self._tagging_client.get_resources(
            TagFilters=[
                {
                    "Key": "JobId",
                    "Values": [
                        str(run.job_id),
                    ],
                },
            ]
        )
        resource_arns = [
            item["ResourceARN"] for item in resources["ResourceTagMappingList"]
        ]
        if len(resource_arns) > 0:
            for arn in resource_arns:
                self._tagging_client.tag_resources(
                    ResourceARNList=[arn],
                    Tags={
                        "jobId": str(run.job_id),
                        "engine": self.engine,
                        "executionMode": self.executionMode,
                    },
                )
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

    def transfer_data(source, destination):
        """Transfer data from source to destination."""
        try:
            data = source.recv(1024)
            if len(data) == 0:
                return False  # Signal to close connection
            destination.send(data)
            return True
        except socket.error:
            return False  # Handle socket errors

    def handle_client_connection(self, client_socket, channel):
        """Handle the connection for a single client."""
        try:
            while self.process_connection(client_socket, channel):
                pass
        finally:
            self.cleanup_connection(client_socket, channel)

    def process_connection(self, client_socket, channel):
        """Process readable sockets and channels."""
        readable, _, _ = select.select([client_socket, channel], [], [], 1)
        if client_socket in readable:
            if not self.transfer_data(client_socket, channel):
                return False
        if channel in readable:
            if not self.transfer_data(channel, client_socket):
                return False
        return True

    def cleanup_connection(self, client_socket, channel):
        """Clean up resources after connection is closed."""
        client_socket.close()
        channel.close()

    def accept_connection(self, local_server):
        """Accept a new connection."""
        readable, _, _ = select.select([local_server], [], [], 1)
        if local_server in readable:
            return local_server.accept()
        return None, None

    def create_channel(self, transport, remote_host, remote_port, client_socket):
        """Create a new channel for the given connection."""
        try:
            return transport.open_channel(
                "direct-tcpip",
                (remote_host, remote_port),
                client_socket.getpeername(),
            )
        except Exception as e:
            print(f"Failed to create channel: {e}")
            return None

    def forward_tunnel(
        self, local_port: int, remote_host: str, remote_port: int, transport: Transport
    ):
        # Create a socket to act as a local server
        local_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        local_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        local_server.bind(("127.0.0.1", local_port))
        local_server.listen(100)

        try:
            while True:
                client_socket, _ = self.accept_connection(local_server)
                if client_socket:
                    channel = self.create_channel(
                        transport, remote_host, remote_port, client_socket
                    )
                    if channel:
                        self.handle_client_connection(client_socket, channel)
                    else:
                        client_socket.close()
        finally:
            local_server.close()

    def create_ssh_tunnel(
        self, key_path: PKey, dns_name: str, local_port: int, remote_port: int
    ):
        ssh_client = SSHClient()
        ssh_client.set_missing_host_key_policy(AutoAddPolicy())
        ssh_client.connect(hostname=dns_name, username="hadoop", pkey=key_path)
        transport = ssh_client.get_transport()
        if transport is None:
            raise ValueError(
                "SSH Transport is not available. Connection failed or was not established."
            )

        # Forward traffic from local port to remote port via SSH tunnel
        self.forward_tunnel(local_port, dns_name, remote_port, transport)
