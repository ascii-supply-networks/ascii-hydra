import base64
import os
from contextlib import ExitStack, contextmanager
from typing import Iterator, Literal, Optional, Sequence, TextIO

from ascii_library.orchestration.pipes.cloud_context import dbfs_tempdir
from dagster._annotations import experimental
from dagster._core.pipes.utils import (
    PipesBlobStoreMessageReader,
    PipesChunkedLogReader,
    PipesLogReader,
)
from dagster_pipes import PipesParams
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import files


@experimental
class PipesDbfsMessageReader(PipesBlobStoreMessageReader):
    """Message reader that reads messages by periodically reading message chunks from an
    automatically-generated temporary directory on DBFS.

    If `log_readers` is passed, this reader will also start the passed readers
    when the first message is received from the external process.

    Args:
        interval (float): interval in seconds between attempts to download a chunk
        client (WorkspaceClient): A databricks `WorkspaceClient` object.
        cluster_log_root (Optional[str]): The root path on DBFS where the cluster logs are written.
            If set, this will be used to read stderr/stdout logs.
        log_readers (Optional[Sequence[PipesLogReader]]): A set of readers for logs on DBFS.
    """

    def __init__(
        self,
        *,
        interval: float = 10,
        client: WorkspaceClient,
        log_readers: Optional[Sequence[PipesLogReader]] = None,
    ):
        super().__init__(
            interval=interval,
            log_readers=log_readers,
        )
        self.dbfs_client = files.DbfsAPI(client.api_client)

    @contextmanager
    def get_params(self) -> Iterator[PipesParams]:
        with ExitStack() as stack:
            params: PipesParams = {}
            params["path"] = stack.enter_context(dbfs_tempdir(self.dbfs_client))
            yield params

    def download_messages_chunk(self, index: int, params: PipesParams) -> Optional[str]:
        message_path = os.path.join(params["path"], f"{index}.json")
        try:
            raw_message = self.dbfs_client.read(message_path)
            if raw_message.data is not None:
                # Files written to dbfs using the Python IO interface used in PipesDbfsMessageWriter are
                # base64-encoded.
                return base64.b64decode(raw_message.data).decode("utf-8")
            return None
        # An error here is an expected result, since an IOError will be thrown if the next message
        # chunk doesn't yet exist. Swallowing the error here is equivalent to doing a no-op on a
        # status check showing a non-existent file.
        except IOError:
            return None

    def no_messages_debug_text(self) -> str:
        return (
            "Attempted to read messages from a temporary file in dbfs. Expected"
            " PipesDbfsMessageWriter to be explicitly passed to open_dagster_pipes in the external"
            " process."
        )


@experimental
class PipesDbfsLogReader(PipesChunkedLogReader):
    """Reader that reads a log file from DBFS.

    Args:
        interval (float): interval in seconds between attempts to download a log chunk
        remote_log_name (Literal["stdout", "stderr"]): The name of the log file to read.
        target_stream (TextIO): The stream to which to forward log chunk that have been read.
        client (WorkspaceClient): A databricks `WorkspaceClient` object.
    """

    def __init__(
        self,
        *,
        interval: float = 10,
        remote_log_name: Literal["stdout", "stderr"],
        target_stream: TextIO,
        client: WorkspaceClient,
    ):
        super().__init__(interval=interval, target_stream=target_stream)
        self.dbfs_client = files.DbfsAPI(client.api_client)
        self.remote_log_name = remote_log_name
        self.log_position = 0
        self.log_modification_time = None
        self.log_path = None

    def download_log_chunk(self, params: PipesParams) -> Optional[str]:
        log_path = self._get_log_path(params)
        if log_path is None:
            return None
        else:
            try:
                status = self.dbfs_client.get_status(log_path)
                # No need to download again if it hasn't changed
                if status.modification_time == self.log_modification_time:
                    return None
                read_response = self.dbfs_client.read(log_path)
                assert read_response.data
                content = base64.b64decode(read_response.data).decode("utf-8")
                chunk = content[self.log_position :]
                self.log_position = len(content)
                return chunk
            except IOError:
                return None

    def target_is_readable(self, params: PipesParams) -> bool:
        return self._get_log_path(params) is not None

    @property
    def name(self) -> str:
        return f"PipesDbfsLogReader({self.remote_log_name})"

    # The directory containing logs will not exist until either 5 minutes have elapsed or the
    # job has finished.
    def _get_log_path(self, params: PipesParams) -> Optional[str]:
        if self.log_path is None:
            cluster_driver_log_root = params["extras"].get("cluster_driver_log_root")
            if cluster_driver_log_root is None:
                return None
            try:
                child_dirs = list(self.dbfs_client.list(cluster_driver_log_root))
            except IOError:
                child_dirs = []  # log root doesn't exist yet
            match = next(
                (
                    child_dir
                    for child_dir in child_dirs
                    if child_dir.path and child_dir.path.endswith(self.remote_log_name)
                ),
                None,
            )
            if match:
                self.log_path = f"dbfs:{match.path}"  # type: ignore

        return self.log_path
