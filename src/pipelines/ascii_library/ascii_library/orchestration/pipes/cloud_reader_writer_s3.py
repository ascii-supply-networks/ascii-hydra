import os
from contextlib import contextmanager
from typing import Any, Iterator, Optional, Sequence

import dagster._check as check
from boto3 import client
from botocore.exceptions import ClientError
from dagster._core.pipes.utils import PipesBlobStoreMessageReader, PipesLogReader
from dagster_pipes import (
    PipesBlobStoreMessageWriter,
    PipesParams,
    PipesS3MessageWriterChannel,
)


class PipesS3MessageReader(PipesBlobStoreMessageReader):
    """Message reader that reads messages by periodically reading message chunks from a specified S3
    bucket.

    If `log_readers` is passed, this reader will also start the passed readers
    when the first message is received from the external process.

    Args:
        interval (float): interval in seconds between attempts to download a chunk
        bucket (str): The S3 bucket to read from.
        client (WorkspaceClient): A boto3 client.
        log_readers (Optional[Sequence[PipesLogReader]]): A set of readers for logs on S3.
    """

    def __init__(
        self,
        *,
        interval: float = 10,
        bucket: str,
        key_prefix: str,
        client: client,
        log_readers: Optional[Sequence[PipesLogReader]] = None,
    ):
        super().__init__(
            interval=interval,
            log_readers=log_readers,
        )
        self.key_prefix = key_prefix
        self.bucket = check.str_param(bucket, "bucket")
        self.client = client
        self.last_index = 0

    @contextmanager
    def get_params(self) -> Iterator[PipesParams]:
        yield {"bucket": self.bucket, "key_prefix": self.key_prefix}

    def download_messages_chunk(self, index: int, params: PipesParams) -> Optional[str]:
        key = f"{params['key_prefix']}/{index}.json"
        response = None
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
            response = obj["Body"].read().decode("utf-8")
        except ClientError:
            pass
        return response

    def no_messages_debug_text(self) -> str:
        return (
            f"Attempted to read messages from S3 bucket {self.bucket}. Expected"
            " PipesS3MessageWriter to be explicitly passed to open_dagster_pipes in the external"
            " process."
        )


class PipesS3MessageWriter(PipesBlobStoreMessageWriter):
    """Message writer that writes messages by periodically writing message chunks to an S3 bucket.

    Args:
        client (Any): A boto3.client("s3") object.
        interval (float): interval in seconds between upload chunk uploads
    """

    # client is a boto3.client("s3") object
    def __init__(self, client: Any, *, interval: float = 10):
        super().__init__(interval=interval)
        # Not checking client type for now because it's a boto3.client object and we don't want to
        # depend on boto3.
        self._client = client

    def make_channel(
        self,
        params: PipesParams,
    ) -> "PipesS3MessageWriterChannel":
        bucket = os.environ["bucket"]
        key = os.environ["key"]
        return PipesS3MessageWriterChannel(
            client=self._client,
            bucket=bucket,
            key_prefix=key,
            interval=self.interval,
        )
