import json
import os
from contextlib import contextmanager
from typing import Any, Iterator

import dagster._check as check
from dagster import get_dagster_logger
from dagster._annotations import experimental
from dagster._core.pipes.client import PipesContextInjector
from dagster_pipes import (
    PipesContextData,
    PipesContextLoader,
    PipesParams,
    PipesParamsLoader,
)

_CONTEXT_FILENAME = "context.json"


@experimental
class PipesS3ContextInjector(PipesContextInjector):
    """A context injector that injects context by writing to a temporary S3 location.

    Args:
        bucket (str): The S3 bucket to write to.
        client (boto3.client): A boto3 client to use to write to S3.
        key_prefix (Optional[str]): An optional prefix to use for the S3 key. Defaults to a random
            string.

    """

    def __init__(self, *, bucket: str, client, key_prefix: str):
        super().__init__()
        self.bucket = check.str_param(bucket, "bucket")
        self.client = client
        self.key_prefix = key_prefix
        self.key = os.path.join(key_prefix, _CONTEXT_FILENAME)

    @contextmanager
    def inject_context(self, context: "PipesContextData") -> Iterator[PipesParams]:
        context["bucket"] = self.bucket  # type: ignore
        context["key"] = self.key_prefix  # type: ignore
        get_dagster_logger().debug(json.dumps(context))
        try:
            self.client.put_object(
                Body=json.dumps(context).encode("utf-8"),
                Bucket=self.bucket,
                Key=self.key,
            )
            yield {"bucket": self.bucket, "key": self.key_prefix}
        finally:
            self.clean_s3_context(key=self.key_prefix)

    def no_messages_debug_text(self) -> str:
        return (
            "Attempted to inject context via a temporary file in s3. Expected"
            " PipesS3ContextLoader to be explicitly passed to open_dagster_pipes in the external"
            " process."
        )

    def clean_s3_context(self, key: str):
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=self.bucket, Prefix=key)

        # Use a mutable list to collect objects
        objects_to_delete = []

        for item in pages.search("Contents"):
            if item is not None and "Key" in item:
                objects_to_delete.append({"Key": item["Key"]})

            # Flush once AWS limit reached
            if len(objects_to_delete) >= 1000:
                delete_us = {"Objects": objects_to_delete}
                self.client.delete_objects(Bucket=self.bucket, Delete=delete_us)
                objects_to_delete = []

        # Flush remaining objects
        if objects_to_delete:
            delete_us = {"Objects": objects_to_delete}
            self.client.delete_objects(Bucket=self.bucket, Delete=delete_us)


DAGSTER_PIPES_CONTEXT_ENV_VAR = "DAGSTER_PIPES_CONTEXT"


@experimental
class PipesS3EnvVarParamsLoader(PipesParamsLoader):
    """Params loader that extracts params from environment variables."""

    def is_dagster_pipes_process(self) -> bool:
        # use the presence of DAGSTER_PIPES_CONTEXT to discern if we are in a pipes process
        return (
            DAGSTER_PIPES_CONTEXT_ENV_VAR in os.environ["DAGSTER_PIPES_CONTEXT_ENV_VAR"]
        )

    def load_context_params(self) -> PipesParams:
        return {
            "DAGSTER_PIPES_CONTEXT_ENV_VAR": os.environ["DAGSTER_PIPES_CONTEXT_ENV_VAR"]
        }

    def load_messages_params(self) -> PipesParams:
        return {
            "DAGSTER_PIPES_MESSAGES_ENV_VAR": os.environ[
                "DAGSTER_PIPES_MESSAGES_ENV_VAR"
            ]
        }


class PipesS3ContextLoader(PipesContextLoader):
    """Context loader that reads context from a JSON file on S3.

    Args:
        client (Any): A boto3.client("s3") object.
    """

    def __init__(self, client: Any):
        self._client = client

    @contextmanager
    def load_context(self, params: PipesParams) -> Iterator[PipesContextData]:
        bucket = os.environ["bucket"]
        key = f"{os.environ['key']}/{_CONTEXT_FILENAME}"
        obj = self._client.get_object(Bucket=bucket, Key=key)
        yield json.loads(obj["Body"].read().decode("utf-8"))
