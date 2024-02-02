import json
import os
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List

import dagster._check as check
from boto3 import client
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


# TODO: can this be done with PipesContextInjector?
@experimental
class PipesS3ContextInjector(PipesContextInjector):
    """A context injector that injects context by writing to a temporary S3 location.

    Args:
        bucket (str): The S3 bucket to write to.
        client (boto3.client): A boto3 client to use to write to S3.
        key_prefix (Optional[str]): An optional prefix to use for the S3 key. Defaults to a random
            string.

    """

    def __init__(self, *, bucket: str, client: client, key_prefix: str, mock=bool):
        super().__init__()
        self.bucket = check.str_param(bucket, "bucket")
        self.client = client
        self.key_prefix = key_prefix
        self.key = os.path.join(key_prefix, _CONTEXT_FILENAME)
        self.mock = bool

    @contextmanager
    def inject_context(self, context: "PipesContextData") -> Iterator[PipesParams]:
        context["bucket"] = self.bucket
        context["key"] = self.key_prefix
        get_dagster_logger().debug(json.dumps(context))
        try:
            if self.mock is not True:
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

        delete_us: Dict[str, List[Dict[str, str]]] = {"Objects": []}
        for item in pages.search("Contents"):
            delete_us["Objects"].append({"Key": item["Key"]})

            # flush once aws limit reached
            if len(delete_us["Objects"]) >= 1000:
                self.client.delete_objects(Bucket=self.bucket, Delete=delete_us)
                delete_us = {"Objects": []}

        # flush rest
        if len(delete_us["Objects"]):
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

    def __init__(self, client: Any, mock: bool):
        self._client = client
        self.mock = bool

    @contextmanager
    def load_context(self, params: PipesParams) -> Iterator[PipesContextData]:
        bucket = os.environ["bucket"]
        mock = os.environ["mock"] == str(True)
        if mock:
            key = f"./ascii_library/ascii_library/orchestration/resources/{os.environ['key']}/{_CONTEXT_FILENAME}"
            f = open(key).read()
            yield json.loads(f)
        else:
            key = f"{os.environ['key']}/{_CONTEXT_FILENAME}"
            obj = self._client.get_object(Bucket=bucket, Key=key)
            yield json.loads(obj["Body"].read().decode("utf-8"))
