import base64
import json
import os
import random
import string
from contextlib import contextmanager
from typing import Iterator

from dagster._annotations import experimental
from dagster._core.pipes.client import PipesContextInjector
from dagster_pipes import PipesContextData, PipesParams
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import files

_CONTEXT_FILENAME = "context.json"


@experimental
class PipesDbfsContextInjector(PipesContextInjector):
    """A context injector that injects context into a Databricks job by writing a JSON file to DBFS.

    Args:
        client (WorkspaceClient): A databricks `WorkspaceClient` object.
    """

    def __init__(self, *, client: WorkspaceClient):
        super().__init__()
        self.dbfs_client = files.DbfsAPI(client.api_client)

    @contextmanager
    def inject_context(self, context: "PipesContextData") -> Iterator[PipesParams]:
        """Inject context to external environment by writing it to an automatically-generated
        DBFS temporary file as JSON and exposing the path to the file.

        Args:
            context_data (PipesContextData): The context data to inject.

        Yields:
            PipesParams: A dict of parameters that can be used by the external process to locate and
                load the injected context data.
        """
        with dbfs_tempdir(self.dbfs_client) as tempdir:
            path = os.path.join(tempdir, _CONTEXT_FILENAME)
            contents = base64.b64encode(json.dumps(context).encode("utf-8")).decode(
                "utf-8"
            )
            self.dbfs_client.put(path, contents=contents, overwrite=True)
            yield {"path": path}

    def no_messages_debug_text(self) -> str:
        return (
            "Attempted to inject context via a temporary file in dbfs. Expected"
            " PipesDbfsContextLoader to be explicitly passed to open_dagster_pipes in the external"
            " process."
        )


@contextmanager
def dbfs_tempdir(dbfs_client: files.DbfsAPI) -> Iterator[str]:
    dirname = "".join(random.choices(string.ascii_letters, k=30))
    tempdir = f"/tmp/{dirname}"
    dbfs_client.mkdirs(tempdir)
    try:
        yield tempdir
    finally:
        dbfs_client.delete(tempdir, recursive=True)
