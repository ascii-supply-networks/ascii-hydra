import os
import sys
import time
from typing import Any, Dict, List, Mapping, Optional

from dagster import (
    OpExecutionContext,
    PipesContextInjector,
    PipesMessageReader,
    ResourceParam,
    get_dagster_logger,
    open_pipes_session,
)
from dagster._core.pipes.client import PipesClientCompletedInvocation  # type: ignore
from dagster_databricks import (
    PipesDbfsContextInjector,
    PipesDbfsLogReader,
    PipesDbfsMessageReader,
)
from dagster_pipes import PipesExtras
from dagster_shared.check.functions import bool_param, opt_inst_param
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from mypy_boto3_resourcegroupstaggingapi import ResourceGroupsTaggingAPIClient
from pydantic import Field

from ascii_library.orchestration.pipes.cloud_client import _PipesBaseCloudClient
from ascii_library.orchestration.pipes.exceptions import CustomPipesException


class _PipesDatabricksClient(_PipesBaseCloudClient):
    """Pipes client for Databricks.

    :param client: A Databricks ``WorkspaceClient`` object.
    :type client: WorkspaceClient
    :param tagging_client: A Boto3 client for resource tagging.
    :type tagging_client: ResourceGroupsTaggingAPIClient
    :param context_injector: A context injector to use to inject context into the
        Databricks job. Defaults to a configured ``PipesDbfsContextInjector``.
    :type context_injector: Optional[PipesContextInjector]
    :param message_reader: A message reader to use to read messages from the
        Databricks job. Defaults to a configured ``PipesDbfsMessageReader``.
    :type message_reader: Optional[PipesMessageReader]
    :param forward_termination: If True, the Databricks job will be canceled if the
        orchestration process is interrupted. Defaults to True.
    :type forward_termination: bool
    :param kwargs: Additional keyword arguments.
    :type kwargs: Any
    """

    env: Optional[Mapping[str, str]] = Field(
        default=None,
        description="An optional dict of environment variables to pass to the subprocess.",
    )

    def __init__(
        self,
        client: WorkspaceClient,
        tagging_client: ResourceGroupsTaggingAPIClient,
        context_injector: Optional[PipesContextInjector] = None,
        message_reader: Optional[PipesMessageReader] = None,
        forward_termination: bool = True,
        **kwargs,
    ):
        super().__init__(
            main_client=client,
            context_injector=context_injector,
            message_reader=message_reader,
            tagging_client=tagging_client,
        )
        self._s3_client = kwargs.get("s3_client")
        self._tagging_client = tagging_client
        self.client = client
        self.context_injector = opt_inst_param(
            context_injector,
            "context_injector",
            PipesContextInjector,
        ) or PipesDbfsContextInjector(client=self.client)
        self.message_reader = opt_inst_param(
            message_reader,
            "message_reader",
            PipesMessageReader,
        )
        self.forward_termination = bool_param(
            forward_termination, "forward_termination"
        )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return False

    def get_default_message_reader(
        self, task: jobs.SubmitTask
    ) -> "PipesDbfsMessageReader":
        # include log readers if the user is writing their logs to DBFS
        if (
            task.as_dict()
            .get("new_cluster", {})
            .get("cluster_log_conf", {})
            .get("dbfs", None)
        ):
            log_readers = [
                PipesDbfsLogReader(
                    client=self.client,
                    remote_log_name="stdout",
                    target_stream=sys.stdout,
                ),
                PipesDbfsLogReader(
                    client=self.client,
                    remote_log_name="stderr",
                    target_stream=sys.stderr,
                ),
            ]
        else:
            log_readers = None
        return PipesDbfsMessageReader(
            client=self.client,
            log_readers=log_readers,
        )

    def _prepare_environment(
        self,
        local_file_path: str,
        dbfs_path: str,
        libraries_to_build_and_upload: Optional[List[str]],
    ) -> None:
        """Prepare the environment by uploading files and ensuring libraries are available."""
        self._upload_file_to_cloud(
            local_file_path=local_file_path, cloud_path=dbfs_path
        )
        self.filesystem = "s3"
        self._ensure_library_on_cloud(
            libraries_to_build_and_upload=libraries_to_build_and_upload
        )

    def _process_submit_args(
        self, submit_args: Optional[Mapping[str, Any]]
    ) -> Dict[str, Any]:
        """Process submit_args to ensure they are of the allowed types."""
        if not submit_args:
            return {}

        allowed_types = (
            list,
            jobs.JobEmailNotifications,
            jobs.GitSource,
            jobs.JobsHealthRules,
            jobs.JobNotificationSettings,
            jobs.QueueSettings,
            int,
            jobs.WebhookNotifications,
        )
        submit_kwargs = {}
        for key, value in submit_args.items():
            if isinstance(value, allowed_types):
                submit_kwargs[key] = value
            else:
                raise TypeError(f"Unexpected type for submit_arg {key}: {type(value)}")
        return submit_kwargs

    def run(  # type: ignore
        self,
        env: Optional[Mapping[str, str]],
        context: OpExecutionContext,
        extras: Optional[PipesExtras],
        task: jobs.SubmitTask,
        submit_args: Optional[Mapping[str, str]],
        local_file_path: str,
        dbfs_path: str,
        libraries_to_build_and_upload: Optional[List[str]] = None,
    ) -> PipesClientCompletedInvocation:
        """Synchronously execute a Databricks job with the pipes protocol.

        :param env: An optional dict of environment variables to pass.
        :type env: Optional[Mapping[str, str]]
        :param context: The context from the executing op or asset.
        :type context: OpExecutionContext
        :param extras: An optional dict of extra parameters to pass to the subprocess.
        :type extras: Optional[PipesExtras]
        :param task: Specification of the Databricks task to run.
        :type task: databricks.sdk.service.jobs.SubmitTask
        :param submit_args: Additional keyword arguments that will be forwarded
                            as-is to ``WorkspaceClient.jobs.submit``.
        :type submit_args: Optional[Mapping[str, str]]
        :param local_file_path: The local path to the script to be executed.
        :type local_file_path: str
        :param dbfs_path: The corresponding path on DBFS where the script will be uploaded.
        :type dbfs_path: str
        :param libraries_to_build_and_upload: A list of local Python packages to build
                                            and upload as libraries.
        :type libraries_to_build_and_upload: Optional[List[str]]

        :return: Wrapper containing results reported by the external process.
        :rtype: PipesClientCompletedInvocation
        """
        self._prepare_environment(
            local_file_path, dbfs_path, libraries_to_build_and_upload
        )
        submit_kwargs = self._process_submit_args(submit_args)

        message_reader = self.message_reader or self.get_default_message_reader(task)
        with open_pipes_session(
            context=context,
            extras=extras,
            context_injector=self.context_injector,
            message_reader=message_reader,
        ) as pipes_session:
            submit_task_dict = task.as_dict()
            ascii_wandb_value = {"ASCII_WANDB": os.environ.get("ASCII_WANDB", "")}
            if not ascii_wandb_value:
                get_dagster_logger().warning(
                    "Environment variable 'ASCII_WANDB' is not set; defaulting to empty value."
                )
            submit_task_dict["new_cluster"]["spark_env_vars"] = {
                **submit_task_dict["new_cluster"].get("spark_env_vars", {}),
                **(env or {}),
                **pipes_session.get_bootstrap_env_vars(),
                **ascii_wandb_value,
            }
            task = jobs.SubmitTask.from_dict(submit_task_dict)
            submission = self.client.jobs.submit(
                run_name=extras.get("job_name"),  # type: ignore
                tasks=[task],
                **(submit_kwargs or {}),
            )
            run_id = submission.run_id
            context.log.info(
                f"Databricks url: {self.client.jobs.get_run(run_id).run_page_url}"
            )
            try:
                self._poll_till_success(
                    run_id=run_id, extras=extras, tagging_client=self._tagging_client
                )
            except CustomPipesException:
                if self.forward_termination:
                    context.log.info(
                        "[pipes] execution interrupted, canceling Databricks job."
                    )
                    self.client.jobs.cancel_run(run_id)
                    self._poll_til_terminating(str(run_id))
                raise
        return PipesClientCompletedInvocation(pipes_session)

    def _poll_til_terminating(self, run_id: str) -> None:
        # Wait to see the job enters a state that indicates the underlying task is no longer executing
        # TERMINATING: "The task of this run has completed, and the cluster and execution context are being cleaned up."
        run_id_int = int(run_id)
        while True:
            run = self.client.jobs.get_run(run_id_int)
            if (
                run
                and run.state
                and run.state.life_cycle_state
                in (
                    jobs.RunLifeCycleState.TERMINATING,
                    jobs.RunLifeCycleState.TERMINATED,
                    jobs.RunLifeCycleState.SKIPPED,
                    jobs.RunLifeCycleState.INTERNAL_ERROR,
                )
            ):
                return

            time.sleep(self.poll_interval_seconds)


PipesDatabricksEnhancedClient = ResourceParam[_PipesDatabricksClient]
