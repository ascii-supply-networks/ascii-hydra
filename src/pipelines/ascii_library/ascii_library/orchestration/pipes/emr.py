import random
import string
from io import BytesIO, StringIO
from typing import List, Optional

from ascii_library.orchestration.pipes import LibraryConfig, LibraryKind
from ascii_library.orchestration.pipes.cloud_client import _PipesBaseCloudClient
from ascii_library.orchestration.pipes.cloud_context_s3 import PipesS3ContextInjector
from ascii_library.orchestration.pipes.cloud_reader_writer_s3 import (
    PipesS3MessageReader,
)
from ascii_library.orchestration.pipes.instance_config import CloudInstanceConfig
from ascii_library.orchestration.pipes.utils import (
    library_from_dbfs_paths,
    library_to_cloud_paths,
)
from ascii_library.orchestration.resources.constants import aws_region, rackspace_user
from ascii_library.orchestration.resources.emr_constants import pipeline_bucket
from ascii_library.orchestration.resources.utils import (
    get_dagster_deployment_environment,
)
from boto3 import client
from dagster import get_dagster_logger
from dagster._annotations import experimental
from dagster._core.definitions.resource_annotation import ResourceParam
from dagster._core.errors import DagsterExecutionInterruptedError
from dagster._core.execution.context.compute import OpExecutionContext
from dagster._core.pipes.client import (
    PipesClientCompletedInvocation,
    PipesContextInjector,
    PipesMessageReader,
)
from dagster._core.pipes.utils import open_pipes_session
from dagster_pipes import PipesExtras


@experimental
class _PipesEmrClient(_PipesBaseCloudClient):
    """Pipes client for EMR.

    Args:
        emr_job_runner (EmrJobRunner): An instance of EmrJobRunner.
        env (Optional[Mapping[str,str]]): An optional dict of environment variables to pass to the EMR job.
        context_injector (Optional[PipesContextInjector]): A context injector to use to inject context into the EMR process.
        message_reader (Optional[PipesMessageReader]): A message reader to use to read messages from the EMR job.
        poll_interval_seconds (float): How long to sleep between checking the status of the job run.
    """

    def __init__(
        self,
        emr_client: client,
        s3_client: client,
        price_client: client,
        bucket: str,
        context_injector: Optional[PipesContextInjector] = None,
        message_reader: Optional[PipesMessageReader] = None,
    ):
        super().__init__(
            main_client=emr_client,
            context_injector=context_injector,
            message_reader=message_reader,
            s3_client=s3_client,
        )
        self._price_client = price_client
        self._emr_client = emr_client
        get_dagster_logger().debug(
            f"context_injector: bucket={bucket}, s3_client={s3_client}, emr_client={emr_client}"
        )
        self._s3_client = s3_client
        # self._message_reader = message_reader or PipesEMRLogMessageReader(
        #    s3_client=s3_client,
        #    emr_client=emr_client,
        #    check_cluster_every=check_cluster_every,
        # )
        key_prefix = "".join(random.choices(string.ascii_letters, k=30))
        self._key_prefix = key_prefix
        self._context_injector = context_injector or PipesS3ContextInjector(
            bucket=bucket, client=s3_client, key_prefix=key_prefix
        )
        self._message_reader = message_reader or PipesS3MessageReader(
            bucket=bucket, key_prefix=key_prefix, client=s3_client
        )

    def create_bootstrap_script(
        self,
        output_file: str = "bootstrap.sh",
        bucket: str = pipeline_bucket,
        libraries: Optional[List[LibraryConfig]] = None,
    ):
        dagster_deployment = get_dagster_deployment_environment()
        content = StringIO()
        content.write("#!/bin/bash\n")
        if libraries is not None:
            content.write("sudo yum update -y\n")
            content.write("sudo yum install -y python3 python3-pip\n")
            content.write("sudo pip3 uninstall -y py-dateutil\n")
            for lib in libraries:
                if lib.kind == LibraryKind.Wheel:
                    self.handle_wheel(bucket, content, lib)
                elif lib.kind == LibraryKind.Pypi:
                    self.handle_pypi(content, lib)

        destination = f"external_pipes/{dagster_deployment}/{output_file}"
        # content.write("export SPARK_PIPES_ENGINE=emr\n")
        content.seek(0)
        get_dagster_logger().debug(f"Bootstrap file content: \n\n{content.getvalue()}")
        self._s3_client.upload_fileobj(
            BytesIO(content.read().encode()), bucket, destination
        )
        return f"s3://{bucket}/{destination}"

    def handle_pypi(self, content, lib):
        package_install = lib.name_id
        if lib.version:
            package_install += f"{lib.version}"
        get_dagster_logger().debug(f"Installing library: {package_install}")
        content.write(f"sudo pip install '{package_install}' \n")

    def handle_wheel(self, bucket, content, lib):
        name_id = library_from_dbfs_paths(lib.name_id)
        path = library_to_cloud_paths(lib_name=name_id, filesystem="s3")
        content.write(f"aws s3 cp s3://{bucket}/{path} /tmp \n")
        get_dagster_logger().debug(f"Installing library: {name_id}")
        content.write(f"sudo pip install /tmp/{name_id}-0.0.0-py3-none-any.whl \n")

    def modify_env_var(self, cluster_config: dict, key: str, value: str):
        configs = cluster_config.get("Configurations", [])
        i = 0
        for config in configs:
            if config.get("Classification") == "spark-defaults":
                props = config.get("Properties")
                # props = config.get("Configurations")[0].get("Properties")
                props[f"spark.yarn.appMasterEnv.{key}"] = value
                # props[f"spark.executorEnv.{key}"] = value
                # props[f"spark.yarn.appMasterEnv.{key}"] = value
                cluster_config["Configurations"][i]["Properties"] = props
            i += 1
        return cluster_config

    def prepare_emr_job(
        self,
        local_file_path: str,
        bucket: str,
        s3_path: str,
        emr_job_config: dict,
        step_config: dict,
        libraries_to_build_and_upload: Optional[List[str]] = None,
        libraries: Optional[List[LibraryConfig]] = None,
        extras: Optional[PipesExtras] = None,
    ):
        self._upload_file_to_cloud(
            local_file_path=local_file_path, bucket=bucket, cloud_path=s3_path
        )
        if libraries_to_build_and_upload is not None:
            self._ensure_library_on_cloud(
                libraries_to_build_and_upload=libraries_to_build_and_upload
            )
            destination = self.create_bootstrap_script(libraries=libraries)
            emr_job_config["BootstrapActions"] = [
                {
                    "Name": "Install custom packages",
                    "ScriptBootstrapAction": {"Path": destination},
                }
            ]
        if extras:
            extras["emr_job_config"] = emr_job_config
            extras["step_config"] = step_config
        return extras, emr_job_config

    def run(  # type: ignore
        self,
        *,
        context: OpExecutionContext,
        emr_job_config: dict,
        step_config: dict,
        local_file_path: str,
        bucket: str,
        s3_path: str,
        libraries_to_build_and_upload: Optional[List[str]] = None,
        libraries: Optional[List[LibraryConfig]] = None,
        extras: Optional[PipesExtras] = None,
        fleet_config: Optional[CloudInstanceConfig] = None,
    ) -> PipesClientCompletedInvocation:
        """Synchronously execute an EMR job with the pipes protocol."""
        if (
            emr_job_config["Instances"].get("InstanceGroups") is None
            and emr_job_config["Instances"].get("InstanceFleets") is None
            and fleet_config is not None
        ):
            emr_job_config["Instances"]["InstanceFleets"] = (
                fleet_config.get_fleet_programatically(
                    emrClient=self._emr_client, priceClient=self._price_client
                )
            )
            emr_job_config["ManagedScalingPolicy"]["ComputeLimits"][
                "UnitType"
            ] = "InstanceFleetUnits"
        elif (
            emr_job_config["Instances"].get("InstanceGroups") is None
            and emr_job_config["Instances"].get("InstanceFleets") is None
            and fleet_config is None
        ):
            raise
        extras, emr_job_config = self.prepare_emr_job(
            local_file_path=local_file_path,
            bucket=bucket,
            s3_path=s3_path,
            emr_job_config=emr_job_config,
            step_config=step_config,
            libraries_to_build_and_upload=libraries_to_build_and_upload,
            libraries=libraries,
            extras=extras,
        )
        with open_pipes_session(
            context=context,
            message_reader=self._message_reader,
            context_injector=self._context_injector,
            extras=extras,
        ) as session:
            emr_job_config = extras.get("emr_job_config")
            emr_job_config = self.modify_env_var(
                cluster_config=emr_job_config, key="bucket", value=bucket
            )
            emr_job_config = self.modify_env_var(
                cluster_config=emr_job_config, key="key", value=self._key_prefix
            )
            try:
                job_flow = self._emr_client.run_job_flow(**emr_job_config)
                get_dagster_logger().debug(f"EMR configuration: {job_flow}")
                self._emr_client.add_tags(
                    ResourceId=job_flow["JobFlowId"],
                    Tags=[
                        {"Key": "jobId", "Value": job_flow["JobFlowId"]},
                        {"Key": "executionMode", "Value": extras["execution_mode"]},
                        {"Key": "engine", "Value": extras["engine"]},
                    ],
                )
                self._emr_client.add_job_flow_steps(
                    JobFlowId=job_flow["JobFlowId"],
                    Steps=[extras.get("step_config")],
                )
                get_dagster_logger().info(
                    f"If not sign in on Rackspace, please do it now: https://manage.rackspace.com/aws/account/{rackspace_user}/consoleSignin"
                )
                get_dagster_logger().info(
                    f"EMR url: https://{aws_region}.console.aws.amazon.com/emr/home?region={aws_region}#/clusterDetails/{job_flow['JobFlowId']}"
                )
                self._poll_till_success(cluster_id=job_flow["JobFlowId"])
            except DagsterExecutionInterruptedError:
                context.log.info("[pipes] execution interrupted, canceling EMR job.")
                self._emr_client.terminate_job_flows(JobFlowIds=[job_flow["JobFlowId"]])
                raise
            finally:
                get_dagster_logger().debug("finished")
        return PipesClientCompletedInvocation(session)


PipesEmrEnhancedClient = ResourceParam[_PipesEmrClient]
