import os
import shutil
from typing import List, Optional, Sequence, Union

from ascii_library.orchestration.pipes import LibraryConfig, LibraryKind
from ascii_library.orchestration.pipes.databricks import PipesDatabricksEnhancedClient
from ascii_library.orchestration.pipes.emr import PipesEmrEnhancedClient
from ascii_library.orchestration.pipes.emr_constants import pipeline_bucket
from ascii_library.orchestration.pipes.spark_pipes import Engine, SparkPipesResource
from ascii_library.orchestration.resources.utils import (
    get_dagster_deployment_environment,
)
from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Mapping,
    MaterializeResult,
    PartitionsDefinition,
    PipesSubprocessClient,
    asset,
)
from databricks.sdk.service import jobs

deployment_env = get_dagster_deployment_environment()


def get_libs_dict(
    cfg: Optional[List[LibraryConfig]],
) -> List[Mapping[str, Union[str, Mapping]]]:
    libs_dict: List[Mapping[str, Union[str, Mapping]]] = []

    if cfg is not None:
        for lib in cfg:
            if lib.kind == LibraryKind.Pypi:
                package_str = (
                    f"{lib.name_id}{lib.version}" if lib.version else lib.name_id
                )
                libs_dict.append({lib.kind.value: {"package": package_str}})
            elif lib.kind == LibraryKind.Wheel:
                libs_dict.append({lib.kind.value: lib.name_id})

    return libs_dict


def generate_uploaded_script_paths(
    local_input_path: str, prefix: str = "dbfs:/"
) -> str:
    """Retrieve file name from path.
    construct full path as: "dbfs:/external_pipes/<<filename>>.py"
    """
    # TODO potential race condition (file in DBFS), in case of parallel runs
    # this is not yet including versioning. When launching more than 1 instance of a databricks spark job in parallel this file is overwritten

    filename_without_extension = os.path.splitext(os.path.basename(local_input_path))[0]
    new_path = f"{prefix}/{deployment_env}/{filename_without_extension}.py"
    return new_path


def spark_pipes_asset_factory(  # noqa: C901
    name: str,
    key_prefix: Sequence[str],
    spark_pipes_client: SparkPipesResource,
    external_script_file: str,
    partitions_def: Optional[PartitionsDefinition] = None,
    deps: Optional[Sequence[AssetsDefinition]] = None,
    group_name: Optional[str] = None,
    local_spark_config: Optional[Mapping[str, str]] = None,
    libraries_to_build_and_upload: Sequence[str] = None,
    databricks_cluster_config: Optional[Mapping[str, str]] = None,
    libraries_config: Optional[Sequence[LibraryConfig]] = None,
    emr_additional_libraries: Optional[Sequence[LibraryConfig]] = None,
    dbr_additional_libraries: Optional[Sequence[LibraryConfig]] = None,
    emr_job_config: Optional[dict] = None,
    override_default_engine: Optional[Engine] = None,
    # TODO: in the future support IO manager perhaps for python reading directly from S3 io_manager_key:Optional[str]: None
    # but maybe we intend to keep the offline sync process
    # TODO: should we use asset KWARGS here instead for flexibility?
):
    """
    Construct dagster-pipes based Spark assets for multiple engines: local pyspark and databricks

    Automatically configure the right pipes clients.
    In the case of Databricks: Ensure dependencies and scripts are present and ready to be executed - automatically build the dependent libraries and upload these to DBFS.
    """
    client: Union[
        PipesSubprocessClient,
        PipesDatabricksEnhancedClient,
        PipesEmrEnhancedClient,
    ] = spark_pipes_client.get_spark_pipes_client()
    if override_default_engine is not None:
        engine_to_use = override_default_engine
    else:
        engine_to_use = spark_pipes_client.engine

    # TODO: auto upload is a convenience feature for now in the future this should be a CI pipeline step with a defined version - this task should only be executed once on ci build and not per each task
    @asset(
        name=name,
        compute_kind=engine_to_use.value,
        deps=deps,
        partitions_def=partitions_def,
        group_name=group_name,
        key_prefix=key_prefix,
    )
    def inner_spark_pipes_asset(context: AssetExecutionContext) -> MaterializeResult:
        client_params = handle_shared_parameters(context)

        if engine_to_use == Engine.Local:
            return handle_local(client_params, context)  # type: ignore
        elif engine_to_use == Engine.Databricks:
            return handle_databricks(client_params, context)  # type: ignore
        elif engine_to_use == Engine.EMR:
            return handle_emr(client_params, context)  # type: ignore
        else:
            raise ValueError(f"Unsupported engine mode: {engine_to_use.value}")

    def handle_emr(client_params, context):
        s3_script_path = generate_uploaded_script_paths(
            local_input_path=external_script_file, prefix="external_pipes"
        )
        step_config = {
            "Name": "Spark Step",
            "ActionOnFailure": "TERMINATE_JOB_FLOW",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": ["spark-submit", f"s3://{pipeline_bucket}/{s3_script_path}"],
            },
        }
        emr_job_config["Name"] = client_params["job_name"]
        if emr_additional_libraries is not None:
            engine_specific_libs = libraries_config.copy()
            engine_specific_libs.extend(emr_additional_libraries)
        else:
            engine_specific_libs = libraries_config
        return client.run(  # type: ignore
            context=context,
            emr_job_config=emr_job_config,
            bucket=pipeline_bucket,
            local_file_path=external_script_file,
            s3_path=s3_script_path,
            step_config=step_config,
            libraries_to_build_and_upload=libraries_to_build_and_upload,
            libraries=engine_specific_libs,
            extras=client_params,
        ).get_materialize_result()

    def handle_databricks(client_params, context):
        # we are using databricks engine
        script_file_path_after_upload = generate_uploaded_script_paths(
            external_script_file, prefix="dbfs:/external_pipes"
        )
        if dbr_additional_libraries is not None:
            engine_specific_libs = libraries_config.copy()
            engine_specific_libs.extend(dbr_additional_libraries)
        else:
            engine_specific_libs = libraries_config

        task = jobs.SubmitTask.from_dict(
            {
                "new_cluster": databricks_cluster_config,
                "libraries": get_libs_dict(engine_specific_libs),
                "task_key": "dagster-launched",
                "spark_python_task": {
                    "python_file": script_file_path_after_upload,
                    "source": jobs.Source.WORKSPACE,
                },
            }
        )
        return client.run(  # type: ignore
            task=task,
            context=context,
            env={
                "SPARK_PIPES_ENGINE": "databricks",
            },
            extras=client_params,
            libraries_to_build_and_upload=libraries_to_build_and_upload,
            local_file_path=external_script_file,
            dbfs_path=script_file_path_after_upload,
        ).get_materialize_result()

    def handle_local(client_params, context):
        cmd = [shutil.which("python"), external_script_file]
        client_params["local_spark_config"] = local_spark_config
        return client.run(  # type: ignore
            command=cmd,
            context=context,
            extras=client_params,
        ).get_materialize_result()

    def handle_shared_parameters(context):
        client_params = {
            "execution_mode": spark_pipes_client.execution_mode.value,
            "engine": engine_to_use.value,
        }
        # TODO check if we have to update dagster that context.has_asset_partitions() dpesn't work
        if partitions_def is not None:
            client_params["partition_key"] = context.partition_key
            job_name = f"{name}_{deployment_env}_{spark_pipes_client.execution_mode.value}_{context.partition_key}"
        else:
            client_params["partition_key"] = None
            job_name = (
                f"{name}_{spark_pipes_client.execution_mode.value}_{deployment_env}"
            )
        client_params["job_name"] = job_name
        return client_params

    return inner_spark_pipes_asset
