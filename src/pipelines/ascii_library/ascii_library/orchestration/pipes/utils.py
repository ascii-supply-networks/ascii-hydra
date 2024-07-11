import glob
import os
import subprocess

from ascii_library.orchestration.pipes import ExecutionMode
from ascii_library.orchestration.resources.utils import (
    get_dagster_deployment_environment,
)


def library_to_cloud_paths(lib_name: str, filesystem: str = "dbfs"):
    dagster_deployment = get_dagster_deployment_environment()
    if filesystem == "dbfs":
        # TODO: potential race condition for parallel runs. fix version number
        return f"{filesystem}:/customlibs/{dagster_deployment}/{lib_name}-0.0.0-py3-none-any.whl"
    else:
        # TODO: should it be elif?
        return f"customlibs/{dagster_deployment}/{lib_name}-0.0.0-py3-none-any.whl"


def library_from_dbfs_paths(dbfs_path: str):
    last_part = dbfs_path.split("/")[-1]
    return last_part.split("-")[0]


def package_library(mylib_path):
    mylib_path = os.path.abspath(mylib_path)
    dist_path = os.path.join(mylib_path, "dist")
    # Clear the dist directory if it already exists
    if os.path.exists(dist_path):
        for f in glob.glob(os.path.join(dist_path, "*")):
            os.remove(f)
    else:
        os.makedirs(dist_path)
    subprocess.check_call(
        ["python", "-m", "build", "--wheel", "--outdir", dist_path], cwd=mylib_path
    )
    wheel_files = glob.glob(os.path.join(dist_path, "*.whl"))
    if wheel_files:
        wheel_path = wheel_files[
            0
        ]  # this assumes that there is only one wheel, maybe we will work with multiple versions
        package_name = os.path.basename(wheel_path)
        return wheel_path, package_name
    else:
        raise FileNotFoundError("No wheel file found in the dist directory.")


def get_input_path(io_nodes, part_seed, part_cc, lang):
    if lang == "all":
        return f"{io_nodes}/seed_nodes={part_seed}/crawl_id={part_cc}/main_language=*"
    else:
        return (
            f"{io_nodes}/seed_nodes={part_seed}/crawl_id={part_cc}/main_language={lang}"
        )


def calculate_parallelism(spark, input_path):
    record_count = spark.sparkContext.textFile(input_path).count()
    spark_max_sensible_paralellism = 90000
    if record_count >= spark_max_sensible_paralellism:
        return spark_max_sensible_paralellism
    else:
        return max(200, int(record_count / 4))


def configure_spark(  # noqa: C901
    spark,
    execution_mode,
    compression_codec,
    default_parallelism,
    shuffle_partitions,
    partitionDiscovery_parallelism,
):
    cfgs = [
        ("spark.sql.parquet.compression.codec", compression_codec),
        ("spark.sql.files.maxPartitionBytes", 50 * 1024 * 1024),
        ("spark.databricks.delta.retentionDurationCheck.enabled", "false"),
        ("spark.databricks.delta.vacuum.parallelDelete.enabled", "true"),
        ("spark.sql.sources.partitionOverwriteMode", "dynamic"),
        ("spark.databricks.delta.schema.autoMerge.enabled", "True"),
        ("spark.databricks.delta.schema.autoMerge.enabledOnWrite", "True"),
    ]
    if execution_mode == ExecutionMode.Full:
        if default_parallelism:
            cfgs.append(("spark.default.parallelism", default_parallelism))
        if shuffle_partitions:
            cfgs.append(("spark.sql.shuffle.partitions", shuffle_partitions))
        if partitionDiscovery_parallelism:
            cfgs.append(
                ("spark.sql.shuffle.partitions", partitionDiscovery_parallelism)
            )

    for item in cfgs:
        spark.conf.set(item[0], item[1])  # type: ignore
