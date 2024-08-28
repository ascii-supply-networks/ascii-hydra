import os

from ascii_library.orchestration.pipes.instance_config import CloudInstanceConfig
from ascii_library.orchestration.pipes.spark_pipes_factory import (
    LibraryConfig,
    LibraryKind,
    spark_pipes_asset_factory,
)
from ascii_library.orchestration.pipes.utils import library_to_cloud_paths
from ascii_library.orchestration.resources.constants import instance_profile
from ascii_library.orchestration.resources.emr import get_emr_cluster_config
from ascii_library.orchestration.resources.emr_constants import (
    CapacityReservation,
    InstanceFamilyId,
    TimeoutAction,
)
from ascii_library.orchestration.resources.spark import dev_spark_config
from dagster import define_asset_job, file_relative_path, get_dagster_logger

from pipeline_example.resources import resource_defs_by_deployment_name

deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "dev")
get_dagster_logger().info(f"Using deployment of: {deployment_name}")
resource_defs = resource_defs_by_deployment_name[deployment_name]

spark_pipes_client = resource_defs["spark_pipes_client"]

ag = "mini_example"
key_prefix = ["example_s3", ag]
spot_bid_price_percent = 10


basic_libs_upload = ["ascii", "ascii_library"]
basic_libraries = [
    LibraryConfig(
        kind=LibraryKind.Wheel, name_id=library_to_cloud_paths("ascii_library")
    ),
    LibraryConfig(kind=LibraryKind.Wheel, name_id=library_to_cloud_paths("ascii")),
]

dbr_additional_libraries = [
    LibraryConfig(
        kind=LibraryKind.Pypi, version=">=0.8<0.17", name_id="databricks-sdk"
    ),
]

emr_additional_libraries = [
    LibraryConfig(
        kind=LibraryKind.Pypi, version=">=2.8.2<2.9", name_id="python-dateutil"
    ),
]

step1 = spark_pipes_asset_factory(
    name="step1",
    key_prefix=key_prefix,
    spark_pipes_client=spark_pipes_client,
    external_script_file=file_relative_path(__file__, "step_1.py"),
    cfg=None,
    group_name=ag,
    local_spark_config=dev_spark_config(),
    databricks_cluster_config={
        "autoscale": {"min_workers": 1, "max_workers": 5},
        "spark_version": "14.3.x-scala2.12",
        "aws_attributes": {
            "first_on_demand": 2,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1d",
            "instance_profile_arn": instance_profile,
        },
        "node_type_id": "m6id.2xlarge",
        "driver_node_type_id": "md-fleet.xlarge",
        "enable_elastic_disk": True,
        "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
    },
    libraries_to_build_and_upload=basic_libs_upload,
    libraries_config=basic_libraries,
    emr_additional_libraries=emr_additional_libraries,
    dbr_additional_libraries=dbr_additional_libraries,
    emr_job_config=get_emr_cluster_config(
        minimumCapacityUnits=10,  # 2vCPU+4vCPU+4vCPU
        maximumCapacityUnits=18,  # 2vCPU+4vCPU*2 Core node+ 4 vCPU*2 tasknode
        maximumOnDemandCapacityUnits=10 + 8,  # 2vCPU+4vCPU*2 Core node
        maximumCoreCapacityUnits=4,  # 4vCPU*2 Task node
    ),
    fleet_filters=CloudInstanceConfig(
        TargetOnSpot=4,
        TargetOnDemand=6,
        ReservationPreference=CapacityReservation.open,
        TimeoutAction=TimeoutAction.switch,
        PercentageOfOnDemandPrice=spot_bid_price_percent,
        Filters={
            "InstanceFamilyId": InstanceFamilyId.balanceCur,
            "WorkerPrefix": "m6id",
            "WorkerSuffix": "2xlarge",
            "Suffix": "xlarge",
            "StorageGB": 100,
            "MemoryGB": 4,
        },
    ),
    # override_default_engine=Engine.Local,
)

step2 = spark_pipes_asset_factory(
    name="step2",
    key_prefix=key_prefix,
    spark_pipes_client=spark_pipes_client,
    external_script_file=file_relative_path(__file__, "step_2.py"),
    cfg=None,
    deps=[step1],
    group_name=ag,
    local_spark_config=dev_spark_config(),
    databricks_cluster_config={
        "autoscale": {"min_workers": 1, "max_workers": 5},
        "spark_version": "14.3.x-scala2.12",
        "aws_attributes": {
            "first_on_demand": 2,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-1d",
            "instance_profile_arn": instance_profile,
        },
        "node_type_id": "m6id.2xlarge",
        "driver_node_type_id": "md-fleet.xlarge",
        "enable_elastic_disk": True,
        "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
    },
    libraries_to_build_and_upload=basic_libs_upload,
    libraries_config=basic_libraries,
    emr_additional_libraries=emr_additional_libraries,
    dbr_additional_libraries=dbr_additional_libraries,
    emr_job_config=get_emr_cluster_config(
        minimumCapacityUnits=10,  # 2vCPU+4vCPU+4vCPU
        maximumCapacityUnits=18,  # 2vCPU+4vCPU*2 Core node+ 4 vCPU*2 tasknode
        maximumOnDemandCapacityUnits=10 + 8,  # 2vCPU+4vCPU*2 Core node
        maximumCoreCapacityUnits=4,  # 4vCPU*2 Task node
    ),
    fleet_filters=CloudInstanceConfig(
        TargetOnSpot=4,
        TargetOnDemand=6,
        ReservationPreference=CapacityReservation.open,
        TimeoutAction=TimeoutAction.switch,
        PercentageOfOnDemandPrice=spot_bid_price_percent,
        Filters={
            "InstanceFamilyId": InstanceFamilyId.balanceCur,
            "WorkerPrefix": "m6id",
            "WorkerSuffix": "2xlarge",
            "Suffix": "xlarge",
            "StorageGB": 100,
            "MemoryGB": 4,
        },
    ),
    # override_default_engine=Engine.Local,
)

example_pipeline = define_asset_job(
    "example_pipeline",
    selection=[step1, step2],
)
