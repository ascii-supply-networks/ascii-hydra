from typing import Dict, Mapping, Optional, Sequence

from ascii_library.orchestration.pipes.instance_config import CloudInstanceConfig
from ascii_library.orchestration.resources import emr_constants

from .constants import (
    job_role,
    master_security_group,
    service_role,
    slave_security_group,
)


def add_spark_settings(
    spark_settings: Dict[str, dict], additional_spark_args: Optional[Dict[str, str]]
) -> Dict[str, dict]:
    """Helper function to add additional Spark settings."""
    if additional_spark_args:
        for key, value in additional_spark_args.items():
            spark_settings["Properties"][key] = value
    return spark_settings or {}


def get_basic_config(name, s3_log_uri, job_role, service_role):
    """Create the basic cluster configuration."""
    return {
        "Name": name,
        "LogUri": s3_log_uri,
        "ReleaseLabel": emr_constants.releaseLabel,
        "Applications": [{"Name": "Spark"}],
        "VisibleToAllUsers": True,
        "JobFlowRole": job_role,
        "ServiceRole": service_role,
        "Tags": [{"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"}],
        "ScaleDownBehavior": "TERMINATE_AT_TASK_COMPLETION",
        "AutoTerminationPolicy": {"IdleTimeout": 180},
    }


def get_emr_cluster_config(
    minimumCapacityUnits: int,
    maximumCapacityUnits: int,
    maximumOnDemandCapacityUnits: int,
    # we do not store anything to HDFS, want to limit core nodes to a minimum
    maximumCoreCapacityUnits: int = 1,
    s3_log_uri: Optional[str] = None,
    subnet_id: Optional[str] = None,
    master_security_group: str = master_security_group,
    slave_security_group: str = slave_security_group,
    service_role: str = service_role,
    job_role: str = job_role,
    additional_master_security_groups: Optional[list[str]] = None,
    additional_slave_security_groups: Optional[list[str]] = None,
    ssh: str = "emr_key",
    name: Optional[str] = "unnamed",
    group: Optional[Sequence[CloudInstanceConfig]] = None,
    fleet: Optional[Sequence[CloudInstanceConfig]] = None,
    additional_spark_args: Optional[Mapping[str, str]] = None,
) -> Dict:
    if additional_slave_security_groups is None:
        additional_slave_security_groups = [
            master_security_group,
            slave_security_group,
        ]
    if additional_master_security_groups is None:
        additional_master_security_groups = [
            master_security_group,
            slave_security_group,
        ]
    spark_settings = {
        "Classification": "spark-defaults",
        "Properties": {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            # "spark.databricks.delta.schema.autoMerge.enabled": "True",
            "spark.databricks.delta.schema.autoMerge.enabledOnWrite": "True",
            "spark.repositories": "https://repo1.maven.org/maven2/",
            "spark.jars.packages": "io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-hadoop-cloud_2.12:3.5.0,com.johnsnowlabs.nlp:spark-nlp_2.12:5.3.3",
            "spark.hadoop.fs.s3a.s3guard.ddb.region": "us-east-1",
            "spark.submit.deployMode": "cluster",
            # not needed, these paramters are DIRECTLY managed by the compute provider by selecting an instance type
            # "spark.executor.memory": executorMemory,
            # "spark.driver.memory": driverMemory,
            "spark.driver.maxResultSize": "10G",
            # "spark.storage.memoryFraction": storageMemoryFraction,
            # "spark.shuffle.memoryFraction": "0.2",
            # hard coding here TODO pass in dynamically
            # "spark.driver.memory": "30G",
            # "spark.executor.memory": "8575m",
            # "spark.executor.memory": "33G",
            # "spark.executor.memory": "8G",
            # "spark.executor.memoryOverhead": "2G",
            "spark.sql.parquet.columnarReaderBatchSize": "2048",  # half of default
            # "spark.memory.offHeap.enabled": "true",
            # "spark.memory.offHeap.size": "24G",
            # "spark.memory.offHeap.size":36974886912,
            "spark.blacklist.decommissioning.timeout": "300s",
            "spark.yarn.appMasterEnv.SPARK_PIPES_ENGINE": "emr",
            "spark.yarn.maxAppAttempts": "1",
        },
    }
    yarn_settings = {
        "Classification": "yarn-site",
        "Properties": {
            "yarn.resourcemanager.nodemanager-graceful-decommission-timeout-secs": "300",
            "yarn.node-labels.enabled": "true",
            "yarn.node-labels.am.default-node-label-expression": "CORE",
        },
    }
    spark_settings = add_spark_settings(spark_settings, additional_spark_args)  # type: ignore
    basic = get_basic_config(name, s3_log_uri, job_role, service_role)

    basic["Configurations"] = [
        {
            "Classification": "delta-defaults",
            "Properties": {"delta.enabled": "true"},
        },
        spark_settings,
        {
            "Classification": "spark",
            "Properties": {"maximizeResourceAllocation": "true"},
        },
        yarn_settings,
    ]

    instances = {
        # "MasterInstanceType":
        # "SlaveInstanceType":
        "Ec2KeyName": ssh,
        #'Placement': {
        #    'AvailabilityZone': 'string',
        #    'AvailabilityZones': [
        #        'string',
        #    ]
        # },
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
        #'HadoopVersion': 'string',
        "Ec2SubnetId": subnet_id,
        #'Ec2SubnetIds': [
        #    'string',
        # ],
        "EmrManagedMasterSecurityGroup": master_security_group,
        "EmrManagedSlaveSecurityGroup": slave_security_group,
        #'ServiceAccessSecurityGroup': 'string',
        "AdditionalMasterSecurityGroups": additional_master_security_groups,
        "AdditionalSlaveSecurityGroups": additional_slave_security_groups,
    }
    computeLimits = {
        "MinimumCapacityUnits": minimumCapacityUnits,
        "MaximumCapacityUnits": maximumCapacityUnits,
        "MaximumOnDemandCapacityUnits": maximumOnDemandCapacityUnits,
        "MaximumCoreCapacityUnits": maximumCoreCapacityUnits,
    }
    if group is not None:
        computeLimits["UnitType"] = "Instances"  # type: ignore
        instances["InstanceGroups"] = group
        basic["AutoScalingRole"] = "EMR_AutoScaling_DefaultRole"
    elif fleet is not None:
        computeLimits["UnitType"] = "InstanceFleetUnits"  # type: ignore
        instances["InstanceFleets"] = fleet
    basic["ManagedScalingPolicy"] = {"ComputeLimits": computeLimits}  # type: ignore
    basic["Instances"] = instances  # type: ignore
    return basic
