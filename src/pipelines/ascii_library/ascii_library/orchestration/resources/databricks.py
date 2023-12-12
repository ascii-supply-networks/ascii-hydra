# ---------------------------------------------------
# based on https://github.com/dagster-io/hooli-data-eng-pipelines/blob/3b43ceb2ae4bb20acc014ab2d305e238294b11a4/hooli_data_eng/resources/databricks.py
# This resource allows the spark asset to
# run on a databricks cluster in branch or prod
# (Locally the asset uses a local pyspark session)

# During asset evaluation, a new cluster is created
# and the project code is sent to the cluster using a
# *step launcher*
# Existing clusters can be used instead of creating a
# job-specific cluster by changing the cluster key to
# "cluster": {"existing": "cluster-id"}

import os
from pathlib import Path

from dagster_databricks import databricks_pyspark_step_launcher

instance_profile = "arn:aws:iam::018967853140:instance-profile/data-access-profile"

# https://github.com/dagster-io/dagster/issues/18425#issuecomment-1837225243
# this should work
# we will not use the step launcher - rather pipes
# cluster:
#           new:
#             aws_attributes:
#               ebs_volume_count: 1
#               ebs_volume_size: 32
#             nodes:
#               node_types:
#                 driver_node_type_id: c5.xlarge
#                 node_type_id: c5.xlarge
#             size:
#               autoscale:
#                 max_workers: 3
#                 min_workers: 1
#             spark_version: 10.4.x-scala2.12
# cluster_config = {
#     "autoscale": {"min_workers": 1, "max_workers": 3},
#     "cluster_name": "dagster-step-launcher",
#     "spark_version": "14.2.x-scala2.12",
#     "aws_attributes": {
#         "first_on_demand": 2,
#         "availability": "SPOT_WITH_FALLBACK",
#         "zone_id": "us-east-1d",
#         "instance_profile_arn": instance_profile,
#         "spot_bid_price_percent": 100,
#     },
#     "node_type_id": "m6id.2xlarge",
#     "driver_node_type_id": "md-fleet.xlarge",
#     "enable_elastic_disk": True,
# }
cluster_config = {
    "size": {"num_workers": 1},
    "spark_version": "11.2.x-scala2.12",
    "nodes": {"node_types": {"node_type_id": "i3.xlarge"}},
}

dagster_version = ">=1.5.8,<1.6"
dagster_library_version = ">=0.21.8,<0.22"

db_step_launcher_dev = databricks_pyspark_step_launcher.configured(
    {
        "run_config": {
            "run_name": "launch_step",
            # "cluster": {"new": cluster_config},
            # TODO test with both new and existing mode
            # TODO test with step launcher and job launcher mode
            # TODO evaluate external tracking mode
            # "cluster": {"existing": {"env": "DATABRICKS_EXISTING_CLUSTER_ID"}},
            "cluster": {"existing": "1201-111856-56hu41et"},
            # "cluster": {"existing": "1201-111856-56hu41et"},
            "libraries": [
                # dagster-databricks==0.21.10
                # PyPI
                # -
                # -
                # dagster-pyspark==0.21.10
                # PyPI
                # -
                # -
                # dagster==1.5.10
                # {"pypi": {"package": f"dagster-aws {dagster_library_version}"}},
                # {"pypi": {"package": f"dagster-cloud {dagster_version}"}},
                {"pypi": {"package": "databricks-sdk<0.9"}},
                # {"pypi": {"package": f"dagster {dagster_version}"}},
                # {"pypi": {"package": f"dagster-pyspark {dagster_library_version}"}},
                # {"pypi": {"package": f"dagster-databricks {dagster_library_version}"}},
                {"pypi": {"package": "fastwarc==0.14.5"}},
                {"pypi": {"package": "surt==0.3.1"}},
                {"pypi": {"package": "warcio==1.7.4"}},
            ],
        },
        "databricks_host": {"env": "DATABRICKS_HOST"},
        "databricks_token": {"env": "DATABRICKS_TOKEN"},
        "permissions": {},
        # TODO is this needed
        "staging_prefix": "/dbfs/tmp",
        # TODO is this needed
        "wait_for_logs": True,
        # TODO is this needed? What for long runnign jobs?
        "max_completion_wait_time_seconds": 3600,
        # TODO what is the right path here
        # TODO how does this work with 2 packages (library and pipeline) should we inline one?
        # TODO dagster docs say something like legacy here - figure out what is the right thing to do
        "local_pipeline_package_path": str(Path(__file__).parent.parent.parent.parent),
        "env_variables": {
            "DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT": os.getenv(
                "DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", ""
            ),
            "DAGSTER_CLOUD_DEPLOYMENT_NAME": os.getenv(
                "DAGSTER_CLOUD_DEPLOYMENT_NAME", ""
            ),
        },
        "secrets_to_env_variables": [
            # {"name": "DATABRICKS_HOST", "key": "adls2_key", "scope": "dagster-test"},
        ],
        # should be handled from instance profile
        # "storage": {
        #     "s3": {
        #         "access_key_key": "access_key_key",
        #         "secret_key_key": "secret_key_key",
        #         "secret_scope": "dagster-test",
        #     }
        # },
    }
)
