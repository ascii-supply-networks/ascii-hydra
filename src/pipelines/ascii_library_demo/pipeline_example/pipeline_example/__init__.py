import os

from dagster import Definitions, get_dagster_logger, load_assets_from_modules

from pipeline_example.resources import resource_defs_by_deployment_name

from . import assets
from .assets import example_pipeline

deployment_name = os.environ.get("DAGSTER_DEPLOYMENT", "dev")
get_dagster_logger().info(f"Using deployment of: {deployment_name}")
resource_defs = resource_defs_by_deployment_name[deployment_name]

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[example_pipeline],
    resources=resource_defs,
)
