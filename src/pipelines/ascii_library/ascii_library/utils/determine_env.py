import os

import dagster as dg


def get_env(deployment_key: str = "DAGSTER_DEPLOYMENT", default_value="dev"):
    if (
        (os.getenv("DAGSTER_CLOUD_IS_BRANCH_DEPLOYMENT", "") == "1")
        or (os.environ.get(deployment_key, default_value) == "dev")
        or (os.environ.get(deployment_key, default_value) == "BRANCH")
    ):
        return "BRANCH"
    if (
        (os.getenv("DAGSTER_CLOUD_DEPLOYMENT_NAME", "") == "prod")
        or (os.environ.get(deployment_key, default_value) == "prod")
        or (os.environ.get(deployment_key, default_value) == "PROD")
    ):
        return "PROD"
    elif os.environ.get(deployment_key, default_value) == "INTEGRATION_TEST":
        return "INTEGRATION_TEST"
    raise ValueError(
        f"Unknown environment: {os.environ.get(deployment_key, default_value)}"
    )


def get_dagster_deployment_environment(
    deployment_key: str = "DAGSTER_DEPLOYMENT", default_value="dev"
):
    deplyoment = get_env(deployment_key, default_value)
    dg.get_dagster_logger().debug("dagster deployment environment: %s", deplyoment)
    return deplyoment
