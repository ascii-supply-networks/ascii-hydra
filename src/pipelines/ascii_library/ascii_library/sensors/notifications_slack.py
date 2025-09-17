import os

from dagster import DefaultSensorStatus, SensorDefinition
from dagster_slack import (
    make_slack_on_run_failure_sensor,
)

# see https://docs.dagster.io/_apidocs/libraries/dagster-slack


def make_slack_on_failure(base_url: str) -> SensorDefinition:
    return make_slack_on_run_failure_sensor(  # type: ignore
        channel="#ascii-pipeline-notifications",
        slack_token=os.environ.get("ALERT_NOTIFICATION_TOKEN", ""),
        webserver_base_url=base_url,
        default_status=DefaultSensorStatus.RUNNING,
    )
