import base64
import os
import re
import tempfile
from unittest.mock import MagicMock, call, create_autospec, mock_open, patch

import boto3
import pytest
from ascii_library.orchestration.pipes.cloud_client import (
    _PipesBaseCloudClient,
    after_retry,
)
from ascii_library.orchestration.pipes.exceptions import CustomPipesException
from botocore.exceptions import ClientError, NoCredentialsError
from dagster import get_dagster_logger
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState, RunState
from moto import mock_aws
from mypy_boto3_resourcegroupstaggingapi import ResourceGroupsTaggingAPIClient
from tenacity import (
    BaseRetrying,
    RetryCallState,
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


class NonAbstractPipesCloudClient(_PipesBaseCloudClient):
    def run(self):
        pass

    @retry(
        stop=stop_after_attempt(2),
        wait=wait_exponential(multiplier=1, max=60),
        after=after_retry,
        retry=retry_if_exception_type(DatabricksError),
    )
    def _retrieve_state_dbr(self, run_id):
        return self.main_client.jobs.get_run(run_id)


class MockRetrying(BaseRetrying):
    def __init__(self):
        self.attempt_number = 0

    def __call__(self, fn, *args, **kwargs):
        self.attempt_number += 1
        if self.attempt_number == 1:
            raise DatabricksError("Mocked Databricks error")
        else:
            return fn(*args, **kwargs)


@pytest.fixture
def retry_state():
    return MockRetrying(), None, (), {}


@pytest.fixture
@mock_aws
def mock_emr_client():
    emr_client = boto3.client("emr", region_name="us-east-1")
    emr_client.run_job_flow(
        Name="test-cluster",
        Instances={
            "MasterInstanceType": "m4.large",
            "SlaveInstanceType": "m4.large",
            "InstanceCount": 3,
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
            "Ec2KeyName": "my-key",
        },
        JobFlowRole="EMR_EC2_DefaultRole",
        ServiceRole="EMR_DefaultRole",
        Applications=[{"Name": "Hadoop"}],
    )
    return emr_client


@pytest.fixture
@mock_aws
def mock_s3_client():
    s3_client = boto3.client("s3", region_name="us-east-1")
    return s3_client


@pytest.fixture
def mock_dbr_run():
    mock_run = MagicMock()
    mock_run.job_id = 123
    mock_state = create_autospec(RunState, instance=True)
    mock_state.life_cycle_state = RunLifeCycleState.TERMINATED
    mock_state.result_state = RunResultState.SUCCESS
    mock_state.state_message = "Run successful"
    mock_run.state = mock_state
    return mock_run


@pytest.fixture
def mock_workspace_client(mock_dbr_run):
    mock = create_autospec(WorkspaceClient, instance=True)
    mock.jobs = MagicMock()
    mock.jobs.get_run.return_value = mock_dbr_run
    mock.dbfs = MagicMock()
    return mock


@pytest.fixture
def mock_tagging_client():
    mock = create_autospec(ResourceGroupsTaggingAPIClient, instance=True)
    return mock


@pytest.fixture
def temp_library():
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file_path = os.path.join(temp_dir, "test_library.zip")
        with open(temp_file_path, "w") as f:
            f.write("test content")
        yield temp_file_path


###
# after_retry
###
def test_log_with_next_action(mocker, retry_state):
    mock_logger = mocker.patch.object(get_dagster_logger(), "debug")
    retry_manager, fn, args, kwargs = retry_state
    retry_state = RetryCallState(retry_manager, fn, args, kwargs)
    retry_state.attempt_number = 3
    retry_state.outcome = None
    retry_state.next_action = MagicMock(sleep=5)
    after_retry(retry_state)
    mock_logger.assert_called_once_with(
        "Retry attempt: 3. Waiting 5 seconds before next try."
    )


def test_log_without_next_action(mocker, retry_state):
    mock_logger = mocker.patch.object(get_dagster_logger(), "debug")
    retry_manager, fn, args, kwargs = retry_state
    retry_state = RetryCallState(retry_manager, fn, args, kwargs)
    retry_state.attempt_number = 1
    retry_state.outcome = None
    retry_state.next_action = None
    after_retry(retry_state)
    mock_logger.assert_called_once_with(
        "Retry attempt: 1. Waiting 0.0 seconds before next try."
    )


def test_log_with_no_next_action(mocker, retry_state):
    mock_logger = mocker.patch.object(get_dagster_logger(), "debug")
    retry_manager, fn, args, kwargs = retry_state
    retry_state = RetryCallState(retry_manager, fn, args, kwargs)
    retry_state.attempt_number = 2
    retry_state.outcome = None
    retry_state.next_action = None
    after_retry(retry_state)
    mock_logger.assert_called_once_with(
        "Retry attempt: 2. Waiting 0.0 seconds before next try."
    )


def test_high_attempt_number_handling(mocker, retry_state):
    mock_logger = mocker.patch.object(get_dagster_logger(), "debug")
    retry_manager, fn, args, kwargs = retry_state
    retry_state = RetryCallState(retry_manager, fn, args, kwargs)
    retry_state.attempt_number = 10
    retry_state.outcome = None
    retry_state.next_action = MagicMock(sleep=3)
    after_retry(retry_state)
    mock_logger.assert_called_once_with(
        "Retry attempt: 10. Waiting 3 seconds before next try."
    )


####
# init
####
def test_init_with_boto_client(mock_emr_client):
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    assert client.filesystem == ""
    assert client.poll_interval_seconds == 5


def test_init_with_workspace_client(mock_workspace_client, mock_tagging_client):
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    assert client.filesystem == "dbfs"
    assert client.poll_interval_seconds == 5


####
# _retrieve_state_emr
####


def test_retrieves_cluster_state_with_valid_cluster_id(mock_emr_client):
    mock_emr_client.describe_cluster = MagicMock(
        return_value={"Cluster": {"Status": {"State": "RUNNING"}}}
    )

    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    result = client._retrieve_state_emr(cluster_id="cluster-123456")
    assert result["Cluster"]["Status"]["State"] == "RUNNING"
    mock_emr_client.describe_cluster.assert_called_once_with(ClusterId="cluster-123456")


####
# _handle_emr_polling
####


def test_emr_cluster_description_missing_cluster_key(mock_emr_client):
    mock_emr_client.describe_cluster = MagicMock(return_value={})
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    with pytest.raises(KeyError):
        client._handle_emr_polling(cluster_id="example_cluster_id")


def test_master_public_dns_present(mock_emr_client):
    mock_emr_client.describe_cluster = MagicMock(
        return_value={
            "Cluster": {
                "Status": {"State": "RUNNING"},
                "MasterPublicDnsName": "example-dns",
            }
        }
    )
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    result = client._handle_emr_polling(cluster_id="example_cluster_id")
    assert result is True
    assert client.last_observed_state == "RUNNING"


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_log_transition_state(mock_get_dagster_logger, mock_emr_client):
    mock_logger = mock_get_dagster_logger.return_value
    mock_emr_client.describe_cluster = MagicMock(
        return_value={
            "Cluster": {
                "Status": {"State": "RUNNING"},
                "MasterPublicDnsName": "example-dns",
            }
        }
    )
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    client.last_observed_state = "STARTING"
    result = client._handle_emr_polling(cluster_id="example_cluster_id")
    assert result is True
    assert client.last_observed_state == "RUNNING"
    mock_logger.info.assert_any_call(
        "[pipes] EMR cluster id example_cluster_id observed state transition to RUNNING"
    )
    mock_logger.debug.assert_any_call("dns: example-dns")
    mock_emr_client.describe_cluster.assert_called_once_with(
        ClusterId="example_cluster_id"
    )


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_handle_emr_polling_terminated_state(mock_get_dagster_logger, mock_emr_client):
    mock_logger = mock_get_dagster_logger.return_value
    mock_emr_client.describe_cluster = MagicMock(
        return_value={
            "Cluster": {
                "Status": {
                    "State": "TERMINATED",
                    "StateChangeReason": {"Message": "Job flow completed successfully"},
                },
                "MasterPublicDnsName": "example-dns",
            }
        }
    )
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    client.last_observed_state = "RUNNING"
    result = client._handle_emr_polling(cluster_id="example_cluster_id")
    assert result is False
    assert client.last_observed_state == "TERMINATED"
    mock_logger.info.assert_any_call(
        "[pipes] EMR cluster id example_cluster_id observed state transition to TERMINATED"
    )
    mock_emr_client.describe_cluster.assert_called_once_with(
        ClusterId="example_cluster_id"
    )


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_handle_emr_polling_terminated_with_errors_state(
    mock_get_dagster_logger, mock_emr_client
):
    mock_logger = mock_get_dagster_logger.return_value
    mock_emr_client.describe_cluster = MagicMock(
        return_value={
            "Cluster": {
                "Status": {
                    "State": "TERMINATED_WITH_ERRORS",
                    "StateChangeReason": {"Message": "Job flow terminated with errors"},
                },
                "MasterPublicDnsName": "example-dns",
            }
        }
    )
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    client.last_observed_state = "RUNNING"
    with pytest.raises(
        CustomPipesException,
        match=re.escape("Error running EMR job flow: example_cluster_id"),
    ) as exc_info:
        client._handle_emr_polling(cluster_id="example_cluster_id")
    assert "Error running EMR job flow: example_cluster_id" in str(exc_info.value)
    mock_logger.info.assert_any_call(
        "[pipes] EMR cluster id example_cluster_id observed state transition to TERMINATED_WITH_ERRORS"
    )
    mock_emr_client.describe_cluster.assert_called_once_with(
        ClusterId="example_cluster_id"
    )


#######
# _retrieve_state_dbr
######
def test_retrieve_state_dbr_success(mock_workspace_client, mock_tagging_client):
    mock_run = mock_workspace_client.jobs.get_run.return_value
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    result = client._retrieve_state_dbr("12345")
    assert result == mock_run
    client.main_client.jobs.get_run.assert_called_once_with("12345")  # type: ignore


def test_retrieve_state_dbr_run_id_not_exist(
    mock_workspace_client, mock_tagging_client
):
    mock_workspace_client.jobs.get_run.side_effect = DatabricksError(
        "Run ID does not exist"
    )
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )

    with pytest.raises(RetryError) as excinfo:
        client._retrieve_state_dbr("non_existent_run_id")

    assert isinstance(excinfo.value.last_attempt.exception(), DatabricksError)
    assert str(excinfo.value.last_attempt.exception()) == "Run ID does not exist"
    mock_workspace_client.jobs.get_run.assert_called_with("non_existent_run_id")


def test_retrieve_state_dbr_retry_success(mock_workspace_client, mock_tagging_client):
    mock_run = mock_workspace_client.jobs.get_run.return_value
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    mock_workspace_client.jobs.get_run.side_effect = [DatabricksError, mock_run]
    result = client._retrieve_state_dbr("12345")
    assert result == mock_run
    assert mock_workspace_client.jobs.get_run.call_count == 2
    client.main_client.jobs.get_run.assert_called_with("12345")  # type: ignore


def test_correctly_returns_run_state(mock_workspace_client, mock_tagging_client):
    mock_run = mock_workspace_client.jobs.get_run.return_value
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    result = client._retrieve_state_dbr("12345")
    assert result == mock_run
    client.main_client.jobs.get_run.assert_called_once_with("12345")  # type: ignore


def test_logs_retry_attempts_and_sleep_time(
    caplog, mock_workspace_client, mock_tagging_client
):
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    mock_workspace_client.jobs.get_run.side_effect = DatabricksError(
        "Run ID does not exist"
    )

    with pytest.raises(RetryError):
        client._retrieve_state_dbr("12345")
    retry_log_found = any(
        "Retry attempt" in record.message for record in caplog.records
    )
    assert retry_log_found, "Expected retry log messages not found"
    assert mock_workspace_client.jobs.get_run.call_count > 1
    mock_workspace_client.jobs.get_run.assert_called_with("12345")


#######
# _handle_dbr_polling
######


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_successfully_retrieves_state(
    mock_get_dagster_logger, mock_workspace_client, mock_dbr_run
):
    mock_dbr_run.state.life_cycle_state = jobs.RunLifeCycleState.RUNNING
    pipes_client = NonAbstractPipesCloudClient(main_client=mock_workspace_client)
    run_id = str(mock_dbr_run.job_id)
    result = pipes_client._handle_dbr_polling(run_id)
    assert result is True
    mock_workspace_client.jobs.get_run.assert_called_once_with(run_id)
    mock_get_dagster_logger().debug.assert_called_once_with(
        f"[pipes] Databricks run {run_id} observed state transition to {jobs.RunLifeCycleState.RUNNING}"
    )
    assert pipes_client.last_observed_state == jobs.RunLifeCycleState.RUNNING


def test_invalid_run_id(mock_workspace_client, mock_tagging_client):
    mock_workspace_client.jobs.get_run.side_effect = DatabricksError("Run ID not found")
    pipes_client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    run_id = "invalid_run_id"

    with pytest.raises(RetryError):
        pipes_client._handle_dbr_polling(run_id)
    assert mock_workspace_client.jobs.get_run.call_count == 2
    mock_workspace_client.jobs.get_run.assert_called_with(run_id)


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
@patch.object(
    NonAbstractPipesCloudClient, "_handle_terminated_state_dbr", return_value=False
)
def test_logs_state_transitions(
    mock_handle_terminated_state,
    mock_get_dagster_logger,
    mock_workspace_client,
    mock_dbr_run,
    mock_tagging_client,
):
    mock_logger = MagicMock()
    mock_get_dagster_logger.return_value = mock_logger
    mock_dbr_run.state.life_cycle_state = jobs.RunLifeCycleState.TERMINATED
    mock_workspace_client.jobs.get_run.return_value = mock_dbr_run
    pipes_client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    run_id = "54321"
    call_args_list = mock_logger.debug.call_args_list
    result = pipes_client._handle_dbr_polling(run_id)
    assert len(call_args_list) == 2, "2 logs were made"
    state_transition_log_call = call_args_list[0]
    assert (
        "[pipes] Databricks run 54321 observed state transition to RunLifeCycleState.TERMINATED"
        in state_transition_log_call[0][0]
    )
    handling_log_call = call_args_list[1]
    assert "Handling terminated state for run: 54321" in handling_log_call[0][0]
    assert result is False
    mock_workspace_client.jobs.get_run.assert_called_once_with(run_id)
    assert pipes_client.last_observed_state == jobs.RunLifeCycleState.TERMINATED
    mock_handle_terminated_state.assert_called_once_with(run=mock_dbr_run)


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
@patch.object(
    NonAbstractPipesCloudClient, "_handle_terminated_state_dbr", return_value=False
)
def test_handles_internal_error_state_correctly(
    mock_handle_terminated_state,
    mock_get_dagster_logger,
    mock_workspace_client,
    mock_dbr_run,
    mock_tagging_client,
):
    mock_logger = MagicMock()
    mock_get_dagster_logger.return_value = mock_logger
    mock_dbr_run.state.life_cycle_state = jobs.RunLifeCycleState.INTERNAL_ERROR
    mock_workspace_client.jobs.get_run.return_value = mock_dbr_run
    pipes_client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    run_id = "12345"
    result = pipes_client._handle_dbr_polling(run_id)
    assert result is False
    mock_workspace_client.jobs.get_run.assert_called_once_with(run_id)
    assert pipes_client.last_observed_state == jobs.RunLifeCycleState.INTERNAL_ERROR
    mock_handle_terminated_state.assert_called_once_with(run=mock_dbr_run)


def test_continues_polling_if_state_not_terminal(
    mock_workspace_client, mock_tagging_client, mock_dbr_run
):
    mock_dbr_run.state.life_cycle_state = jobs.RunLifeCycleState.RUNNING
    pipes_client = NonAbstractPipesCloudClient(main_client=mock_workspace_client)
    run_id = str(mock_dbr_run.job_id)
    pipes_client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    result = pipes_client._handle_dbr_polling(run_id)
    assert result is True
    mock_workspace_client.jobs.get_run.assert_called_once_with(run_id)
    assert pipes_client.last_observed_state == RunLifeCycleState.RUNNING


def test_correctly_updates_last_observed_state(
    mock_workspace_client, mock_tagging_client, mock_dbr_run
):
    mock_dbr_run.state.life_cycle_state = jobs.RunLifeCycleState.TERMINATED
    pipes_client = NonAbstractPipesCloudClient(main_client=mock_workspace_client)
    pipes_client.last_observed_state = jobs.RunLifeCycleState.RUNNING
    run_id = str(mock_dbr_run.job_id)
    pipes_client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    pipes_client._handle_dbr_polling(run_id)
    mock_workspace_client.jobs.get_run.assert_called_once_with(run_id)
    get_dagster_logger().debug(f"Handling terminated state for run: {run_id}")
    assert pipes_client.last_observed_state == RunLifeCycleState.TERMINATED


#######
# _poll_till_success
######


@patch("time.sleep", return_value=None)
@patch.object(NonAbstractPipesCloudClient, "_handle_emr_polling", return_value=False)
def test_successful_emr_polling(mock_handle_emr_polling, mock_sleep, mock_emr_client):
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)

    client._poll_till_success(
        cluster_id="cluster123",
        tagging_client=MagicMock(),
        extras={"engine": "engine1", "execution_mode": "mode1"},
    )

    mock_handle_emr_polling.assert_called_with("cluster123")


@patch("time.sleep", return_value=None)
@patch.object(NonAbstractPipesCloudClient, "_handle_dbr_polling", return_value=False)
def test_successful_dbr_polling(
    mock_handle_dbr_polling, mock_sleep, mock_workspace_client, mock_tagging_client
):
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )

    client._poll_till_success(
        run_id="run123",
        tagging_client=MagicMock(),
        extras={"engine": "engine1", "execution_mode": "mode1"},
    )

    mock_handle_dbr_polling.assert_called_with(run_id="run123")


@patch("time.sleep", return_value=None)
@patch.object(NonAbstractPipesCloudClient, "_handle_emr_polling", return_value=False)
def test_sleep_interval_polling(mock_handle_emr_polling, mock_sleep, mock_emr_client):
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)

    client._poll_till_success(
        cluster_id="cluster123",
        tagging_client=MagicMock(),
        extras={"engine": "engine1", "execution_mode": "mode1"},
    )

    mock_handle_emr_polling.assert_called_with("cluster123")


#######
# _handle_terminated_state_emr
######


def test_returns_false_when_state_is_terminated_success(mock_emr_client):
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    job_flow = "example_job_flow"
    description = {
        "Cluster": {"Status": {"StateChangeReason": {"Message": "normal message"}}}
    }
    state = "TERMINATED"
    result = client._handle_terminated_state_emr(job_flow, description, state)
    assert not result


def test_returns_false_when_state_is_terminated_running(mock_emr_client):
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    job_flow = "example_job_flow"
    description = {
        "Cluster": {"Status": {"StateChangeReason": {"Message": "normal message"}}}
    }
    state = "RUNNING"
    result = client._handle_terminated_state_emr(job_flow, description, state)
    assert result


def test_raises_error_when_state_is_terminated_with_errors_error_message(
    mock_emr_client,
):
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    job_flow = "example_job_flow"
    description = {
        "Cluster": {"Status": {"StateChangeReason": {"Message": "Error message"}}}
    }
    state = "TERMINATED"
    with pytest.raises(CustomPipesException):
        client._handle_terminated_state_emr(job_flow, description, state)


def test_raises_error_when_state_is_terminated_with_errors_status(
    mock_emr_client, mock_tagging_client
):
    mock_tagging_client.get_resources.return_value = {"ResourceTagMappingList": []}
    client = NonAbstractPipesCloudClient(
        main_client=mock_emr_client, tagging_client=mock_tagging_client
    )
    job_flow = "example_job_flow"
    description = {
        "Cluster": {"Status": {"StateChangeReason": {"Message": "normal message"}}}
    }
    state = "TERMINATED_WITH_ERRORS"
    with pytest.raises(CustomPipesException):
        client._handle_terminated_state_emr(job_flow, description, state)


#######
# _handle_terminated_state_dbr
######


def test_correct_tagging_of_resources_success(
    mock_workspace_client, mock_tagging_client, mock_dbr_run
):
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    client.engine = "TestEngine"
    client.executionMode = "TestMode"

    # Mock get_resources to return a list of ARNs
    mock_tagging_client.get_resources.return_value = {
        "ResourceTagMappingList": [
            {"ResourceARN": "arn:aws:s3:::example_bucket1"},
            {"ResourceARN": "arn:aws:s3:::example_bucket2"},
        ]
    }

    result = client._handle_terminated_state_dbr(mock_dbr_run)

    assert not result
    mock_tagging_client.get_resources.assert_called_once_with(
        TagFilters=[{"Key": "JobId", "Values": [str(mock_dbr_run.job_id)]}]
    )

    expected_tags = {
        "jobId": str(mock_dbr_run.job_id),
        "engine": client.engine,
        "executionMode": client.executionMode,
    }

    mock_tagging_client.tag_resources.assert_any_call(
        ResourceARNList=["arn:aws:s3:::example_bucket1"], Tags=expected_tags
    )
    mock_tagging_client.tag_resources.assert_any_call(
        ResourceARNList=["arn:aws:s3:::example_bucket2"], Tags=expected_tags
    )


def test_correct_tagging_of_resources_failure(
    mock_workspace_client, mock_tagging_client, mock_dbr_run
):
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    client.engine = "TestEngine"
    client.executionMode = "TestMode"

    # Mock a failed run
    mock_dbr_run.state.result_state = jobs.RunResultState.FAILED
    mock_dbr_run.state.state_message = "Run failed"

    with pytest.raises(CustomPipesException) as excinfo:
        client._handle_terminated_state_dbr(mock_dbr_run)

    assert str(excinfo.value) == "Error running Databricks job: Run failed"


#######
# _ensure_library_on_cloud
######
@patch("ascii_library.orchestration.pipes.cloud_client.library_to_cloud_paths")
@patch("ascii_library.orchestration.pipes.cloud_client.package_library")
@patch("ascii_library.orchestration.pipes.cloud_client.file_relative_path")
@patch.object(_PipesBaseCloudClient, "_upload_file_to_cloud")
def test_ensure_library_on_cloud(
    mock_upload_file_to_cloud,
    mock_file_relative_path,
    mock_package_library,
    mock_library_to_cloud_paths,
    temp_library,
    mock_emr_client,
    mock_s3_client,
):
    mock_file_relative_path.return_value = temp_library
    mock_package_library.return_value = [temp_library]
    mock_library_to_cloud_paths.return_value = "s3://test-bucket/test_library.zip"
    client_file_path = os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../ascii_library/ascii_library/orchestration/pipes/cloud_client.py",
        )
    )

    client = NonAbstractPipesCloudClient(
        main_client=mock_emr_client, s3_client=mock_s3_client
    )

    client._ensure_library_on_cloud(
        libraries_to_build_and_upload=["test_library"], bucket="test-bucket"
    )

    expected_calls = [
        call(client_file_path, "../../../../test_library"),
    ]

    mock_file_relative_path.assert_has_calls(expected_calls, any_order=True)
    mock_package_library.assert_called_once()
    mock_upload_file_to_cloud.assert_called_once_with(
        local_file_path=temp_library,
        cloud_path="s3://test-bucket/test_library.zip",
        bucket="test-bucket",
    )


@patch("ascii_library.orchestration.pipes.cloud_client.library_to_cloud_paths")
@patch("ascii_library.orchestration.pipes.cloud_client.package_library")
@patch("ascii_library.orchestration.pipes.cloud_client.file_relative_path")
@patch.object(_PipesBaseCloudClient, "_upload_file_to_cloud")
def test_ensure_library_on_cloud_with_empty_list(
    mock_upload_file_to_cloud,
    mock_file_relative_path,
    mock_package_library,
    mock_library_to_cloud_paths,
    mock_emr_client,
    mock_s3_client,
):
    client = NonAbstractPipesCloudClient(
        main_client=mock_emr_client, s3_client=mock_s3_client
    )

    client._ensure_library_on_cloud(
        libraries_to_build_and_upload=[], bucket="test-bucket"
    )

    mock_file_relative_path.assert_not_called()
    mock_package_library.assert_not_called()
    mock_upload_file_to_cloud.assert_not_called()


#######
# _upload_file_to_cloud
######


@patch.object(NonAbstractPipesCloudClient, "_upload_file_to_s3")
@patch.object(NonAbstractPipesCloudClient, "_upload_file_to_dbfs")
@patch.object(NonAbstractPipesCloudClient, "handle_exep")
def test_upload_file_to_cloud_s3(
    mock_handle_exep,
    mock_upload_file_to_dbfs,
    mock_upload_file_to_s3,
    mock_emr_client,
    temp_library,
    mock_s3_client,
):
    client = NonAbstractPipesCloudClient(
        main_client=mock_emr_client, s3_client=mock_s3_client
    )
    cloud_path = "s3://test-bucket/test_file.zip"

    client._upload_file_to_cloud(local_file_path=temp_library, cloud_path=cloud_path)

    mock_upload_file_to_s3.assert_called_once_with(temp_library, cloud_path)
    mock_upload_file_to_dbfs.assert_not_called()
    mock_handle_exep.assert_not_called()


@patch.object(NonAbstractPipesCloudClient, "_upload_file_to_s3")
@patch.object(NonAbstractPipesCloudClient, "_upload_file_to_dbfs")
@patch.object(NonAbstractPipesCloudClient, "handle_exep")
def test_upload_file_to_cloud_dbfs(
    mock_handle_exep,
    mock_upload_file_to_dbfs,
    mock_upload_file_to_s3,
    temp_library,
    mock_workspace_client,
    mock_tagging_client,
):
    temp_library = "/path/to/test_file.py"
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    cloud_path = "dbfs:/test-file.zip"

    client._upload_file_to_cloud(local_file_path=temp_library, cloud_path=cloud_path)

    mock_upload_file_to_s3.assert_not_called()
    mock_upload_file_to_dbfs.assert_called_once_with(temp_library, cloud_path)
    mock_handle_exep.assert_not_called()


@patch.object(NonAbstractPipesCloudClient, "_upload_file_to_s3")
@patch.object(NonAbstractPipesCloudClient, "_upload_file_to_dbfs")
@patch.object(NonAbstractPipesCloudClient, "handle_exep")
def test_upload_file_to_cloud_s3_if_whl(
    mock_handle_exep,
    mock_upload_file_to_dbfs,
    mock_upload_file_to_s3,
    temp_library,
    mock_workspace_client,
    mock_tagging_client,
):
    temp_library = "/path/to/test_file.whl"
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    cloud_path = "s3:/bucket/test-file.zip"

    client._upload_file_to_cloud(local_file_path=temp_library, cloud_path=cloud_path)

    mock_upload_file_to_s3.assert_called_once_with(temp_library, cloud_path)
    mock_upload_file_to_dbfs.assert_not_called()
    mock_handle_exep.assert_not_called()


@patch.object(NonAbstractPipesCloudClient, "_upload_file_to_s3")
@patch.object(NonAbstractPipesCloudClient, "_upload_file_to_dbfs")
@patch.object(NonAbstractPipesCloudClient, "handle_exep")
def test_upload_file_to_cloud_exception_handling(
    mock_handle_exep,
    mock_upload_file_to_dbfs,
    mock_upload_file_to_s3,
    mock_emr_client,
    temp_library,
):
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client)
    cloud_path = "s3://test-bucket/test_file.zip"

    mock_upload_file_to_s3.side_effect = Exception("Test Exception")

    client._upload_file_to_cloud(local_file_path=temp_library, cloud_path=cloud_path)

    mock_upload_file_to_s3.assert_called_once_with(temp_library, cloud_path)
    mock_upload_file_to_dbfs.assert_not_called()
    mock_handle_exep.assert_called_once()


#######
# handle_exep
######


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_logs_error_message_for_FileNotFoundError(mock_get_dagster_logger):
    client = NonAbstractPipesCloudClient(main_client=MagicMock())

    with pytest.raises(FileNotFoundError):
        try:
            raise FileNotFoundError("File not found")
        except FileNotFoundError as e:
            client.handle_exep(e)
    mock_get_dagster_logger().error.assert_called_with("The file was not found")


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_logs_error_message_for_NoCredentialsError(mock_get_dagster_logger):
    client = NonAbstractPipesCloudClient(main_client=MagicMock())
    with pytest.raises(NoCredentialsError):
        client.handle_exep(NoCredentialsError())
    mock_get_dagster_logger().error.assert_called_once_with("Credentials not available")


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_logs_error_message_for_ClientError(mock_get_dagster_logger):
    client = NonAbstractPipesCloudClient(main_client=MagicMock())
    with pytest.raises(ClientError):
        client.handle_exep(
            ClientError(
                {"Error": {"Code": "Test", "Message": "Test Error"}}, "TestOperation"
            )
        )
    mock_get_dagster_logger().error.assert_called_once_with(
        "Client error while uploading"
    )


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_handle_exep_unknown_error(mock_get_dagster_logger):
    client = NonAbstractPipesCloudClient(main_client=MagicMock())
    mock_logger = mock_get_dagster_logger.return_value
    try:
        raise ValueError("Unknown error")
    except ValueError as e:
        client.handle_exep(e)
        # Ensure that the logger was not called
        mock_logger.error.assert_not_called()


#######
# _upload_file_to_s3
######
@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_upload_file_to_s3(mock_get_dagster_logger, mock_s3_client, mock_emr_client):
    mock_logger = mock_get_dagster_logger.return_value

    with patch.object(
        mock_s3_client, "upload_file", return_value=None
    ) as mock_upload_file:
        client = NonAbstractPipesCloudClient(
            main_client=mock_emr_client, s3_client=mock_s3_client
        )
        client._upload_file_to_s3("local/path", "cloud/path", bucket="test-bucket")
        mock_upload_file.assert_called_once_with(
            "local/path", "test-bucket", "cloud/path"
        )
        mock_logger.debug.assert_called_with(
            "uploading: cloud/path into bucket: test-bucket"
        )


@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_upload_file_to_s3_failure(mock_get_dagster_logger, mock_emr_client):
    mock_logger = mock_get_dagster_logger.return_value
    client = NonAbstractPipesCloudClient(main_client=mock_emr_client, s3_client=None)
    client._upload_file_to_s3("local/path", "cloud/path", bucket="test-bucket")
    mock_logger.debug.assert_any_call("uploading: cloud/path into bucket: test-bucket")
    mock_logger.debug.assert_any_call("fail to upload to S3")


#######
# _upload_file_to_dbfs
######


@patch("builtins.open", new_callable=mock_open, read_data=b"data")
@patch("ascii_library.orchestration.pipes.cloud_client.get_dagster_logger")
def test_upload_file_to_dbfs(
    mock_get_dagster_logger, mock_open, mock_workspace_client, mock_tagging_client
):
    mock_logger = mock_get_dagster_logger.return_value
    client = NonAbstractPipesCloudClient(
        main_client=mock_workspace_client, tagging_client=mock_tagging_client
    )
    client._upload_file_to_dbfs("local/path", "dbfs/path")
    mock_open.assert_called_once_with("local/path", "rb")
    mock_workspace_client.dbfs.put.assert_called_once_with(
        path="dbfs/path",
        contents=base64.b64encode(b"data").decode("utf-8"),
        overwrite=True,
    )
    mock_logger.debug.assert_called_once_with(
        "uploading: local/path to DBFS at: dbfs/path"
    )
