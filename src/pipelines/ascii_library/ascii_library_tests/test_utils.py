from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from ascii_library.orchestration.pipes import ExecutionMode
from ascii_library.orchestration.pipes.utils import (
    calculate_parallelism,
    configure_spark,
    get_input_path,
    library_from_dbfs_paths,
    library_to_cloud_paths,
    package_library,
)
from ascii_library.utils.determine_env import (
    get_dagster_deployment_environment,
)

PKG_ROOT = "src/pipelines/ascii_library/ascii_library"
ABS_PKG_ROOT = str(Path(PKG_ROOT).resolve())
DIST_DIR = f"{PKG_ROOT}/dist"
ABS_DIST_DIR = str(Path(DIST_DIR).resolve())


def test_get_dagster_deployment_environment_set():
    with patch.dict("os.environ", {"DAGSTER_DEPLOYMENT": "prod"}):
        result = get_dagster_deployment_environment()
        assert result == "PROD"


def test_get_dagster_deployment_environment_not_set():
    with patch.dict("os.environ", {}, clear=True):
        result = get_dagster_deployment_environment()
        assert result == "BRANCH"


def test_get_dagster_deployment_environment_with_custom_key():
    with patch.dict("os.environ", {"CUSTOM_DEPLOYMENT": "dev"}):
        result = get_dagster_deployment_environment(deployment_key="CUSTOM_DEPLOYMENT")
        assert result == "BRANCH"


def test_get_dagster_deployment_environment_with_default_value():
    with patch.dict("os.environ", {}, clear=True):
        result = get_dagster_deployment_environment(default_value="dev")
        assert result == "BRANCH"


#######
# library_to_cloud_paths
#######


def test_library_to_cloud_paths_dbfs():
    result = library_to_cloud_paths("random_lib", "dbfs")
    assert result == "dbfs:/customlibs/BRANCH/random_lib-0.0.0-py3-none-any.whl"


def test_library_to_cloud_paths_non_dbfs():
    result = library_to_cloud_paths("random_lib", "s3")
    assert result == "customlibs/BRANCH/random_lib-0.0.0-py3-none-any.whl"


#######
# library_from_dbfs_paths
#######


def test_library_from_dbfs_paths():
    result = library_from_dbfs_paths(
        "dbfs:/customlibs/test/mylib-0.0.0-py3-none-any.whl"
    )
    assert result == "mylib"


#######
# package_library
#######


@patch("ascii_library.orchestration.pipes.utils.Path.mkdir", autospec=True)
@patch("os.path.exists")
@patch("glob.glob")
@patch("os.makedirs")
@patch("subprocess.check_call")
@patch("os.remove")
def test_package_library(
    mock_remove, mock_check_call, mock_makedirs, mock_glob, mock_exists, mock_path_mkdir
):
    mock_path_mkdir.return_value = None
    mock_exists.return_value = True
    mock_glob.return_value = [f"{ABS_DIST_DIR}/mylib-0.0.0-py3-none-any.whl"]

    result = package_library(PKG_ROOT)

    mock_check_call.assert_called_once_with(
        ["python", "-m", "build", "--wheel", "--outdir", ABS_DIST_DIR],
        cwd=ABS_PKG_ROOT,
    )
    assert result == (
        f"{ABS_DIST_DIR}/mylib-0.0.0-py3-none-any.whl",
        "mylib-0.0.0-py3-none-any.whl",
    )


@patch("ascii_library.orchestration.pipes.utils.Path.mkdir", autospec=True)
@patch("os.path.exists")
@patch("glob.glob")
@patch("os.makedirs")
@patch("subprocess.check_call")
@patch("os.remove")
def test_package_library_dist_not_exists(
    mock_remove, mock_check_call, mock_makedirs, mock_glob, mock_exists, mock_path_mkdir
):
    mock_path_mkdir.return_value = None
    mock_exists.side_effect = lambda path: (False if path == ABS_DIST_DIR else True)
    mock_glob.return_value = [f"{ABS_DIST_DIR}/mylib-0.0.0-py3-none-any.whl"]

    result = package_library(PKG_ROOT)

    mock_check_call.assert_called_once_with(
        ["python", "-m", "build", "--wheel", "--outdir", ABS_DIST_DIR],
        cwd=ABS_PKG_ROOT,
    )
    assert result == (
        f"{ABS_DIST_DIR}/mylib-0.0.0-py3-none-any.whl",
        "mylib-0.0.0-py3-none-any.whl",
    )

    mock_makedirs.assert_called_once_with(ABS_DIST_DIR)


@patch("ascii_library.orchestration.pipes.utils.Path.mkdir", autospec=True)
@patch("os.path.exists")
@patch("glob.glob")
@patch("os.makedirs")
@patch("subprocess.check_call")
@patch("os.remove")
def test_package_library_no_wheel_found(
    mock_remove, mock_check_call, mock_makedirs, mock_glob, mock_exists, mock_path_mkdir
):
    mock_path_mkdir.return_value = None
    mock_exists.return_value = True
    mock_glob.return_value = []

    with pytest.raises(
        FileNotFoundError, match="No wheel file found in the dist directory."
    ):
        package_library(PKG_ROOT)

    mock_check_call.assert_called_once_with(
        ["python", "-m", "build", "--wheel", "--outdir", ABS_DIST_DIR],
        cwd=ABS_PKG_ROOT,
    )


#######
# get_input_path
#######


def test_get_input_path_all():
    result = get_input_path("/io_nodes", "seed1", "cc1", "all")
    assert result == "/io_nodes/seed_nodes=seed1/crawl_id=cc1/main_language=*"


def test_get_input_path_specific_lang():
    result = get_input_path("/io_nodes", "seed1", "cc1", "en")
    assert result == "/io_nodes/seed_nodes=seed1/crawl_id=cc1/main_language=en"


#######
# calculate_parallelism
#######
def test_calculate_parallelism():
    spark = MagicMock()
    spark.sparkContext.textFile().count.return_value = 100000

    result = calculate_parallelism(spark, "/path/to/input")
    assert result == 90000

    spark.sparkContext.textFile().count.return_value = 1000
    result = calculate_parallelism(spark, "/path/to/input")
    assert result == 250

    spark.sparkContext.textFile().count.return_value = 600
    result = calculate_parallelism(spark, "/path/to/input")
    assert result == 200


#######
# configure_spark
#######


def test_configure_spark():
    spark = MagicMock()
    execution_mode = ExecutionMode.Full
    compression_codec = "gzip"
    default_parallelism = 100
    shuffle_partitions = 200
    partitionDiscovery_parallelism = 300

    configure_spark(
        spark,
        execution_mode,
        compression_codec,
        default_parallelism,
        shuffle_partitions,
        partitionDiscovery_parallelism,
    )

    expected_calls = [
        ("spark.sql.parquet.compression.codec", "gzip"),
        ("spark.sql.files.maxPartitionBytes", 50 * 1024 * 1024),
        ("spark.databricks.delta.retentionDurationCheck.enabled", "true"),
        ("spark.databricks.delta.vacuum.parallelDelete.enabled", "true"),
        ("spark.sql.sources.partitionOverwriteMode", "dynamic"),
        ("spark.databricks.delta.schema.autoMerge.enabled", "True"),
        ("spark.databricks.delta.schema.autoMerge.enabledOnWrite", "True"),
        ("spark.default.parallelism", 100),
        ("spark.sql.shuffle.partitions", 200),
        ("spark.sql.shuffle.partitions", 300),
    ]

    for call in expected_calls:
        spark.conf.set.assert_any_call(call[0], call[1])
