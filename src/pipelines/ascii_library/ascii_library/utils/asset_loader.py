from pathlib import Path

import dagster as dg

from ascii_library.sensors.notifications_slack import make_slack_on_failure
from ascii_library.utils.determine_env import get_dagster_deployment_environment


def load_assets(
    prefix: str,
    root_dir: str,
    git_url: str = "https://github.com/ascii-supply-networks/research-space.git",
    git_branch: str = "main",
    make_error_sensor: bool = True,
    ascii_dagster_deployment_url: str = "https://orchestration.ascii.ac.at",
):
    """Load assets from the ascii_library definitions folder.

    Args:
        root_dir (str): The root directory of the respective dagster codelocation project.
    """
    base = dg.load_from_defs_folder(path_within_project=Path(root_dir).parent.parent)

    materializable = [a for a in base.assets if isinstance(a, dg.AssetsDefinition)]  # type: ignore
    passthrough = [
        a
        for a in base.assets  # type: ignore
        if not isinstance(a, dg.AssetsDefinition)
    ]  # SourceAsset

    with_refs = dg.with_source_code_references(materializable)

    linked = dg.link_code_references_to_git(
        assets_defs=with_refs,
        git_url=git_url,
        git_branch=git_branch,
        file_path_mapping=dg.AnchorBasedFilePathMapping(
            local_file_anchor=Path(root_dir).parent,
            file_anchor_path_in_repository=prefix,
        ),
    )

    if make_error_sensor:
        deployment_name = get_dagster_deployment_environment()
        if deployment_name in ["PROD"]:
            slack_failure_sensor = make_slack_on_failure(
                base_url=ascii_dagster_deployment_url
            )
            base.sensors.append(slack_failure_sensor)  # pyrefly: ignore
    return base, linked, passthrough
