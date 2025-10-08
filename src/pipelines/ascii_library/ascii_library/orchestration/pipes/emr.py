import os
import shlex
import signal
from io import BytesIO, StringIO
from textwrap import dedent
from types import FrameType
from typing import Any, Dict, List, Optional, Tuple

from dagster import (
    OpExecutionContext,
    PipesContextInjector,
    PipesMessageReader,
    ResourceParam,
    get_dagster_logger,
    open_pipes_session,
)
from dagster._core.pipes.client import PipesClientCompletedInvocation  # type: ignore
from dagster_aws.pipes import PipesS3ContextInjector, PipesS3MessageReader
from dagster_pipes import PipesExtras

from ascii_library.orchestration.pipes import LibraryConfig, LibraryKind
from ascii_library.orchestration.pipes.cloud_client import _PipesBaseCloudClient
from ascii_library.orchestration.pipes.exceptions import CustomPipesException
from ascii_library.orchestration.pipes.instance_config import CloudInstanceConfig
from ascii_library.orchestration.pipes.utils import (
    library_from_dbfs_paths,
    library_to_cloud_paths,
)
from ascii_library.orchestration.resources.constants import aws_region, rackspace_user
from ascii_library.orchestration.resources.emr_constants import pipeline_bucket
from ascii_library.utils.determine_env import (
    get_dagster_deployment_environment,
)

TARGET_DIR = "/mnt/pydeps/ascii_extras"
LOG_FILE = "/var/log/emr-bootstrap.log"


class _PipesEmrClient(_PipesBaseCloudClient):
    """Pipes client for EMR.

    Args:
        emr_job_runner (EmrJobRunner): An instance of EmrJobRunner.
        env (Optional[Mapping[str, str]]): An optional dict of environment variables to pass to the EMR job.
        context_injector (Optional[PipesContextInjector]): A context injector to use to inject context into the EMR process.
        message_reader (Optional[PipesMessageReader]): A message reader to use to read messages from the EMR job.
        poll_interval_seconds (float): How long to sleep between checking the status of the job run.
    """

    def __init__(
        self,
        emr_client,
        s3_client,
        price_client,
        bucket: str,
        context_injector: Optional[PipesContextInjector] = None,
        message_reader: Optional[PipesMessageReader] = None,
    ):
        super().__init__(
            main_client=emr_client,
            context_injector=context_injector,
            message_reader=message_reader,
            s3_client=s3_client,
        )
        self._price_client = price_client
        self._emr_client = emr_client
        self._s3_client = s3_client

        self._context_injector = context_injector or PipesS3ContextInjector(
            bucket=bucket, client=s3_client
        )
        self._message_reader = message_reader or PipesS3MessageReader(
            bucket=bucket, client=s3_client
        )

    def write_block(self, content, s: str):
        content.write(dedent(s).lstrip())

    def create_bootstrap_script(  # noqa: C901
        self,
        output_file: str = "bootstrap.sh",
        bucket: str = pipeline_bucket,
        libraries: Optional[List[LibraryConfig]] = None,
    ):
        dagster_deployment = get_dagster_deployment_environment()
        content = StringIO()
        self.write_block(
            content,
            """
                #!/bin/bash
                set -euo pipefail
                """,
        )
        # TODO: Fix eventually and move to pixi controlled venv; this is a bit hacky
        if libraries is not None:
            self.write_block(
                content,
                f"""
                LOG="{LOG_FILE}"
                sudo mkdir -p "$(dirname "$LOG")"
                # log the whole script as root → no permission errors
                exec > >(sudo tee -a "$LOG") 2>&1
                trap 'echo "== $(date) bootstrap FAILED (rc=$?) ==" >&2' ERR
                echo "== $(date) bootstrap start =="

                # make pip quiet and suppress 'running as root' warning
                export PIP_DISABLE_PIP_VERSION_CHECK=1
                export PIP_ROOT_USER_ACTION=ignore

                TARGET="{TARGET_DIR}"
                sudo mkdir -p "$TARGET"
                sudo chmod -R a+rX "$TARGET"

                # base Python
                # sudo yum -y update
                # sudo yum -y install python3 python3-pip
                command -v /usr/bin/python3 >/dev/null 2>&1 || sudo yum -y install python3
                /usr/bin/python3 -m pip --version >/dev/null 2>&1 || sudo yum -y install python3-pip
                /usr/bin/python3 --version
                /usr/bin/python3 -m pip --version

                # make target visible to future shells (write LITERAL path)
                echo 'export PYTHONPATH="{TARGET_DIR}${{PYTHONPATH:+:$PYTHONPATH}}"' | sudo tee /etc/profile.d/ascii_extras_path.sh >/dev/null
                sudo chmod +x /etc/profile.d/ascii_extras_path.sh
                # also for this shell:
                export PYTHONPATH="{TARGET_DIR}${{PYTHONPATH:+:$PYTHONPATH}}"
            """,
            )
            # load pypi first so we can reference in wheel
            for kind in (LibraryKind.Pypi, LibraryKind.Wheel):
                for lib in libraries:
                    if lib.kind == kind:
                        if kind == LibraryKind.Pypi:
                            self.handle_pypi(content, lib)
                        else:
                            self.handle_wheel(bucket, content, lib)
        self.write_block(
            content,
            r"""
            SITEPKG="$(
            /usr/bin/python3 -c 'import site,sys;p=[x for x in site.getsitepackages() if x.endswith("site-packages")];sys.stdout.write(p[0] if p else "")' \
            2>/dev/null || echo ""
            )"
            if [ -n "$SITEPKG" ] && [ -d "$SITEPKG" ]; then
            if ! echo "{TARGET_DIR}" | sudo tee "$SITEPKG/zzz_ascii_extras.pth" >/dev/null; then
                echo "[WARN] Could not write zzz_ascii_extras.pth to $SITEPKG; relying on PYTHONPATH"
            fi
            fi
        """.replace("{TARGET_DIR}", TARGET_DIR),
        )

        destination = f"external_pipes/{dagster_deployment}/{output_file}"
        content.seek(0)
        get_dagster_logger().debug(f"Bootstrap file content: \n\n{content.getvalue()}")
        self._s3_client.upload_fileobj(
            BytesIO(content.read().encode()), bucket, destination
        )
        return f"s3://{bucket}/{destination}"

    def handle_pypi(self, content, lib):
        pkg = f"{lib.name_id}{lib.version or ''}"
        flags = (lib.extra_flags or "").strip()
        get_dagster_logger().debug(f"Installing library: {pkg}")

        # Build the pip command once (into TARGET, ignore RPM packages)
        base = (
            "sudo /usr/bin/python3 -m pip install "
            "--upgrade --ignore-installed --no-cache-dir "
            '--target "$TARGET"'
        )
        if flags:
            base += f" {flags}"

        cmd = f"{base} {shlex.quote(pkg)}"
        self.write_block(
            content,
            f"""
            # Install {pkg} into $TARGET (isolated; no RPM conflicts)
            {cmd}
        """,
        )

    def handle_wheel(self, bucket, content, lib):
        name_id = library_from_dbfs_paths(lib.name_id)
        path = library_to_cloud_paths(lib_name=name_id, filesystem="s3")
        local = f"/tmp/{name_id}-0.0.0-py3-none-any.whl"

        self.write_block(
            content,
            f"""
            # Wheel: s3://{bucket}/{path}
            aws s3 cp s3://{bucket}/{path} {shlex.quote(local)}
            sudo /usr/bin/python3 -m pip install --upgrade --ignore-installed --no-cache-dir --target "$TARGET" {shlex.quote(local)}
        """,
        )
        get_dagster_logger().debug(f"Installing library: {name_id}")

    def modify_env_var(self, cluster_config: dict, key: str, value: str):
        configs = cluster_config.get("Configurations", [])
        i = 0
        for config in configs:
            if config.get("Classification") == "spark-defaults":
                props = config.get("Properties")
                # props = config.get("Configurations")[0].get("Properties")
                props[f"spark.yarn.appMasterEnv.{key}"] = value
                props[f"spark.executorEnv.{key}"] = value
                cluster_config["Configurations"][i]["Properties"] = props
            i += 1
        return cluster_config

    def extract_filename_without_extension(self, path: str):
        # Extract the base name of the file
        base_name = os.path.basename(path)
        # Remove the extension
        name_without_extension = os.path.splitext(base_name)[0]
        return name_without_extension

    def prepare_emr_job(
        self,
        local_file_path: str,
        bucket: str,
        s3_path: str,
        emr_job_config: Dict[str, Any],
        step_config,
        libraries_to_build_and_upload: Optional[List[str]] = None,
        libraries: Optional[List[LibraryConfig]] = None,
        extras: Optional[PipesExtras] = None,
    ) -> Tuple[(Optional[PipesExtras], Dict[str, Any])]:
        self._upload_file_to_cloud(
            local_file_path=local_file_path, bucket=bucket, cloud_path=s3_path
        )
        if libraries_to_build_and_upload is not None:
            self._ensure_library_on_cloud(
                libraries_to_build_and_upload=libraries_to_build_and_upload
            )
            output_file_name = f"{self.extract_filename_without_extension(local_file_path)}_bootstrap.sh"
            destination = self.create_bootstrap_script(
                output_file=output_file_name, libraries=libraries
            )
            emr_job_config = dict(emr_job_config)
            emr_job_config["BootstrapActions"] = [
                {
                    "Name": "Install custom packages",
                    "ScriptBootstrapAction": {"Path": destination},
                }
            ]
        if extras:
            # Create a mutable copy of extras if it exists
            extras = dict(extras) if extras else {}
            # TODO: do we really have to cast? extras = dict(extras)
            extras["emr_job_config"] = emr_job_config
            extras["step_config"] = step_config
        return extras, emr_job_config

    def adjust_emr_job_config(
        self,
        emr_job_config: dict,
        fleet_config: Optional[CloudInstanceConfig],
    ) -> dict:
        if (
            emr_job_config["Instances"].get("InstanceGroups") is None
            and emr_job_config["Instances"].get("InstanceFleets") is None
        ):
            if fleet_config is not None:
                emr_job_config["Instances"]["InstanceFleets"] = (
                    fleet_config.get_fleet_programatically(
                        emrClient=self._emr_client, priceClient=self._price_client
                    )
                )
                emr_job_config["ManagedScalingPolicy"]["ComputeLimits"]["UnitType"] = (
                    "InstanceFleetUnits"
                )
                emr_job_config["Instances"]["Ec2SubnetId"] = ""
            else:
                raise ValueError(
                    "No instance groups or fleets defined, and fleet_config is None."
                )
        elif emr_job_config["Instances"].get("InstanceGroups") is not None:
            emr_job_config["Instances"]["Ec2SubnetIds"] = []
        return emr_job_config

    def submit_emr_job(
        self,
        bootstrap_env,
        emr_job_config: dict,
        step_config,
        extras: PipesExtras,
    ) -> str:
        # get_dagster_logger().debug(
        #     f"DAGSTER_PIPES_CONTEXT: {bootstrap_env['DAGSTER_PIPES_CONTEXT']}"
        # )
        # get_dagster_logger().debug(
        #     f"DAGSTER_PIPES_MESSAGES: {bootstrap_env['DAGSTER_PIPES_MESSAGES']}"
        # )
        emr_job_config = self.modify_env_var(
            cluster_config=emr_job_config,
            key="DAGSTER_PIPES_CONTEXT",
            value=bootstrap_env["DAGSTER_PIPES_CONTEXT"],
        )
        emr_job_config = self.modify_env_var(
            cluster_config=emr_job_config,
            key="DAGSTER_PIPES_MESSAGES",
            value=bootstrap_env["DAGSTER_PIPES_MESSAGES"],
        )
        ascii_wandb_value = os.environ.get("ASCII_WANDB", "")
        if not ascii_wandb_value:
            get_dagster_logger().warning(
                "Environment variable 'ASCII_WANDB' is not set; defaulting to empty value."
            )
        emr_job_config = self.modify_env_var(
            cluster_config=emr_job_config,
            key="ASCII_WANDB",
            value=ascii_wandb_value,
        )
        emr_job_config = self.modify_env_var(
            cluster_config=emr_job_config,
            key="PYTHONPATH",
            value=f"{TARGET_DIR}:${{PYTHONPATH:+:$PYTHONPATH}}",
        )

        job_flow = self._emr_client.run_job_flow(**emr_job_config)
        get_dagster_logger().debug(f"EMR configuration: {job_flow}")
        self._emr_client.add_tags(
            ResourceId=job_flow["JobFlowId"],
            Tags=[
                {"Key": "jobId", "Value": job_flow["JobFlowId"]},
                {"Key": "executionMode", "Value": extras["execution_mode"]},
                {"Key": "engine", "Value": extras["engine"]},
            ],
        )
        self._emr_client.add_job_flow_steps(
            JobFlowId=job_flow["JobFlowId"],
            Steps=[step_config],
        )
        get_dagster_logger().info(
            f"If not signed in on Rackspace, please do it now: https://manage.rackspace.com/aws/account/{rackspace_user}/consoleSignin"
        )
        get_dagster_logger().info(
            f"EMR URL: https://{aws_region}.console.aws.amazon.com/emr/home?region={aws_region}#/clusterDetails/{job_flow['JobFlowId']}"
        )
        return job_flow["JobFlowId"]

    def run(  # noqa: C901 # type: ignore
        self,
        context: OpExecutionContext,
        emr_job_config: dict,
        step_config,  # Change from 'dict' to 'StepConfigTypeDef'
        local_file_path: str,
        bucket: str,
        s3_path: str,
        libraries_to_build_and_upload: Optional[List[str]] = None,
        libraries: Optional[List[LibraryConfig]] = None,
        extras: Optional[PipesExtras] = None,
        fleet_config: Optional[CloudInstanceConfig] = None,
    ) -> PipesClientCompletedInvocation:
        """Synchronously execute an EMR job with the pipes protocol."""
        emr_job_config = self.adjust_emr_job_config(emr_job_config, fleet_config)
        extras, emr_job_config = self.prepare_emr_job(
            local_file_path=local_file_path,
            bucket=bucket,
            s3_path=s3_path,
            emr_job_config=emr_job_config,
            step_config=step_config,
            libraries_to_build_and_upload=libraries_to_build_and_upload,
            libraries=libraries,
            extras=extras,
        )

        if extras is None:
            raise ValueError("Extras cannot be None.")

        cluster_id: Optional[str] = None

        def _terminate_cluster_safely():
            """Best-effort termination that works in STARTING/BOOTSTRAPPING/RUNNING/WAITING."""
            if not cluster_id:
                return
            try:
                # Optional: tag the cluster to mark the source of termination
                try:
                    self._emr_client.add_tags(
                        ResourceId=cluster_id,
                        Tags=[{"Key": "canceled-by", "Value": "dagster"}],
                    )
                except Exception:
                    pass
                context.log.info(f"[pipes] terminating EMR cluster {cluster_id}")
                self._emr_client.terminate_job_flows(JobFlowIds=[cluster_id])
            except Exception as te:  # swallow; termination is best-effort
                context.log.warning(
                    f"[pipes] could not terminate EMR {cluster_id}: {te!r}"
                )

        old_sigterm = signal.getsignal(signal.SIGTERM)

        def _on_sigterm(signum: int, frame: Optional[FrameType]):
            _terminate_cluster_safely()
            signal.signal(signal.SIGTERM, old_sigterm)
            os.kill(os.getpid(), signal.SIGTERM)

        signal.signal(signal.SIGTERM, _on_sigterm)

        try:
            with open_pipes_session(
                context=context,
                message_reader=self._message_reader,
                context_injector=self._context_injector,
                extras=extras,
            ) as session:
                bootstrap_env = session.get_bootstrap_env_vars()
                emr_job_config = extras.get("emr_job_config")  # type: ignore
                cluster_id = self.submit_emr_job(
                    bootstrap_env=bootstrap_env,
                    emr_job_config=emr_job_config,
                    step_config=step_config,
                    extras=extras,
                )
                self._poll_till_success(cluster_id=cluster_id)
        except CustomPipesException:
            # EMR or steps failed → terminate cluster to avoid lingering capacity
            context.log.info("[pipes] EMR/step failure detected; terminating cluster.")
            _terminate_cluster_safely()
            raise
        except BaseException:
            # Dagster canceled / SIGTERM / KeyboardInterrupt / any runtime error
            context.log.info(
                "[pipes] Dagster side interrupted; terminating EMR cluster."
            )
            _terminate_cluster_safely()
            raise
        finally:
            try:
                signal.signal(signal.SIGTERM, old_sigterm)
            except Exception:
                pass
            get_dagster_logger().debug("finished")
        return PipesClientCompletedInvocation(session)


PipesEmrEnhancedClient = ResourceParam[_PipesEmrClient]
