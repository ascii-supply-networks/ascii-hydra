# pyright: reportArgumentType=false, reportCallIssue=false, reportAssignmentType=false
from typing import Optional

import boto3
from dagster import ConfigurableResource
from obstore.auth.boto3 import Boto3CredentialProvider  # pyrefly: ignore
from obstore.store import S3Store  # pyrefly: ignore
from pydantic import Field


class ObstoreS3Resource(ConfigurableResource):
    """Dagster integration for fast rust based s3 client."""

    region_name: Optional[str] = Field(
        default=None, description="Specifies a custom region for the S3 session."
    )
    endpoint_url: Optional[str] = Field(
        default=None, description="Specifies a custom endpoint for the S3 session."
    )
    profile_name: Optional[str] = Field(
        default=None, description="Specifies a profile to connect that session."
    )
    aws_access_key_id: Optional[str] = Field(
        default=None,
        description="AWS access key ID to use when creating the boto3 session.",
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None,
        description="AWS secret access key to use when creating the boto3 session.",
    )
    allow_http: bool = Field(
        default=False,
        description="Whether to allow http connections. By default, https is used.",
    )
    allow_invalid_certificates: bool = Field(
        default=False,
        description="Whether to allow invalid certificates. By default, valid certs are required.",
    )

    def get_client(
        self,
        bucket: str,
        timeout: str = "60s",
        retry_config=None,
    ) -> S3Store:
        """Creates an S3 object store."""
        config = {
            k: v
            for k, v in {
                "aws_access_key_id": self.aws_access_key_id,
                "aws_secret_access_key": self.aws_secret_access_key,
                "aws_endpoint": self.endpoint_url,
                "aws_region": self.region_name,
                "aws_bucket": bucket,
            }.items()
            if v is not None
        }
        client_options = {
            "timeout": timeout,
            "allow_http": self.allow_http,
            "allow_invalid_certificates": self.allow_invalid_certificates,
        }
        if self.profile_name is not None:
            client_session = boto3.session.Session(profile_name=self.profile_name)
            credential_provider = Boto3CredentialProvider(client_session)
            return S3Store(
                credential_provider=credential_provider,
                config=config,
                retry_config=retry_config,
                client_options=client_options,
            )
        else:
            return S3Store(
                config=config,
                retry_config=retry_config,
                client_options=client_options,
            )
