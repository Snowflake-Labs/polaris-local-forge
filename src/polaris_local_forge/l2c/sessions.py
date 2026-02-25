# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Isolated boto3 session factories for L2C.

Creates two fully isolated boto3 sessions:
- rustfs_session: targets local RustFS S3 (explicit creds, never reads ~/.aws/)
- cloud_session: targets real AWS (reads ~/.aws/, env vars scrubbed)
- scrubbed_aws_env: context manager that keeps RustFS vars scrubbed for an entire block
"""

import os
from contextlib import contextmanager

import boto3
import click
from botocore.config import Config as BotoConfig

_AWS_ENV_VARS = [
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
    "AWS_ENDPOINT_URL", "AWS_DEFAULT_REGION", "AWS_REGION",
    "AWS_PROFILE", "AWS_DEFAULT_PROFILE",
    "AWS_CONFIG_FILE", "AWS_SHARED_CREDENTIALS_FILE",
]


def create_rustfs_session(cfg: dict):
    """Create an isolated boto3 S3 client for local RustFS.

    Reads credentials from .env config dict (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)
    and always targets the local RustFS endpoint. Fully explicit --
    ignores all AWS_* env vars.

    Returns:
        boto3 S3 client configured for RustFS
    """
    session = boto3.Session(
        aws_access_key_id=cfg.get("AWS_ACCESS_KEY_ID", "admin"),
        aws_secret_access_key=cfg.get("AWS_SECRET_ACCESS_KEY", "password"),
        region_name="us-east-1",
    )
    endpoint = cfg.get("AWS_ENDPOINT_URL", "http://localhost:19000")
    return session.client(
        "s3",
        endpoint_url=endpoint,
        region_name="us-east-1",
        config=BotoConfig(s3={"addressing_style": "path"}),
    )


def _resolve_profile(aws_profile: str | None = None) -> str:
    """Resolve AWS profile: CLI flag > env var > default."""
    return aws_profile or os.environ.get("L2C_AWS_PROFILE") or "default"


@contextmanager
def scrubbed_aws_env():
    """Context manager that strips RustFS AWS_* vars for the entire block.

    Use this to wrap any code that calls AWS -- including snow-utils functions
    that internally create their own boto3 clients. Vars are restored on exit.

    Example::

        with scrubbed_aws_env():
            cloud_s3, cloud_iam, cloud_sts = create_cloud_session(profile, region)
            create_s3_bucket(cloud_s3, ...)   # snow-utils, safe
            create_iam_policy(cloud_iam, ...)  # internally calls boto3.client("sts"), safe
    """
    saved = {k: os.environ.pop(k) for k in _AWS_ENV_VARS if k in os.environ}
    try:
        yield
    finally:
        os.environ.update(saved)


def create_cloud_session(
    aws_profile: str | None = None,
    region: str = "us-east-1",
):
    """Create an isolated boto3 session for real AWS.

    IMPORTANT: Call this inside a `scrubbed_aws_env()` context manager
    so that the env stays clean for the entire duration of AWS operations.

    Returns:
        (cloud_s3, cloud_iam, cloud_sts) tuple of boto3 clients
    """
    profile = _resolve_profile(aws_profile)
    session = boto3.Session(profile_name=profile, region_name=region)
    cloud_s3 = session.client("s3", region_name=region)
    cloud_iam = session.client("iam")
    cloud_sts = session.client("sts")

    account_id = cloud_sts.get_caller_identity()["Account"]
    click.echo(
        f"Cloud session: AWS account {account_id}, "
        f"profile '{profile}', region '{region}'"
    )

    return cloud_s3, cloud_iam, cloud_sts
