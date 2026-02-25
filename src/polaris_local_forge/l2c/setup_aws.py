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

"""L2C setup aws command -- S3 bucket, IAM policy/role.

Reuses snow-utils extvolume functions for idempotent AWS resource creation.
"""

import click
from snow_utils.extvolume import (
    create_iam_policy,
    create_iam_role,
    create_s3_bucket,
    generate_external_id,
    get_aws_account_id,
    get_resource_tags,
    to_aws_name,
    to_sql_identifier,
)

from polaris_local_forge.l2c.common import (
    ensure_snowflake_connection,
    load_state,
    now_iso,
    preflight_aws_check,
    resolve_aws_region,
    save_state,
)
from polaris_local_forge.l2c.sessions import create_cloud_session

BUCKET_SUFFIX = "plf-migration"
POLICY_SUFFIX = "plf-migration-policy"
ROLE_SUFFIX = "plf-migration-role"


def _resolve_prefix(prefix, no_prefix, work_dir):
    """Resolve prefix: --no-prefix > explicit --prefix > SNOWFLAKE_USER (with discovery)."""
    if no_prefix:
        return None
    return prefix or ensure_snowflake_connection(work_dir)


def _resource_names(prefix: str | None) -> dict:
    """Derive AWS resource names from prefix."""
    bucket = to_aws_name(BUCKET_SUFFIX, prefix)
    policy = to_aws_name(POLICY_SUFFIX, prefix)
    role = to_aws_name(ROLE_SUFFIX, prefix)
    volume = to_sql_identifier("plf_migration_vol", prefix)
    external_id = generate_external_id(bucket, prefix)
    return {
        "bucket": bucket,
        "policy": policy,
        "role": role,
        "volume": volume,
        "external_id": external_id,
    }


@click.command("setup_aws")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", default=None,
              help="AWS profile (default: L2C_AWS_PROFILE or 'default')")
@click.option("--region", "-r", envvar="L2C_AWS_REGION", default=None,
              help="AWS region (default: from AWS profile config, or us-east-1)")
@click.option("--prefix", "-p", envvar="L2C_PREFIX", default=None,
              help="Prefix for AWS resources (default: SNOWFLAKE_USER, lowercase)")
@click.option("--no-prefix", is_flag=True,
              help="Disable username prefix for AWS resources")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def setup_aws(ctx, aws_profile, region, prefix, no_prefix, dry_run, yes):
    """Create AWS S3 bucket, IAM role/policy for migration."""
    work_dir = ctx.obj["WORK_DIR"]
    region = resolve_aws_region(region, aws_profile)
    resolved_prefix = _resolve_prefix(prefix, no_prefix, work_dir)

    if resolved_prefix:
        click.echo(f"Using prefix: {resolved_prefix}")

    names = _resource_names(resolved_prefix)
    tags = get_resource_tags(resolved_prefix, names["bucket"], names["volume"])

    click.echo("\n--- L2C AWS Setup Plan ---")
    click.echo(f"  S3 Bucket:  {names['bucket']}")
    click.echo(f"  IAM Policy: {names['policy']}")
    click.echo(f"  IAM Role:   {names['role']}")
    click.echo(f"  Region:     {region}")
    click.echo(f"  Profile:    {aws_profile or '(default)'}")
    click.echo()

    if dry_run:
        click.echo("[dry-run] No changes made.")
        return

    if not yes:
        click.confirm("Create these AWS resources?", abort=True)

    preflight_aws_check(aws_profile)

    cloud_s3, cloud_iam, cloud_sts = create_cloud_session(aws_profile, region)
    account_id = get_aws_account_id(cloud_sts)

    create_s3_bucket(cloud_s3, names["bucket"], region, versioning=True, tags=tags)
    policy_arn = create_iam_policy(cloud_iam, names["policy"], names["bucket"], tags=tags)
    role_arn = create_iam_role(
        cloud_iam, names["role"], policy_arn, account_id,
        names["external_id"], tags=tags,
    )

    state = load_state(work_dir)
    state["aws"] = {
        "bucket": names["bucket"],
        "role_name": names["role"],
        "role_arn": role_arn,
        "policy_arn": policy_arn,
        "external_id": names["external_id"],
        "volume_name": names["volume"],
        "account_id": account_id,
        "region": region,
        "profile": aws_profile,
        "prefix": resolved_prefix,
        "updated_at": now_iso(),
    }
    save_state(work_dir, state)

    click.echo("\n✓ AWS setup complete. State saved to .snow-utils/l2c-state.json")
