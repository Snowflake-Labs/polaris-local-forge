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

Resource names are auto-derived from:
  <SNOWFLAKE_USER>-<project>-<catalog>  (AWS, lowercase, hyphens)
  <USER>_<PROJECT>_<CATALOG>            (Snowflake, uppercase, underscores)

No type suffixes -- AWS bucket, IAM policy, and IAM role live in separate
namespaces so the same base name is used for all three.
"""

import click
from snow_utils.extvolume import (
    create_iam_policy,
    create_iam_role,
    create_s3_bucket,
    generate_external_id,
    get_aws_account_id,
    get_resource_tags,
)

from polaris_local_forge.l2c.common import (
    load_state,
    now_iso,
    preflight_aws_check,
    resolve_aws_region,
    resolve_resource_base,
    save_state,
)
from polaris_local_forge.l2c.sessions import create_cloud_session, scrubbed_aws_env


@click.command("setup_aws")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", default=None,
              help="AWS profile (default: L2C_AWS_PROFILE or 'default')")
@click.option("--region", "-r", envvar="L2C_AWS_REGION", default=None,
              help="AWS region (default: from AWS profile config, or us-east-1)")
@click.option("--prefix", "-p", default=None,
              help="Override SNOWFLAKE_USER prefix for resource names")
@click.option("--no-prefix", is_flag=True,
              help="Drop user prefix (resources named <project>-<catalog> only)")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def setup_aws(ctx, aws_profile, region, prefix, no_prefix, dry_run, yes):
    """Create AWS S3 bucket, IAM role/policy for migration."""
    work_dir = ctx.obj["WORK_DIR"]
    region = resolve_aws_region(region, aws_profile)
    rb = resolve_resource_base(work_dir, prefix_override=prefix, no_prefix=no_prefix)
    aws_base = rb["aws_base"]
    sf_base = rb["sf_base"]
    prefix = rb["prefix"]

    external_id = generate_external_id(aws_base, prefix)
    tags = get_resource_tags(prefix, aws_base, sf_base)

    click.echo(f"Project: {rb['project']} | Catalog: {rb['catalog']}")
    click.echo(f"\n--- L2C AWS Setup Plan ---")
    click.echo(f"  S3 Bucket:  {aws_base}")
    click.echo(f"  IAM Policy: {aws_base}")
    click.echo(f"  IAM Role:   {aws_base}")
    click.echo(f"  Ext Volume: {sf_base}")
    click.echo(f"  Region:     {region}")
    click.echo(f"  Profile:    {aws_profile or '(default)'}")
    click.echo()

    if dry_run:
        click.echo("[dry-run] No changes made.")
        return

    if not yes:
        click.confirm("Create these AWS resources?", abort=True)

    with scrubbed_aws_env():
        preflight_aws_check(aws_profile)

        cloud_s3, cloud_iam, cloud_sts = create_cloud_session(aws_profile, region)
        account_id = get_aws_account_id(cloud_sts)

        create_s3_bucket(cloud_s3, aws_base, region, versioning=True, tags=tags)
        policy_arn = create_iam_policy(
            cloud_iam, aws_base, aws_base,
            tags=tags, sts_client=cloud_sts,
        )
        role_arn = create_iam_role(
            cloud_iam, aws_base, policy_arn, account_id,
            external_id, tags=tags,
        )

    state = load_state(work_dir)
    state["aws"] = {
        "bucket": aws_base,
        "role_name": aws_base,
        "role_arn": role_arn,
        "policy_arn": policy_arn,
        "external_id": external_id,
        "account_id": account_id,
        "region": region,
        "profile": aws_profile,
        "prefix": prefix,
        "sf_base": sf_base,
        "project": rb["project"],
        "catalog": rb["catalog"],
        "updated_at": now_iso(),
    }
    save_state(work_dir, state)

    click.echo("\nAWS setup complete. State saved to .snow-utils/l2c-state.json")
