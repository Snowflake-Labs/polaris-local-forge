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

"""L2C setup aws command -- S3 bucket, IAM policy/role."""

import click
from snow_utils.extvolume import get_current_username


@click.command("setup_aws")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", default=None,
              help="AWS profile (default: L2C_AWS_PROFILE or 'default')")
@click.option("--region", "-r", envvar="L2C_AWS_REGION", default="us-east-1",
              help="AWS region")
@click.option("--prefix", "-p", envvar="L2C_PREFIX", default=None,
              help="Prefix for AWS resources (default: SNOWFLAKE_USER, lowercase)")
@click.option("--no-prefix", is_flag=True,
              help="Disable username prefix for AWS resources")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def setup_aws(ctx, aws_profile, region, prefix, no_prefix, dry_run, yes):
    """Create AWS S3 bucket, IAM role/policy for migration."""
    if no_prefix:
        resolved_prefix = None
    elif prefix:
        resolved_prefix = prefix
    else:
        resolved_prefix = get_current_username()
    if resolved_prefix:
        click.echo(f"Using prefix: {resolved_prefix}")
    ctx.ensure_object(dict)
    ctx.obj["prefix"] = resolved_prefix
    click.echo("setup aws: not yet implemented")
