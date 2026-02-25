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

"""L2C sync command -- copy Iceberg data from RustFS to AWS S3."""

import click


@click.command("sync")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", default=None,
              help="AWS profile")
@click.option("--region", "-r", envvar="L2C_AWS_REGION", default="us-east-1",
              help="AWS region")
@click.option("--force", "-f", is_flag=True,
              help="Re-upload all objects (skip smart sync)")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def sync(ctx, aws_profile, region, force, dry_run, yes):
    """Copy Iceberg data from local RustFS to AWS S3."""
    click.echo("sync: not yet implemented")
