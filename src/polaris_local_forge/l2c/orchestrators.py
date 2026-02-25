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

"""L2C orchestrator commands -- migrate, status, clear, cleanup."""

import click


@click.command("migrate")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", help="AWS profile name")
@click.option("--region", "-r", envvar="L2C_AWS_REGION", default="us-east-1",
              help="AWS region")
@click.option("--prefix", "-p", envvar="L2C_PREFIX", default=None,
              help="Prefix for AWS resources (default: SNOWFLAKE_USER, lowercase)")
@click.option("--no-prefix", is_flag=True,
              help="Disable username prefix for AWS resources")
@click.option("--sf-database", "-D", envvar="L2C_SF_DATABASE",
              help="Snowflake target database")
@click.option("--sf-schema", "-S", envvar="L2C_SF_SCHEMA", default="PUBLIC",
              help="Snowflake target schema")
@click.option("--sa-role", envvar="SA_ROLE", help="Service account role")
@click.option("--admin-role", help="Admin role for setup operations")
@click.option("--catalog-integration", envvar="L2C_CATALOG_INTEGRATION",
              help="Catalog integration name")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def migrate(ctx, **kwargs):
    """Full migration -- setup + sync + register."""
    click.echo("migrate: not yet implemented")


@click.command("status")
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text",
              help="Output format")
@click.pass_context
def status(ctx, output):
    """Show migration state for each table."""
    click.echo("status: not yet implemented")


@click.command("clear")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", help="AWS profile name")
@click.option("--prefix", "-p", envvar="L2C_PREFIX", default=None,
              help="Prefix for AWS resources (default: SNOWFLAKE_USER, lowercase)")
@click.option("--no-prefix", is_flag=True,
              help="Disable username prefix for AWS resources")
@click.option("--sa-role", envvar="SA_ROLE", help="Service account role")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def clear(ctx, aws_profile, prefix, no_prefix, sa_role, dry_run, yes):
    """Remove migrated data (S3 objects + Snowflake tables), keep infrastructure.

    Resets table state to pending for re-sync.
    """
    click.echo("clear: not yet implemented")


@click.command("cleanup")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", help="AWS profile name")
@click.option("--prefix", "-p", envvar="L2C_PREFIX", default=None,
              help="Prefix for AWS resources (default: SNOWFLAKE_USER, lowercase)")
@click.option("--no-prefix", is_flag=True,
              help="Disable username prefix for AWS resources")
@click.option("--admin-role", help="Admin role for teardown operations")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def cleanup(ctx, aws_profile, prefix, no_prefix, admin_role, dry_run, yes):
    """Full teardown -- remove all L2C infrastructure and data."""
    click.echo("cleanup: not yet implemented")
