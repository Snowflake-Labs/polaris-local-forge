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

"""L2C setup snowflake command -- catalog integration, external volume, SA_ROLE."""

import click


@click.command("setup_snowflake")
@click.option("--sf-database", "-D", envvar="L2C_SF_DATABASE", required=True,
              help="Target Snowflake database")
@click.option("--sf-schema", "-S", envvar="L2C_SF_SCHEMA", default="PUBLIC",
              help="Target Snowflake schema")
@click.option("--admin-role", default=None,
              help="Snowflake admin role (default: from manifest or ACCOUNTADMIN)")
@click.option("--sa-role", envvar="SA_ROLE", default="PLF_MIGRATION_ROLE",
              help="Service account role for L2C operations")
@click.option("--catalog-integration", envvar="L2C_CATALOG_INTEGRATION",
              default="PLF_L2C_CATALOG_INT",
              help="Catalog integration name for Iceberg object store")
@click.option("--prefix", "-p", envvar="L2C_PREFIX", default=None,
              help="Prefix for AWS resources (default: SNOWFLAKE_USER, lowercase)")
@click.option("--no-prefix", is_flag=True,
              help="Disable username prefix for AWS resources")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", default=None,
              help="AWS profile (for trust policy update)")
@click.option("--region", "-r", envvar="L2C_AWS_REGION", default="us-east-1",
              help="AWS region")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def setup_snowflake(ctx, sf_database, sf_schema, admin_role, sa_role,
                    catalog_integration, prefix, no_prefix, aws_profile,
                    region, dry_run, yes):
    """Create Catalog Integration, External Volume, SA_ROLE, target DB/Schema, GRANTs."""
    click.echo("setup snowflake: not yet implemented")
