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

"""L2C register command -- create Snowflake Iceberg tables."""

import click


@click.command("register")
@click.option("--sf-database", "-D", envvar="L2C_SF_DATABASE", required=True,
              help="Target Snowflake database")
@click.option("--sf-schema", "-S", envvar="L2C_SF_SCHEMA", default="PUBLIC",
              help="Target Snowflake schema")
@click.option("--sa-role", envvar="SA_ROLE", default="PLF_MIGRATION_ROLE",
              help="Service account role")
@click.option("--catalog-integration", envvar="L2C_CATALOG_INTEGRATION",
              default="PLF_L2C_CATALOG_INT",
              help="Catalog integration for Iceberg table creation")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def register(ctx, sf_database, sf_schema, sa_role, catalog_integration, dry_run, yes):
    """Register migrated tables as Snowflake External Iceberg Tables.

    Uses catalog integration + METADATA_FILE_PATH (schema inferred
    from existing Iceberg metadata).
    """
    click.echo("register: not yet implemented")
