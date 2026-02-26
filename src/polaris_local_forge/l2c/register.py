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
@click.option("--sf-database", "-D", envvar="L2C_SF_DATABASE", default=None,
              help="Target Snowflake database (default: from state)")
@click.option("--sf-schema", "-S", envvar="L2C_SF_SCHEMA", default="L2C",
              help="Target Snowflake schema")
@click.option("--prefix", "-p", default=None,
              help="Override SNOWFLAKE_USER prefix for resource names")
@click.option("--no-prefix", is_flag=True,
              help="Drop user prefix from resource names")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def register(ctx, sf_database, sf_schema, prefix, no_prefix, dry_run, yes):
    """Register migrated tables as Snowflake External Iceberg Tables.

    Uses catalog integration + METADATA_FILE_PATH (schema inferred
    from existing Iceberg metadata). SA_ROLE, catalog integration, and
    database are resolved from state (set by setup commands).
    """
    click.echo("register: not yet implemented")
