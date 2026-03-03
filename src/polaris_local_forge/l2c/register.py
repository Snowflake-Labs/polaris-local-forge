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

"""L2C register command -- create Snowflake External Iceberg Tables.

For each synced table, finds the latest Iceberg metadata file in AWS S3
and generates CREATE ICEBERG TABLE SQL with METADATA_FILE_PATH.
Snowflake infers the schema from the metadata -- no column definitions needed.

Tables are registered using SA_ROLE (least-privilege, restricted to target DB/Schema).
Table names are prefixed with namespace to avoid collisions: NAMESPACE_TABLE.
"""

import click

from polaris_local_forge.l2c.common import (
    find_latest_metadata,
    load_state,
    now_iso,
    resolve_resource_base,
    run_l2c_sql_file,
    save_state,
)
from polaris_local_forge.l2c.sessions import create_cloud_session, scrubbed_aws_env


def _sf_table_name(namespace: str, table: str) -> str:
    """Snowflake table name: NAMESPACE_TABLE (uppercase, hyphens to underscores)."""
    return f"{namespace}_{table}".upper().replace("-", "_")


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

    Only tables with sync status 'synced' are registered.
    """
    work_dir = ctx.obj["WORK_DIR"]
    state = load_state(work_dir)

    aws_state = state.get("aws")
    sf_state = state.get("snowflake")
    if not aws_state:
        raise click.ClickException("AWS setup not found. Run 'plf l2c setup aws' first.")
    if not sf_state:
        raise click.ClickException("Snowflake setup not found. Run 'plf l2c setup snowflake' first.")

    tables_state = state.get("tables", {})
    if not tables_state:
        raise click.ClickException("No tables in state. Run 'plf l2c sync' first.")

    rb = resolve_resource_base(work_dir, prefix_override=prefix, no_prefix=no_prefix)

    bucket = aws_state["bucket"]
    sa_role = sf_state.get("sa_role", rb["sf_base"])
    catalog_integration = sf_state.get("catalog_integration", rb["sf_base"])
    external_volume = sf_state.get("external_volume", rb["sf_base"])
    database = sf_database or sf_state.get("database", rb["sf_base"])
    aws_profile = aws_state.get("profile")
    region = aws_state.get("region", "us-east-1")

    registerable = []
    for key, tbl in tables_state.items():
        sync_status = tbl.get("sync", {}).get("status")
        reg_status = tbl.get("register", {}).get("status", "pending")
        if sync_status == "synced" and reg_status != "done":
            registerable.append((key, tbl))

    if not registerable:
        click.echo("No tables ready for registration.")
        click.echo("Tables must be synced (status='synced') and not yet registered.")
        return

    click.echo(f"\nProject: {rb['project']} | Catalog: {rb['catalog']}")
    click.echo(f"\n--- L2C Register Plan ---")
    click.echo(f"  SA_ROLE:             {sa_role}")
    click.echo(f"  Catalog Integration: {catalog_integration}")
    click.echo(f"  External Volume:     {external_volume}")
    click.echo(f"  Database:            {database}")
    click.echo(f"  Schema:              {sf_schema}")
    click.echo(f"  Tables:              {len(registerable)}")
    click.echo()

    with scrubbed_aws_env():
        cloud_s3, _, _ = create_cloud_session(aws_profile, region)

    registered = 0
    failed = 0

    for key, tbl in registerable:
        ns = tbl["namespace"]
        tbl_name = tbl["table"]
        sf_name = _sf_table_name(ns, tbl_name)

        click.echo(f"  {ns}.{tbl_name} -> {database}.{sf_schema}.{sf_name}")

        metadata_key = find_latest_metadata(cloud_s3, bucket, ns, tbl_name)
        if not metadata_key:
            click.secho(
                f"    SKIP: No metadata files found in s3://{bucket}/{ns}/{tbl_name}/metadata/",
                fg="yellow",
            )
            if not dry_run:
                tables_state[key].setdefault("register", {})["status"] = "failed"
                tables_state[key]["register"]["error"] = "No metadata files found"
            failed += 1
            continue
        metadata_path = metadata_key
        click.echo(f"    metadata: {metadata_path}")

        sql_vars = {
            "sa_role": sa_role,
            "database": database,
            "schema": sf_schema,
            "table_name": sf_name,
            "external_volume": external_volume,
            "catalog_integration": catalog_integration,
            "metadata_file_path": metadata_path,
        }

        if dry_run:
            run_l2c_sql_file("register_table.sql", sql_vars, dry_run=True)
            click.echo()
            continue

        if not yes:
            if not click.confirm(f"    Register {sf_name}?", default=True):
                click.echo(f"    Skipped.")
                continue

        try:
            run_l2c_sql_file("register_table.sql", sql_vars)
            tables_state[key]["register"] = {
                "status": "done",
                "registered_at": now_iso(),
                "sf_table": f"{database}.{sf_schema}.{sf_name}",
                "metadata_path": metadata_path,
            }
            registered += 1
            click.echo(f"    Registered.")
        except Exception as e:
            click.secho(f"    FAILED: {e}", fg="red")
            tables_state[key]["register"] = {
                "status": "failed",
                "error": str(e),
            }
            failed += 1

    save_state(work_dir, state)

    if dry_run:
        click.echo("[dry-run] No changes made.")
    else:
        click.echo(f"\nRegister complete: {registered} registered, {failed} failed.")
