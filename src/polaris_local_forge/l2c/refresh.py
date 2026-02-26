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

"""L2C refresh command -- zero-downtime metadata pointer update.

For each registered table, compares the stored metadata_path with the
latest Iceberg metadata file on S3. When a newer snapshot exists
(e.g. after a local data mutation + sync), runs ALTER ICEBERG TABLE ...
REFRESH to update Snowflake without dropping the table.

Tables that have never been registered are skipped (use 'register' for those).
"""

import click

from polaris_local_forge.l2c.common import (
    find_latest_metadata,
    load_state,
    now_iso,
    preflight_aws_check,
    resolve_resource_base,
    run_l2c_sql_file,
    save_state,
)
from polaris_local_forge.l2c.sessions import create_cloud_session, scrubbed_aws_env


@click.command("refresh")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", help="AWS profile name")
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
def refresh(ctx, aws_profile, sf_database, sf_schema, prefix, no_prefix, dry_run, yes):
    """Refresh registered Iceberg tables to point at the latest metadata.

    After 'sync --force' uploads new data/metadata, this command runs
    ALTER ICEBERG TABLE ... REFRESH for each registered table whose
    metadata has changed on S3. Zero-downtime -- no DROP required.

    Only tables with register.status='done' are considered.
    Tables where S3 metadata matches the stored path are skipped.
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
    database = sf_database or sf_state.get("database", rb["sf_base"])
    aws_profile = aws_profile or aws_state.get("profile")
    region = aws_state.get("region", "us-east-1")

    registered = [
        (key, tbl) for key, tbl in tables_state.items()
        if tbl.get("register", {}).get("status") == "done"
    ]

    if not registered:
        click.echo("No registered tables to refresh.")
        click.echo("Tables must have register.status='done'. Run 'plf l2c register' first.")
        return

    click.echo(f"\nProject: {rb['project']} | Catalog: {rb['catalog']}")
    click.echo(f"\n--- L2C Refresh Plan ---")
    click.echo(f"  SA_ROLE:    {sa_role}")
    click.echo(f"  Database:   {database}")
    click.echo(f"  Schema:     {sf_schema}")
    click.echo(f"  Candidates: {len(registered)} registered table(s)")
    click.echo()

    with scrubbed_aws_env():
        cloud_s3, _, _ = create_cloud_session(aws_profile, region)

    refreshed = 0
    skipped = 0
    failed = 0

    for key, tbl in registered:
        ns = tbl["namespace"]
        tbl_name = tbl["table"]
        sf_table = tbl.get("register", {}).get("sf_table", "")
        current_path = tbl.get("register", {}).get("metadata_path", "")
        short_name = sf_table.rsplit(".", 1)[-1] if sf_table else f"{ns}_{tbl_name}".upper().replace("-", "_")

        latest_path = find_latest_metadata(cloud_s3, bucket, ns, tbl_name)
        if not latest_path:
            click.secho(
                f"  {ns}.{tbl_name}: SKIP (no metadata in S3)",
                fg="yellow",
            )
            skipped += 1
            continue

        if latest_path == current_path:
            click.echo(f"  {ns}.{tbl_name}: up-to-date ({current_path})")
            skipped += 1
            continue

        click.echo(f"  {ns}.{tbl_name}:")
        click.echo(f"    current:  {current_path or '(none)'}")
        click.echo(f"    latest:   {latest_path}")

        sql_vars = {
            "sa_role": sa_role,
            "database": database,
            "schema": sf_schema,
            "table_name": short_name,
            "metadata_file_path": latest_path,
        }

        if dry_run:
            run_l2c_sql_file("refresh_table.sql", sql_vars, dry_run=True)
            click.echo()
            continue

        if not yes:
            if not click.confirm(f"    Refresh {short_name}?", default=True):
                click.echo(f"    Skipped.")
                skipped += 1
                continue

        try:
            run_l2c_sql_file("refresh_table.sql", sql_vars)
            tables_state[key]["register"]["metadata_path"] = latest_path
            tables_state[key]["register"]["refreshed_at"] = now_iso()
            refreshed += 1
            click.echo(f"    Refreshed.")
        except Exception as e:
            click.secho(f"    FAILED: {e}", fg="red")
            tables_state[key]["register"]["refresh_error"] = str(e)
            failed += 1

    save_state(work_dir, state)

    if dry_run:
        click.echo("[dry-run] No changes made.")
    else:
        click.echo(f"\nRefresh complete: {refreshed} refreshed, {skipped} skipped, {failed} failed.")
