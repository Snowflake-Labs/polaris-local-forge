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

"""L2C orchestrator commands -- migrate, status, clear, cleanup.

All resource names (sa_role, catalog_integration, database, bucket, etc.)
are resolved from the state file written by setup commands. No hardcoded
defaults -- naming follows the project-scoped convention.
"""

import json

import click
from snow_utils.extvolume import delete_iam_policy, delete_iam_role, delete_s3_bucket

from polaris_local_forge.l2c.common import (
    load_state,
    now_iso,
    preflight_aws_check,
    resolve_aws_region,
    resolve_resource_base,
    run_l2c_sql_file,
    save_state,
)
from polaris_local_forge.l2c.sessions import create_cloud_session, scrubbed_aws_env


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@click.command("status")
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text",
              help="Output format")
@click.pass_context
def status(ctx, output):
    """Show migration state for each table."""
    work_dir = ctx.obj["WORK_DIR"]
    state = load_state(work_dir)

    if not state:
        click.echo("No L2C state found. Run 'plf l2c setup aws' to start.")
        return

    if output == "json":
        click.echo(json.dumps(state, indent=2))
        return

    aws = state.get("aws", {})
    sf = state.get("snowflake", {})
    tables = state.get("tables", {})

    click.echo("--- L2C Migration Status ---\n")

    if aws:
        click.echo("  AWS:")
        click.echo(f"    Bucket:  {aws.get('bucket', '(not set)')}")
        click.echo(f"    Role:    {aws.get('role_name', '(not set)')}")
        click.echo(f"    Region:  {aws.get('region', '(not set)')}")
        click.echo(f"    Updated: {aws.get('updated_at', '(unknown)')}")
    else:
        click.secho("  AWS: not configured", fg="yellow")

    click.echo()
    if sf:
        click.echo("  Snowflake:")
        click.echo(f"    SA Role:      {sf.get('sa_role', '(not set)')}")
        click.echo(f"    Ext Volume:   {sf.get('external_volume', '(not set)')}")
        click.echo(f"    Catalog Int:  {sf.get('catalog_integration', '(not set)')}")
        click.echo(f"    Database:     {sf.get('database', '(not set)')}")
        click.echo(f"    Schema:       {sf.get('schema', '(not set)')}")
        click.echo(f"    Updated:      {sf.get('updated_at', '(unknown)')}")
    else:
        click.secho("  Snowflake: not configured", fg="yellow")

    # Drift detection: check naming consistency between aws and snowflake
    aws_sf_base = aws.get("sf_base")
    sf_sf_base = sf.get("sf_base")
    if aws_sf_base and sf_sf_base and aws_sf_base != sf_sf_base:
        click.echo()
        click.secho(
            f"  WARNING: Naming drift detected!\n"
            f"    AWS sf_base:       {aws_sf_base}\n"
            f"    Snowflake sf_base: {sf_sf_base}\n"
            f"    Re-run setup with consistent --prefix to fix.",
            fg="yellow",
        )

    click.echo()
    if not tables:
        click.echo("  Tables: none (run 'plf l2c sync' to start)")
        return

    click.echo(f"  Tables ({len(tables)}):\n")
    for key, tbl in sorted(tables.items()):
        ns = tbl.get("namespace", "?")
        name = tbl.get("table", "?")
        sync_st = tbl.get("sync", {}).get("status", "pending")
        reg_st = tbl.get("register", {}).get("status", "pending")

        sync_color = {"synced": "green", "failed": "red", "in_progress": "cyan"}.get(sync_st, None)
        reg_color = {"done": "green", "failed": "red"}.get(reg_st, None)

        click.echo(f"    {ns}.{name}")
        click.echo(f"      sync:     ", nl=False)
        click.secho(sync_st, fg=sync_color)
        click.echo(f"      register: ", nl=False)
        click.secho(reg_st, fg=reg_color)

        if tbl.get("sync", {}).get("error"):
            click.secho(f"      error:    {tbl['sync']['error']}", fg="red")
        if tbl.get("register", {}).get("error"):
            click.secho(f"      error:    {tbl['register']['error']}", fg="red")
        if tbl.get("register", {}).get("sf_table"):
            click.echo(f"      sf_table: {tbl['register']['sf_table']}")


# ---------------------------------------------------------------------------
# clear -- remove data, keep infrastructure
# ---------------------------------------------------------------------------

def _clear_s3_objects(cloud_s3, bucket: str, tables: dict, dry_run: bool) -> int:
    """Delete all S3 objects for each table's prefix. Returns count deleted."""
    deleted = 0
    for key, tbl in tables.items():
        ns = tbl.get("namespace", "")
        name = tbl.get("table", "")
        prefix = f"{ns}/{name}/"
        if not ns or not name:
            continue

        paginator = cloud_s3.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects.append({"Key": obj["Key"]})

        if not objects:
            click.echo(f"    {ns}.{name}: no objects")
            continue

        if dry_run:
            click.echo(f"    {ns}.{name}: {len(objects)} objects would be deleted")
        else:
            for i in range(0, len(objects), 1000):
                batch = objects[i:i + 1000]
                cloud_s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
            click.echo(f"    {ns}.{name}: {len(objects)} objects deleted")
            deleted += len(objects)
    return deleted


@click.command("clear")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", help="AWS profile name")
@click.option("--prefix", "-p", default=None,
              help="Override SNOWFLAKE_USER prefix for resource names")
@click.option("--no-prefix", is_flag=True,
              help="Drop user prefix from resource names")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def clear(ctx, aws_profile, prefix, no_prefix, dry_run, yes):
    """Remove migrated data (S3 objects + Snowflake tables), keep infrastructure.

    Resets table state to pending for re-sync. All resource names are
    resolved from the state file.
    """
    work_dir = ctx.obj["WORK_DIR"]
    state = load_state(work_dir)

    aws_state = state.get("aws")
    sf_state = state.get("snowflake")
    tables_state = state.get("tables", {})

    if not aws_state:
        raise click.ClickException("AWS setup not found. Nothing to clear.")
    if not tables_state:
        click.echo("No tables in state. Nothing to clear.")
        return

    bucket = aws_state["bucket"]
    region = aws_state.get("region", "us-east-1")
    aws_profile = aws_profile or aws_state.get("profile")

    sa_role = sf_state.get("sa_role", "") if sf_state else ""
    database = sf_state.get("database", "") if sf_state else ""
    schema = sf_state.get("schema", "L2C") if sf_state else "L2C"

    registered_tables = [
        (k, t) for k, t in tables_state.items()
        if t.get("register", {}).get("status") == "done"
    ]

    click.echo("\n--- L2C Clear Plan ---")
    click.echo(f"  S3 Bucket:  {bucket} (delete objects, keep bucket)")
    click.echo(f"  Tables:     {len(tables_state)} in state")
    if registered_tables and sa_role:
        click.echo(f"  SF Tables:  {len(registered_tables)} registered (will DROP)")
        click.echo(f"  SA Role:    {sa_role}")
        click.echo(f"  Database:   {database}.{schema}")
    click.echo()

    if dry_run:
        click.echo("S3 objects to delete:")
        with scrubbed_aws_env():
            cloud_s3, _, _ = create_cloud_session(aws_profile, region)
            _clear_s3_objects(cloud_s3, bucket, tables_state, dry_run=True)

        if registered_tables and sa_role:
            click.echo("\nSnowflake tables to drop:")
            for key, tbl in registered_tables:
                sf_table = tbl.get("register", {}).get("sf_table", "")
                ns = tbl.get("namespace", "?")
                name = tbl.get("table", "?")
                sf_name = f"{ns}_{name}".upper().replace("-", "_")
                click.echo(f"    DROP ICEBERG TABLE IF EXISTS {database}.{schema}.{sf_name}")

        click.echo("\n[dry-run] No changes made.")
        return

    if not yes:
        click.confirm(
            f"Delete all S3 objects and {len(registered_tables)} Snowflake table(s)?",
            abort=True,
        )

    with scrubbed_aws_env():
        preflight_aws_check(aws_profile)
        cloud_s3, _, _ = create_cloud_session(aws_profile, region)

    click.echo("Deleting S3 objects...")
    with scrubbed_aws_env():
        cloud_s3, _, _ = create_cloud_session(aws_profile, region)
        _clear_s3_objects(cloud_s3, bucket, tables_state, dry_run=False)

    if registered_tables and sa_role:
        click.echo("\nDropping Snowflake tables...")
        for key, tbl in registered_tables:
            ns = tbl.get("namespace", "?")
            name = tbl.get("table", "?")
            sf_name = f"{ns}_{name}".upper().replace("-", "_")
            try:
                run_l2c_sql_file("drop_table.sql", {
                    "sa_role": sa_role,
                    "database": database,
                    "schema": schema,
                    "table_name": sf_name,
                })
                click.echo(f"    Dropped {sf_name}")
            except Exception as e:
                click.secho(f"    Failed to drop {sf_name}: {e}", fg="red")

    for key in tables_state:
        tables_state[key]["sync"] = {"status": "pending"}
        tables_state[key]["register"] = {"status": "pending"}
    state["tables"] = tables_state
    save_state(work_dir, state)

    click.echo("\nClear complete. Table state reset to 'pending'.")


# ---------------------------------------------------------------------------
# cleanup -- full teardown
# ---------------------------------------------------------------------------

@click.command("cleanup")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", help="AWS profile name")
@click.option("--admin-role", help="Admin role for teardown operations")
@click.option("--prefix", "-p", default=None,
              help="Override SNOWFLAKE_USER prefix for resource names")
@click.option("--no-prefix", is_flag=True,
              help="Drop user prefix from resource names")
@click.option("--force", "-f", is_flag=True,
              help="Also delete the S3 bucket (irreversible)")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def cleanup(ctx, aws_profile, admin_role, prefix, no_prefix, force, dry_run, yes):
    """Full teardown -- remove all L2C infrastructure and data.

    Drops Snowflake resources first (ext vol, catalog integration, SA_ROLE),
    then deletes AWS resources (IAM role/policy). The S3 bucket is only
    deleted with --force (irreversible).

    All resource names are resolved from the state file.
    """
    work_dir = ctx.obj["WORK_DIR"]
    state = load_state(work_dir)

    aws_state = state.get("aws")
    sf_state = state.get("snowflake")
    tables_state = state.get("tables", {})

    if not aws_state and not sf_state:
        click.echo("No L2C state found. Nothing to cleanup.")
        return

    bucket = aws_state.get("bucket", "") if aws_state else ""
    role_name = aws_state.get("role_name", "") if aws_state else ""
    role_arn = aws_state.get("role_arn", "") if aws_state else ""
    policy_arn = aws_state.get("policy_arn", "") if aws_state else ""
    region = aws_state.get("region", "us-east-1") if aws_state else "us-east-1"
    aws_profile = aws_profile or (aws_state.get("profile") if aws_state else None)

    sa_role = sf_state.get("sa_role", "") if sf_state else ""
    ext_vol = sf_state.get("external_volume", "") if sf_state else ""
    cat_int = sf_state.get("catalog_integration", "") if sf_state else ""
    database = sf_state.get("database", "") if sf_state else ""
    schema = sf_state.get("schema", "L2C") if sf_state else "L2C"
    admin_role = admin_role or (sf_state.get("admin_role", "ACCOUNTADMIN") if sf_state else "ACCOUNTADMIN")

    click.echo("\n--- L2C Full Cleanup Plan ---")
    if tables_state:
        click.echo(f"\n  Step 1 - Clear data:")
        click.echo(f"    Tables: {len(tables_state)} (S3 objects + SF tables)")
    if sf_state:
        click.echo(f"\n  Step 2 - Drop Snowflake resources (before AWS):")
        click.echo(f"    SA Role:          {sa_role}")
        click.echo(f"    External Volume:  {ext_vol}")
        click.echo(f"    Catalog Int:      {cat_int}")
        click.echo(f"    Database:         {database} (prompted separately)")
        click.echo(f"    Admin Role:       {admin_role} (used for DROP)")
    if aws_state:
        click.echo(f"\n  Step 3 - Delete AWS resources:")
        click.echo(f"    IAM Role:   {role_name}")
        click.echo(f"    IAM Policy: {policy_arn}")
        if force:
            click.echo(f"    S3 Bucket:  {bucket} (--force: WILL BE DELETED)")
        else:
            click.echo(f"    S3 Bucket:  {bucket} (kept unless --force)")
    click.echo()

    if dry_run:
        click.echo("[dry-run] No changes made.")
        return

    if not yes:
        click.confirm(
            "This will PERMANENTLY delete L2C infrastructure. Continue?",
            abort=True,
        )

    # Step 1: Clear data (S3 objects + SF tables)
    if tables_state:
        click.echo("Step 1: Clearing migrated data...")
        ctx.invoke(clear, aws_profile=aws_profile, prefix=prefix,
                   no_prefix=no_prefix, dry_run=False, yes=True)

    # Step 2: Drop Snowflake infrastructure FIRST (references AWS resources)
    if sf_state:
        click.echo("\nStep 2: Removing Snowflake infrastructure...")
        sf_vars = {
            "admin_role": admin_role,
            "sa_role": sa_role,
            "database": database,
            "schema": schema,
            "volume_name": ext_vol,
            "catalog_integration": cat_int,
        }
        for sql_file in ["cleanup_role.sql", "cleanup_catalog_integration.sql",
                         "cleanup_external_volume.sql"]:
            try:
                run_l2c_sql_file(sql_file, sf_vars)
            except Exception as e:
                click.secho(f"  Warning: {sql_file} failed: {e}", fg="yellow")

        if database:
            drop_db = yes or click.confirm(
                f"\n  Also drop database '{database}'?", default=False,
            )
            if drop_db:
                try:
                    run_l2c_sql_file("cleanup_database.sql", {
                        "admin_role": admin_role,
                        "database": database,
                    })
                except Exception as e:
                    click.secho(f"  Warning: database drop failed: {e}", fg="yellow")
            else:
                click.echo(f"  Keeping database '{database}'.")

    # Step 3: Delete AWS infrastructure AFTER Snowflake (ext vol must be gone first)
    if aws_state:
        click.echo("\nStep 3: Removing AWS infrastructure...")
        with scrubbed_aws_env():
            preflight_aws_check(aws_profile)
            _, cloud_iam, _ = create_cloud_session(aws_profile, region)

            if role_name and policy_arn:
                try:
                    delete_iam_role(cloud_iam, role_name, policy_arn)
                except Exception as e:
                    click.secho(f"  Warning: role delete failed: {e}", fg="yellow")
            if policy_arn:
                try:
                    delete_iam_policy(cloud_iam, policy_arn)
                except Exception as e:
                    click.secho(f"  Warning: policy delete failed: {e}", fg="yellow")

            if bucket and force:
                cloud_s3, _, _ = create_cloud_session(aws_profile, region)
                try:
                    delete_s3_bucket(cloud_s3, bucket, force=True)
                except Exception as e:
                    click.secho(f"  Warning: bucket delete failed: {e}", fg="yellow")
            elif bucket:
                click.echo(f"  S3 bucket '{bucket}' kept (use --force to delete).")

    # Step 4: Clear state file
    from polaris_local_forge.l2c.common import get_state_path
    state_path = get_state_path(work_dir)
    if state_path.exists():
        state_path.write_text("{}\n")
        click.echo("\nState file cleared.")

    click.echo("\nCleanup complete. All L2C resources removed.")


# ---------------------------------------------------------------------------
# migrate -- full pipeline orchestrator
# ---------------------------------------------------------------------------

@click.command("migrate")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", help="AWS profile name")
@click.option("--region", "-r", envvar="L2C_AWS_REGION", default=None,
              help="AWS region")
@click.option("--sf-database", "-D", envvar="L2C_SF_DATABASE", default=None,
              help="Snowflake target database (default: from state)")
@click.option("--sf-schema", "-S", envvar="L2C_SF_SCHEMA", default="L2C",
              help="Snowflake target schema")
@click.option("--admin-role", help="Admin role for setup operations")
@click.option("--prefix", "-p", default=None,
              help="Override SNOWFLAKE_USER prefix for resource names")
@click.option("--no-prefix", is_flag=True,
              help="Drop user prefix from resource names")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def migrate(ctx, aws_profile, region, sf_database, sf_schema, admin_role,
            prefix, no_prefix, dry_run, yes):
    """Full migration -- setup + sync + register.

    Runs the complete L2C pipeline:
      1. setup aws     (S3 bucket + IAM role/policy)
      2. setup snowflake (ext vol + catalog integration + SA_ROLE + DB/Schema)
      3. sync          (copy data from RustFS to AWS S3 + rewrite metadata)
      4. register      (create Snowflake External Iceberg Tables)

    Each step is idempotent -- re-running migrate skips already-completed steps.
    """
    from polaris_local_forge.l2c.setup_aws import setup_aws
    from polaris_local_forge.l2c.setup_snowflake import setup_snowflake
    from polaris_local_forge.l2c.sync import sync
    from polaris_local_forge.l2c.register import register

    click.echo("--- L2C Full Migration ---\n")

    click.echo("Step 1/4: Setup AWS")
    ctx.invoke(setup_aws, aws_profile=aws_profile, region=region,
               prefix=prefix, no_prefix=no_prefix,
               dry_run=dry_run, yes=yes)

    click.echo("\nStep 2/4: Setup Snowflake")
    ctx.invoke(setup_snowflake, sf_database=sf_database, sf_schema=sf_schema,
               admin_role=admin_role, prefix=prefix, no_prefix=no_prefix,
               dry_run=dry_run, yes=yes)

    click.echo("\nStep 3/4: Sync")
    ctx.invoke(sync, aws_profile=aws_profile, region=region,
               prefix=prefix, no_prefix=no_prefix,
               force=False, skip_rewrite=False,
               dry_run=dry_run, yes=yes)

    click.echo("\nStep 4/4: Register")
    ctx.invoke(register, sf_database=sf_database, sf_schema=sf_schema,
               prefix=prefix, no_prefix=no_prefix,
               dry_run=dry_run, yes=yes)

    if dry_run:
        click.echo("\n[dry-run] Full migration preview complete.")
    else:
        click.echo("\nFull migration complete.")
