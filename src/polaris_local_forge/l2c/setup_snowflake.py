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

"""L2C setup snowflake -- Catalog Integration, External Volume, SA_ROLE, DB/Schema, GRANTs.

Uses admin_role for all account-level object creation. SA_ROLE receives only
least-privilege grants restricted to the target DB/Schema (no account-level
privileges). Follows the kamesh-demo-skills RBAC pattern.

Resource names are auto-derived from the project-scoped naming convention:
  <SNOWFLAKE_USER>_<PROJECT>_<CATALOG>  (all Snowflake objects share this base)

SQL lives in separate Jinja templates under sql/ for separation of concerns.
"""

import click
from snow_utils.extvolume import (
    ExternalVolumeConfig,
    create_external_volume,
    describe_external_volume,
    update_role_trust_policy,
    verify_external_volume,
)

from polaris_local_forge.l2c.common import (
    load_state,
    now_iso,
    resolve_resource_base,
    run_l2c_sql_file,
    save_state,
)
from polaris_local_forge.l2c.sessions import create_cloud_session, scrubbed_aws_env

MANIFEST_FILENAME = "snow-utils-manifest.md"


# ---------------------------------------------------------------------------
# admin_role resolution (follows kamesh-demo-skills pattern)
# ---------------------------------------------------------------------------

def _resolve_admin_role(admin_role: str | None, work_dir) -> str:
    """Resolve admin_role: CLI flag > manifest > interactive prompt.

    admin_role is stored only in the manifest, never in .env.
    """
    if admin_role:
        return admin_role

    manifest_path = work_dir / ".snow-utils" / MANIFEST_FILENAME
    if manifest_path.exists():
        for line in manifest_path.read_text().splitlines():
            if line.startswith("**Admin Role:**"):
                existing = line.split(":**", 1)[1].strip()
                if existing:
                    if click.confirm(f"Reuse admin role '{existing}' from manifest?", default=True):
                        return existing

    return click.prompt("Admin role for elevated operations", default="ACCOUNTADMIN")


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def _manifest_section(admin_role: str, sf_base: str,
                      database: str, schema: str,
                      aws_role_name: str) -> str:
    ts = now_iso()
    return f"""<!-- START -- l2c:snowflake -->
## L2C Snowflake Resources

**Created:** {ts}
**Admin Role:** {admin_role}
**SA_ROLE:** {sf_base}
**Catalog Integration:** {sf_base}
**External Volume:** {sf_base}
**Database:** {database}
**Schema:** {schema}
**Status:** COMPLETE

| # | Type | Name | Status |
|---|------|------|--------|
| 1 | Catalog Integration | {sf_base} | DONE |
| 2 | External Volume | {sf_base} | DONE |
| 3 | Trust Policy Update | {aws_role_name} | DONE |
| 4 | Role | {sf_base} | DONE |
| 5 | Role Grant | {sf_base} → {aws_role_name} | DONE |
| 6 | Database | {database} | DONE |
| 7 | Schema | {database}.{schema} | DONE |
| 8 | Grants | SA_ROLE privileges | DONE |
<!-- END -- l2c:snowflake -->
"""


def _update_manifest(work_dir, section_text: str) -> None:
    manifest_dir = work_dir / ".snow-utils"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = manifest_dir / MANIFEST_FILENAME

    if manifest_path.exists():
        content = manifest_path.read_text()
        start = "<!-- START -- l2c:snowflake -->"
        end = "<!-- END -- l2c:snowflake -->"
        if start in content:
            before = content[:content.index(start)]
            after = content[content.index(end) + len(end):]
            content = before + section_text.strip() + after
        else:
            content = content.rstrip() + "\n\n" + section_text
    else:
        content = section_text

    manifest_path.write_text(content)
    manifest_path.chmod(0o600)


# ---------------------------------------------------------------------------
# Click command
# ---------------------------------------------------------------------------

@click.command("setup_snowflake")
@click.option("--sf-database", "-D", envvar="L2C_SF_DATABASE", default=None,
              help="Target Snowflake database (default: auto-derived from naming)")
@click.option("--sf-schema", "-S", envvar="L2C_SF_SCHEMA", default="L2C",
              help="Target Snowflake schema")
@click.option("--admin-role", default=None,
              help="Snowflake admin role (default: from manifest or ACCOUNTADMIN)")
@click.option("--prefix", "-p", default=None,
              help="Override SNOWFLAKE_USER prefix for resource names")
@click.option("--no-prefix", is_flag=True,
              help="Drop user prefix (resources named <project>_<catalog> only)")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", default=None,
              help="AWS profile (for trust policy update)")
@click.option("--region", "-r", envvar="L2C_AWS_REGION", default=None,
              help="AWS region")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def setup_snowflake(ctx, sf_database, sf_schema, admin_role, prefix, no_prefix,
                    aws_profile, region, dry_run, yes):
    """Create Catalog Integration, External Volume, SA_ROLE, target DB/Schema, GRANTs."""
    work_dir = ctx.obj["WORK_DIR"]
    state = load_state(work_dir)

    aws_state = state.get("aws")
    if not aws_state:
        raise click.ClickException(
            "AWS setup not found. Run 'plf l2c setup aws' first."
        )

    bucket = aws_state["bucket"]
    aws_role_name = aws_state["role_name"]
    role_arn = aws_state["role_arn"]
    external_id = aws_state["external_id"]
    aws_region = region or aws_state.get("region", "us-east-1")
    aws_profile = aws_profile or aws_state.get("profile")

    rb = resolve_resource_base(work_dir, prefix_override=prefix, no_prefix=no_prefix)
    sf_base = rb["sf_base"]

    stored_sf_base = aws_state.get("sf_base")
    if stored_sf_base and stored_sf_base != sf_base:
        click.secho(
            f"Warning: AWS resources were created with naming '{stored_sf_base}' "
            f"but current convention resolves to '{sf_base}'.\n"
            f"  AWS bucket/role: {bucket} / {aws_role_name}\n"
            f"Consider re-running 'plf l2c setup aws' to align names, "
            f"or use --prefix to match the original.",
            fg="yellow",
        )

    volume_name = sf_base
    database = sf_database or sf_base
    resolved_admin_role = _resolve_admin_role(admin_role, work_dir)

    catalog_vars = {
        "admin_role": resolved_admin_role,
        "catalog_integration": sf_base,
    }
    role_vars = {
        "admin_role": resolved_admin_role,
        "sa_role": sf_base,
        "snowflake_user": rb["prefix"].upper(),
        "database": database,
        "schema": sf_schema,
        "volume_name": volume_name,
        "catalog_integration": sf_base,
    }

    ext_vol_config = ExternalVolumeConfig(
        bucket_name=bucket,
        role_name=aws_role_name,
        policy_name=aws_state.get("policy_arn", "").split("/")[-1],
        volume_name=volume_name,
        storage_location_name=f"{volume_name}_S3",
        external_id=external_id,
        aws_region=aws_region,
        allow_writes=False,
    )

    click.echo()
    click.echo(f"Project: {rb['project']} | Catalog: {rb['catalog']}")
    click.echo(f"\n--- L2C Snowflake Setup Plan ---")
    click.echo(f"  Admin Role:          {resolved_admin_role}")
    click.echo(f"  SA_ROLE:             {sf_base}")
    click.echo(f"  Catalog Integration: {sf_base}")
    click.echo(f"  External Volume:     {volume_name}")
    click.echo(f"  Database:            {database}")
    click.echo(f"  Schema:              {sf_schema}")
    click.echo(f"  S3 Bucket:           {bucket}")
    click.echo(f"  IAM Role:            {aws_role_name}")
    click.echo(f"  Region:              {aws_region}")
    click.echo()

    if dry_run:
        run_l2c_sql_file("setup_catalog_integration.sql", catalog_vars, dry_run=True)
        click.echo()
        from snow_utils.extvolume import get_external_volume_sql
        click.echo("--- External Volume SQL ---")
        click.echo(get_external_volume_sql(ext_vol_config, role_arn))
        click.echo()
        run_l2c_sql_file("setup_role.sql", role_vars, dry_run=True)
        click.echo()
        click.echo("[dry-run] No changes made.")
        return

    if not yes:
        click.confirm("Create these Snowflake resources?", abort=True)

    click.echo("\nStep 1: Creating Catalog Integration...")
    run_l2c_sql_file("setup_catalog_integration.sql", catalog_vars)

    click.echo("\nStep 2: Creating External Volume...")
    create_external_volume(ext_vol_config, role_arn)

    click.echo("\nStep 3: Updating AWS trust policy with Snowflake IAM user ARN...")
    vol_info = describe_external_volume(volume_name)
    snowflake_iam_arn = vol_info.get("iam_user_arn", "")
    sf_external_id = vol_info.get("external_id", external_id)

    if not snowflake_iam_arn:
        raise click.ClickException(
            f"Could not retrieve Snowflake IAM user ARN from external volume '{volume_name}'. "
            "Check that the volume was created successfully."
        )

    with scrubbed_aws_env():
        _, cloud_iam, _ = create_cloud_session(aws_profile, aws_region)
        update_role_trust_policy(cloud_iam, aws_role_name, snowflake_iam_arn, sf_external_id)

    click.echo("\nStep 4-7: Creating SA_ROLE, DB/Schema, GRANTs...")
    run_l2c_sql_file("setup_role.sql", role_vars)

    click.echo("\nVerifying external volume...")
    verify_external_volume(volume_name)

    state["snowflake"] = {
        "catalog_integration": sf_base,
        "external_volume": volume_name,
        "sa_role": sf_base,
        "admin_role": resolved_admin_role,
        "database": database,
        "schema": sf_schema,
        "snowflake_iam_arn": snowflake_iam_arn,
        "sf_base": sf_base,
        "prefix": rb["prefix"],
        "updated_at": now_iso(),
    }
    save_state(work_dir, state)

    manifest_text = _manifest_section(
        resolved_admin_role, sf_base,
        database, sf_schema, aws_role_name,
    )
    _update_manifest(work_dir, manifest_text)

    click.echo("\nSnowflake setup complete. State and manifest updated.")
