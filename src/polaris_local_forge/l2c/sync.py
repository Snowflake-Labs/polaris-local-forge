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

"""L2C sync command -- copy Iceberg data from RustFS to AWS S3.

Smart sync (default): compares key+size between source (RustFS) and destination
(AWS S3), only transfers new or size-changed objects. Iceberg tables are
append-heavy so subsequent runs transfer only new data/metadata files.

--force: re-upload all objects regardless of what exists at the destination.

Per-object retry with exponential backoff (3 attempts, 1s/2s/4s).
Per-table failure isolation: a failed table is marked 'failed' but sync
continues for remaining tables.
"""

import time

import click
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import dotenv_values

from polaris_local_forge.l2c.common import (
    get_local_catalog_name,
    get_local_polaris_url,
    load_state,
    now_iso,
    preflight_aws_check,
    read_principal,
    resolve_aws_region,
    resolve_resource_base,
    save_state,
)
from polaris_local_forge.l2c.inventory import PolarisRestClient, _discover_tables
from polaris_local_forge.l2c.rewrite import rewrite_table_paths
from polaris_local_forge.l2c.sessions import (
    create_cloud_session,
    create_rustfs_session,
    scrubbed_aws_env,
)

_MAX_RETRIES = 3
_BASE_DELAY = 1.0


# ---------------------------------------------------------------------------
# S3 helpers
# ---------------------------------------------------------------------------

def _list_objects(s3_client, bucket: str, prefix: str) -> dict[str, int]:
    """List all objects under prefix, returning {key: size}."""
    objects: dict[str, int] = {}
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects[obj["Key"]] = obj["Size"]
    return objects


def _transfer_object(
    rustfs_s3, cloud_s3, bucket_src: str, bucket_dst: str, key: str,
) -> int:
    """Download from RustFS and upload to AWS S3, with exponential backoff retry.

    Returns the number of bytes transferred.
    """
    for attempt in range(_MAX_RETRIES):
        try:
            obj = rustfs_s3.get_object(Bucket=bucket_src, Key=key)
            body = obj["Body"]
            size = obj.get("ContentLength", 0)
            cloud_s3.upload_fileobj(body, bucket_dst, key)
            return size
        except (ClientError, EndpointConnectionError, ConnectionError) as e:
            if attempt == _MAX_RETRIES - 1:
                raise
            delay = _BASE_DELAY * (2 ** attempt)
            click.echo(f"  Retry {attempt + 1}/{_MAX_RETRIES} for {key} in {delay:.0f}s: {e}")
            time.sleep(delay)
    return 0


def _compute_transfer_plan(
    src_objects: dict[str, int],
    dst_objects: dict[str, int],
    force: bool,
) -> list[str]:
    """Return list of keys that need to be transferred.

    Smart sync: transfer keys that are new or have a different size.
    Force: transfer all source keys.
    """
    if force:
        return sorted(src_objects.keys())
    return sorted(
        k for k, size in src_objects.items()
        if k not in dst_objects or dst_objects[k] != size
    )


def _table_state_key(namespace: str, table: str) -> str:
    """State dict key for a table: NAMESPACE_TABLE (uppercase, hyphens to underscores)."""
    return f"{namespace}_{table}".upper().replace("-", "_")


# ---------------------------------------------------------------------------
# Per-table sync
# ---------------------------------------------------------------------------

def _sync_table(
    rustfs_s3,
    cloud_s3,
    src_bucket: str,
    dst_bucket: str,
    namespace: str,
    table: str,
    force: bool,
    dry_run: bool,
) -> dict:
    """Sync a single table's objects from RustFS to AWS S3.

    Returns a dict suitable for updating state['tables'][key]['sync'].
    """
    prefix = f"{namespace}/{table}/"
    click.echo(f"\n  {namespace}.{table} (prefix: {prefix})")

    src_objects = _list_objects(rustfs_s3, src_bucket, prefix)
    if not src_objects:
        click.echo(f"    No source objects found under {prefix}")
        return {"status": "synced", "last_sync": now_iso(), "object_count": 0, "total_bytes": 0}

    if dry_run or force or cloud_s3 is None:
        dst_objects: dict[str, int] = {}
    else:
        dst_objects = _list_objects(cloud_s3, dst_bucket, prefix)
    to_transfer = _compute_transfer_plan(src_objects, dst_objects, force=(force or dry_run))

    total_src_bytes = sum(src_objects[k] for k in to_transfer) if to_transfer else 0
    skipped = len(src_objects) - len(to_transfer)

    click.echo(f"    Source: {len(src_objects)} objects ({_fmt_bytes(sum(src_objects.values()))})")
    if not force and skipped > 0:
        click.echo(f"    Already synced: {skipped} objects (skipped)")
    click.echo(f"    To transfer: {len(to_transfer)} objects ({_fmt_bytes(total_src_bytes)})")

    if dry_run:
        if to_transfer:
            for key in to_transfer[:10]:
                click.echo(f"      {key} ({_fmt_bytes(src_objects[key])})")
            if len(to_transfer) > 10:
                click.echo(f"      ... and {len(to_transfer) - 10} more")
        return {"status": "pending", "object_count": len(to_transfer), "total_bytes": total_src_bytes}

    if not to_transfer:
        click.echo("    Up to date.")
        return {"status": "synced", "last_sync": now_iso(), "object_count": 0, "total_bytes": 0}

    transferred = 0
    total_bytes = 0
    for key in to_transfer:
        try:
            nbytes = _transfer_object(rustfs_s3, cloud_s3, src_bucket, dst_bucket, key)
            transferred += 1
            total_bytes += nbytes
            if transferred % 50 == 0 or transferred == len(to_transfer):
                click.echo(f"    Progress: {transferred}/{len(to_transfer)} objects")
        except Exception as e:
            click.secho(f"    FAILED on {key} after {_MAX_RETRIES} retries: {e}", fg="red")
            return {
                "status": "failed",
                "last_sync": now_iso(),
                "object_count": transferred,
                "total_bytes": total_bytes,
                "error": f"{key}: {e}",
            }

    click.echo(f"    Done: {transferred} objects, {_fmt_bytes(total_bytes)}")
    return {
        "status": "synced",
        "last_sync": now_iso(),
        "object_count": transferred,
        "total_bytes": total_bytes,
    }


def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


# ---------------------------------------------------------------------------
# Click command
# ---------------------------------------------------------------------------

@click.command("sync")
@click.option("--aws-profile", envvar="L2C_AWS_PROFILE", default=None,
              help="AWS profile")
@click.option("--region", "-r", envvar="L2C_AWS_REGION", default=None,
              help="AWS region")
@click.option("--prefix", "-p", default=None,
              help="Override SNOWFLAKE_USER prefix for resource names")
@click.option("--no-prefix", is_flag=True,
              help="Drop user prefix from resource names")
@click.option("--force", "-f", is_flag=True,
              help="Re-upload all objects (skip smart sync comparison)")
@click.option("--skip-rewrite", is_flag=True,
              help="Skip Iceberg metadata path rewriting after sync")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def sync(ctx, aws_profile, region, prefix, no_prefix, force, skip_rewrite, dry_run, yes):
    """Copy Iceberg data from local RustFS to AWS S3.

    Default: smart sync (compare key+size, transfer only new/changed objects).
    Use --force to re-upload everything.

    Processes tables with status: pending, in_progress, or failed.
    Synced tables are skipped unless --force is used.
    """
    work_dir = ctx.obj["WORK_DIR"]
    state = load_state(work_dir)

    aws_state = state.get("aws")
    if not aws_state:
        raise click.ClickException(
            "AWS setup not found. Run 'plf l2c setup aws' first."
        )

    dst_bucket = aws_state["bucket"]
    region = resolve_aws_region(region, aws_profile)
    aws_profile = aws_profile or aws_state.get("profile")

    env_file = work_dir / ".env"
    cfg = dotenv_values(env_file) if env_file.exists() else {}
    src_bucket = get_local_catalog_name(cfg)

    rb = resolve_resource_base(work_dir, prefix_override=prefix, no_prefix=no_prefix)

    realm, client_id, client_secret = read_principal(work_dir)
    polaris_url = get_local_polaris_url(cfg)

    click.echo("Discovering tables from local Polaris...")
    try:
        client = PolarisRestClient(
            polaris_url, src_bucket, realm, client_id, client_secret,
        )
        tables = _discover_tables(client)
    except Exception as e:
        raise click.ClickException(f"Cannot discover tables: {e}")

    if not tables:
        click.echo("No tables found in local Polaris catalog.")
        return

    errored = [t for t in tables if "error" in t]
    if errored:
        click.secho(
            f"Warning: {len(errored)} table(s) had metadata errors "
            f"(sync proceeds using namespace/table only):",
            fg="yellow",
        )
        for t in errored:
            click.secho(f"  {t.get('fqn', t['namespace'] + '.' + t['table'])}: {t['error']}", fg="yellow")

    tables_state = state.setdefault("tables", {})

    actionable = []
    skipped_synced = 0
    for t in tables:
        if "namespace" not in t or "table" not in t:
            continue
        key = _table_state_key(t["namespace"], t["table"])
        tbl_state = tables_state.get(key, {})
        sync_status = tbl_state.get("sync", {}).get("status", "pending")
        if force or sync_status in ("pending", "in_progress", "failed"):
            actionable.append(t)
        else:
            skipped_synced += 1

    if not actionable:
        if skipped_synced > 0:
            msg = f"All {skipped_synced} table(s) already synced."
            if not force:
                msg += " Use --force to re-sync."
            click.echo(msg)
        else:
            click.echo("No tables found eligible for sync.")
        return

    click.echo(f"\nProject: {rb['project']} | Catalog: {rb['catalog']}")
    click.echo(f"\n--- L2C Sync Plan ---")
    click.echo(f"  Source:      RustFS ({polaris_url}) bucket '{src_bucket}'")
    click.echo(f"  Destination: AWS S3 bucket '{dst_bucket}'")
    click.echo(f"  Region:      {region}")
    click.echo(f"  Mode:        {'force (re-upload all)' if force else 'smart sync (key+size)'}")
    click.echo(f"  Rewrite:     {'disabled (--skip-rewrite)' if skip_rewrite else 'enabled (fix Iceberg metadata paths)'}")
    click.echo(f"  Tables:      {len(actionable)}/{len(tables)}")
    click.echo()

    if not dry_run and not yes:
        click.confirm(f"Sync {len(actionable)} table(s) to S3?", abort=True)

    if not dry_run:
        with scrubbed_aws_env():
            preflight_aws_check(aws_profile)

    rustfs_s3 = create_rustfs_session(cfg)

    synced_count = 0
    failed_count = 0

    for t in actionable:
        ns = t["namespace"]
        tbl = t["table"]
        key = _table_state_key(ns, tbl)

        tables_state.setdefault(key, {
            "namespace": ns,
            "table": tbl,
            "sync": {"status": "pending"},
            "register": {"status": "pending"},
        })

        if not dry_run:
            tables_state[key]["sync"] = {"status": "in_progress", "last_sync": now_iso()}
            save_state(work_dir, state)

        if dry_run:
            result = _sync_table(
                rustfs_s3, None, src_bucket, dst_bucket, ns, tbl,
                force=force, dry_run=True,
            )
            tables_state[key]["sync"] = {
                **tables_state[key].get("sync", {}),
                "object_count": result.get("object_count", 0),
                "total_bytes": result.get("total_bytes", 0),
            }
        else:
            with scrubbed_aws_env():
                cloud_s3, _, _ = create_cloud_session(aws_profile, region)
                result = _sync_table(
                    rustfs_s3, cloud_s3, src_bucket, dst_bucket, ns, tbl,
                    force=force, dry_run=False,
                )

                if result["status"] == "synced" and not skip_rewrite:
                    source_prefix = f"s3://{src_bucket}/"
                    target_prefix = f"s3://{dst_bucket}/"
                    try:
                        n = rewrite_table_paths(
                            cloud_s3, dst_bucket, ns, tbl,
                            source_prefix, target_prefix,
                        )
                        result["rewrite_count"] = n
                    except Exception as e:
                        click.secho(
                            f"    Metadata rewrite failed: {e}\n"
                            "    Table synced but metadata paths not updated.\n"
                            "    Re-run sync or use 'plf l2c sync --skip-rewrite'.",
                            fg="yellow",
                        )
                        result["rewrite_error"] = str(e)

            tables_state[key]["sync"] = result
            save_state(work_dir, state)

            if result["status"] == "synced":
                synced_count += 1
            else:
                failed_count += 1

    if dry_run:
        click.echo("\n[dry-run] No changes made.")
    else:
        click.echo(f"\nSync complete: {synced_count} synced, {failed_count} failed.")
        if failed_count > 0:
            click.secho(
                "Re-run 'plf l2c sync' to retry failed tables (smart sync resumes automatically).",
                fg="yellow",
            )
