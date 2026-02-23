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

"""Catalog management commands for Polaris Local Forge.

This module provides catalog operations: setup, cleanup, verify-sql, explore-sql, query.
"""

import os
import shutil
import subprocess
import sys

import click
from dotenv import load_dotenv

from polaris_local_forge.common import run_ansible


@click.group()
def catalog():
    """Catalog management."""
    pass


@catalog.command("setup")
@click.option("--tags", "-t", help="Ansible tags (comma-separated)")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def catalog_setup(ctx, tags: str | None, dry_run: bool, verbose: bool):
    """Configure Polaris catalog via Ansible."""
    work_dir = ctx.obj["WORK_DIR"]
    exit_code = run_ansible("catalog_setup.yml", work_dir, tags=tags, dry_run=dry_run, verbose=verbose)
    sys.exit(exit_code)


@catalog.command("cleanup")
@click.option("--tags", "-t", help="Ansible tags (comma-separated)")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def catalog_cleanup(ctx, tags: str | None, dry_run: bool, verbose: bool, yes: bool):
    """Clean up Polaris catalog via Ansible.
    
    Idempotent: skips if prerequisites don't exist (nothing to clean up).
    """
    work_dir = ctx.obj["WORK_DIR"]
    
    # Check prerequisites - if they don't exist, nothing to clean up (idempotent)
    aws_config = work_dir / ".aws" / "config"
    if not aws_config.exists():
        click.echo("Catalog cleanup: skipping (no .aws/config - nothing to clean up)")
        return
    
    if not yes and not dry_run:
        if not click.confirm("Clean up catalog?"):
            click.echo("Aborted.")
            return
    exit_code = run_ansible("catalog_cleanup.yml", work_dir, tags=tags, dry_run=dry_run, verbose=verbose)
    sys.exit(exit_code)


@catalog.command("verify-sql")
@click.pass_context
def catalog_verify_sql(ctx):
    """Run DuckDB verification using generated SQL script."""
    work_dir = ctx.obj["WORK_DIR"]
    sql_file = work_dir / "scripts" / "explore_catalog.sql"

    if not sql_file.exists():
        click.echo(f"SQL script not found: {sql_file}", err=True)
        click.echo("Run 'catalog setup' first to generate it.", err=True)
        sys.exit(1)

    if not shutil.which("duckdb"):
        click.echo("DuckDB CLI not found. Install with: brew install duckdb", err=True)
        sys.exit(1)

    click.echo(f"Running DuckDB verification from {sql_file}...")

    result = subprocess.run(
        ["duckdb", "-bail", "-init", str(sql_file), "-c", ".exit"],
        capture_output=False
    )

    if result.returncode == 0:
        click.echo("Verification completed successfully.")
    else:
        click.echo("Verification failed.", err=True)
    sys.exit(result.returncode)


@catalog.command("explore-sql")
@click.pass_context
def catalog_explore_sql(ctx):
    """Open interactive DuckDB session with catalog pre-loaded."""
    work_dir = ctx.obj["WORK_DIR"]
    sql_file = work_dir / "scripts" / "explore_catalog.sql"

    if not sql_file.exists():
        click.echo(f"SQL script not found: {sql_file}", err=True)
        click.echo("Run 'catalog setup' first to generate it.", err=True)
        sys.exit(1)

    click.echo(f"Opening interactive DuckDB with {sql_file}...")
    click.echo("Type '.exit' or Ctrl+D to quit.")

    result = subprocess.run(["duckdb", "-init", str(sql_file)])
    sys.exit(result.returncode)


@catalog.command("query")
@click.option("--sql", "-s", required=True, help="SQL query to execute (use polaris_catalog.schema.table)")
@click.pass_context
def catalog_query(ctx, sql: str):
    """Execute read-only SQL query against catalog.
    
    Thin wrapper that handles connection setup automatically.
    Table references should use: polaris_catalog.<schema>.<table>
    
    Examples:
    
        plf catalog query --sql "SELECT COUNT(*) FROM polaris_catalog.wildlife.penguins"
        
        plf catalog query --sql "SHOW ALL TABLES"
    """
    work_dir = ctx.obj["WORK_DIR"]
    
    # Load .env for config
    env_file = work_dir / ".env"
    if env_file.exists():
        load_dotenv(env_file, override=True)
    
    # Read credentials from principal.txt
    principal_file = work_dir / "work" / "principal.txt"
    if not principal_file.exists():
        click.echo(f"Principal file not found: {principal_file}", err=True)
        click.echo("Run 'catalog setup' first.", err=True)
        sys.exit(1)
    
    try:
        realm, client_id, client_secret = principal_file.read_text().strip().split(",")
    except ValueError:
        click.echo(f"Invalid principal file format: {principal_file}", err=True)
        sys.exit(1)
    
    if not shutil.which("duckdb"):
        click.echo("DuckDB CLI not found. Install with: brew install duckdb", err=True)
        sys.exit(1)
    
    # Get config from environment
    polaris_url = os.getenv("POLARIS_URL", "http://localhost:18181")
    catalog_name = os.getenv("PLF_POLARIS_CATALOG_NAME", "polardb")
    
    # Build connection setup SQL
    setup_sql = f"""
INSTALL iceberg;
LOAD iceberg;
CREATE OR REPLACE SECRET polaris_secret (
    TYPE iceberg,
    CLIENT_ID '{client_id}',
    CLIENT_SECRET '{client_secret}',
    OAUTH2_SERVER_URI '{polaris_url}/api/catalog/v1/oauth/tokens'
);
ATTACH '{catalog_name}' AS polaris_catalog (
    TYPE iceberg,
    SECRET polaris_secret,
    ENDPOINT '{polaris_url}/api/catalog'
);
"""
    # Combine setup + user query
    full_sql = setup_sql + sql + ";"
    
    result = subprocess.run(
        ["duckdb", "-c", full_sql],
        capture_output=False
    )
    sys.exit(result.returncode)
