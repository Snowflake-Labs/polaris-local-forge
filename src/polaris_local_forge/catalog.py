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

This module provides catalog operations: setup, cleanup, verify-sql, explore-sql.
"""

import shutil
import subprocess
import sys

import click

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
    """Clean up Polaris catalog via Ansible."""
    work_dir = ctx.obj["WORK_DIR"]
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
