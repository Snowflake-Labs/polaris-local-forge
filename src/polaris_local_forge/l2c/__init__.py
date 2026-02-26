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

"""L2C (Local to Cloud) migration commands for Polaris Local Forge.

Migrates local Polaris PoC experiments to AWS S3 and registers them
as Snowflake External Iceberg Tables.
"""

import click

from polaris_local_forge.l2c.inventory import inventory
from polaris_local_forge.l2c.setup_aws import setup_aws
from polaris_local_forge.l2c.setup_snowflake import setup_snowflake
from polaris_local_forge.l2c.sync import sync
from polaris_local_forge.l2c.register import register
from polaris_local_forge.l2c.orchestrators import migrate, status, clear, cleanup


@click.group()
@click.pass_context
def l2c(ctx):
    """Migrate local Polaris to AWS S3 + Snowflake."""
    pass


@l2c.group(invoke_without_command=True)
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def setup(ctx, dry_run, yes):
    """Provision AWS and Snowflake infrastructure.

    When invoked without a subcommand, runs 'setup aws' then 'setup snowflake'
    in sequence (orchestrator mode).
    """
    if ctx.invoked_subcommand is not None:
        return

    click.echo("Running setup orchestrator: aws → snowflake\n")
    ctx.invoke(setup_aws, dry_run=dry_run, yes=yes)
    ctx.invoke(setup_snowflake, dry_run=dry_run, yes=yes)


setup.add_command(setup_aws, "aws")
setup.add_command(setup_snowflake, "snowflake")

l2c.add_command(inventory)
l2c.add_command(sync)
l2c.add_command(register)
l2c.add_command(migrate)
l2c.add_command(status)
l2c.add_command(clear)
l2c.add_command(cleanup)
