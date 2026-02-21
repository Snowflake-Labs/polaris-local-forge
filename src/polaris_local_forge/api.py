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

"""API query commands for Polaris Local Forge.

This module provides REST API query operations against Apache Polaris.
Currently supports read-only (GET) operations only.
"""

import json
import os
import re
import subprocess
import sys

import click

from polaris_local_forge.common import ANSIBLE_DIR


@click.group()
def api():
    """Query Apache Polaris REST API (read-only)."""
    pass


@api.command("query")
@click.argument("endpoint")
@click.option("--output", "-o", type=click.Choice(["json", "text"]), default="json",
              help="Output format (default: json)")
@click.option("--verbose", "-v", is_flag=True, help="Show Ansible output")
@click.pass_context
def api_query(ctx, endpoint: str, output: str, verbose: bool):
    """Query any Polaris API endpoint (GET only).
    
    ENDPOINT is the API path, e.g.:
    
    \b
      /api/management/v1/catalogs
      /api/catalog/v1/polardb/namespaces
      /api/catalog/v1/polardb/namespaces/default/tables
    
    Examples:
    
    \b
      plf api query /api/management/v1/catalogs
      plf api query /api/catalog/v1/polardb/namespaces
      plf api query /api/management/v1/principals
    """
    work_dir = ctx.obj["WORK_DIR"]
    
    # Validate endpoint starts with /
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    
    # Check credentials exist
    principal_file = work_dir / "work" / "principal.txt"
    if not principal_file.exists():
        click.echo("Error: work/principal.txt not found.", err=True)
        click.echo("Run 'plf catalog setup' first to generate credentials.", err=True)
        sys.exit(1)
    
    playbook_path = ANSIBLE_DIR / "api_query.yml"
    if not playbook_path.exists():
        click.echo(f"Error: Playbook not found: {playbook_path}", err=True)
        sys.exit(1)
    
    # Build command
    cmd = [
        "uv", "run", "ansible-playbook",
        str(playbook_path),
        "-e", f"plf_output_base={work_dir}",
        "-e", f"endpoint={endpoint}",
        "-e", f"ansible_python_interpreter={sys.executable}",
    ]
    
    # Set up environment
    env = os.environ.copy()
    env["ANSIBLE_CONFIG"] = str(ANSIBLE_DIR / "ansible.cfg")
    # Suppress VIRTUAL_ENV warning
    env.pop("VIRTUAL_ENV", None)
    # Suppress stdout buffering for cleaner output
    env["PYTHONUNBUFFERED"] = "1"
    
    if verbose:
        cmd.append("-v")
        result = subprocess.run(cmd, cwd=work_dir, env=env)
    else:
        # Capture output and extract just the JSON result
        result = subprocess.run(cmd, cwd=work_dir, env=env, 
                                capture_output=True, text=True)
        
        if result.returncode != 0:
            # Show error output
            click.echo("Error querying API:", err=True)
            if result.stderr:
                click.echo(result.stderr, err=True)
            sys.exit(result.returncode)
        
        # Parse Ansible output to extract the JSON result
        stdout = result.stdout
        
        # Look for the debug output line containing the result
        # Ansible debug outputs: "result.json": { ... }
        
        # Try to find JSON in the output
        json_match = re.search(r'"result\.json":\s*(\{.*?\}|\[.*?\])', stdout, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                if output == "json":
                    click.echo(json.dumps(data, indent=2))
                else:
                    # Text format - simple key: value
                    _print_text(data)
            except json.JSONDecodeError:
                # Fallback: show raw output
                click.echo(stdout)
        else:
            # No JSON found, might be 204 No Content
            if "Request successful (no content returned)" in stdout:
                click.echo("Success (no content)")
            else:
                # Show raw output
                click.echo(stdout)
    
    sys.exit(result.returncode)


def _print_text(data, indent=0):
    """Print data in simple text format."""
    prefix = "  " * indent
    if isinstance(data, dict):
        for k, v in data.items():
            if isinstance(v, (dict, list)):
                click.echo(f"{prefix}{k}:")
                _print_text(v, indent + 1)
            else:
                click.echo(f"{prefix}{k}: {v}")
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                _print_text(item, indent)
                click.echo()  # Blank line between items
            else:
                click.echo(f"{prefix}- {item}")
    else:
        click.echo(f"{prefix}{data}")
