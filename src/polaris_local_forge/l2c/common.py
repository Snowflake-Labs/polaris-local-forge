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

"""Shared helpers for L2C commands.

Includes: preflight AWS check, state file read/write, manifest read/write,
env loading, Snowflake SQL wrappers.
"""

import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import click

from polaris_local_forge.l2c.sessions import _resolve_profile

STATE_FILENAME = "l2c-state.json"
SNOW_UTILS_DIR = ".snow-utils"


def get_state_path(work_dir: Path) -> Path:
    return work_dir / SNOW_UTILS_DIR / STATE_FILENAME


def load_state(work_dir: Path) -> dict:
    """Load L2C state from .snow-utils/l2c-state.json."""
    path = get_state_path(work_dir)
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save_state(work_dir: Path, state: dict) -> None:
    """Save L2C state to .snow-utils/l2c-state.json (chmod 600)."""
    state_dir = work_dir / SNOW_UTILS_DIR
    state_dir.mkdir(parents=True, exist_ok=True)
    path = get_state_path(work_dir)
    path.write_text(json.dumps(state, indent=2) + "\n")
    path.chmod(0o600)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def preflight_aws_check(aws_profile: str | None = None) -> str:
    """Verify AWS credentials are valid before starting.

    Uses `aws sts get-caller-identity` via subprocess (reads ~/.aws/ directly,
    unaffected by RustFS env vars). Returns the AWS account ID on success.

    On failure (expired SSO, missing creds), raises ClickException with
    a clear message telling the user how to fix it.
    """
    profile = _resolve_profile(aws_profile)
    cmd = ["aws", "sts", "get-caller-identity", "--profile", profile, "--output", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        stderr = result.stderr.strip()
        msg = f"AWS credentials not valid for profile '{profile}'.\n"
        if "ExpiredToken" in stderr or "SSO" in stderr:
            msg += f"Run: aws sso login --profile {profile}"
        elif "NoCredentialProviders" in stderr or "could not be found" in stderr:
            msg += f"Run: aws configure --profile {profile}"
        else:
            msg += f"Error: {stderr}\nRun: aws sts get-caller-identity --profile {profile}"
        raise click.ClickException(msg)

    identity = json.loads(result.stdout)
    return identity["Account"]


def read_principal(work_dir: Path) -> tuple[str, str, str]:
    """Read principal credentials from work/principal.txt.

    Returns:
        (realm, client_id, client_secret) tuple
    """
    principal_file = work_dir / "work" / "principal.txt"
    if not principal_file.exists():
        raise click.ClickException(
            f"Principal file not found: {principal_file}\n"
            "Run 'plf catalog setup' first."
        )
    try:
        realm, client_id, client_secret = principal_file.read_text().strip().split(",")
        return realm, client_id, client_secret
    except ValueError:
        raise click.ClickException(f"Invalid principal file format: {principal_file}")


def get_local_polaris_url(cfg: dict) -> str:
    """Get the local Polaris API URL from config."""
    return cfg.get("POLARIS_URL") or os.getenv("POLARIS_URL", "http://localhost:18181")


def get_local_catalog_name(cfg: dict) -> str:
    """Get the local catalog name from config."""
    return cfg.get("PLF_POLARIS_CATALOG_NAME") or os.getenv("PLF_POLARIS_CATALOG_NAME", "polardb")


def run_snow_sql(query: str, *, role: str | None = None, check: bool = True):
    """Execute a snow sql command and return parsed JSON output."""
    cmd = ["snow", "sql", "--query", query, "--format", "json"]
    if role:
        cmd.extend(["--role", role])
    result = subprocess.run(cmd, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise click.ClickException(f"snow sql failed: {result.stderr}")
    if result.stdout.strip():
        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return None
    return None


def run_snow_sql_stdin(sql: str, *, check: bool = True):
    """Execute multi-statement SQL via stdin."""
    cmd = ["snow", "sql", "--stdin"]
    result = subprocess.run(cmd, input=sql, capture_output=True, text=True)
    if check and result.returncode != 0:
        raise click.ClickException(f"snow sql failed: {result.stderr}")
    return result
