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


def _clean_aws_env() -> dict:
    """Build a copy of os.environ with RustFS AWS_* vars stripped.

    Prevents RustFS credentials (AWS_ACCESS_KEY_ID=admin, AWS_ENDPOINT_URL=localhost)
    from contaminating subprocess calls to the real AWS CLI.
    """
    from polaris_local_forge.l2c.sessions import _AWS_ENV_VARS
    return {k: v for k, v in os.environ.items() if k not in _AWS_ENV_VARS}


def resolve_aws_region(region: str | None, aws_profile: str | None = None) -> str:
    """Resolve AWS region: explicit flag > L2C_AWS_REGION > AWS profile config > us-east-1."""
    if region:
        return region
    profile = _resolve_profile(aws_profile)
    try:
        result = subprocess.run(
            ["aws", "configure", "get", "region", "--profile", profile],
            capture_output=True, text=True, env=_clean_aws_env(),
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except FileNotFoundError:
        pass
    return "us-east-1"


def preflight_aws_check(aws_profile: str | None = None) -> str:
    """Verify AWS credentials are valid before starting.

    Runs `aws sts get-caller-identity` with RustFS env vars scrubbed so the
    subprocess reads only from ~/.aws/ (user's real AWS config).

    On failure (expired SSO, missing creds), raises ClickException with
    a clear message telling the user how to fix it.
    """
    profile = _resolve_profile(aws_profile)
    cmd = ["aws", "sts", "get-caller-identity", "--profile", profile, "--output", "json"]
    clean_env = _clean_aws_env()
    result = subprocess.run(cmd, capture_output=True, text=True, env=clean_env)

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


def ensure_snowflake_connection(work_dir: Path, connection_name: str | None = None) -> str:
    """Ensure SNOWFLAKE_USER is available, running connection discovery if needed.

    Supports both interactive (Power CLI) and non-interactive (Cortex Code) modes:
    - If SNOWFLAKE_USER is already in .env/env, returns it immediately.
    - Otherwise delegates to snow_utils_common.discover_snowflake_connection().
    - For Cortex Code: pass connection_name explicitly, or set
      SNOWFLAKE_DEFAULT_CONNECTION_NAME in .env so discovery is non-interactive.

    Returns:
        The Snowflake username (lowercase, for use as resource prefix).
    """
    from dotenv import dotenv_values
    from polaris_local_forge.common import set_env_var
    from snow_utils_common import discover_snowflake_connection

    env_file = work_dir / ".env"
    cfg = dotenv_values(env_file) if env_file.exists() else {}

    sf_user = cfg.get("SNOWFLAKE_USER") or os.environ.get("SNOWFLAKE_USER")
    if sf_user:
        click.echo(f"Using Snowflake user: {sf_user} (from .env)")
        return sf_user.lower()

    # For non-interactive mode, try SNOWFLAKE_DEFAULT_CONNECTION_NAME as fallback
    conn = connection_name or cfg.get("SNOWFLAKE_DEFAULT_CONNECTION_NAME") or os.environ.get("SNOWFLAKE_DEFAULT_CONNECTION_NAME")

    click.echo("SNOWFLAKE_USER not found in .env -- running connection discovery.\n")
    info = discover_snowflake_connection(connection_name=conn)

    set_env_var(env_file, "SNOWFLAKE_DEFAULT_CONNECTION_NAME", info["connection_name"])
    if info.get("account"):
        set_env_var(env_file, "SNOWFLAKE_ACCOUNT", info["account"])
    if info.get("user"):
        set_env_var(env_file, "SNOWFLAKE_USER", info["user"])
    if info.get("host"):
        set_env_var(env_file, "SNOWFLAKE_ACCOUNT_URL", f"https://{info['host']}")

    sf_user = info.get("user", "")
    click.echo(f"\nSaved connection '{info['connection_name']}' to .env")
    click.echo(f"  SNOWFLAKE_USER={sf_user}")
    click.echo(f"  SNOWFLAKE_ACCOUNT={info.get('account', '')}\n")

    return sf_user.lower()


def resolve_resource_base(
    work_dir: Path,
    *,
    prefix_override: str | None = None,
    no_prefix: bool = False,
) -> dict:
    """Return resource naming components.

    Pattern: <prefix>-<project>-<catalog> (AWS) / <PREFIX>_<PROJECT>_<CATALOG> (SF)

    Sources (auto-derived by default):
      prefix  = SNOWFLAKE_USER (from .env or connection discovery)
      project = basename(work_dir)  e.g. polaris-dev
      catalog = PLF_POLARIS_CATALOG_NAME from .env  e.g. polardb

    Override behavior:
      --prefix foo   -> foo-polaris-dev-polardb  (replaces user portion)
      --no-prefix    -> polaris-dev-polardb      (drops user, keeps project+catalog)

    Returns dict with keys: aws_base, sf_base, prefix, project, catalog
    """
    from dotenv import dotenv_values

    env_file = work_dir / ".env"
    cfg = dotenv_values(env_file) if env_file.exists() else {}

    if no_prefix:
        prefix = None
    elif prefix_override:
        prefix = prefix_override.lower()
    else:
        prefix = ensure_snowflake_connection(work_dir)

    project = Path(work_dir).name
    catalog = get_local_catalog_name(cfg)

    base_parts = [p for p in [prefix, project, catalog] if p]
    aws_base = "-".join(base_parts).lower()
    sf_base = "_".join(base_parts).upper().replace("-", "_")
    return {
        "aws_base": aws_base,
        "sf_base": sf_base,
        "prefix": prefix,
        "project": project,
        "catalog": catalog,
    }


SQL_DIR = Path(__file__).parent / "sql"


def run_l2c_sql_file(
    sql_file: str,
    variables: dict[str, str] | None = None,
    *,
    check: bool = True,
    dry_run: bool = False,
) -> subprocess.CompletedProcess | None:
    """Execute an L2C SQL template via snow_utils_common.run_snow_sql_file.

    Resolves sql_file relative to the L2C sql/ directory.
    """
    from snow_utils_common import run_snow_sql_file
    return run_snow_sql_file(
        SQL_DIR / sql_file, variables, check=check, dry_run=dry_run,
    )
