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

"""CLI entry point for Polaris Local Forge."""

import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import click
import requests
from dotenv import load_dotenv

# Skill directory: where the source repo lives (read-only for --work-dir mode)
SKILL_DIR = Path(__file__).parent.parent.parent.resolve()
ANSIBLE_DIR = SKILL_DIR / "polaris-forge-setup"
CONFIG_DIR = SKILL_DIR / "config"

# Static k8s files that must be copied to WORK_DIR when it differs from SKILL_DIR
STATIC_K8S_FILES = [
    "k8s/features/rustfs.yaml",
    "k8s/polaris/kustomization.yaml",
    "k8s/polaris/jobs/kustomization.yaml",
    "k8s/polaris/jobs/job-bootstrap.yaml",
    "k8s/polaris/jobs/job-purge.yaml",
]

# Directories that hold sensitive content (mode 0700)
_SENSITIVE_DIRS = ["work", ".snow-utils", ".kube", ".aws"]
# Files that contain credentials or keys (mode 0600)
_SENSITIVE_FILES = [
    ".env",
    ".aws/config",
    ".aws/credentials",
    "work/principal.txt",
    "k8s/polaris/rsa_key",
    "k8s/polaris/.bootstrap-credentials.env",
    "k8s/polaris/.polaris.env",
    "k8s/polaris/polaris-secrets.yaml",
    ".kube/config",
]


def get_config(work_dir: Path) -> dict:
    """Get all configuration from environment variables.

    All defaults are defined here and should match .env.example.
    The .env file is the single source of truth for configuration.

    Args:
        work_dir: Working directory for path-based defaults (KUBECONFIG).
    """
    return {
        # Cluster Configuration
        "K3D_CLUSTER_NAME": os.getenv("K3D_CLUSTER_NAME", "polaris-local-forge"),
        "K3S_VERSION": os.getenv("K3S_VERSION", "v1.31.5-k3s1"),
        "KUBECONFIG": os.getenv("KUBECONFIG", str(work_dir / ".kube" / "config")),
        # RustFS S3 Configuration
        "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
        "RUSTFS_CONSOLE_URL": os.getenv("RUSTFS_CONSOLE_URL", "http://localhost:9001"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
        # Polaris Configuration
        "POLARIS_URL": os.getenv("POLARIS_URL", "http://localhost:18181"),
        "POLARIS_REALM": os.getenv("POLARIS_REALM", "default-realm"),
        # Catalog Configuration
        "PLF_POLARIS_S3_BUCKET": os.getenv("PLF_POLARIS_S3_BUCKET", "polaris"),
        "PLF_POLARIS_CATALOG_NAME": os.getenv("PLF_POLARIS_CATALOG_NAME", "polardb"),
        "PLF_POLARIS_PRINCIPAL_NAME": os.getenv("PLF_POLARIS_PRINCIPAL_NAME", "iceberg"),
    }


def check_prerequisites() -> list[str]:
    """Check for required binaries, return list of missing ones."""
    required = ["docker", "k3d"]
    missing = [cmd for cmd in required if shutil.which(cmd) is None]
    return missing


def check_docker_running() -> bool:
    """Check if Docker daemon is running."""
    try:
        result = subprocess.run(
            ["docker", "info"], capture_output=True, timeout=10
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def get_cluster_env(cfg: dict, k8s_dir: Path) -> dict:
    """Get cluster environment variables from config.

    Args:
        cfg: Configuration dict from get_config().
        k8s_dir: Path to k8s manifests directory (in WORK_DIR).
    """
    return {
        "K3D_CLUSTER_NAME": cfg["K3D_CLUSTER_NAME"],
        "K3S_VERSION": cfg["K3S_VERSION"],
        "FEATURES_DIR": str(k8s_dir),
        "KUBECONFIG": cfg["KUBECONFIG"],
    }


def get_aws_env() -> dict:
    """Get AWS environment variables for RustFS S3 from config."""
    cfg = get_config()
    return {
        "AWS_ENDPOINT_URL": cfg["AWS_ENDPOINT_URL"],
        "AWS_ACCESS_KEY_ID": cfg["AWS_ACCESS_KEY_ID"],
        "AWS_SECRET_ACCESS_KEY": cfg["AWS_SECRET_ACCESS_KEY"],
        "AWS_REGION": cfg["AWS_REGION"],
    }


def run_ansible_playbook(
    playbook: str,
    work_dir: Path,
    tags: str | None = None,
    extra_vars: dict | None = None,
    dry_run: bool = False,
    verbose: bool = False,
    with_aws_env: bool = False,
) -> int:
    """Run an Ansible playbook.

    Args:
        playbook: Path to playbook relative to ANSIBLE_DIR
        work_dir: Working directory for generated output
        tags: Comma-separated list of tags to run
        extra_vars: Dictionary of extra variables to pass
        dry_run: If True, print command without executing
        verbose: If True, add verbose flag to ansible-playbook
        with_aws_env: If True, include AWS environment variables

    Returns:
        Exit code from ansible-playbook
    """
    playbook_path = ANSIBLE_DIR / playbook
    cmd = ["uv", "run", "--project", str(SKILL_DIR), "ansible-playbook", str(playbook_path)]

    merged_vars = dict(extra_vars) if extra_vars else {}
    if work_dir != SKILL_DIR:
        merged_vars["plf_output_base"] = str(work_dir)

    if tags:
        cmd.extend(["--tags", tags])

    for key, value in merged_vars.items():
        cmd.extend(["-e", f"{key}={value}"])

    if verbose:
        cmd.append("-v")

    env = os.environ.copy()
    if with_aws_env:
        env.update(get_aws_env())
        env.pop("AWS_PROFILE", None)

    if dry_run:
        env_str = ""
        if with_aws_env:
            aws_env = get_aws_env()
            env_str = " ".join(f"{k}={v}" for k, v in aws_env.items()) + " "
        click.echo(f"[DRY RUN] {env_str}{' '.join(cmd)}")
        return 0

    result = subprocess.run(cmd, env=env, cwd=work_dir)
    return result.returncode


def copy_static_files(work_dir: Path) -> None:
    """Copy static k8s files from SKILL_DIR to WORK_DIR.

    Only needed when WORK_DIR differs from SKILL_DIR.
    """
    if work_dir == SKILL_DIR:
        return
    for rel_path in STATIC_K8S_FILES:
        src = SKILL_DIR / rel_path
        dst = work_dir / rel_path
        if src.exists():
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dst)


def secure_work_dir(work_dir: Path) -> None:
    """Set restrictive permissions on sensitive directories and files."""
    if work_dir == SKILL_DIR:
        return
    for d in _SENSITIVE_DIRS:
        p = work_dir / d
        if p.exists():
            p.chmod(0o700)
    for f in _SENSITIVE_FILES:
        p = work_dir / f
        if p.exists():
            p.chmod(0o600)


@click.group()
@click.version_option()
@click.option(
    "--work-dir",
    type=click.Path(exists=True, file_okay=False, resolve_path=True),
    default=None,
    help="Working directory for generated files (default: skill directory).",
)
@click.option(
    "--env-file",
    type=click.Path(dir_okay=False, resolve_path=True),
    default=None,
    help="Path to .env file (default: <work-dir>/.env).",
)
@click.pass_context
def cli(ctx, work_dir: str | None, env_file: str | None):
    """Polaris Local Forge - Manage your local Apache Polaris environment."""
    ctx.ensure_object(dict)
    work = Path(work_dir) if work_dir else SKILL_DIR
    env_path = Path(env_file) if env_file else work / ".env"

    ctx.obj["SKILL_DIR"] = SKILL_DIR
    ctx.obj["WORK_DIR"] = work
    ctx.obj["ENV_FILE"] = env_path
    ctx.obj["BIN_DIR"] = work / "bin"
    ctx.obj["K8S_DIR"] = work / "k8s"
    ctx.obj["WORK_OUTPUT_DIR"] = work / "work"

    if env_path.exists():
        load_dotenv(env_path, override=True)


@cli.command()
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text", help="Output format")
@click.pass_context
def config(ctx, output: str):
    """Show current configuration from .env file."""
    work_dir = ctx.obj["WORK_DIR"]
    cfg = get_config(work_dir)

    if output == "json":
        click.echo(json.dumps(cfg, indent=2))
    else:
        click.echo("Current Configuration:")
        click.echo(f"  Config file: {ctx.obj['ENV_FILE']}")
        click.echo(f"  Work dir:    {work_dir}")
        click.echo()
        click.secho("Cluster:", bold=True)
        click.echo(f"  K3D_CLUSTER_NAME:  {cfg['K3D_CLUSTER_NAME']}")
        click.echo(f"  K3S_VERSION:       {cfg['K3S_VERSION']}")
        click.echo(f"  KUBECONFIG:        {cfg['KUBECONFIG']}")
        click.echo()
        click.secho("RustFS S3:", bold=True)
        click.echo(f"  AWS_ENDPOINT_URL:   {cfg['AWS_ENDPOINT_URL']}")
        click.echo(f"  RUSTFS_CONSOLE_URL: {cfg['RUSTFS_CONSOLE_URL']}")
        click.echo(f"  AWS_ACCESS_KEY_ID:  {cfg['AWS_ACCESS_KEY_ID']}")
        click.echo(f"  AWS_REGION:         {cfg['AWS_REGION']}")
        click.echo()
        click.secho("Polaris:", bold=True)
        click.echo(f"  POLARIS_URL:       {cfg['POLARIS_URL']}")
        click.echo(f"  POLARIS_REALM:     {cfg['POLARIS_REALM']}")
        click.echo()
        click.secho("Catalog:", bold=True)
        click.echo(f"  PLF_POLARIS_S3_BUCKET:      {cfg['PLF_POLARIS_S3_BUCKET']}")
        click.echo(f"  PLF_POLARIS_CATALOG_NAME:   {cfg['PLF_POLARIS_CATALOG_NAME']}")
        click.echo(f"  PLF_POLARIS_PRINCIPAL_NAME: {cfg['PLF_POLARIS_PRINCIPAL_NAME']}")


def get_tool_version(cmd: list[str]) -> str | None:
    """Get version string from a command, return None if not found."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            return result.stdout.strip().split("\n")[0]
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    return None


def get_docker_memory() -> str | None:
    """Get Docker total memory allocation."""
    try:
        result = subprocess.run(
            ["docker", "info", "--format", "{{.MemTotal}}"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            mem_bytes = int(result.stdout.strip())
            mem_gb = mem_bytes / (1024**3)
            return f"{mem_gb:.1f}GB"
    except (subprocess.TimeoutExpired, FileNotFoundError, ValueError):
        pass
    return None


def check_cluster_exists(cluster_name: str) -> bool:
    """Check if a k3d cluster exists."""
    try:
        result = subprocess.run(
            ["k3d", "cluster", "list", "-o", "json"],
            capture_output=True, text=True, timeout=10
        )
        if result.returncode == 0:
            clusters = json.loads(result.stdout)
            return any(c.get("name") == cluster_name for c in clusters)
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError):
        pass
    return False


@cli.command()
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text", help="Output format")
@click.pass_context
def doctor(ctx, output: str):
    """Check system prerequisites and health.

    Verifies all required tools are installed, Docker is running,
    and the environment is ready for setup.
    """
    work_dir = ctx.obj["WORK_DIR"]
    env_file = ctx.obj["ENV_FILE"]
    cfg = get_config(work_dir)
    checks = {
        "required": {},
        "optional": {},
        "environment": {},
    }
    all_required_ok = True

    # Required: Docker
    docker_path = shutil.which("docker")
    if docker_path:
        docker_version = get_tool_version(["docker", "--version"])
        docker_running = check_docker_running()
        docker_memory = get_docker_memory() if docker_running else None
        checks["required"]["docker"] = {
            "installed": True,
            "version": docker_version,
            "running": docker_running,
            "memory": docker_memory,
            "path": docker_path,
        }
        if not docker_running:
            all_required_ok = False
    else:
        checks["required"]["docker"] = {"installed": False}
        all_required_ok = False

    # Required: k3d
    k3d_path = shutil.which("k3d")
    if k3d_path:
        k3d_version = get_tool_version(["k3d", "version"])
        checks["required"]["k3d"] = {
            "installed": True,
            "version": k3d_version,
            "path": k3d_path,
        }
    else:
        checks["required"]["k3d"] = {"installed": False}
        all_required_ok = False

    # Required: Python
    python_path = shutil.which("python3") or shutil.which("python")
    if python_path:
        python_version = get_tool_version(["python3", "--version"]) or get_tool_version(["python", "--version"])
        version_parts = python_version.replace("Python ", "").split(".") if python_version else []
        meets_requirement = len(version_parts) >= 2 and int(version_parts[0]) >= 3 and int(version_parts[1]) >= 12
        checks["required"]["python"] = {
            "installed": True,
            "version": python_version,
            "meets_requirement": meets_requirement,
            "required_version": ">=3.12",
            "path": python_path,
        }
        if not meets_requirement:
            all_required_ok = False
    else:
        checks["required"]["python"] = {"installed": False, "required_version": ">=3.12"}
        all_required_ok = False

    # Required: uv
    uv_path = shutil.which("uv")
    if uv_path:
        uv_version = get_tool_version(["uv", "--version"])
        checks["required"]["uv"] = {
            "installed": True,
            "version": uv_version,
            "path": uv_path,
        }
    else:
        checks["required"]["uv"] = {"installed": False}
        all_required_ok = False

    # Optional: Task
    task_path = shutil.which("task")
    if task_path:
        task_version = get_tool_version(["task", "--version"])
        checks["optional"]["task"] = {
            "installed": True,
            "version": task_version,
            "path": task_path,
        }
    else:
        checks["optional"]["task"] = {"installed": False}

    # Optional: DuckDB CLI
    duckdb_path = shutil.which("duckdb")
    if duckdb_path:
        duckdb_version = get_tool_version(["duckdb", "--version"])
        checks["optional"]["duckdb"] = {
            "installed": True,
            "version": duckdb_version,
            "path": duckdb_path,
        }
    else:
        checks["optional"]["duckdb"] = {"installed": False}

    # Optional: direnv
    direnv_path = shutil.which("direnv")
    if direnv_path:
        direnv_version = get_tool_version(["direnv", "--version"])
        checks["optional"]["direnv"] = {
            "installed": True,
            "version": direnv_version,
            "path": direnv_path,
        }
    else:
        checks["optional"]["direnv"] = {"installed": False}

    # Environment: Python venv
    venv_exists = (work_dir / ".venv").exists()
    checks["environment"]["venv"] = {
        "exists": venv_exists,
        "path": str(work_dir / ".venv"),
    }

    # Environment: .env file
    env_exists = env_file.exists()
    checks["environment"]["env_file"] = {
        "exists": env_exists,
        "path": str(env_file),
    }

    # Environment: Cluster
    cluster_name = cfg["K3D_CLUSTER_NAME"]
    cluster_exists = False
    if checks["required"]["k3d"].get("installed") and checks["required"]["docker"].get("running"):
        cluster_exists = check_cluster_exists(cluster_name)
    checks["environment"]["cluster"] = {
        "name": cluster_name,
        "exists": cluster_exists,
    }

    # Summary
    checks["summary"] = {
        "all_required_ok": all_required_ok,
        "ready_for_setup": all_required_ok and env_exists,
    }

    if output == "json":
        click.echo(json.dumps(checks, indent=2))
    else:
        click.secho("Polaris Local Forge - System Doctor", bold=True)
        click.echo("=" * 42)
        click.echo()

        click.secho("Required Tools:", bold=True)
        for tool, info in checks["required"].items():
            if info.get("installed"):
                status = "✓" if tool != "docker" or info.get("running") else "✗"
                color = "green" if status == "✓" else "red"
                version = info.get("version", "").split("\n")[0]
                extra = ""
                if tool == "docker":
                    if info.get("running"):
                        mem = info.get("memory", "")
                        extra = f" (running, {mem})" if mem else " (running)"
                    else:
                        extra = " (not running)"
                elif tool == "python" and not info.get("meets_requirement"):
                    extra = f" (need {info.get('required_version')})"
                    color = "yellow"
                click.secho(f"  {status} {tool}: {version}{extra}", fg=color)
            else:
                click.secho(f"  ✗ {tool}: not installed", fg="red")
                if tool == "docker":
                    click.echo("    -> Install: https://www.docker.com/products/docker-desktop/")
                elif tool == "k3d":
                    click.echo("    -> Install: brew install k3d")
                elif tool == "python":
                    click.echo("    -> Install: https://www.python.org/downloads/")
                elif tool == "uv":
                    click.echo("    -> Install: curl -LsSf https://astral.sh/uv/install.sh | sh")

        click.echo()
        click.secho("Optional Tools:", bold=True)
        for tool, info in checks["optional"].items():
            if info.get("installed"):
                version = info.get("version", "")
                click.secho(f"  ✓ {tool}: {version}", fg="green")
            else:
                click.echo(f"  - {tool}: not installed")
                if tool == "task":
                    click.echo("    -> Install: brew install go-task")
                elif tool == "duckdb":
                    click.echo("    -> Install: brew install duckdb")
                elif tool == "direnv":
                    click.echo("    -> Install: brew install direnv")

        click.echo()
        click.secho("Environment:", bold=True)
        env_info = checks["environment"]
        if env_info["venv"]["exists"]:
            click.secho("  ✓ Python venv: exists", fg="green")
        else:
            click.echo("  - Python venv: not created")
            click.echo("    -> Run: uv sync")

        if env_info["env_file"]["exists"]:
            click.secho("  ✓ Configuration: .env exists", fg="green")
        else:
            click.echo("  - Configuration: .env not found")
            click.echo("    -> Run: cp .env.example .env")

        if env_info["cluster"]["exists"]:
            click.secho(f"  ✓ Cluster '{cluster_name}': exists", fg="green")
        else:
            click.echo(f"  - Cluster '{cluster_name}': not created")
            click.echo("    -> Run: polaris-local-forge setup")

        click.echo()
        click.echo("=" * 42)
        if all_required_ok:
            click.secho("✓ All required prerequisites installed!", fg="green")
            click.echo()
            click.echo("Next steps:")
            if not env_info["env_file"]["exists"]:
                click.echo("  1. cp .env.example .env")
                click.echo("  2. polaris-local-forge setup")
            elif not env_info["cluster"]["exists"]:
                click.echo("  1. polaris-local-forge setup")
            else:
                click.echo("  Environment ready! Run 'polaris-local-forge cluster status'.")
        else:
            click.secho("✗ Some prerequisites missing. Install them first.", fg="red")

    sys.exit(0 if all_required_ok else 1)


@cli.command()
@click.option("--tags", "-t", help="Ansible tags to run (comma-separated)")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def prepare(ctx, tags: str | None, dry_run: bool, verbose: bool):
    """Generate required sensitive files from templates."""
    work_dir = ctx.obj["WORK_DIR"]
    exit_code = run_ansible_playbook("prepare.yml", work_dir, tags=tags, dry_run=dry_run, verbose=verbose)
    if exit_code == 0 and not dry_run:
        copy_static_files(work_dir)
        secure_work_dir(work_dir)
    sys.exit(exit_code)


@cli.command()
@click.option("--dry-run", "-n", is_flag=True, help="Show plan without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompts")
@click.pass_context
def setup(ctx: click.Context, dry_run: bool, yes: bool):
    """Complete setup - create cluster, deploy Polaris, setup catalog.

    This command orchestrates the full setup workflow:
    1. prepare - Generate configuration files
    2. cluster create - Create k3d cluster
    3. cluster bootstrap-check - Wait for bootstrap deployments
    4. polaris deploy - Deploy Polaris
    5. polaris check - Wait for Polaris to be ready
    6. catalog setup - Setup demo catalog
    """
    work_dir = ctx.obj["WORK_DIR"]
    k8s_dir = ctx.obj["K8S_DIR"]
    start_time = time.time()

    missing = check_prerequisites()
    if missing:
        click.secho(f"Error: Missing required tools: {', '.join(missing)}", fg="red")
        click.echo("Please install them before running setup.")
        sys.exit(1)

    if not check_docker_running():
        click.secho("Error: Docker is not running. Please start Docker first.", fg="red")
        sys.exit(1)

    steps = [
        ("prepare", "Generate configuration files"),
        ("cluster create", "Create k3d cluster"),
        ("cluster bootstrap-check", "Wait for bootstrap deployments"),
        ("polaris deploy", "Deploy Polaris to cluster"),
        ("polaris check", "Wait for Polaris to be ready"),
        ("catalog setup", "Setup demo catalog"),
    ]

    if dry_run:
        click.echo("[DRY RUN] Setup plan:")
        for i, (cmd, desc) in enumerate(steps, 1):
            click.echo(f"  {i}. {desc} (polaris-local-forge {cmd})")
        click.echo("\nRun without --dry-run to execute.")
        sys.exit(0)

    if not yes:
        click.echo("Setup plan:")
        for i, (cmd, desc) in enumerate(steps, 1):
            click.echo(f"  {i}. {desc}")
        click.echo()
        if not click.confirm("Proceed with setup?"):
            click.echo("Aborted.")
            sys.exit(0)

    click.secho("\n=== Starting Polaris Local Forge Setup ===\n", fg="cyan", bold=True)

    if run_ansible_playbook("prepare.yml", work_dir) != 0:
        click.secho("Error: prepare failed", fg="red")
        sys.exit(1)
    copy_static_files(work_dir)
    secure_work_dir(work_dir)
    click.secho("✓ Configuration files generated", fg="green")

    ctx.invoke(cluster_create, dry_run=False)
    click.secho("✓ Cluster created", fg="green")

    if run_ansible_playbook("cluster_checks.yml", work_dir, tags="bootstrap") != 0:
        click.secho("Error: bootstrap-check failed", fg="red")
        sys.exit(1)
    click.secho("✓ Bootstrap deployments ready", fg="green")

    ctx.invoke(polaris_deploy, dry_run=False)
    click.secho("✓ Polaris deployed", fg="green")

    if run_ansible_playbook("cluster_checks.yml", work_dir, tags="polaris") != 0:
        click.secho("Error: polaris-check failed", fg="red")
        sys.exit(1)
    click.secho("✓ Polaris ready", fg="green")

    if run_ansible_playbook("catalog_setup.yml", work_dir, with_aws_env=True) != 0:
        click.secho("Error: catalog setup failed", fg="red")
        sys.exit(1)
    secure_work_dir(work_dir)
    click.secho("✓ Catalog setup complete", fg="green")

    elapsed = time.time() - start_time
    click.secho(f"\n=== Setup Complete ({elapsed:.1f}s) ===", fg="green", bold=True)

    cfg = get_config(work_dir)
    click.echo("\nService URLs:")
    click.echo(f"  Polaris UI:     {cfg['POLARIS_URL']}")
    click.echo(f"  RustFS S3 API:  {cfg['AWS_ENDPOINT_URL']}")
    click.echo(f"  RustFS Console: {cfg['RUSTFS_CONSOLE_URL']}")

    click.echo("\nRustFS Credentials:")
    click.echo(f"  Access Key: {cfg['AWS_ACCESS_KEY_ID']}")
    click.echo(f"  Secret Key: {cfg['AWS_SECRET_ACCESS_KEY']}")

    click.echo("\nPolaris Credentials:")
    click.echo(f"  See: {k8s_dir / 'polaris' / '.bootstrap-credentials.env'}")

    click.echo("\nNext steps:")
    click.echo("  - Verify:  polaris-local-forge catalog verify")
    click.echo("  - Explore: polaris-local-forge catalog explore-sql")
    click.echo("  - Config:  polaris-local-forge config")


@cli.command()
@click.option("--dry-run", "-n", is_flag=True, help="Show plan without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompts")
@click.pass_context
def teardown(ctx: click.Context, dry_run: bool, yes: bool):
    """Complete teardown - cleanup catalog and delete cluster.

    This command orchestrates the full teardown workflow:
    1. catalog cleanup - Remove catalog resources
    2. cluster delete - Delete k3d cluster and generated files
    """
    start_time = time.time()

    steps = [
        ("catalog cleanup", "Remove catalog resources"),
        ("cluster delete", "Delete k3d cluster and cleanup files"),
    ]

    if dry_run:
        click.echo("[DRY RUN] Teardown plan:")
        for i, (cmd, desc) in enumerate(steps, 1):
            click.echo(f"  {i}. {desc} (polaris-forge {cmd})")
        click.echo("\nRun without --dry-run to execute.")
        sys.exit(0)

    if not yes:
        click.secho("Warning: This will delete your cluster and all data!", fg="yellow")
        click.echo("Teardown plan:")
        for i, (cmd, desc) in enumerate(steps, 1):
            click.echo(f"  {i}. {desc}")
        click.echo()
        if not click.confirm("Proceed with teardown?"):
            click.echo("Aborted.")
            sys.exit(0)

    click.secho("\n=== Starting Teardown ===\n", fg="cyan", bold=True)

    run_ansible_playbook("catalog_cleanup.yml", ctx.obj["WORK_DIR"], with_aws_env=True)
    click.secho("✓ Catalog cleanup complete", fg="green")

    ctx.invoke(cluster_delete, dry_run=False, yes=True)
    click.secho("✓ Cluster deleted", fg="green")

    elapsed = time.time() - start_time
    click.secho(f"\n=== Teardown Complete ({elapsed:.1f}s) ===", fg="green", bold=True)


@cli.group()
def cluster():
    """Cluster management commands."""
    pass


def get_k8s_version_from_k3s(k3s_version: str) -> str:
    """Extract kubernetes version from k3s version string (e.g., v1.31.5-k3s1 -> v1.31.5)."""
    return k3s_version.split("-")[0].split("+")[0]


def get_kubectl_url(k3s_version: str) -> str:
    """Get kubectl download URL for current platform."""
    k8s_version = get_k8s_version_from_k3s(k3s_version)
    os_name = platform.system().lower()
    arch = platform.machine()
    if arch == "x86_64":
        arch = "amd64"
    elif arch == "aarch64" or arch == "arm64":
        arch = "arm64"
    return f"https://dl.k8s.io/release/{k8s_version}/bin/{os_name}/{arch}/kubectl"


def get_installed_kubectl_version(kubectl_path: Path) -> str | None:
    """Get the version of an installed kubectl binary, or None if not found."""
    if not kubectl_path.exists():
        return None
    try:
        result = subprocess.run(
            [str(kubectl_path), "version", "--client", "-o", "json"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode == 0:
            data = json.loads(result.stdout)
            return data.get("clientVersion", {}).get("gitVersion")
    except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError):
        pass
    return None


def ensure_kubectl(k3s_version: str, dest: Path) -> None:
    """Ensure kubectl binary exists and matches the required k3s version.

    Downloads kubectl only if:
    - kubectl doesn't exist at dest, OR
    - kubectl exists but version doesn't match the k8s version from k3s
    """
    required_version = get_k8s_version_from_k3s(k3s_version)
    installed_version = get_installed_kubectl_version(dest)

    if installed_version:
        installed_k8s = get_k8s_version_from_k3s(installed_version)
        if installed_k8s == required_version:
            click.echo(f"kubectl {installed_version} already installed (matches k3s {k3s_version})")
            return
        else:
            click.echo(f"kubectl version mismatch: have {installed_version}, need {required_version}")

    url = get_kubectl_url(k3s_version)
    click.echo(f"Downloading kubectl {required_version} from {url}...")
    dest.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, dest)
    dest.chmod(0o755)
    click.echo(f"kubectl {required_version} downloaded to {dest}")


@cluster.command("create")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.pass_context
def cluster_create(ctx, dry_run: bool):
    """Create the k3d cluster and deploy initial components.

    This downloads kubectl and creates a k3d cluster using the project's configuration.
    """
    work_dir = ctx.obj["WORK_DIR"]
    bin_dir = ctx.obj["BIN_DIR"]
    k8s_dir = ctx.obj["K8S_DIR"]
    cfg = get_config(work_dir)
    cluster_env = get_cluster_env(cfg, k8s_dir)
    env = os.environ.copy()
    env.update(cluster_env)

    kubectl_path = bin_dir / "kubectl"
    cluster_config = CONFIG_DIR / "cluster-config.yaml"

    required_k8s_version = get_k8s_version_from_k3s(cluster_env["K3S_VERSION"])

    if dry_run:
        click.echo("[DRY RUN] Would execute the following steps:")
        click.echo(f"  1. Create directory: {Path(cluster_env['KUBECONFIG']).parent}")
        click.echo(f"  2. Ensure kubectl {required_k8s_version} (for k3s {cluster_env['K3S_VERSION']})")
        installed = get_installed_kubectl_version(kubectl_path)
        if installed:
            click.echo(f"     Current: {installed}")
        else:
            click.echo("     Current: not installed")
        click.echo(f"     Destination: {kubectl_path}")
        click.echo(f"  3. Create k3d cluster: {cluster_env['K3D_CLUSTER_NAME']}")
        click.echo(f"     Config: {cluster_config}")
        return

    kubeconfig_dir = Path(cluster_env["KUBECONFIG"]).parent
    kubeconfig_dir.mkdir(parents=True, exist_ok=True)

    bin_dir.mkdir(parents=True, exist_ok=True)
    ensure_kubectl(cluster_env["K3S_VERSION"], kubectl_path)

    check_result = subprocess.run(
        ["k3d", "cluster", "list", "-o", "json"],
        capture_output=True,
        text=True,
    )
    if check_result.returncode == 0:
        try:
            clusters = json.loads(check_result.stdout)
            for c in clusters:
                if c.get("name") == cluster_env["K3D_CLUSTER_NAME"]:
                    click.secho(f"Cluster '{cluster_env['K3D_CLUSTER_NAME']}' already exists, skipping creation", fg="yellow")
                    return
        except json.JSONDecodeError:
            pass

    result = subprocess.run(
        ["k3d", "cluster", "create", "--config", str(cluster_config)],
        env=env,
        cwd=work_dir,
    )
    if result.returncode == 0:
        kubeconfig_path = Path(cluster_env["KUBECONFIG"])
        if kubeconfig_path.exists():
            kubeconfig_path.chmod(0o600)
    else:
        sys.exit(result.returncode)


@cluster.command("delete")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def cluster_delete(ctx, dry_run: bool, yes: bool):
    """Delete the k3d cluster and clean up generated files."""
    work_dir = ctx.obj["WORK_DIR"]
    k8s_dir = ctx.obj["K8S_DIR"]
    cfg = get_config(work_dir)
    cluster_env = get_cluster_env(cfg, k8s_dir)
    env = os.environ.copy()
    env.update(cluster_env)

    files_to_remove = [
        k8s_dir / "features" / "polaris.yaml",
        k8s_dir / "features" / "postgresql.yaml",
        k8s_dir / "polaris" / "polaris-secrets.yaml",
        k8s_dir / "polaris" / ".bootstrap-credentials.env",
        k8s_dir / "polaris" / ".polaris.env",
        k8s_dir / "polaris" / "rsa_key",
        k8s_dir / "polaris" / "rsa_key.pub",
    ]

    if dry_run:
        click.echo("[DRY RUN] Would execute the following steps:")
        click.echo(f"  1. Delete k3d cluster: {cluster_env['K3D_CLUSTER_NAME']}")
        click.echo("  2. Remove generated files:")
        for f in files_to_remove:
            click.echo(f"     - {f}")
        sys.exit(0)

    if not yes:
        click.secho("Warning: This will delete the cluster and all generated files!", fg="yellow")
        if not click.confirm("Proceed?"):
            click.echo("Aborted.")
            sys.exit(0)

    result = subprocess.run(
        ["k3d", "cluster", "delete", cluster_env["K3D_CLUSTER_NAME"]],
        env=env,
        cwd=work_dir,
    )

    for f in files_to_remove:
        if f.exists():
            if f.is_dir():
                shutil.rmtree(f)
            else:
                f.unlink()
            click.echo(f"Removed: {f}")

    sys.exit(result.returncode)


@cluster.command("bootstrap-check")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def cluster_bootstrap_check(ctx, dry_run: bool, verbose: bool):
    """Wait for bootstrap deployments to be ready."""
    sys.exit(run_ansible_playbook("cluster_checks.yml", ctx.obj["WORK_DIR"], tags="bootstrap", dry_run=dry_run, verbose=verbose))


@cluster.command("polaris-check")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def cluster_polaris_check(ctx, dry_run: bool, verbose: bool):
    """Wait for Polaris deployments to be ready."""
    sys.exit(run_ansible_playbook("cluster_checks.yml", ctx.obj["WORK_DIR"], tags="polaris", dry_run=dry_run, verbose=verbose))


@cli.group()
def catalog():
    """Catalog management commands."""
    pass


@catalog.command("setup")
@click.option("--tags", "-t", help="Ansible tags to run (comma-separated)")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def catalog_setup(ctx, tags: str | None, dry_run: bool, verbose: bool):
    """Set up demo catalog with S3 bucket, catalog, principal, and roles."""
    work_dir = ctx.obj["WORK_DIR"]
    exit_code = run_ansible_playbook("catalog_setup.yml", work_dir, tags=tags, dry_run=dry_run, verbose=verbose, with_aws_env=True)
    if exit_code == 0 and not dry_run:
        secure_work_dir(work_dir)
    sys.exit(exit_code)


@catalog.command("cleanup")
@click.option("--tags", "-t", help="Ansible tags to run (comma-separated)")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def catalog_cleanup(ctx, tags: str | None, dry_run: bool, verbose: bool, yes: bool):
    """Cleanup Polaris catalog resources."""
    if not dry_run and not yes:
        click.secho("Warning: This will remove catalog resources!", fg="yellow")
        if not click.confirm("Proceed?"):
            click.echo("Aborted.")
            sys.exit(0)
    sys.exit(
        run_ansible_playbook("catalog_cleanup.yml", ctx.obj["WORK_DIR"], tags=tags, dry_run=dry_run, verbose=verbose, with_aws_env=True)
    )


@catalog.command("generate-notebook")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def catalog_generate_notebook(ctx, dry_run: bool, verbose: bool):
    """Generate and prepare verification notebook."""
    work_dir = ctx.obj["WORK_DIR"]
    exit_code = run_ansible_playbook("catalog_setup.yml", work_dir, tags="verify", dry_run=dry_run, verbose=verbose, with_aws_env=True)
    if exit_code == 0 and not dry_run:
        notebook_path = work_dir / "notebooks" / "verify_polaris.ipynb"
        click.echo(f"\nRun the notebook at: {notebook_path}")
    sys.exit(exit_code)


@catalog.command("verify")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.argument("args", nargs=-1)
@click.pass_context
def catalog_verify(ctx, dry_run: bool, args: tuple):
    """Verify Polaris catalog using DuckDB Iceberg extension (Python)."""
    work_dir = ctx.obj["WORK_DIR"]
    script_path = SKILL_DIR / "scripts" / "explore_catalog.py"
    credentials_file = ctx.obj["WORK_OUTPUT_DIR"] / "principal.txt"
    cmd = ["uv", "run", "--project", str(SKILL_DIR), "python", str(script_path),
           "--credentials-file", str(credentials_file)] + list(args)

    env = os.environ.copy()
    env.update(get_aws_env())
    env.pop("AWS_PROFILE", None)

    if dry_run:
        aws_env = get_aws_env()
        env_str = " ".join(f"{k}={v}" for k, v in aws_env.items())
        click.echo(f"[DRY RUN] {env_str} {' '.join(cmd)}")
        sys.exit(0)

    result = subprocess.run(cmd, env=env, cwd=work_dir)
    sys.exit(result.returncode)


@catalog.command("verify-sql")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.pass_context
def catalog_verify_sql(ctx, dry_run: bool):
    """Verify Polaris catalog using DuckDB CLI with SQL script."""
    work_dir = ctx.obj["WORK_DIR"]
    script_path = work_dir / "scripts" / "explore_catalog.sql"

    env = os.environ.copy()
    env.update(get_aws_env())
    env.pop("AWS_PROFILE", None)

    if dry_run:
        aws_env = get_aws_env()
        env_str = " ".join(f"{k}={v}" for k, v in aws_env.items())
        click.echo(f"[DRY RUN] {env_str} duckdb < {script_path}")
        sys.exit(0)

    with open(script_path) as f:
        result = subprocess.run(["duckdb"], stdin=f, env=env, cwd=work_dir)
    sys.exit(result.returncode)


@catalog.command("explore-sql")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.pass_context
def catalog_explore_sql(ctx, dry_run: bool):
    """Explore Polaris catalog with DuckDB CLI in interactive mode."""
    work_dir = ctx.obj["WORK_DIR"]
    script_path = work_dir / "scripts" / "explore_catalog.sql"

    env = os.environ.copy()
    env.update(get_aws_env())
    env.pop("AWS_PROFILE", None)

    if dry_run:
        aws_env = get_aws_env()
        env_str = " ".join(f"{k}={v}" for k, v in aws_env.items())
        click.echo(f"[DRY RUN] {env_str} duckdb -init {script_path}")
        sys.exit(0)

    result = subprocess.run(["duckdb", "-init", str(script_path)], env=env, cwd=work_dir)
    sys.exit(result.returncode)


@cli.group()
def polaris():
    """Polaris management commands."""
    pass


@polaris.command("deploy")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.pass_context
def polaris_deploy(ctx, dry_run: bool):
    """Deploy Polaris to the cluster."""
    k8s_dir = ctx.obj["K8S_DIR"]
    polaris_dir = k8s_dir / "polaris"
    cmd = ["kubectl", "apply", "-k", str(polaris_dir)]

    if dry_run:
        click.echo(f"[DRY RUN] {' '.join(cmd)}")
        return

    result = subprocess.run(cmd, cwd=ctx.obj["WORK_DIR"])
    if result.returncode != 0:
        sys.exit(result.returncode)


@polaris.command("check")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@click.pass_context
def polaris_check(ctx, dry_run: bool, verbose: bool):
    """Ensure all Polaris deployments and jobs have succeeded."""
    sys.exit(run_ansible_playbook("cluster_checks.yml", ctx.obj["WORK_DIR"], tags="polaris", dry_run=dry_run, verbose=verbose))


@polaris.command("purge")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.pass_context
def polaris_purge(ctx, dry_run: bool):
    """Purge Polaris data by running the purge job."""
    work_dir = ctx.obj["WORK_DIR"]
    if dry_run:
        click.echo("[DRY RUN] Would execute the following steps:")
        click.echo("  1. kubectl patch job polaris-purge -n polaris -p '{\"spec\":{\"suspend\":false}}'")
        click.echo("  2. kubectl wait --for=condition=complete --timeout=300s job/polaris-purge -n polaris")
        click.echo("  3. kubectl logs -n polaris jobs/polaris-purge")
        sys.exit(0)

    subprocess.run(
        ["kubectl", "patch", "job", "polaris-purge", "-n", "polaris", "-p", '{"spec":{"suspend":false}}'],
        cwd=work_dir,
    )
    click.echo("Waiting for purge to complete...")
    result = subprocess.run(
        ["kubectl", "wait", "--for=condition=complete", "--timeout=300s", "job/polaris-purge", "-n", "polaris"],
        cwd=work_dir,
    )
    subprocess.run(["kubectl", "logs", "-n", "polaris", "jobs/polaris-purge"], cwd=work_dir)
    sys.exit(result.returncode)


@polaris.command("bootstrap")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.pass_context
def polaris_bootstrap(ctx, dry_run: bool):
    """Bootstrap Polaris (run after purge)."""
    work_dir = ctx.obj["WORK_DIR"]
    k8s_dir = ctx.obj["K8S_DIR"]
    job_dir = k8s_dir / "polaris" / "jobs"

    if dry_run:
        click.echo("[DRY RUN] Would execute the following steps:")
        click.echo(f"  1. kubectl delete -k {job_dir}")
        click.echo(f"  2. kubectl apply -k {job_dir}")
        click.echo("  3. kubectl wait --for=condition=complete --timeout=300s job/polaris-bootstrap -n polaris")
        click.echo("  4. kubectl logs -n polaris jobs/polaris-bootstrap")
        sys.exit(0)

    subprocess.run(["kubectl", "delete", "-k", str(job_dir)], cwd=work_dir)
    subprocess.run(["kubectl", "apply", "-k", str(job_dir)], cwd=work_dir)
    click.echo("Waiting for bootstrap to complete...")
    result = subprocess.run(
        ["kubectl", "wait", "--for=condition=complete", "--timeout=300s", "job/polaris-bootstrap", "-n", "polaris"],
        cwd=work_dir,
    )
    subprocess.run(["kubectl", "logs", "-n", "polaris", "jobs/polaris-bootstrap"], cwd=work_dir)
    sys.exit(result.returncode)


@polaris.command("reset")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation prompt")
@click.pass_context
def polaris_reset(ctx: click.Context, dry_run: bool, yes: bool):
    """Purge and re-bootstrap Polaris."""
    if dry_run:
        click.echo("[DRY RUN] Would execute: polaris purge, then polaris bootstrap")
        sys.exit(0)

    if not yes:
        click.secho("Warning: This will purge all Polaris data and re-bootstrap!", fg="yellow")
        if not click.confirm("Proceed?"):
            click.echo("Aborted.")
            sys.exit(0)

    ctx.invoke(polaris_purge, dry_run=False)
    ctx.invoke(polaris_bootstrap, dry_run=False)


# =============================================================================
# Status Commands (with JSON output)
# =============================================================================


@cluster.command("status")
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text", help="Output format")
@click.pass_context
def cluster_status(ctx, output: str):
    """Show cluster status."""
    work_dir = ctx.obj["WORK_DIR"]
    cfg = get_config(work_dir)
    cluster_env = get_cluster_env(cfg, ctx.obj["K8S_DIR"])
    cluster_name = cluster_env["K3D_CLUSTER_NAME"]

    result = subprocess.run(
        ["k3d", "cluster", "list", "-o", "json"],
        capture_output=True,
        text=True,
    )

    running = False
    nodes = []
    if result.returncode == 0:
        try:
            clusters = json.loads(result.stdout)
            for c in clusters:
                if c.get("name") == cluster_name:
                    running = True
                    nodes = [n.get("name", "") for n in c.get("nodes", [])]
                    break
        except json.JSONDecodeError:
            pass

    status_data = {
        "running": running,
        "name": cluster_name,
        "k3s_version": cluster_env["K3S_VERSION"],
        "nodes": nodes,
    }

    if output == "json":
        click.echo(json.dumps(status_data))
    else:
        if running:
            click.secho(f"✓ Cluster '{cluster_name}' is running", fg="green")
            click.echo(f"  K3S Version: {cluster_env['K3S_VERSION']}")
            click.echo(f"  Nodes: {len(nodes)}")
            for node in nodes:
                click.echo(f"    - {node}")
        else:
            click.secho(f"✗ Cluster '{cluster_name}' is not running", fg="red")


@polaris.command("status")
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text", help="Output format")
def polaris_status(output: str):
    """Show Polaris deployment status."""
    result = subprocess.run(
        ["kubectl", "get", "deployment", "polaris", "-n", "polaris", "-o", "json"],
        capture_output=True,
        text=True,
    )

    ready = False
    replicas = 0
    available = 0
    version = "unknown"

    if result.returncode == 0:
        try:
            deployment = json.loads(result.stdout)
            replicas = deployment.get("status", {}).get("replicas", 0)
            available = deployment.get("status", {}).get("availableReplicas", 0)
            ready = replicas > 0 and replicas == available
            containers = deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
            if containers:
                image = containers[0].get("image", "")
                if ":" in image:
                    version = image.split(":")[-1]
        except json.JSONDecodeError:
            pass

    status_data = {
        "ready": ready,
        "replicas": replicas,
        "available": available,
        "version": version,
    }

    if output == "json":
        click.echo(json.dumps(status_data))
    else:
        if ready:
            click.secho("✓ Polaris is ready", fg="green")
            click.echo(f"  Version: {version}")
            click.echo(f"  Replicas: {available}/{replicas}")
        else:
            click.secho("✗ Polaris is not ready", fg="red")
            click.echo(f"  Replicas: {available}/{replicas}")


@catalog.command("list")
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text", help="Output format")
@click.pass_context
def catalog_list(ctx, output: str):
    """List catalogs in Polaris."""
    principal_file = ctx.obj["WORK_OUTPUT_DIR"] / "principal.txt"

    if not principal_file.exists():
        if output == "json":
            click.echo(json.dumps({"error": "Principal credentials not found", "catalogs": []}))
        else:
            click.secho("Error: Principal credentials not found. Run 'catalog setup' first.", fg="red")
        sys.exit(1)

    with open(principal_file) as f:
        line = f.readline().strip()
        parts = line.split(",")
        if len(parts) >= 3:
            realm, client_id, client_secret = parts[0], parts[1], parts[2]
        else:
            if output == "json":
                click.echo(json.dumps({"error": "Invalid principal file format", "catalogs": []}))
            else:
                click.secho("Error: Invalid principal file format", fg="red")
            sys.exit(1)

    token_result = subprocess.run(
        [
            "curl", "-s", "-X", "POST",
            "http://localhost:18181/api/catalog/v1/oauth/tokens",
            "-H", f"Authorization: Bearer {client_id}:{client_secret}",
            "-H", f"Polaris-Realm: {realm}",
            "-H", "Content-Type: application/x-www-form-urlencoded",
            "-d", f"grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope=PRINCIPAL_ROLE:ALL",
        ],
        capture_output=True,
        text=True,
    )

    try:
        token_data = json.loads(token_result.stdout)
        token = token_data.get("access_token")
    except json.JSONDecodeError:
        token = None

    if not token:
        if output == "json":
            click.echo(json.dumps({"error": "Failed to get access token", "catalogs": []}))
        else:
            click.secho("Error: Failed to get access token", fg="red")
        sys.exit(1)

    catalogs_result = subprocess.run(
        [
            "curl", "-s",
            "http://localhost:18181/api/management/v1/catalogs",
            "-H", f"Authorization: Bearer {token}",
            "-H", f"Polaris-Realm: {realm}",
        ],
        capture_output=True,
        text=True,
    )

    catalogs = []
    try:
        data = json.loads(catalogs_result.stdout)
        for cat in data.get("catalogs", []):
            catalogs.append({
                "name": cat.get("name", ""),
                "type": cat.get("type", ""),
            })
    except json.JSONDecodeError:
        pass

    if output == "json":
        click.echo(json.dumps({"catalogs": catalogs}))
    else:
        if catalogs:
            click.echo("Catalogs:")
            for cat in catalogs:
                click.echo(f"  - {cat['name']} ({cat['type']})")
        else:
            click.echo("No catalogs found.")


# =============================================================================
# Version Bump Commands
# =============================================================================


def get_latest_polaris_version() -> str:
    """Fetch latest apache/polaris tag from Docker Hub."""
    url = "https://hub.docker.com/v2/repositories/apache/polaris/tags?page_size=100"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    tags = resp.json().get("results", [])
    versions = [t["name"] for t in tags if t["name"] != "latest" and "incubating" in t["name"]]
    if not versions:
        raise ValueError("No valid Polaris versions found")
    versions.sort(key=lambda v: [int(x) for x in re.findall(r"\d+", v.split("-")[0])], reverse=True)
    return versions[0]


def get_latest_k3s_version() -> str:
    """Fetch latest rancher/k3s tag from Docker Hub."""
    url = "https://hub.docker.com/v2/repositories/rancher/k3s/tags?page_size=100&ordering=last_updated"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    tags = resp.json().get("results", [])
    versions = [
        t["name"] for t in tags
        if t["name"].startswith("v") and "-k3s" in t["name"]
        and "rc" not in t["name"].lower() and "alpha" not in t["name"].lower() and "beta" not in t["name"].lower()
    ]
    if not versions:
        raise ValueError("No valid K3S versions found")
    return versions[0]


@polaris.command("bump-version")
@click.option("--dry-run", "-n", is_flag=True, help="Show changes without applying")
def polaris_bump_version(dry_run: bool):
    """Update Polaris to latest version from Docker Hub (apache/polaris)."""
    try:
        latest = get_latest_polaris_version()
    except Exception as e:
        click.secho(f"Error fetching latest version: {e}", fg="red")
        sys.exit(1)

    click.echo(f"Latest Polaris version: {latest}")

    static_k8s = SKILL_DIR / "k8s"
    files_to_update = [
        (ANSIBLE_DIR / "templates" / "polaris.yaml.j2", r"(version:\s*)[\d.]+-incubating", rf"\g<1>{latest}"),
        (ANSIBLE_DIR / "templates" / "polaris.yaml.j2", r"(tag:\s*)[\d.]+-incubating", rf"\g<1>{latest}"),
        (static_k8s / "polaris" / "jobs" / "job-bootstrap.yaml", r"(apache/polaris-admin-tool:)[\d.]+-incubating", rf"\g<1>{latest}"),
        (static_k8s / "polaris" / "jobs" / "job-purge.yaml", r"(apache/polaris-admin-tool:)[\d.]+-incubating", rf"\g<1>{latest}"),
    ]

    if dry_run:
        click.echo("\n[DRY RUN] Would update the following files:")
        for path, pattern, replacement in files_to_update:
            if path.exists():
                click.echo(f"  - {path}")
        sys.exit(0)

    for path, pattern, replacement in files_to_update:
        if path.exists():
            content = path.read_text()
            new_content = re.sub(pattern, replacement, content)
            if content != new_content:
                path.write_text(new_content)
                click.secho(f"✓ Updated {path}", fg="green")
            else:
                click.echo(f"  No changes needed: {path}")
        else:
            click.secho(f"  File not found: {path}", fg="yellow")

    click.secho(f"\n✓ Polaris version updated to {latest}", fg="green", bold=True)


@cluster.command("bump-k3s")
@click.option("--dry-run", "-n", is_flag=True, help="Show changes without applying")
def cluster_bump_k3s(dry_run: bool):
    """Update K3S to latest version from Docker Hub (rancher/k3s)."""
    try:
        latest = get_latest_k3s_version()
    except Exception as e:
        click.secho(f"Error fetching latest version: {e}", fg="red")
        sys.exit(1)

    click.echo(f"Latest K3S version: {latest}")

    env_example_path = SKILL_DIR / ".env.example"
    taskfile_path = SKILL_DIR / "Taskfile.yml"

    files_to_update = [
        (env_example_path, r"(K3S_VERSION=)v[\d.]+-k3s\d+", rf"\g<1>{latest}"),
        (taskfile_path, r"(K3S_VERSION:\s*)v[\d.]+-k3s\d+", rf"\g<1>{latest}"),
    ]

    if dry_run:
        click.echo("\n[DRY RUN] Would update the following files:")
        for path, pattern, replacement in files_to_update:
            if path.exists():
                click.echo(f"  - {path}")
        sys.exit(0)

    for path, pattern, replacement in files_to_update:
        if path.exists():
            content = path.read_text()
            new_content = re.sub(pattern, replacement, content)
            if content != new_content:
                path.write_text(new_content)
                click.secho(f"✓ Updated {path}", fg="green")
            else:
                click.echo(f"  No changes needed: {path}")
        else:
            click.secho(f"  File not found: {path}", fg="yellow")

    click.secho(f"\n✓ K3S version updated to {latest}", fg="green", bold=True)
    click.echo("\nNote: Copy .env.example to .env to use the new version.")


if __name__ == "__main__":
    cli()
