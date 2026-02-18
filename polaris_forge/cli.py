"""CLI entry point for Polaris Local Forge."""

import os
import subprocess
import sys
from pathlib import Path

import click
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory
PROJECT_HOME = Path(__file__).parent.parent.resolve()
ANSIBLE_DIR = PROJECT_HOME / "polaris-forge-setup"


def get_aws_env() -> dict:
    """Get AWS environment variables for RustFS S3."""
    return {
        "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "admin"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "password"),
        "AWS_REGION": os.getenv("AWS_REGION", "us-east-1"),
    }


def run_ansible_playbook(
    playbook: str,
    tags: str | None = None,
    extra_vars: dict | None = None,
    dry_run: bool = False,
    verbose: bool = False,
    with_aws_env: bool = False,
) -> int:
    """Run an Ansible playbook.

    Args:
        playbook: Path to playbook relative to ANSIBLE_DIR
        tags: Comma-separated list of tags to run
        extra_vars: Dictionary of extra variables to pass
        dry_run: If True, print command without executing
        verbose: If True, add verbose flag to ansible-playbook
        with_aws_env: If True, include AWS environment variables

    Returns:
        Exit code from ansible-playbook
    """
    playbook_path = ANSIBLE_DIR / playbook
    cmd = ["uv", "run", "ansible-playbook", str(playbook_path)]

    if tags:
        cmd.extend(["--tags", tags])

    if extra_vars:
        for key, value in extra_vars.items():
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

    result = subprocess.run(cmd, env=env, cwd=PROJECT_HOME)
    return result.returncode


@click.group()
@click.version_option()
def cli():
    """Polaris Local Forge - Manage your local Apache Polaris environment."""
    pass


@cli.command()
@click.option("--tags", "-t", help="Ansible tags to run (comma-separated)")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def prepare(tags: str | None, dry_run: bool, verbose: bool):
    """Generate required sensitive files from templates."""
    sys.exit(run_ansible_playbook("prepare.yml", tags=tags, dry_run=dry_run, verbose=verbose))


@cli.group()
def cluster():
    """Cluster management commands."""
    pass


@cluster.command("bootstrap-check")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def cluster_bootstrap_check(dry_run: bool, verbose: bool):
    """Wait for bootstrap deployments to be ready."""
    sys.exit(run_ansible_playbook("cluster_checks.yml", tags="bootstrap", dry_run=dry_run, verbose=verbose))


@cluster.command("polaris-check")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def cluster_polaris_check(dry_run: bool, verbose: bool):
    """Wait for Polaris deployments to be ready."""
    sys.exit(run_ansible_playbook("cluster_checks.yml", tags="polaris", dry_run=dry_run, verbose=verbose))


@cli.group()
def catalog():
    """Catalog management commands."""
    pass


@catalog.command("setup")
@click.option("--tags", "-t", help="Ansible tags to run (comma-separated)")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def catalog_setup(tags: str | None, dry_run: bool, verbose: bool):
    """Set up demo catalog with S3 bucket, catalog, principal, and roles."""
    sys.exit(
        run_ansible_playbook("catalog_setup.yml", tags=tags, dry_run=dry_run, verbose=verbose, with_aws_env=True)
    )


@catalog.command("cleanup")
@click.option("--tags", "-t", help="Ansible tags to run (comma-separated)")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def catalog_cleanup(tags: str | None, dry_run: bool, verbose: bool):
    """Cleanup Polaris catalog resources."""
    sys.exit(
        run_ansible_playbook("catalog_cleanup.yml", tags=tags, dry_run=dry_run, verbose=verbose, with_aws_env=True)
    )


@catalog.command("generate-notebook")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def catalog_generate_notebook(dry_run: bool, verbose: bool):
    """Generate and prepare verification notebook."""
    exit_code = run_ansible_playbook("catalog_setup.yml", tags="verify", dry_run=dry_run, verbose=verbose, with_aws_env=True)
    if exit_code == 0 and not dry_run:
        notebook_path = PROJECT_HOME / "notebooks" / "verify_setup.ipynb"
        click.echo(f"\nRun the notebook at: {notebook_path}")
    sys.exit(exit_code)


@cli.group()
def polaris():
    """Polaris management commands."""
    pass


@polaris.command("check")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def polaris_check(dry_run: bool, verbose: bool):
    """Ensure all Polaris deployments and jobs have succeeded."""
    sys.exit(run_ansible_playbook("cluster_checks.yml", tags="polaris", dry_run=dry_run, verbose=verbose))


if __name__ == "__main__":
    cli()
