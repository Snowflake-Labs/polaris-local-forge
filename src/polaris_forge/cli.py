"""CLI entry point for Polaris Local Forge."""

import os
import platform
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

import click
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project root directory (src/polaris_forge/cli.py -> src/polaris_forge -> src -> project root)
PROJECT_HOME = Path(__file__).parent.parent.parent.resolve()
ANSIBLE_DIR = PROJECT_HOME / "polaris-forge-setup"
BIN_DIR = PROJECT_HOME / "bin"
K8S_DIR = PROJECT_HOME / "k8s"
CONFIG_DIR = PROJECT_HOME / "config"

# Default cluster configuration
DEFAULT_CLUSTER_NAME = "polaris-local-forge"
DEFAULT_K3S_VERSION = "v1.31.5-k3s1"


def get_cluster_env() -> dict:
    """Get cluster environment variables."""
    return {
        "K3D_CLUSTER_NAME": os.getenv("K3D_CLUSTER_NAME", DEFAULT_CLUSTER_NAME),
        "K3S_VERSION": os.getenv("K3S_VERSION", DEFAULT_K3S_VERSION),
        "FEATURES_DIR": str(K8S_DIR),
        "KUBECONFIG": os.getenv("KUBECONFIG", str(PROJECT_HOME / ".kube" / "config")),
    }


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


def get_kubectl_url(k3s_version: str) -> str:
    """Get kubectl download URL for current platform."""
    k8s_version = k3s_version.split("-")[0].split("+")[0]
    os_name = platform.system().lower()
    arch = platform.machine()
    if arch == "x86_64":
        arch = "amd64"
    elif arch == "aarch64" or arch == "arm64":
        arch = "arm64"
    return f"https://dl.k8s.io/release/{k8s_version}/bin/{os_name}/{arch}/kubectl"


def download_kubectl(k3s_version: str, dest: Path) -> None:
    """Download kubectl binary for the specified k3s version."""
    url = get_kubectl_url(k3s_version)
    click.echo(f"Downloading kubectl from {url}...")
    urllib.request.urlretrieve(url, dest)
    dest.chmod(0o755)
    click.echo(f"kubectl downloaded to {dest}")


@cluster.command("create")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
def cluster_create(dry_run: bool):
    """Create the k3d cluster and deploy initial components.

    This downloads kubectl and creates a k3d cluster using the project's configuration.
    """
    cluster_env = get_cluster_env()
    env = os.environ.copy()
    env.update(cluster_env)

    kubectl_path = BIN_DIR / "kubectl"
    cluster_config = CONFIG_DIR / "cluster-config.yaml"

    if dry_run:
        click.echo("[DRY RUN] Would execute the following steps:")
        click.echo(f"  1. Create directory: {Path(cluster_env['KUBECONFIG']).parent}")
        click.echo(f"  2. Download kubectl for k3s version: {cluster_env['K3S_VERSION']}")
        click.echo(f"     URL: {get_kubectl_url(cluster_env['K3S_VERSION'])}")
        click.echo(f"     Destination: {kubectl_path}")
        click.echo(f"  3. Create k3d cluster: {cluster_env['K3D_CLUSTER_NAME']}")
        click.echo(f"     Config: {cluster_config}")
        sys.exit(0)

    kubeconfig_dir = Path(cluster_env["KUBECONFIG"]).parent
    kubeconfig_dir.mkdir(parents=True, exist_ok=True)

    BIN_DIR.mkdir(parents=True, exist_ok=True)
    download_kubectl(cluster_env["K3S_VERSION"], kubectl_path)

    result = subprocess.run(
        ["k3d", "cluster", "create", "--config", str(cluster_config)],
        env=env,
        cwd=PROJECT_HOME,
    )
    sys.exit(result.returncode)


@cluster.command("delete")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
def cluster_delete(dry_run: bool):
    """Delete the k3d cluster and clean up generated files."""
    cluster_env = get_cluster_env()
    env = os.environ.copy()
    env.update(cluster_env)

    files_to_remove = [
        K8S_DIR / "features" / "polaris.yaml",
        K8S_DIR / "features" / "postgresql.yaml",
        K8S_DIR / "polaris" / "polaris-secrets.yaml",
        K8S_DIR / "polaris" / ".bootstrap-credentials.env",
        K8S_DIR / "polaris" / ".polaris.env",
        K8S_DIR / "polaris" / "rsa_key",
        K8S_DIR / "polaris" / "rsa_key.pub",
    ]

    if dry_run:
        click.echo("[DRY RUN] Would execute the following steps:")
        click.echo(f"  1. Delete k3d cluster: {cluster_env['K3D_CLUSTER_NAME']}")
        click.echo("  2. Remove generated files:")
        for f in files_to_remove:
            click.echo(f"     - {f}")
        sys.exit(0)

    result = subprocess.run(
        ["k3d", "cluster", "delete", cluster_env["K3D_CLUSTER_NAME"]],
        env=env,
        cwd=PROJECT_HOME,
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


@catalog.command("verify")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.argument("args", nargs=-1)
def catalog_verify(dry_run: bool, args: tuple):
    """Verify Polaris catalog using DuckDB Iceberg extension (Python)."""
    script_path = PROJECT_HOME / "scripts" / "explore_catalog.py"
    cmd = ["uv", "run", "python", str(script_path)] + list(args)

    env = os.environ.copy()
    env.update(get_aws_env())
    env.pop("AWS_PROFILE", None)

    if dry_run:
        aws_env = get_aws_env()
        env_str = " ".join(f"{k}={v}" for k, v in aws_env.items())
        click.echo(f"[DRY RUN] {env_str} {' '.join(cmd)}")
        sys.exit(0)

    result = subprocess.run(cmd, env=env, cwd=PROJECT_HOME)
    sys.exit(result.returncode)


@catalog.command("verify-sql")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
def catalog_verify_sql(dry_run: bool):
    """Verify Polaris catalog using DuckDB CLI with SQL script."""
    script_path = PROJECT_HOME / "scripts" / "explore_catalog.sql"

    env = os.environ.copy()
    env.update(get_aws_env())
    env.pop("AWS_PROFILE", None)

    if dry_run:
        aws_env = get_aws_env()
        env_str = " ".join(f"{k}={v}" for k, v in aws_env.items())
        click.echo(f"[DRY RUN] {env_str} duckdb < {script_path}")
        sys.exit(0)

    with open(script_path) as f:
        result = subprocess.run(["duckdb"], stdin=f, env=env, cwd=PROJECT_HOME)
    sys.exit(result.returncode)


@catalog.command("explore-sql")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
def catalog_explore_sql(dry_run: bool):
    """Explore Polaris catalog with DuckDB CLI in interactive mode."""
    script_path = PROJECT_HOME / "scripts" / "explore_catalog.sql"

    env = os.environ.copy()
    env.update(get_aws_env())
    env.pop("AWS_PROFILE", None)

    if dry_run:
        aws_env = get_aws_env()
        env_str = " ".join(f"{k}={v}" for k, v in aws_env.items())
        click.echo(f"[DRY RUN] {env_str} duckdb -init {script_path}")
        sys.exit(0)

    result = subprocess.run(["duckdb", "-init", str(script_path)], env=env, cwd=PROJECT_HOME)
    sys.exit(result.returncode)


@cli.group()
def polaris():
    """Polaris management commands."""
    pass


@polaris.command("deploy")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
def polaris_deploy(dry_run: bool):
    """Deploy Polaris to the cluster."""
    polaris_dir = K8S_DIR / "polaris"
    cmd = ["kubectl", "apply", "-k", str(polaris_dir)]

    if dry_run:
        click.echo(f"[DRY RUN] {' '.join(cmd)}")
        sys.exit(0)

    result = subprocess.run(cmd, cwd=PROJECT_HOME)
    sys.exit(result.returncode)


@polaris.command("check")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def polaris_check(dry_run: bool, verbose: bool):
    """Ensure all Polaris deployments and jobs have succeeded."""
    sys.exit(run_ansible_playbook("cluster_checks.yml", tags="polaris", dry_run=dry_run, verbose=verbose))


@polaris.command("purge")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
def polaris_purge(dry_run: bool):
    """Purge Polaris data by running the purge job."""
    if dry_run:
        click.echo("[DRY RUN] Would execute the following steps:")
        click.echo("  1. kubectl patch job polaris-purge -n polaris -p '{\"spec\":{\"suspend\":false}}'")
        click.echo("  2. kubectl wait --for=condition=complete --timeout=300s job/polaris-purge -n polaris")
        click.echo("  3. kubectl logs -n polaris jobs/polaris-purge")
        sys.exit(0)

    subprocess.run(
        ["kubectl", "patch", "job", "polaris-purge", "-n", "polaris", "-p", '{"spec":{"suspend":false}}'],
        cwd=PROJECT_HOME,
    )
    click.echo("Waiting for purge to complete...")
    result = subprocess.run(
        ["kubectl", "wait", "--for=condition=complete", "--timeout=300s", "job/polaris-purge", "-n", "polaris"],
        cwd=PROJECT_HOME,
    )
    subprocess.run(["kubectl", "logs", "-n", "polaris", "jobs/polaris-purge"], cwd=PROJECT_HOME)
    sys.exit(result.returncode)


@polaris.command("bootstrap")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
def polaris_bootstrap(dry_run: bool):
    """Bootstrap Polaris (run after purge)."""
    job_dir = K8S_DIR / "polaris" / "job"

    if dry_run:
        click.echo("[DRY RUN] Would execute the following steps:")
        click.echo(f"  1. kubectl delete -k {job_dir}")
        click.echo(f"  2. kubectl apply -k {job_dir}")
        click.echo("  3. kubectl wait --for=condition=complete --timeout=300s job/polaris-bootstrap -n polaris")
        click.echo("  4. kubectl logs -n polaris jobs/polaris-bootstrap")
        sys.exit(0)

    subprocess.run(["kubectl", "delete", "-k", str(job_dir)], cwd=PROJECT_HOME)
    subprocess.run(["kubectl", "apply", "-k", str(job_dir)], cwd=PROJECT_HOME)
    click.echo("Waiting for bootstrap to complete...")
    result = subprocess.run(
        ["kubectl", "wait", "--for=condition=complete", "--timeout=300s", "job/polaris-bootstrap", "-n", "polaris"],
        cwd=PROJECT_HOME,
    )
    subprocess.run(["kubectl", "logs", "-n", "polaris", "jobs/polaris-bootstrap"], cwd=PROJECT_HOME)
    sys.exit(result.returncode)


@polaris.command("reset")
@click.option("--dry-run", "-n", is_flag=True, help="Show command without executing")
@click.pass_context
def polaris_reset(ctx: click.Context, dry_run: bool):
    """Purge and re-bootstrap Polaris."""
    if dry_run:
        click.echo("[DRY RUN] Would execute: polaris purge, then polaris bootstrap")
        sys.exit(0)

    ctx.invoke(polaris_purge, dry_run=False)
    ctx.invoke(polaris_bootstrap, dry_run=False)


if __name__ == "__main__":
    cli()
