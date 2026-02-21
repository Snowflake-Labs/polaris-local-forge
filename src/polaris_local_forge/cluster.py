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

"""Cluster management commands for Polaris Local Forge.

This module provides k3d cluster operations: create, delete, wait, list, status.
"""

import json
import subprocess
import sys
import time

import click

from polaris_local_forge.common import (
    SKILL_DIR,
    get_config,
    run_ansible,
)
from polaris_local_forge.container_runtime import (
    get_runtime_env,
    check_runtime_available,
)


# =============================================================================
# Helper Functions for Cluster State Detection
# =============================================================================

def _cluster_exists(cluster_name: str, env: dict) -> tuple[bool, bool]:
    """Check if cluster exists in k3d registry and if it's running.
    
    Args:
        cluster_name: Name of the cluster to check
        env: Environment variables for subprocess
        
    Returns:
        (exists, running) tuple where:
        - exists: True if cluster is registered in k3d
        - running: True if cluster has running servers
    """
    try:
        result = subprocess.run(
            ["k3d", "cluster", "list", "-o", "json"],
            env=env, capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            return False, False
        
        stdout = result.stdout.strip()
        if not stdout:
            return False, False
            
        clusters = json.loads(stdout)
        
        # k3d may return a list or a single object depending on version
        if isinstance(clusters, dict):
            clusters = [clusters]
        elif not isinstance(clusters, list):
            return False, False
            
        for c in clusters:
            if c.get("name") == cluster_name:
                running = c.get("serversRunning", 0) > 0
                return True, running
        return False, False
    except (json.JSONDecodeError, subprocess.TimeoutExpired, Exception):
        return False, False


def _detect_ghost_cluster(cluster_name: str, env: dict, cfg: dict) -> bool:
    """Detect ghost cluster by checking for Docker resources without k3d registry entry.
    
    A ghost cluster exists when:
    - k3d cluster list shows no cluster
    - BUT Docker/Podman has containers, networks, or volumes with k3d-{cluster_name} prefix
    
    Args:
        cluster_name: Name of the cluster to check
        env: Environment variables for subprocess
        cfg: Configuration dict with PLF_CONTAINER_RUNTIME
        
    Returns:
        True if ghost cluster detected
    """
    exists, _ = _cluster_exists(cluster_name, env)
    if exists:
        return False
    
    runtime = cfg.get("PLF_CONTAINER_RUNTIME", "docker")
    docker_cmd = "docker" if runtime == "docker" else "podman"
    
    result = subprocess.run(
        [docker_cmd, "ps", "-a", "--filter", f"name=k3d-{cluster_name}", "--format", "{{.Names}}"],
        env=env, capture_output=True, text=True
    )
    if result.returncode == 0 and result.stdout.strip():
        return True
    
    result = subprocess.run(
        [docker_cmd, "network", "ls", "--filter", f"name=k3d-{cluster_name}", "--format", "{{.Name}}"],
        env=env, capture_output=True, text=True
    )
    if result.returncode == 0 and result.stdout.strip():
        return True
    
    return False


def _cleanup_ghost_cluster(cluster_name: str, env: dict) -> bool:
    """Clean up ghost cluster references using k3d delete --all.
    
    Args:
        cluster_name: Name of the cluster to clean up
        env: Environment variables for subprocess
        
    Returns:
        True if cleanup succeeded
    """
    click.echo(f"Cleaning up stale references for '{cluster_name}'...")
    result = subprocess.run(
        ["k3d", "cluster", "delete", cluster_name, "--all"],
        env=env, capture_output=True, text=True
    )
    return result.returncode == 0


def _wait_for_api_server(env: dict, timeout: int = 120) -> tuple[bool, float]:
    """Wait for k3s API server to be ready using exponential backoff.
    
    Args:
        env: Environment variables for subprocess (must include KUBECONFIG)
        timeout: Maximum seconds to wait (default: 120)
        
    Returns:
        (ready, elapsed) tuple where:
        - ready: True if API server is responding
        - elapsed: Time in seconds waited
    """
    start = time.time()
    delay = 2.0
    max_delay = 10.0
    
    while True:
        elapsed = time.time() - start
        if elapsed >= timeout:
            return False, elapsed
        
        result = subprocess.run(
            ["kubectl", "get", "namespaces"],
            env=env, capture_output=True, text=True
        )
        if result.returncode == 0:
            return True, elapsed
        
        time.sleep(delay)
        delay = min(delay * 1.5, max_delay)


@click.group()
def cluster():
    """Kubernetes cluster management."""
    pass


@cluster.command("create")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--force", "-f", is_flag=True, help="Clean up stale references before creating")
@click.option("--wait-timeout", type=int, default=120,
              help="Seconds to wait for API server (default: 120)")
@click.option("--skip-wait", is_flag=True, help="Skip API server readiness check")
@click.pass_context
def cluster_create(ctx, dry_run: bool, force: bool, wait_timeout: int, skip_wait: bool):
    """Create k3d cluster using config/cluster-config.yaml."""
    work_dir = ctx.obj["WORK_DIR"]
    # Protect source directory from accidental initialization
    if work_dir.resolve() == SKILL_DIR.resolve() and not (work_dir / ".env").exists():
        click.echo("Error: Cannot run cluster create in source directory.", err=True)
        click.echo("Use --work-dir to specify a project directory.", err=True)
        sys.exit(1)
    # Auto-run init if .env doesn't exist
    if not (work_dir / ".env").exists():
        click.echo("Project not initialized. Running 'init' first...")
        from polaris_local_forge.cli import init_project
        ctx.invoke(init_project)
        ctx.obj["CONFIG"] = get_config(work_dir)
    cfg = ctx.obj["CONFIG"]
    k8s_dir = ctx.obj["K8S_DIR"]
    config_file = SKILL_DIR / "config" / "cluster-config.yaml"
    cluster_name = cfg["K3D_CLUSTER_NAME"]

    env = get_runtime_env(cfg)
    env["KUBECONFIG"] = str(work_dir / ".kube" / "config")
    env["K3D_CLUSTER_NAME"] = cluster_name
    env["K3S_VERSION"] = cfg["K3S_VERSION"]
    env["FEATURES_DIR"] = str(k8s_dir)

    # --- Pre-creation validation ---
    exists, running = _cluster_exists(cluster_name, env)
    ghost = _detect_ghost_cluster(cluster_name, env, cfg) if not exists else False

    if dry_run:
        cmd = ["k3d", "cluster", "create", "--config", str(config_file)]
        click.echo(f"Would run: {' '.join(cmd)}")
        click.echo(f"  K3D_CLUSTER_NAME={cluster_name}")
        click.echo(f"  K3S_VERSION={env['K3S_VERSION']}")
        click.echo(f"  FEATURES_DIR={env['FEATURES_DIR']}")
        if exists:
            click.echo(f"  Note: Cluster '{cluster_name}' already exists (running={running})")
            if force:
                click.echo(f"  Would delete existing cluster first (--force)")
        elif ghost:
            click.echo(f"  Note: Ghost cluster detected for '{cluster_name}'")
            if force:
                click.echo(f"  Would clean up stale references first (--force)")
        if not skip_wait:
            click.echo(f"  Would wait for API server (timeout: {wait_timeout}s)")
        return

    # Check if cluster already exists
    if exists:
        if running:
            click.echo(
                f"Error: Cluster '{cluster_name}' is already running.\n"
                f"Use `plf cluster delete` first, or `plf setup` to resume.",
                err=True
            )
            sys.exit(1)
        else:
            if not force:
                click.echo(
                    f"Error: Cluster '{cluster_name}' exists but is stopped.\n"
                    f"Use `--force` to recreate, or start with: k3d cluster start {cluster_name}",
                    err=True
                )
                sys.exit(1)
            click.echo(f"Deleting existing stopped cluster '{cluster_name}'...")
            subprocess.run(["k3d", "cluster", "delete", cluster_name], env=env)

    # Check for ghost cluster
    if ghost:
        if not force:
            click.echo(
                f"Error: Ghost cluster detected for '{cluster_name}'.\n"
                f"Stale Docker resources exist but cluster is not registered in k3d.\n"
                f"Use `--force` to clean up stale references, or run:\n"
                f"  k3d cluster delete {cluster_name} --all",
                err=True
            )
            sys.exit(1)
        if not _cleanup_ghost_cluster(cluster_name, env):
            click.echo(f"Warning: Failed to clean up ghost cluster, attempting create anyway...")

    # --- Create cluster ---
    click.echo(f"Creating k3d cluster '{cluster_name}'...")
    cmd = ["k3d", "cluster", "create", "--config", str(config_file)]
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    if result.returncode != 0:
        stderr = result.stderr or ""
        if "already exists" in stderr.lower():
            if force:
                click.echo("Creation failed with 'already exists' - cleaning up and retrying...")
                _cleanup_ghost_cluster(cluster_name, env)
                result = subprocess.run(cmd, env=env)
                if result.returncode != 0:
                    sys.exit(result.returncode)
            else:
                click.echo(
                    f"Error: Ghost cluster detected during creation.\n"
                    f"Use `--force` to clean up stale references.",
                    err=True
                )
                sys.exit(1)
        else:
            click.echo(f"Error creating cluster:\n{stderr}", err=True)
            sys.exit(result.returncode)

    # --- Post-creation: Wait for API server ---
    if skip_wait:
        click.echo(f"Cluster '{cluster_name}' created (API readiness check skipped).")
        return

    click.echo("Waiting for API server to be ready...")
    ready, elapsed = _wait_for_api_server(env, timeout=wait_timeout)

    if ready:
        click.echo(f"Cluster '{cluster_name}' created successfully! (API ready in {elapsed:.1f}s)")
    else:
        click.echo(
            f"Warning: API server not ready after {wait_timeout}s.\n"
            f"Run `plf cluster wait` to check status, or `kubectl get namespaces` to verify.",
            err=True
        )
        sys.exit(1)


@cluster.command("delete")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.pass_context
def cluster_delete(ctx, dry_run: bool, yes: bool):
    """Delete k3d cluster."""
    cfg = ctx.obj["CONFIG"]
    cluster_name = cfg["K3D_CLUSTER_NAME"]
    env = get_runtime_env(cfg)

    if not yes and not dry_run:
        if not click.confirm(f"Delete cluster '{cluster_name}'?"):
            click.echo("Aborted.")
            return

    cmd = ["k3d", "cluster", "delete", cluster_name]

    if dry_run:
        click.echo(f"Would run: {' '.join(cmd)}")
        return

    result = subprocess.run(cmd, env=env)
    sys.exit(result.returncode)


@cluster.command("wait")
@click.option("--tags", "-t", help="Ansible tags: bootstrap (rustfs+pg), polaris")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def cluster_wait(ctx, tags: str | None, dry_run: bool, verbose: bool):
    """Wait for cluster resources to be ready (wraps cluster_checks.yml)."""
    cfg = ctx.obj["CONFIG"]
    work_dir = ctx.obj["WORK_DIR"]

    if not check_runtime_available(cfg):
        click.echo("Container runtime not running. Run: plf doctor --fix")
        sys.exit(1)

    # require_aws=False because cluster_checks.yml only uses kubernetes.core
    exit_code = run_ansible("cluster_checks.yml", work_dir, tags=tags,
                           dry_run=dry_run, verbose=verbose, require_aws=False)
    sys.exit(exit_code)


@cluster.command("list")
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text",
              help="Output format")
@click.pass_context
def cluster_list(ctx, output: str):
    """List k3d clusters."""
    cfg = ctx.obj["CONFIG"]
    env = get_runtime_env(cfg)

    if not check_runtime_available(cfg):
        if output == "json":
            click.echo(json.dumps({"error": "runtime_not_running", "clusters": []}))
        else:
            click.echo("Container runtime not running. Run: plf doctor --fix")
        sys.exit(1)

    result = subprocess.run(
        ["k3d", "cluster", "list", "-o", "json"],
        env=env, capture_output=True, text=True
    )

    if result.returncode != 0:
        click.echo(result.stderr, err=True)
        sys.exit(result.returncode)

    if output == "json":
        click.echo(result.stdout)
    else:
        clusters = json.loads(result.stdout) if result.stdout.strip() else []
        if not clusters:
            click.echo("No clusters found.")
        else:
            click.echo("k3d Clusters:")
            for c in clusters:
                name = c.get("name", "unknown")
                servers = c.get("serversCount", 0)
                agents = c.get("agentsCount", 0)
                running = c.get("serversRunning", 0)
                click.echo(f"  {name}: {running}/{servers} servers, {agents} agents")


@cluster.command("status")
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text",
              help="Output format")
@click.pass_context
def cluster_status(ctx, output: str):
    """Show cluster and services status."""
    cfg = ctx.obj["CONFIG"]
    work_dir = ctx.obj["WORK_DIR"]
    cluster_name = cfg["K3D_CLUSTER_NAME"]
    env = get_runtime_env(cfg)
    env["KUBECONFIG"] = str(work_dir / ".kube" / "config")

    if not check_runtime_available(cfg):
        if output == "json":
            click.echo(json.dumps({"error": "runtime_not_running"}))
        else:
            click.echo("Container runtime not running. Run: plf doctor --fix")
        sys.exit(1)

    status = {"cluster": cluster_name, "services": {}}

    # Check cluster exists
    result = subprocess.run(
        ["k3d", "cluster", "list", "-o", "json"],
        env=env, capture_output=True, text=True
    )
    clusters = json.loads(result.stdout) if result.returncode == 0 and result.stdout.strip() else []
    cluster_info = next((c for c in clusters if c.get("name") == cluster_name), None)

    if not cluster_info:
        status["state"] = "not_found"
        if output == "json":
            click.echo(json.dumps(status))
        else:
            click.echo(f"Cluster '{cluster_name}' not found.")
        sys.exit(1)

    status["state"] = "running" if cluster_info.get("serversRunning", 0) > 0 else "stopped"

    # Get pod status if cluster is running
    if status["state"] == "running":
        for ns, label in [("rustfs", "app=rustfs"), ("polaris", "app=polaris"),
                          ("polaris", "app=postgresql")]:
            result = subprocess.run(
                ["kubectl", "get", "pods", "-n", ns, "-l", label, "-o", "json"],
                env=env, capture_output=True, text=True
            )
            if result.returncode == 0:
                pods = json.loads(result.stdout).get("items", [])
                for pod in pods:
                    name = pod["metadata"]["name"]
                    phase = pod["status"].get("phase", "Unknown")
                    status["services"][name] = phase

    if output == "json":
        click.echo(json.dumps(status, indent=2))
    else:
        click.echo(f"Cluster: {cluster_name} ({status['state']})")
        if status["services"]:
            click.echo("Services:")
            for svc, state in status["services"].items():
                click.echo(f"  {svc}: {state}")
        else:
            click.echo("No services found (cluster may not be ready).")
