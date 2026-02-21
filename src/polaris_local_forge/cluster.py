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


@click.group()
def cluster():
    """Kubernetes cluster management."""
    pass


@cluster.command("create")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.pass_context
def cluster_create(ctx, dry_run: bool):
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

    env = get_runtime_env(cfg)
    env["KUBECONFIG"] = str(work_dir / ".kube" / "config")
    # Env vars for cluster-config.yaml substitution
    env["K3D_CLUSTER_NAME"] = cfg["K3D_CLUSTER_NAME"]
    env["K3S_VERSION"] = cfg["K3S_VERSION"]
    env["FEATURES_DIR"] = str(k8s_dir)

    cmd = ["k3d", "cluster", "create", "--config", str(config_file)]

    if dry_run:
        click.echo(f"Would run: {' '.join(cmd)}")
        click.echo(f"  K3D_CLUSTER_NAME={env['K3D_CLUSTER_NAME']}")
        click.echo(f"  K3S_VERSION={env['K3S_VERSION']}")
        click.echo(f"  FEATURES_DIR={env['FEATURES_DIR']}")
        return

    result = subprocess.run(cmd, env=env)
    sys.exit(result.returncode)


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
