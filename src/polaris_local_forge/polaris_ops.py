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

"""Polaris deployment commands for Polaris Local Forge.

This module provides Polaris operations: deploy, purge, bootstrap.
"""

import subprocess
import sys

import click

from polaris_local_forge.container_runtime import get_runtime_env


@click.group()
def polaris():
    """Polaris deployment management."""
    pass


@polaris.command("deploy")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.pass_context
def polaris_deploy(ctx, dry_run: bool):
    """Deploy Polaris secrets and Helm chart to the cluster."""
    work_dir = ctx.obj["WORK_DIR"]
    cfg = ctx.obj["CONFIG"]
    k8s_dir = ctx.obj["K8S_DIR"]
    env = get_runtime_env(cfg)
    env["KUBECONFIG"] = str(work_dir / ".kube" / "config")

    polaris_dir = k8s_dir / "polaris"
    polaris_chart = k8s_dir / "features" / "polaris.yaml"

    # Step 1: Create polaris namespace (must exist before secrets)
    cmd_ns = ["kubectl", "create", "namespace", "polaris", "--dry-run=client", "-o", "yaml"]
    cmd_ns_apply = ["kubectl", "apply", "-f", "-"]
    if dry_run:
        click.echo("Would create namespace: polaris")
    else:
        click.echo("Creating polaris namespace...")
        ns_result = subprocess.run(cmd_ns, capture_output=True, env=env)
        if ns_result.returncode == 0:
            subprocess.run(cmd_ns_apply, input=ns_result.stdout, env=env)

    # Step 2: Apply secrets via kustomization (must be before Helm chart)
    cmd_secrets = ["kubectl", "apply", "-k", str(polaris_dir)]
    if dry_run:
        click.echo(f"Would run: {' '.join(cmd_secrets)}")
    else:
        click.echo("Applying Polaris secrets...")
        result = subprocess.run(cmd_secrets, env=env)
        if result.returncode != 0:
            sys.exit(result.returncode)

    # Step 3: Apply Polaris HelmChart
    if polaris_chart.exists():
        cmd_chart = ["kubectl", "apply", "-f", str(polaris_chart)]
        if dry_run:
            click.echo(f"Would run: {' '.join(cmd_chart)}")
        else:
            click.echo("Deploying Polaris Helm chart...")
            result = subprocess.run(cmd_chart, env=env)
            sys.exit(result.returncode)
    else:
        click.echo(f"Warning: Polaris chart not found: {polaris_chart}", err=True)
        sys.exit(1)


@polaris.command("purge")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.pass_context
def polaris_purge(ctx, dry_run: bool):
    """Delete Polaris deployment."""
    work_dir = ctx.obj["WORK_DIR"]
    cfg = ctx.obj["CONFIG"]
    k8s_dir = ctx.obj["K8S_DIR"]
    env = get_runtime_env(cfg)
    env["KUBECONFIG"] = str(work_dir / ".kube" / "config")

    # Apply purge job
    purge_job = k8s_dir / "polaris" / "jobs" / "job-purge.yaml"
    if purge_job.exists():
        cmd = ["kubectl", "apply", "-f", str(purge_job)]
        if dry_run:
            click.echo(f"Would run: {' '.join(cmd)}")
            return
        subprocess.run(cmd, env=env)

    # Delete polaris namespace
    cmd = ["kubectl", "delete", "namespace", "polaris", "--ignore-not-found"]
    if dry_run:
        click.echo(f"Would run: {' '.join(cmd)}")
        return
    subprocess.run(cmd, env=env)


@polaris.command("bootstrap")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.pass_context
def polaris_bootstrap(ctx, dry_run: bool):
    """Run Polaris bootstrap job to create principal and catalog."""
    work_dir = ctx.obj["WORK_DIR"]
    cfg = ctx.obj["CONFIG"]
    k8s_dir = ctx.obj["K8S_DIR"]
    env = get_runtime_env(cfg)
    env["KUBECONFIG"] = str(work_dir / ".kube" / "config")

    bootstrap_job = k8s_dir / "polaris" / "jobs" / "job-bootstrap.yaml"
    cmd = ["kubectl", "apply", "-f", str(bootstrap_job)]

    if dry_run:
        click.echo(f"Would run: {' '.join(cmd)}")
        return

    result = subprocess.run(cmd, env=env)
    sys.exit(result.returncode)
