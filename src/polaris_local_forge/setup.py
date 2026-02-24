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

"""Setup workflow commands for Polaris Local Forge.

This module provides:
- Manifest management (init, status, update, start, complete, remove)
- Runtime setup/teardown
- Replay/resume workflow orchestration

Commands are organized to replace inline Taskfile scripts with a clean CLI interface.
"""

import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

import click

from polaris_local_forge.common import (
    SKILL_DIR,
    ANSIBLE_DIR,
    get_config,
    render_manifest,
    set_env_var,
    prompt_runtime_choice,
)
from polaris_local_forge.container_runtime import (
    detect_container_runtime,
    get_runtime_env,
    get_podman_machine_state,
)


# =============================================================================
# Helper Functions
# =============================================================================

def _detect_runtime(work_dir: Path) -> tuple[str | None, str]:
    """Detect container runtime with priority: .env > env var > auto-detect.
    
    Args:
        work_dir: Project working directory
        
    Returns:
        (runtime, reason) tuple where runtime is "docker", "podman", or None
    """
    env_file = work_dir / ".env"
    if env_file.exists():
        content = env_file.read_text()
        match = re.search(r'^PLF_CONTAINER_RUNTIME=([^\s#]+)', content, re.MULTILINE)
        if match:
            runtime = match.group(1).strip('"').strip("'")
            return runtime, f"from .env"
    
    if os.environ.get("PLF_CONTAINER_RUNTIME"):
        return os.environ["PLF_CONTAINER_RUNTIME"], "from env var"
    
    cfg = get_config(work_dir)
    machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"
    detected, reason = detect_container_runtime(podman_machine=machine)
    
    if detected and detected != "choice":
        return detected, f"auto-detected ({reason})"
    
    if detected == "choice":
        return "choice", reason
    
    return None, reason if reason else "no runtime found"


def _parse_manifest(manifest_path: Path) -> dict | None:
    """Parse manifest markdown into structured dict.
    
    Args:
        manifest_path: Path to snow-utils-manifest.md
        
    Returns:
        Dict with status and resources, or None if not found
    """
    if not manifest_path.exists():
        return None
    
    content = manifest_path.read_text()
    
    status_match = re.search(r'^\*\*Status:\*\*\s*(\w+)', content, re.MULTILINE)
    status = status_match.group(1) if status_match else "UNKNOWN"
    
    resources = {}
    resource_pattern = re.compile(r'^\|\s*(\d+)\s*\|\s*([^|]+)\|\s*([^|]+)\|\s*(\w+)\s*\|', re.MULTILINE)
    for match in resource_pattern.finditer(content):
        num = int(match.group(1))
        name = match.group(2).strip()
        rtype = match.group(3).strip()
        rstatus = match.group(4).strip()
        resources[num] = {"name": name, "type": rtype, "status": rstatus}
    
    config = {}
    for key in ["container_runtime", "podman_machine", "cluster_name", "project_name"]:
        match = re.search(rf'^{key}:\s*(.+)$', content, re.MULTILINE)
        if match:
            config[key] = match.group(1).strip()
    
    return {"status": status, "resources": resources, "config": config}


def _run_manifest_ansible(work_dir: Path, tag: str, extra_vars: dict | None = None) -> int:
    """Run manifest.yml Ansible playbook with specified tag.
    
    Args:
        work_dir: Project working directory
        tag: Ansible tag to run (init, start, update, complete, remove)
        extra_vars: Additional variables to pass to Ansible
        
    Returns:
        Exit code from ansible-playbook
    """
    playbook = ANSIBLE_DIR / "manifest.yml"
    cmd = [
        "uv", "run", "--project", str(SKILL_DIR),
        "ansible-playbook", str(playbook),
        "--tags", tag,
        "-e", f"plf_output_base={work_dir}",
    ]
    
    if extra_vars:
        for key, value in extra_vars.items():
            cmd.extend(["-e", f"{key}={value}"])
    
    env = os.environ.copy()
    env["ANSIBLE_CONFIG"] = str(ANSIBLE_DIR / "ansible.cfg")
    
    result = subprocess.run(cmd, cwd=work_dir, env=env)
    return result.returncode


# =============================================================================
# Setup Command Group
# =============================================================================

@click.group()
@click.pass_context
def setup(ctx):
    """Setup workflow commands."""
    pass


# =============================================================================
# Manifest Subgroup
# =============================================================================

@setup.group()
def manifest():
    """Manifest management commands."""
    pass


@manifest.command("init")
@click.option("--runtime", "-r", type=click.Choice(["docker", "podman"]),
              help="Container runtime (skips auto-detection)")
@click.pass_context
def manifest_init(ctx, runtime: str | None):
    """Initialize manifest with auto-detected runtime.
    
    Creates .snow-utils/snow-utils-manifest.md with all resources set to PENDING.
    Runtime detection priority: --runtime flag > .env > env var > auto-detect.
    """
    work_dir = ctx.obj["WORK_DIR"]
    cfg = ctx.obj["CONFIG"]
    
    if runtime:
        detected_runtime = runtime
        click.echo(f"Runtime: {detected_runtime} (specified via --runtime)")
    else:
        detected_runtime, reason = _detect_runtime(work_dir)
        if detected_runtime == "choice":
            detected_runtime = prompt_runtime_choice(reason)
        elif not detected_runtime:
            click.echo("ERROR: Could not detect container runtime.", err=True)
            click.echo("Ensure Docker or Podman is installed and running, or use --runtime.", err=True)
            sys.exit(1)
        else:
            click.echo(f"Runtime: {detected_runtime} ({reason})")
    
    # Persist to .env so downstream commands (plf init) don't prompt again
    env_file = work_dir / ".env"
    if env_file.exists():
        set_env_var(env_file, "PLF_CONTAINER_RUNTIME", detected_runtime)
    
    project_name = cfg.get("K3D_CLUSTER_NAME") or work_dir.name
    podman_machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"
    
    exit_code = _run_manifest_ansible(work_dir, "init", {
        "project_name": project_name,
        "container_runtime": detected_runtime,
        "podman_machine": podman_machine if detected_runtime == "podman" else "N/A",
        "cluster_name": project_name,
    })
    sys.exit(exit_code)


@manifest.command("status")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON")
@click.pass_context
def manifest_status(ctx, as_json: bool):
    """Get manifest status.
    
    Exit codes:
      0 - Manifest found
      1 - Manifest not found
    """
    work_dir = ctx.obj["WORK_DIR"]
    manifest_path = work_dir / ".snow-utils" / "snow-utils-manifest.md"
    
    parsed = _parse_manifest(manifest_path)
    if not parsed:
        if as_json:
            click.echo(json.dumps({"error": "manifest_not_found"}))
        else:
            click.echo(f"Manifest not found at {manifest_path}", err=True)
        sys.exit(1)
    
    if as_json:
        click.echo(json.dumps(parsed, indent=2))
    else:
        click.echo(f"Status: {parsed['status']}")
        click.echo("")
        click.echo("Resources:")
        for num, res in sorted(parsed["resources"].items()):
            click.echo(f"  {num}. {res['name']}: {res['status']}")


@manifest.command("update")
@click.argument("resource_num", type=int)
@click.pass_context
def manifest_update(ctx, resource_num: int):
    """Update resource status to DONE.
    
    RESOURCE_NUM is 1-7:
      1=k3d cluster, 2=RustFS, 3=PostgreSQL, 4=Polaris,
      5=Catalog, 6=Principal, 7=Demo data
    """
    work_dir = ctx.obj["WORK_DIR"]
    
    if not 1 <= resource_num <= 7:
        click.echo("Error: Resource number must be 1-7", err=True)
        sys.exit(1)
    
    exit_code = _run_manifest_ansible(work_dir, "update", {
        "resource_num": str(resource_num),
    })
    sys.exit(exit_code)


@manifest.command("start")
@click.pass_context
def manifest_start(ctx):
    """Set manifest status to IN_PROGRESS."""
    work_dir = ctx.obj["WORK_DIR"]
    exit_code = _run_manifest_ansible(work_dir, "start")
    sys.exit(exit_code)


@manifest.command("complete")
@click.pass_context
def manifest_complete(ctx):
    """Set manifest status to COMPLETE."""
    work_dir = ctx.obj["WORK_DIR"]
    exit_code = _run_manifest_ansible(work_dir, "complete")
    sys.exit(exit_code)


@manifest.command("remove")
@click.pass_context
def manifest_remove(ctx):
    """Set manifest status to REMOVED."""
    work_dir = ctx.obj["WORK_DIR"]
    exit_code = _run_manifest_ansible(work_dir, "remove")
    sys.exit(exit_code)


# =============================================================================
# Runtime Subgroup
# =============================================================================

@setup.group()
def runtime():
    """Container runtime management."""
    pass


@runtime.command("ensure")
@click.pass_context
def runtime_ensure(ctx):
    """Ensure container runtime is ready.
    
    For Podman on macOS: starts the Podman machine if not running.
    For Docker: verifies Docker daemon is running.
    """
    work_dir = ctx.obj["WORK_DIR"]
    cfg = ctx.obj["CONFIG"]
    is_macos = platform.system() == "Darwin"
    
    runtime_name, reason = _detect_runtime(work_dir)
    if runtime_name == "choice":
        runtime_name = prompt_runtime_choice(reason)
        set_env_var(work_dir / ".env", "PLF_CONTAINER_RUNTIME", runtime_name)
    elif not runtime_name:
        click.echo("ERROR: Could not detect container runtime.", err=True)
        click.echo("Ensure Docker or Podman is installed and running.", err=True)
        sys.exit(1)
    
    click.echo(f"Container runtime: {runtime_name}")
    
    if runtime_name == "podman" and is_macos:
        machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"
        state = get_podman_machine_state(machine)
        
        if state is None:
            click.echo(f"Podman machine '{machine}' not found. Run: plf doctor --fix", err=True)
            sys.exit(1)
        elif state != "running":
            click.echo(f"Starting Podman machine '{machine}'...")
            result = subprocess.run(["podman", "machine", "start", machine])
            if result.returncode != 0:
                click.echo(f"Failed to start Podman machine", err=True)
                sys.exit(1)
            click.echo(f"Podman machine '{machine}' started.")
        else:
            click.echo(f"Podman machine '{machine}' is running.")
    elif runtime_name == "docker":
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            click.echo("Docker daemon is not running. Please start Docker Desktop.", err=True)
            sys.exit(1)
        click.echo("Docker daemon is running.")


@runtime.command("stop")
@click.pass_context
def runtime_stop(ctx):
    """Stop container runtime (Podman machine on macOS)."""
    work_dir = ctx.obj["WORK_DIR"]
    cfg = ctx.obj["CONFIG"]
    is_macos = platform.system() == "Darwin"
    
    runtime_name, reason = _detect_runtime(work_dir)
    if runtime_name == "choice":
        runtime_name = prompt_runtime_choice(reason)
        set_env_var(work_dir / ".env", "PLF_CONTAINER_RUNTIME", runtime_name)
    
    if runtime_name == "podman" and is_macos:
        machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"
        state = get_podman_machine_state(machine)
        
        if state == "running":
            click.echo(f"Stopping Podman machine '{machine}'...")
            subprocess.run(["podman", "machine", "stop", machine])
            click.echo("Podman machine stopped.")
        elif state is None:
            click.echo(f"Podman machine '{machine}' not found.")
        else:
            click.echo(f"Podman machine '{machine}' is already {state}.")
    else:
        click.echo("Runtime stop is only needed for Podman on macOS.")


# =============================================================================
# Workflow Commands
# =============================================================================

@setup.command("replay")
@click.pass_context
def setup_replay(ctx):
    """Check manifest status and return exit code for orchestration.
    
    Exit codes:
      0  - COMPLETE and healthy (nothing to do)
      10 - REMOVED status (caller should run setup:all)
      11 - IN_PROGRESS/PENDING (caller should run setup:resume)
      1  - Error (manifest not found, unknown status)
    """
    work_dir = ctx.obj["WORK_DIR"]
    cfg = ctx.obj["CONFIG"]
    manifest_path = work_dir / ".snow-utils" / "snow-utils-manifest.md"
    
    parsed = _parse_manifest(manifest_path)
    if not parsed:
        click.echo(f"No manifest found at {manifest_path}", err=True)
        click.echo("Run 'task setup:all' for fresh setup.")
        sys.exit(1)
    
    status = parsed["status"]
    click.echo(f"Manifest status: {status}")
    
    if status == "REMOVED":
        click.echo("Starting full replay from REMOVED state...")
        sys.exit(10)
    
    if status in ("IN_PROGRESS", "PENDING"):
        click.echo(f"Resuming from {status} state...")
        sys.exit(11)
    
    if status == "COMPLETE":
        click.echo("Setup already COMPLETE. Verifying cluster health...")
        cluster_name = cfg.get("K3D_CLUSTER_NAME") or work_dir.name
        env = get_runtime_env(cfg)
        env["KUBECONFIG"] = str(work_dir / ".kube" / "config")
        
        result = subprocess.run(
            ["k3d", "cluster", "list", "-o", "json"],
            env=env, capture_output=True, text=True
        )
        clusters = json.loads(result.stdout) if result.returncode == 0 and result.stdout.strip() else []
        cluster_exists = any(c.get("name") == cluster_name for c in clusters)
        
        if cluster_exists:
            click.echo(f"✓ Cluster '{cluster_name}' is running")
            
            result = subprocess.run(
                ["kubectl", "get", "deployment", "polaris", "-n", "polaris",
                 "-o", "jsonpath={.status.readyReplicas}"],
                env=env, capture_output=True, text=True
            )
            ready = result.stdout.strip() if result.returncode == 0 else "0"
            ready_count = int(ready) if ready.isdigit() else 0
            
            if ready_count >= 1:
                click.echo(f"✓ Polaris is healthy ({ready_count} replica(s) ready)")
                click.echo("")
                click.echo("Your environment is ready to use!")
                click.echo("  - Polaris API: http://localhost:18181")
                click.echo("  - RustFS S3:   http://localhost:19000")
                sys.exit(0)
            else:
                click.echo("⚠ Polaris deployment exists but not ready")
                click.echo("  Check with: plf cluster status")
                sys.exit(0)
        else:
            click.echo(f"⚠ Cluster not running but manifest shows COMPLETE")
            click.echo("  Run teardown first, then replay.")
            sys.exit(1)
    
    click.echo(f"Unknown manifest status: {status}", err=True)
    sys.exit(1)


@setup.command("resume")
@click.pass_context
def setup_resume(ctx):
    """Resume setup from IN_PROGRESS manifest.
    
    Checks which resources are still PENDING and runs only those steps.
    Marks each resource DONE after successful completion.
    """
    work_dir = ctx.obj["WORK_DIR"]
    cfg = ctx.obj["CONFIG"]
    manifest_path = work_dir / ".snow-utils" / "snow-utils-manifest.md"
    
    parsed = _parse_manifest(manifest_path)
    if not parsed:
        click.echo(f"No manifest found at {manifest_path}", err=True)
        sys.exit(1)
    
    env = get_runtime_env(cfg)
    env["KUBECONFIG"] = str(work_dir / ".kube" / "config")
    plf_cmd = ["uv", "run", "--project", str(SKILL_DIR), "polaris-local-forge",
               "--work-dir", str(work_dir)]
    
    def is_pending(num: int) -> bool:
        return parsed["resources"].get(num, {}).get("status") == "PENDING"
    
    def mark_done(num: int):
        _run_manifest_ansible(work_dir, "update", {"resource_num": str(num)})
    
    if is_pending(1):
        click.echo("Creating k3d cluster...")
        result = subprocess.run(plf_cmd + ["cluster", "create"], env=env)
        if result.returncode != 0:
            click.echo("Failed to create cluster", err=True)
            sys.exit(1)
        mark_done(1)
    else:
        click.echo("Resource 1 (k3d cluster) is DONE, skipping.")
    
    if is_pending(2) or is_pending(3):
        click.echo("Waiting for bootstrap (RustFS + PostgreSQL)...")
        result = subprocess.run(plf_cmd + ["cluster", "wait", "--tags", "bootstrap"], env=env)
        if result.returncode != 0:
            click.echo("Bootstrap wait failed", err=True)
            sys.exit(1)
        mark_done(2)
        mark_done(3)
    else:
        click.echo("Resources 2-3 (RustFS, PostgreSQL) are DONE, skipping.")
    
    if is_pending(4):
        click.echo("Deploying Polaris...")
        result = subprocess.run(plf_cmd + ["polaris", "deploy"], env=env)
        if result.returncode != 0:
            click.echo("Polaris deploy failed", err=True)
            sys.exit(1)
        result = subprocess.run(plf_cmd + ["polaris", "bootstrap"], env=env)
        if result.returncode != 0:
            click.echo("Polaris bootstrap failed", err=True)
            sys.exit(1)
        result = subprocess.run(plf_cmd + ["cluster", "wait", "--tags", "polaris"], env=env)
        if result.returncode != 0:
            click.echo("Polaris wait failed", err=True)
            sys.exit(1)
        mark_done(4)
    else:
        click.echo("Resource 4 (Polaris) is DONE, skipping.")
    
    if is_pending(6):
        mark_done(6)
    else:
        click.echo("Resource 6 (Principal) is DONE, skipping.")
    
    if is_pending(5):
        click.echo("Setting up catalog...")
        result = subprocess.run(plf_cmd + ["catalog", "setup"], env=env)
        if result.returncode != 0:
            click.echo("Catalog setup failed", err=True)
            sys.exit(1)
        mark_done(5)
    else:
        click.echo("Resource 5 (Catalog) is DONE, skipping.")
    
    if is_pending(7):
        click.echo("Verifying demo data...")
        result = subprocess.run(
            ["uv", "run", "--project", str(SKILL_DIR),
             "ansible-playbook", str(ANSIBLE_DIR / "verify_sql.yml"),
             "-e", f"plf_output_base={work_dir}"],
            env=env, cwd=work_dir
        )
        if result.returncode != 0:
            click.echo("SQL verification failed", err=True)
            sys.exit(1)
        mark_done(7)
    else:
        click.echo("Resource 7 (Demo data) is DONE, skipping.")
    
    _run_manifest_ansible(work_dir, "complete")
    click.echo("")
    click.echo("Resume complete! All resources are DONE.")
