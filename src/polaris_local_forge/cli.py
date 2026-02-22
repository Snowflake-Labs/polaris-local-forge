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

"""CLI entry point for Polaris Local Forge.

Thin wrapper following kamesh-demo-skills pattern:
- CLI reads .env via load_dotenv(), never writes
- Each command does ONE thing: validate → run subprocess → exit
- SKILL.md handles orchestration, manifest updates, .env updates

Commands are organized into modules:
- cli.py: Main entry, init, doctor, prepare, teardown, runtime
- cluster.py: Kubernetes cluster operations
- polaris_ops.py: Polaris deployment operations
- catalog.py: Catalog management operations
- common.py: Shared utilities
- container_runtime.py: Runtime detection and management
"""

import json
import platform
import re
import shutil
import subprocess
import sys
from pathlib import Path

import click

from polaris_local_forge.common import (
    SKILL_DIR,
    TEMPLATES,
    INIT_DIRECTORIES,
    render_manifest,
    get_config,
    set_env_var,
    run_ansible,
    copy_static_files,
    check_tool,
)
from polaris_local_forge.container_runtime import (
    detect_container_runtime,
    get_runtime_env,
    get_podman_machine_state,
    setup_ssh_config,
    kill_gvproxy,
    check_port,
)
from polaris_local_forge.cluster import cluster
from polaris_local_forge.polaris_ops import polaris
from polaris_local_forge.catalog import catalog
from polaris_local_forge.api import api
from polaris_local_forge.setup import setup


# =============================================================================
# CLI Entry Point
# =============================================================================

def expand_path_callback(ctx, param, value):
    """Expand ~ and validate path exists."""
    if value is None:
        return None
    expanded = Path(value).expanduser()
    if not expanded.exists():
        raise click.BadParameter(f"Directory '{value}' does not exist.")
    if not expanded.is_dir():
        raise click.BadParameter(f"'{value}' is not a directory.")
    return str(expanded)


@click.group()
@click.option("--work-dir", "-w", callback=expand_path_callback,
              help="Project directory (defaults to current directory)")
@click.pass_context
def cli(ctx, work_dir: str | None):
    """Polaris Local Forge - Local Iceberg development environment."""
    ctx.ensure_object(dict)
    ctx.obj["WORK_DIR"] = Path(work_dir).resolve() if work_dir else Path.cwd().resolve()
    ctx.obj["K8S_DIR"] = ctx.obj["WORK_DIR"] / "k8s"
    ctx.obj["CONFIG"] = get_config(ctx.obj["WORK_DIR"])


# Register command groups from modules
cli.add_command(cluster)
cli.add_command(polaris)
cli.add_command(catalog)
cli.add_command(api)
cli.add_command(setup)


# =============================================================================
# Init Command
# =============================================================================

@cli.command("init")
@click.option("--force", "-f", is_flag=True, help="Overwrite existing files")
@click.option("--cluster-name", "-n", help="Cluster name (defaults to directory name)")
@click.option("--with-manifest", "-m", is_flag=True, help="Initialize .snow-utils manifest")
@click.option("--runtime", "-r", type=click.Choice(["docker", "podman"]), 
              help="Container runtime (skips auto-detection prompt)")
@click.pass_context
def init_project(ctx, force: bool, cluster_name: str | None, with_manifest: bool, runtime: str | None):
    """Initialize project directory with .env and configuration files."""
    work_dir = ctx.obj["WORK_DIR"]
    # Protect source directory from accidental initialization
    if work_dir.resolve() == SKILL_DIR.resolve():
        click.echo("Error: Cannot initialize the source directory as a project.", err=True)
        click.echo("Use --work-dir to specify a different directory, or run:", err=True)
        click.echo("  task test:isolated   # Creates isolated test environment", err=True)
        sys.exit(1)
    project_name = cluster_name or work_dir.name
    created = []
    skipped = []

    # Copy template files
    for src_rel, dst_name, mode in TEMPLATES:
        src = SKILL_DIR / src_rel
        dst = work_dir / dst_name
        if src.exists():
            if not dst.exists() or force:
                shutil.copy2(src, dst)
                if mode:
                    dst.chmod(mode)
                created.append(dst_name)
            else:
                skipped.append(dst_name)

    # Create directories
    for d in INIT_DIRECTORIES:
        dir_path = work_dir / d
        if not dir_path.exists():
            dir_path.mkdir(parents=True, exist_ok=True)
            created.append(f"{d}/")

    # Set PROJECT_HOME, K3D_CLUSTER_NAME, and SKILL_DIR in .env
    env_file = work_dir / ".env"
    env_vars_added = []
    if env_file.exists():
        env_content = env_file.read_text()
        additions = []
        # Use regex to match uncommented lines (not starting with #)
        if not re.search(r'^PROJECT_HOME=', env_content, re.MULTILINE):
            additions.append(f"PROJECT_HOME={work_dir}")
            env_vars_added.append(f"PROJECT_HOME={work_dir}")
        if not re.search(r'^K3D_CLUSTER_NAME=', env_content, re.MULTILINE):
            additions.append(f"K3D_CLUSTER_NAME={project_name}")
            env_vars_added.append(f"K3D_CLUSTER_NAME={project_name}")
        if not re.search(r'^SKILL_DIR=', env_content, re.MULTILINE):
            additions.append(f"SKILL_DIR={SKILL_DIR}")
            env_vars_added.append(f"SKILL_DIR={SKILL_DIR}")
        if additions:
            with open(env_file, "a") as f:
                f.write("\n" + "\n".join(additions) + "\n")

        # Auto-detect and set container runtime if not already set
        if not re.search(r'^PLF_CONTAINER_RUNTIME=', env_content, re.MULTILINE):
            if runtime:
                # Runtime explicitly provided via --runtime flag
                detected_runtime = runtime
                click.echo(f"Container runtime: {detected_runtime} (specified via --runtime)")
            else:
                detected_runtime, reason = detect_container_runtime(podman_machine="k3d")
                if detected_runtime is None:
                    click.echo(f"Error: {reason}", err=True)
                    click.echo("Install Docker Desktop or Podman before running init.", err=True)
                    sys.exit(1)
                elif detected_runtime == "choice":
                    # Both installed but neither running - prompt user (interactive mode)
                    # In non-interactive shells (Cortex Code), use --runtime flag instead
                    click.echo(f"\n{reason}")
                    click.echo("\nWhich container runtime would you like to use?")
                    click.echo("  1) Docker - Start Docker Desktop manually")
                    click.echo("  2) Podman - Machine will be created/started by 'doctor --fix'")
                    click.echo("\nTip: For non-interactive mode, use: ./bin/plf init --runtime docker|podman")
                    choice = click.prompt("Enter choice", type=click.Choice(["1", "2"]), default="2")
                    if choice == "1":
                        detected_runtime = "docker"
                        click.echo("\nSelected: Docker")
                        click.echo("Please start Docker Desktop, then run: task doctor")
                    else:
                        detected_runtime = "podman"
                        click.echo("\nSelected: Podman")
                        click.echo("Run 'task doctor -- --fix' to create and start the Podman machine")
                else:
                    click.echo(f"Container runtime: {detected_runtime} ({reason})")
            set_env_var(env_file, "PLF_CONTAINER_RUNTIME", detected_runtime)
            env_vars_added.append(f"PLF_CONTAINER_RUNTIME={detected_runtime}")

    # Initialize manifest if requested
    if with_manifest:
        manifest_dir = work_dir / ".snow-utils"
        manifest_file = manifest_dir / "snow-utils-manifest.md"
        if not manifest_file.exists() or force:
            manifest_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            # Reload config to get the runtime we just set
            cfg = get_config(work_dir)
            container_runtime = cfg.get("PLF_CONTAINER_RUNTIME") or "podman"
            podman_machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"
            manifest_content = render_manifest(
                project_name=project_name,
                container_runtime=container_runtime,
                podman_machine=podman_machine if container_runtime == "podman" else "N/A",
                cluster_name=project_name,
            )
            manifest_file.write_text(manifest_content)
            manifest_file.chmod(0o600)
            created.append(".snow-utils/snow-utils-manifest.md")

    # Create bin/plf wrapper script
    bin_dir = work_dir / "bin"
    plf_script = bin_dir / "plf"
    if not plf_script.exists() or force:
        bin_dir.mkdir(parents=True, exist_ok=True)
        plf_script.write_text(f'''#!/bin/bash
# Polaris Local Forge wrapper - auto-generated by init
source "$(dirname "$0")/../.env" 2>/dev/null
# Use PLF_DEV=1 to force reinstall during development
if [ "$PLF_DEV" = "1" ]; then
  exec uv run --quiet --reinstall-package polaris-local-forge --project "$SKILL_DIR" polaris-local-forge --work-dir "$PROJECT_HOME" "$@"
else
  exec uv run --quiet --project "$SKILL_DIR" polaris-local-forge --work-dir "$PROJECT_HOME" "$@"
fi
''')
        plf_script.chmod(0o755)
        created.append("bin/plf")

    # Output summary
    if created:
        click.echo(f"Created: {', '.join(created)}")
    if env_vars_added:
        for var in env_vars_added:
            click.echo(f"Set: {var}")
    if skipped:
        click.echo(f"Skipped (exists): {', '.join(skipped)}")
    if not created and not skipped and not env_vars_added:
        click.echo("Nothing to do.")


# =============================================================================
# Doctor Command
# =============================================================================

@cli.command("doctor")
@click.option("--fix", is_flag=True, help="Attempt to fix issues automatically")
@click.option("--output", type=click.Choice(["text", "json"]), default="text",
              help="Output format")
@click.pass_context
def doctor(ctx, fix: bool, output: str):
    """Check prerequisites and environment status."""
    work_dir = ctx.obj["WORK_DIR"]
    # Protect source directory from accidental initialization
    if work_dir.resolve() == SKILL_DIR.resolve() and not (work_dir / ".env").exists():
        click.echo("Error: Cannot run doctor in source directory.", err=True)
        click.echo("Use --work-dir to specify a project directory, or run:", err=True)
        click.echo("  task test:isolated   # Creates isolated test environment", err=True)
        sys.exit(1)
    # Auto-run init if .env doesn't exist
    if not (work_dir / ".env").exists():
        click.echo("Project not initialized. Running 'init' first...")
        ctx.invoke(init_project)
        # Reload config after init
        ctx.obj["CONFIG"] = get_config(work_dir)
    cfg = ctx.obj["CONFIG"]
    runtime = cfg.get("PLF_CONTAINER_RUNTIME") or "podman"
    machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"
    is_macos = platform.system() == "Darwin"

    issues = []
    checks = []

    # Check required tools
    required_tools = ["k3d", "kubectl", "uv"]
    if runtime == "podman":
        required_tools.insert(0, "podman")
    else:
        required_tools.insert(0, "docker")

    for tool in required_tools:
        ok = check_tool(tool)
        checks.append({"name": f"tool:{tool}", "ok": ok})
        if not ok:
            issues.append(f"Tool '{tool}' not found in PATH")

    # Check Podman machine (macOS only)
    if runtime == "podman" and is_macos:
        state = get_podman_machine_state(machine)
        if state is None:
            checks.append({"name": f"podman-machine:{machine}", "ok": False, "state": "not found"})
            if fix:
                click.echo(f"Creating Podman machine '{machine}'...")
                init_cmd = [
                    "podman", "machine", "init", machine,
                    "--cpus", "4",
                    "--memory", "8192",
                    "--disk-size", "50"
                ]
                init_result = subprocess.run(init_cmd, capture_output=True, text=True)
                if init_result.returncode == 0:
                    click.echo(f"Created Podman machine '{machine}'")
                    click.echo(f"Starting Podman machine '{machine}'...")
                    start_result = subprocess.run(
                        ["podman", "machine", "start", machine],
                        capture_output=True, text=True
                    )
                    if start_result.returncode == 0:
                        click.echo(f"Started Podman machine '{machine}'")
                        checks[-1]["ok"] = True
                        checks[-1]["state"] = "running"
                    else:
                        issues.append(f"Created machine but failed to start: {start_result.stderr}")
                else:
                    issues.append(f"Failed to create Podman machine: {init_result.stderr}")
            else:
                issues.append(f"Podman machine '{machine}' not found. Run with --fix to create, or manually: podman machine init {machine}")
        elif state != "running":
            checks.append({"name": f"podman-machine:{machine}", "ok": False, "state": state})
            if fix:
                click.echo(f"Starting Podman machine '{machine}'...")
                result = subprocess.run(["podman", "machine", "start", machine], capture_output=True)
                if result.returncode == 0:
                    click.echo(f"Started Podman machine '{machine}'")
                    checks[-1]["ok"] = True
                    checks[-1]["state"] = "running"
                else:
                    issues.append(f"Failed to start Podman machine '{machine}'")
            else:
                issues.append(f"Podman machine '{machine}' is {state}. Run with --fix or: podman machine start {machine}")
        else:
            checks.append({"name": f"podman-machine:{machine}", "ok": True, "state": "running"})

        # Check SSH config
        ssh_config = Path.home() / ".ssh" / "config"
        marker = "# polaris-local-forge podman machine"
        ssh_configured = ssh_config.exists() and marker in ssh_config.read_text()
        checks.append({"name": "ssh-config", "ok": ssh_configured})
        if not ssh_configured:
            if fix:
                click.echo("Setting up SSH config for Podman VM...")
                if setup_ssh_config(machine):
                    click.echo("SSH config updated")
                    checks[-1]["ok"] = True
                else:
                    issues.append("Failed to setup SSH config")
            else:
                issues.append("SSH config not configured. Run with --fix or: task podman:setup:machine")

    # Check ports (k3d handles 6443 internally)
    ports = [(19000, "RustFS S3 API"), (19001, "RustFS Console"), (18181, "Polaris API")]
    for port, desc in ports:
        available, proc = check_port(port)
        checks.append({"name": f"port:{port}", "ok": available, "desc": desc, "blocker": proc})
        if not available:
            msg = f"Port {port} ({desc}) in use by {proc}"
            if proc == "gvproxy":
                # Enhanced gvproxy-specific recommendation for port 19000
                if port == 19000:
                    msg = (f"Port {port} ({desc}) blocked by gvproxy (Podman network proxy).\n"
                           f"      Recommendations:\n"
                           f"      - Stop the Podman machine: podman machine stop {machine}\n"
                           f"      - OR switch to Docker: set PLF_CONTAINER_RUNTIME=docker in .env")
                if fix:
                    click.echo(f"Killing gvproxy holding port {port}...")
                    kill_gvproxy()
                    # Recheck
                    available, _ = check_port(port)
                    if available:
                        checks[-1]["ok"] = True
                        click.echo(f"Port {port} now available")
                    else:
                        issues.append(msg)
                else:
                    issues.append(msg)
            else:
                issues.append(msg)

    # Check for ghost clusters (stale k3d references)
    from polaris_local_forge.cluster import _detect_ghost_cluster, _cleanup_ghost_cluster
    cluster_name = cfg.get("K3D_CLUSTER_NAME", work_dir.name)
    env = get_runtime_env(cfg)
    ghost_detected = _detect_ghost_cluster(cluster_name, env, cfg)
    checks.append({"name": f"ghost-cluster:{cluster_name}", "ok": not ghost_detected})

    if ghost_detected:
        if fix:
            click.echo(f"Cleaning up ghost cluster '{cluster_name}'...")
            if _cleanup_ghost_cluster(cluster_name, env):
                click.echo(f"Ghost cluster '{cluster_name}' cleaned up")
                checks[-1]["ok"] = True
            else:
                issues.append(f"Failed to clean up ghost cluster '{cluster_name}'")
        else:
            issues.append(
                f"Ghost cluster detected: '{cluster_name}' has stale Docker resources.\n"
                f"      Run with --fix or: k3d cluster delete {cluster_name} --all"
            )

    # Output results
    if output == "json":
        result = {"checks": checks, "issues": issues, "ok": len(issues) == 0}
        click.echo(json.dumps(result, indent=2))
    else:
        click.echo("Polaris Local Forge - Environment Check")
        click.echo("=" * 40)
        for check in checks:
            status = "OK" if check["ok"] else "FAIL"
            name = check["name"]
            extra = ""
            if "state" in check:
                extra = f" ({check['state']})"
            elif "blocker" in check and check["blocker"]:
                extra = f" (blocked by {check['blocker']})"
            click.echo(f"  [{status}] {name}{extra}")

        if issues:
            click.echo("")
            click.echo("Issues:")
            for issue in issues:
                click.echo(f"  - {issue}")
            sys.exit(1)
        else:
            click.echo("")
            click.echo("All checks passed!")


# =============================================================================
# Prepare Command
# =============================================================================

@cli.command()
@click.option("--tags", "-t", help="Ansible tags (comma-separated)")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.pass_context
def prepare(ctx, tags: str | None, dry_run: bool, verbose: bool):
    """Generate configuration files from templates."""
    work_dir = ctx.obj["WORK_DIR"]
    # Protect source directory from accidental initialization
    if work_dir.resolve() == SKILL_DIR.resolve() and not (work_dir / ".env").exists():
        click.echo("Error: Cannot run prepare in source directory.", err=True)
        click.echo("Use --work-dir to specify a project directory.", err=True)
        sys.exit(1)
    # Auto-run init if .env doesn't exist
    if not (work_dir / ".env").exists():
        click.echo("Project not initialized. Running 'init' first...")
        ctx.invoke(init_project)
        ctx.obj["CONFIG"] = get_config(work_dir)
    # require_aws=False because prepare.yml CREATES .aws/config
    exit_code = run_ansible("prepare.yml", work_dir, tags=tags, dry_run=dry_run,
                            verbose=verbose, require_aws=False)
    if exit_code == 0 and not dry_run:
        copy_static_files(work_dir)
    sys.exit(exit_code)


# =============================================================================
# Teardown Command
# =============================================================================

@cli.command("teardown")
@click.option("--dry-run", "-n", is_flag=True, help="Preview without executing")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
@click.option("--stop-podman/--no-stop-podman", default=None,
              help="Stop Podman machine after teardown (macOS only)")
@click.pass_context
def teardown(ctx, dry_run: bool, yes: bool, stop_podman: bool | None):
    """Complete teardown - cleanup catalog, delete cluster, optionally stop Podman."""
    cfg = ctx.obj["CONFIG"]
    work_dir = ctx.obj["WORK_DIR"]
    cluster_name = cfg["K3D_CLUSTER_NAME"]
    runtime = cfg.get("PLF_CONTAINER_RUNTIME") or "podman"
    machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"
    is_macos = platform.system() == "Darwin"
    env = get_runtime_env(cfg)
    env["KUBECONFIG"] = str(work_dir / ".kube" / "config")

    if not yes and not dry_run:
        msg = f"Teardown will:\n  - Delete cluster '{cluster_name}' (includes all data)"
        if is_macos and runtime == "podman":
            msg += f"\n  - Optionally stop Podman machine '{machine}'"
        click.echo(msg)
        if not click.confirm("Proceed?"):
            click.echo("Aborted.")
            return

    # Note: No catalog cleanup needed - cluster deletion wipes everything
    # (RustFS, PostgreSQL, Polaris, all data). Saves time and energy.

    if dry_run:
        click.echo("Would run:")
        click.echo(f"  Delete cluster: k3d cluster delete {cluster_name}")
        if is_macos and runtime == "podman":
            click.echo(f"  Stop Podman: podman machine stop {machine} (if confirmed)")
        return

    click.echo(f"\n=== Delete cluster ===")
    subprocess.run(["k3d", "cluster", "delete", cluster_name], env=env)

    # Handle Podman machine stop (macOS only)
    if is_macos and runtime == "podman":
        state = get_podman_machine_state(machine)
        if state == "running":
            if stop_podman is None:
                if yes:
                    # Non-interactive mode: default to stopping Podman
                    stop_podman = True
                    click.echo(f"\nStopping Podman machine '{machine}' (use --no-stop-podman to keep running)...")
                else:
                    stop_podman = click.confirm(
                        f"\nStop Podman machine '{machine}' to release ports?",
                        default=True
                    )
            if stop_podman:
                if not yes:
                    click.echo(f"\nStopping Podman machine '{machine}'...")
                subprocess.run(["podman", "machine", "stop", machine])
                click.echo("Podman machine stopped.")

    click.echo("\nTeardown complete.")


# =============================================================================
# Runtime Commands
# =============================================================================

@cli.group()
def runtime():
    """Container runtime utilities."""
    pass


@runtime.command("docker-host")
@click.pass_context
def runtime_docker_host(ctx):
    """Output DOCKER_HOST value for current runtime.

    Used by Taskfile for consistent DOCKER_HOST across workflows.
    Outputs SSH URI for Podman on macOS, empty for Docker/Linux.
    
    Only outputs a value when PLF_CONTAINER_RUNTIME is explicitly set to 'podman'.
    If runtime is not configured, outputs nothing (allows Docker detection to work).
    """
    cfg = ctx.obj["CONFIG"]
    runtime_val = cfg.get("PLF_CONTAINER_RUNTIME")
    if runtime_val != "podman":
        return
    env = get_runtime_env(cfg)
    docker_host = env.get("DOCKER_HOST", "")
    if docker_host:
        click.echo(docker_host)


@runtime.command("detect")
@click.option("--json", "as_json", is_flag=True, help="Output as JSON for programmatic use")
@click.pass_context
def runtime_detect(ctx, as_json: bool):
    """Detect and display current container runtime.
    
    Exit codes:
      0 - Runtime detected (docker or podman)
      2 - User choice required (both installed, neither running)
      1 - Error (neither installed)
      
    With --json flag, outputs structured data for agent parsing:
      {"status": "detected|choice|error", "runtime": "...", "reason": "..."}
    """
    import json as json_lib
    cfg = ctx.obj["CONFIG"]
    machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"
    detected, reason = detect_container_runtime(podman_machine=machine)
    
    if as_json:
        if detected == "choice":
            result = {"status": "choice", "runtime": None, "reason": reason,
                      "options": ["docker", "podman"]}
            click.echo(json_lib.dumps(result))
            sys.exit(2)
        elif detected:
            result = {"status": "detected", "runtime": detected, "reason": reason}
            click.echo(json_lib.dumps(result))
        else:
            result = {"status": "error", "runtime": None, "reason": reason}
            click.echo(json_lib.dumps(result))
            sys.exit(1)
    else:
        if detected == "choice":
            click.echo(f"choice: {reason}")
            click.echo("Use --runtime docker|podman with init command")
            sys.exit(2)
        elif detected:
            click.echo(f"{detected}: {reason}")
        else:
            click.echo(f"Error: {reason}", err=True)
            sys.exit(1)


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    cli()
