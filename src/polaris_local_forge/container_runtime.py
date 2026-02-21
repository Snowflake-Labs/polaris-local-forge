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

"""Container runtime detection and management for Polaris Local Forge.

This module provides functions for:
- Detecting which container runtime (Docker/Podman) is available and running
- Getting the correct DOCKER_HOST for Podman on macOS (SSH-based)
- Managing Podman machine state and SSH configuration
- Handling gvproxy port conflicts
"""

import json
import os
import platform
import shutil
import subprocess
from pathlib import Path

__all__ = [
    "is_docker_running",
    "is_podman_running",
    "detect_container_runtime",
    "get_podman_ssh_uri",
    "get_runtime_env",
    "get_podman_machine_state",
    "check_runtime_available",
    "get_podman_identity",
    "setup_ssh_config",
    "kill_gvproxy",
    "check_port",
]


def is_docker_running() -> bool:
    """Check if Docker Desktop (or similar) is running.
    
    Returns True if /var/run/docker.sock exists and docker info succeeds.
    
    Note: Explicitly uses the default Docker socket to avoid interference
    from any DOCKER_HOST environment variable (e.g., pointing to Podman).
    """
    docker_sock = Path("/var/run/docker.sock")
    if not docker_sock.exists():
        return False
    try:
        env = os.environ.copy()
        env["DOCKER_HOST"] = f"unix://{docker_sock}"
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True, timeout=5, env=env
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def is_podman_running(machine_name: str = "k3d") -> bool:
    """Check if Podman machine is running (macOS) or Podman daemon is available (Linux).
    
    Args:
        machine_name: Name of the Podman machine to check (macOS only)
        
    Returns:
        True if Podman is available and running
    """
    if platform.system() == "Darwin":
        # macOS: check if Podman machine is running
        try:
            result = subprocess.run(
                ["podman", "machine", "inspect", machine_name, "--format", "{{.State}}"],
                capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0 and result.stdout.strip().lower() == "running"
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False
    else:
        # Linux: check if Podman is responding
        try:
            result = subprocess.run(
                ["podman", "info"],
                capture_output=True, timeout=5
            )
            return result.returncode == 0
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return False


def detect_container_runtime(podman_machine: str = "k3d") -> tuple[str | None, str]:
    """Auto-detect the preferred container runtime based on what's RUNNING.
    
    Detection priority (prefer running over installed):
    1. If Docker is running → "docker"
    2. If Podman machine is running → "podman"
    3. If neither running but both installed → "choice" (caller should prompt)
    4. If neither running but one installed → that one
    5. Return None if nothing found
    
    Args:
        podman_machine: Name of the Podman machine to check
        
    Returns:
        Tuple of (runtime, reason) where runtime is "docker", "podman", "choice", or None
        "choice" indicates both are installed but neither running - caller should prompt user
    """
    docker_running = is_docker_running()
    podman_running = is_podman_running(podman_machine)
    
    if docker_running and podman_running:
        # Both running - prefer Docker (user likely has Docker Desktop as primary)
        return "docker", "Both Docker and Podman are running; using Docker"
    elif docker_running:
        return "docker", "Docker Desktop detected and running"
    elif podman_running:
        return "podman", f"Podman machine '{podman_machine}' detected and running"
    
    # Neither running - check what's installed
    has_docker = shutil.which("docker") is not None
    has_podman = shutil.which("podman") is not None
    
    if has_docker and has_podman:
        # Both installed but neither running - let caller prompt user
        return "choice", "Both Docker and Podman installed but neither running"
    elif has_podman:
        return "podman", "Podman installed (not running - will be started by doctor --fix)"
    elif has_docker:
        return "docker", "Docker installed (not running - start Docker Desktop)"
    
    # Nothing found - return None to signal failure (caller must handle)
    return None, "No container runtime found. Install Docker Desktop or Podman."


def get_podman_ssh_uri(machine_name: str) -> str | None:
    """Get SSH URI for a Podman machine (macOS only).

    On macOS, k3d must use an SSH-based DOCKER_HOST (not a local Unix socket)
    to avoid volume-mount failures inside the Podman VM.
    
    Args:
        machine_name: Name of the Podman machine
        
    Returns:
        SSH URI string or None if not found
    """
    try:
        result = subprocess.run(
            ["podman", "system", "connection", "ls", "--format", "json"],
            capture_output=True, text=True, check=True
        )
        connections = json.loads(result.stdout)
        # Look for the machine's root connection (e.g., "k3d-root")
        for conn in connections:
            if conn.get("Name") == f"{machine_name}-root":
                return conn.get("URI")  # e.g., ssh://root@127.0.0.1:PORT/run/podman/podman.sock
        # Fallback: try exact machine name
        for conn in connections:
            if conn.get("Name") == machine_name:
                return conn.get("URI")
    except (subprocess.CalledProcessError, json.JSONDecodeError, FileNotFoundError):
        pass
    return None


def get_runtime_env(cfg: dict) -> dict:
    """Get environment variables for container runtime.
    
    For Podman on macOS, this sets DOCKER_HOST to the SSH URI so that
    k3d communicates with the Podman VM correctly.
    
    Args:
        cfg: Configuration dict with PLF_CONTAINER_RUNTIME and PLF_PODMAN_MACHINE
        
    Returns:
        Environment dict with DOCKER_HOST set if needed
    """
    env = os.environ.copy()
    runtime = cfg.get("PLF_CONTAINER_RUNTIME") or "podman"
    machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"

    if runtime == "podman" and platform.system() == "Darwin":
        # macOS: use SSH-based DOCKER_HOST for Podman machine
        ssh_uri = get_podman_ssh_uri(machine)
        if ssh_uri:
            env["DOCKER_HOST"] = ssh_uri
    return env


def get_podman_machine_state(machine_name: str) -> str | None:
    """Get Podman machine state (running, stopped, etc.).
    
    Args:
        machine_name: Name of the Podman machine
        
    Returns:
        State string (e.g., "running", "stopped") or None if not found
    """
    try:
        result = subprocess.run(
            ["podman", "machine", "inspect", machine_name, "--format", "{{.State}}"],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            return result.stdout.strip().lower()
    except Exception:
        pass
    return None


def check_runtime_available(cfg: dict) -> bool:
    """Check if container runtime (Podman/Docker) is available.
    
    Args:
        cfg: Configuration dict with PLF_CONTAINER_RUNTIME and PLF_PODMAN_MACHINE
        
    Returns:
        True if the runtime is available and accessible
    """
    runtime = cfg.get("PLF_CONTAINER_RUNTIME") or "podman"
    machine = cfg.get("PLF_PODMAN_MACHINE") or "k3d"

    if platform.system() == "Darwin" and runtime == "podman":
        state = get_podman_machine_state(machine)
        return state == "running"

    # For Docker or Linux Podman, check if daemon is accessible
    try:
        subprocess.run([runtime, "info"], capture_output=True, check=True)
        return True
    except Exception:
        return False


def get_podman_identity(machine_name: str) -> str | None:
    """Get the identity file path for a Podman machine.
    
    Args:
        machine_name: Name of the Podman machine
        
    Returns:
        Path to the SSH identity file or None
    """
    try:
        result = subprocess.run(
            ["podman", "system", "connection", "ls", "--format", "json"],
            capture_output=True, text=True, check=True
        )
        connections = json.loads(result.stdout)
        for conn in connections:
            if conn.get("Name") == f"{machine_name}-root":
                return conn.get("Identity")
    except Exception:
        pass
    return None


def setup_ssh_config(machine_name: str) -> bool:
    """Setup SSH config for Podman machine (macOS only).
    
    Adds the Podman machine's SSH key to the agent and updates ~/.ssh/config
    to allow passwordless SSH to the VM.
    
    Args:
        machine_name: Name of the Podman machine
        
    Returns:
        True if setup succeeded
    """
    identity = get_podman_identity(machine_name)
    if not identity or not Path(identity).exists():
        return False

    # Add SSH key to agent
    subprocess.run(["ssh-add", identity], capture_output=True)

    # Update ~/.ssh/config
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(mode=0o700, exist_ok=True)
    ssh_config = ssh_dir / "config"

    marker = "# polaris-local-forge podman machine"
    if ssh_config.exists() and marker in ssh_config.read_text():
        return True  # Already configured

    config_entry = f"""
{marker}
Host 127.0.0.1
    IdentityFile {identity}
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
"""
    with open(ssh_config, "a") as f:
        f.write(config_entry)
    ssh_config.chmod(0o600)
    return True


def kill_gvproxy() -> bool:
    """Kill gvproxy processes that may be holding ports.
    
    gvproxy is Podman's network proxy that can hold ports (like 19000)
    even after Podman operations complete.
    
    Returns:
        True if processes were killed
    """
    try:
        result = subprocess.run(
            ["pkill", "-9", "gvproxy"],
            capture_output=True
        )
        return result.returncode == 0
    except Exception:
        return False


def check_port(port: int) -> tuple[bool, str | None]:
    """Check if a port is available.
    
    Args:
        port: Port number to check
        
    Returns:
        Tuple of (available, process_name) where process_name is the
        name of the process holding the port if not available
    """
    try:
        result = subprocess.run(
            ["lsof", "-i", f":{port}", "-t"],
            capture_output=True, text=True
        )
        if result.returncode == 0 and result.stdout.strip():
            pid = result.stdout.strip().split("\n")[0]
            ps_result = subprocess.run(
                ["ps", "-p", pid, "-o", "comm="],
                capture_output=True, text=True
            )
            proc_name = ps_result.stdout.strip() if ps_result.returncode == 0 else "unknown"
            return False, proc_name
        return True, None
    except Exception:
        return True, None
