# Podman Setup for k3d

This guide covers setting up [Podman](https://podman.io/) as the container runtime for Polaris Local Forge. Podman is the preferred (and default) runtime -- it is fully open source and ships pre-installed with Cortex Code.

> **Note:** Podman support in k3d is experimental. See [k3d Podman docs](https://k3d.io/stable/usage/advanced/podman) for upstream details.

## Prerequisites

| Tool | Install |
|------|---------|
| Podman >= 4.0 | **Already installed** - Podman is a dependency of Cortex Code |
| k3d >= 5.0 | `brew install k3d` or [k3d.io](https://k3d.io/) |

> **Note:** Podman is a dependency of Cortex Code and should already be installed on your system. If not present, install via `brew install podman` (macOS) or [podman.io](https://podman.io/docs/installation) (Linux).

## macOS Setup

On macOS, Podman runs inside a Linux VM managed by `podman machine`. Polaris Local Forge creates a **dedicated** machine named `k3d` so your default Podman machine stays untouched.

### Quick Setup (Recommended)

Use the Taskfile shortcut to do everything in one command:

```bash
task podman:setup
```

This will:
1. Create a dedicated `k3d` Podman machine (4 CPUs, 16GB RAM)
2. Add the machine's SSH key to your agent (k3d connects to the Podman VM over SSH)
3. Configure cgroup v2 delegation for rootless k3d
4. Create a DNS-enabled `k3d` network
5. Verify the setup

### Manual Setup

#### 1. Create the Dedicated Machine

```bash
podman machine init k3d --cpus 4 --memory 16384 --now
podman system connection default k3d
```

This creates a machine named `k3d` with 4 CPUs and 16GB RAM and sets it as the active connection.

#### 2. Configure cgroup v2 Delegation

k3d requires cpuset cgroup delegation for rootless operation:

```bash
podman machine ssh k3d bash -e <<'EOF'
  printf '[Service]\nDelegate=cpuset\n' | sudo tee /etc/systemd/system/user@.service.d/k3d.conf
  sudo systemctl daemon-reload
  sudo systemctl restart "user@${UID}"
EOF
```

#### 3. Create the k3d Network

Podman's default network has DNS disabled. k3d needs a DNS-enabled network:

```bash
podman --connection k3d network create k3d
```

#### 4. Verify

```bash
task podman:check
```

Or manually:

```bash
podman machine inspect k3d --format '{{.State}}'       # Should be: running
podman --connection k3d info --format '{{.Host.CgroupsVersion}}'  # Should be: v2
podman --connection k3d network inspect k3d -f '{{ .DNSEnabled }}'  # Should be: true
```

### Switching Between Machines

Your default Podman machine is not affected. To switch connections:

```bash
# Use the k3d machine (for polaris-local-forge)
podman system connection default k3d

# Switch back to default
podman system connection default podman-machine-default

# List all connections
podman system connection ls
```

The CLI automatically resolves the `k3d` machine's SSH connection via `PLF_PODMAN_MACHINE` in `.env`, so you don't need to manually switch connections for polaris-local-forge operations.

### How k3d Connects on macOS

On macOS, k3d communicates with the Podman VM over SSH (not a local Unix socket). The CLI reads the machine's SSH URI from `podman system connection ls` and sets `DOCKER_HOST=ssh://root@127.0.0.1:<port>`. This avoids volume-mount failures that occur when k3d tries to mount macOS temp-directory paths inside the VM.

During setup, two things are configured automatically:
1. The Podman machine's SSH key is added to your agent (`ssh-add`).
2. An entry is added to `~/.ssh/config` for `127.0.0.1` with the identity file and `StrictHostKeyChecking no` (Podman VM host keys change on recreation, so strict checking must be disabled for localhost).

### Resizing the Machine

If you need more (or fewer) resources:

```bash
podman machine rm k3d
podman machine init k3d --cpus 8 --memory 32768 --now
podman system connection default k3d
# Re-run cgroup setup
task podman:setup:cgroup
task podman:setup:network
```

## Linux Setup

On Linux, Podman runs natively without a VM. The setup is simpler.

### Quick Setup

```bash
task podman:setup
```

### Manual Setup

#### 1. Install Podman

Follow your distribution's instructions:

- **Fedora/RHEL:** `sudo dnf install podman`
- **Ubuntu/Debian:** `sudo apt install podman`
- **Arch:** `sudo pacman -S podman`

#### 2. Enable the User Socket

```bash
systemctl --user enable --now podman.socket
```

#### 3. Configure cgroup v2 Delegation

```bash
sudo mkdir -p /etc/systemd/system/user@.service.d
sudo tee /etc/systemd/system/user@.service.d/delegate.conf <<'EOF'
[Service]
Delegate=cpu cpuset io memory pids
EOF
sudo systemctl daemon-reload
```

Log out and back in (or reboot) for the cgroup changes to take effect.

#### 4. Create the k3d Network

```bash
podman network create k3d
```

#### 5. Verify

```bash
podman info | grep cgroupVersion    # Should be: v2
podman network inspect k3d -f '{{ .DNSEnabled }}'  # Should be: true
```

### Disable Podman Service Timeout

For reliability, disable the Podman service timeout:

```bash
sudo mkdir -p /etc/containers/containers.conf.d
echo 'service_timeout=0' | sudo tee /etc/containers/containers.conf.d/timeout.conf
```

## Configuration

In your `.env` file:

```bash
# Leave empty to auto-detect (Podman preferred), or set explicitly
PLF_CONTAINER_RUNTIME=podman

# Podman machine name (macOS only, default: k3d)
PLF_PODMAN_MACHINE=k3d
```

## Using Docker Instead

If you prefer Docker, set in `.env`:

```bash
PLF_CONTAINER_RUNTIME=docker
```

No other changes needed. The CLI will use Docker for all k3d operations.

## Troubleshooting

### "Host key verification failed" (macOS)

SSH refuses to connect to the Podman VM. Re-run setup to configure SSH:

```bash
task podman:setup
```

Or fix manually: ensure `~/.ssh/config` has an entry for `127.0.0.1` with `StrictHostKeyChecking no` and the correct `IdentityFile`. See the "How k3d Connects on macOS" section above.

### "mkdir /var/folders/... operation not supported" (macOS)

k3d is using a local Unix socket instead of SSH to reach the Podman VM. Re-run `task podman:setup` to refresh the connection. Verify `podman system connection ls` shows your `k3d` and `k3d-root` connections.

### "No container runtime found"

Neither `podman` nor `docker` is in your `PATH`. Install one of them.

### Machine not running (macOS)

```bash
podman machine start k3d
```

### cgroup v2 not available

Verify cgroup version:

```bash
# macOS
podman --connection k3d info --format '{{.Host.CgroupsVersion}}'

# Linux
podman info --format '{{.Host.CgroupsVersion}}'
```

If it shows `v1`, you need to enable cgroup v2 on your system. On Linux, add `systemd.unified_cgroup_hierarchy=1` to your kernel boot parameters.

### k3d network DNS not enabled

```bash
podman network rm k3d
podman network create k3d
podman network inspect k3d -f '{{ .DNSEnabled }}'  # Should be: true
```

### Missing cpuset cgroup controller (Linux)

If k3d fails with a missing cpuset cgroup controller, the `xdg-document-portal` service may be interfering:

```bash
systemctl --user stop xdg-document-portal.service
```

See [systemd#18293](https://github.com/systemd/systemd/issues/18293#issuecomment-831397578) for details.

### Under-provisioned machine (macOS)

Run `task doctor` to check capacity. If CPUs or memory are too low:

```bash
podman machine rm k3d
podman machine init k3d --cpus 4 --memory 16384 --now
task podman:setup:cgroup
task podman:setup:network
```

## References

- [k3d Podman documentation](https://k3d.io/stable/usage/advanced/podman)
- [Podman installation](https://podman.io/docs/installation)
- [Rootless containers cgroup v2](https://rootlesscontaine.rs/getting-started/common/cgroup2/)
