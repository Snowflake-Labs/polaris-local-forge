# Polaris Local Forge

![k3d](https://img.shields.io/badge/k3d-v5.8.0-427cc9)
![Podman](https://img.shields.io/badge/Podman-v4.0+-892CA0)
![Docker Desktop](https://img.shields.io/badge/Docker%20Desktop-v4.27+-0db7ed)
![Apache Polaris](https://img.shields.io/badge/Apache%20Polaris-1.3.0--incubating-blue)
![RustFS](https://img.shields.io/badge/RustFS-1.0.0-orange)

A complete local development environment for [Apache Polaris (Incubating)](https://polaris.apache.org/) with [RustFS](https://rustfs.com/) S3-compatible storage running on k3s Kubernetes.

**Features:**

- Automated k3s cluster setup with k3d
- RustFS for high-performance S3-compatible object storage
- PostgreSQL metastore
- Task-based automation and Python CLI
- DuckDB SQL verification and Jupyter notebook

> **Note**: Looking for LocalStack? Check out the [`localstack`](https://github.com/kameshsampath/polaris-local-forge/tree/localstack) branch.

## Prerequisites

### Required Tools

| Tool | Version | Install |
|------|---------|---------|
| Podman (default) | >= 4.0 | `brew install podman` or [podman.io](https://podman.io/) |
| Docker (alternative) | >= 4.27 | [Docker Desktop](https://www.docker.com/products/docker-desktop/) |
| k3d | >= 5.0.0 | `brew install k3d` or [k3d.io](https://k3d.io/) |
| Python | >= 3.12 | [python.org](https://www.python.org/downloads/) |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Task | latest | `brew install go-task` or [taskfile.dev](https://taskfile.dev) |

### Container Runtime

The CLI **auto-detects** the container runtime during `init` based on what's actually running:

```mermaid
flowchart TD
    Start[init command] --> CheckDockerRunning{Docker Desktop<br/>running?}
    CheckDockerRunning -->|Yes| UseDocker[Use Docker]
    CheckDockerRunning -->|No| CheckPodmanRunning{Podman machine<br/>running?}
    CheckPodmanRunning -->|Yes| UsePodman[Use Podman]
    CheckPodmanRunning -->|No| CheckInstalled{What's installed?}
    CheckInstalled -->|Both| PromptUser[Prompt user<br/>to choose]
    CheckInstalled -->|Podman only| UsePodmanInstalled[Use Podman<br/>doctor --fix starts it]
    CheckInstalled -->|Docker only| UseDockerInstalled[Use Docker<br/>start manually]
    CheckInstalled -->|Neither| Fail[Fail with error]
    PromptUser --> UserChoice{User choice}
    UserChoice -->|1| UseDockerInstalled
    UserChoice -->|2| UsePodmanInstalled
```

**Detection priority:**
1. Running runtime preferred over just installed
2. Docker preferred when both are running
3. User prompted when both installed but neither running

Override auto-detection by setting `PLF_CONTAINER_RUNTIME=docker` or `PLF_CONTAINER_RUNTIME=podman` in `.env`.

**First-time Podman users:** See [docs/podman-setup.md](docs/podman-setup.md) for machine setup, cgroup configuration, and network creation.

### Optional Tools

| Tool | Purpose | Install |
|------|---------|---------|
| DuckDB CLI | SQL verification | `brew install duckdb` |
| direnv | Auto-load env vars | `brew install direnv` |

### Verify Prerequisites

```bash
# Quick health check
task doctor

# Or manually verify
podman --version  # or: docker --version
k3d version
python3 --version
uv --version
task --version
```

## Quick Start

```bash
# Clone the repository
git clone https://github.com/kameshsampath/polaris-local-forge
cd polaris-local-forge

# Setup Python environment
task setup:python

# Deploy everything (cluster + Polaris + catalog)
# This auto-detects Docker/Podman and creates Podman machine if needed
task setup:all

# Or specify a work directory to avoid polluting the source tree
task setup:all WORK_DIR=/path/to/my/project
```

> **Note:** `task setup:all` runs `doctor --fix` which automatically creates and starts the Podman machine if using Podman. For manual Podman setup, run `task podman:setup` first.

> **Using Docker instead?** Start Docker Desktop before running `task setup:all`. The runtime is auto-detected.

> **Isolated setup:** Use `WORK_DIR=/path` to run setup in a separate directory, keeping the source tree clean. The CLI will auto-reject running destructive commands in the source directory.

After setup completes, you'll see:

| Service | URL | Credentials |
|---------|-----|-------------|
| Polaris API | http://localhost:18181 | See `k8s/polaris/.bootstrap-credentials.env` |
| RustFS S3 | http://localhost:19000 | `admin` / `password` |
| RustFS Console | http://localhost:19001 | `admin` / `password` |

## Verify Setup

```bash
# Check status
task status

# Verify with DuckDB SQL
task catalog:verify:sql

# Or use interactive DuckDB
task catalog:explore:sql

# Or run the Jupyter notebook
jupyter notebook notebooks/verify_polaris.ipynb
```

## Task Commands

All operations are available via Task commands:

### Podman Setup (one-time)

| Command | Description |
|---------|-------------|
| `task podman:setup` | Full Podman setup (machine + cgroup + network + verify) |
| `task podman:setup:machine` | macOS: create dedicated `k3d` Podman machine (4 CPUs / 16GB) |
| `task podman:setup:cgroup` | Configure cgroup v2 delegation for rootless k3d |
| `task podman:setup:network` | Create DNS-enabled `k3d` network |
| `task podman:check` | Verify Podman machine is ready with sufficient resources |

### Setup & Teardown

| Command | Description |
|---------|-------------|
| `task setup:all` | Complete setup (cluster + Polaris + catalog) |
| `task setup:all WORK_DIR=/path` | Setup in specified directory (keeps source clean) |
| `task teardown` | Complete teardown (cleanup + delete cluster) |
| `task teardown WORK_DIR=/path` | Teardown specific project directory |
| `task reset:all` | Teardown and setup fresh |

### Status & Config

| Command | Description |
|---------|-------------|
| `task doctor` | Check system prerequisites and health |
| `task doctor:json` | Prerequisites check with JSON output |
| `task status` | Show cluster and Polaris status |
| `task status:detailed` | Detailed kubectl output |
| `task config` | Show current configuration |
| `task urls` | Display service URLs |

### Cluster Management

| Command | Description |
|---------|-------------|
| `task cluster:create` | Create k3d cluster |
| `task cluster:delete` | Delete cluster |
| `task cluster:bootstrap-check` | Wait for bootstrap deployments |
| `task cluster:polaris-check` | Wait for Polaris deployment |
| `task cluster:reset` | Delete and recreate cluster |

### Polaris Operations

| Command | Description |
|---------|-------------|
| `task polaris:deploy` | Deploy Polaris to cluster |
| `task polaris:check` | Verify Polaris deployment |
| `task polaris:reset` | Purge and re-bootstrap Polaris |
| `task polaris:purge` | Purge Polaris data |
| `task polaris:bootstrap` | Bootstrap Polaris |

### Catalog Management

| Command | Description |
|---------|-------------|
| `task catalog:setup` | Setup demo catalog |
| `task catalog:cleanup` | Cleanup catalog resources |
| `task catalog:reset` | Cleanup and recreate catalog |
| `task catalog:list` | List catalogs |
| `task catalog:verify:sql` | Verify with DuckDB (non-interactive) |
| `task catalog:explore:sql` | Explore with DuckDB (interactive) |
| `task catalog:verify:duckdb` | Verify with Python DuckDB |
| `task catalog:generate-notebook` | Generate verification notebook |
| `task catalog:info` | Show catalog configuration |

### Version Management

| Command | Description |
|---------|-------------|
| `task bump:polaris` | Update Polaris to latest Docker Hub version |
| `task bump:polaris:dry-run` | Preview Polaris version update |
| `task bump:k3s` | Update K3S to latest Docker Hub version |
| `task bump:k3s:dry-run` | Preview K3S version update |

### Logs & Troubleshooting

| Command | Description |
|---------|-------------|
| `task logs:polaris` | Stream Polaris logs |
| `task logs:postgresql` | Stream PostgreSQL logs |
| `task logs:rustfs` | Stream RustFS logs |
| `task logs:bootstrap` | View bootstrap job logs |
| `task logs:purge` | View purge job logs |
| `task troubleshoot:polaris` | Diagnose Polaris issues |
| `task troubleshoot:postgresql` | Check PostgreSQL connectivity |
| `task troubleshoot:rustfs` | Verify RustFS connectivity |
| `task troubleshoot:events` | Show recent events |

## CLI Reference

The `polaris-local-forge` CLI provides programmatic control with JSON output support:

```bash
uv run polaris-local-forge --help
```

### Commands

| Command | Description |
|---------|-------------|
| `polaris-local-forge init` | Initialize project directory with .env and configuration |
| `polaris-local-forge init --runtime docker\|podman` | Initialize with explicit runtime (skips interactive prompt) |
| `polaris-local-forge doctor` | Check system prerequisites and health |
| `polaris-local-forge doctor --fix` | Auto-fix issues (create/start Podman machine, kill gvproxy) |
| `polaris-local-forge doctor --output json` | Prerequisites as JSON (for automation/skills) |
| `polaris-local-forge prepare` | Generate configuration files from templates |
| `polaris-local-forge teardown --yes` | Execute teardown (stops Podman by default on macOS) |
| `polaris-local-forge cluster create` | Create k3d cluster |
| `polaris-local-forge cluster delete --yes` | Delete cluster |
| `polaris-local-forge cluster status` | Cluster status |
| `polaris-local-forge cluster status --output json` | Cluster status as JSON |
| `polaris-local-forge polaris deploy` | Deploy Polaris to cluster |
| `polaris-local-forge polaris bootstrap` | Run Polaris bootstrap job |
| `polaris-local-forge polaris purge` | Delete Polaris deployment |
| `polaris-local-forge catalog setup` | Configure Polaris catalog |
| `polaris-local-forge catalog cleanup --yes` | Clean up catalog resources |
| `polaris-local-forge catalog verify-sql` | Run DuckDB verification |
| `polaris-local-forge runtime detect` | Detect and display container runtime |
| `polaris-local-forge runtime detect --json` | Detection result as JSON (for agents) |
| `polaris-local-forge runtime docker-host` | Output DOCKER_HOST for current runtime |

All destructive commands support `--dry-run` to preview and `--yes` to skip confirmation.

## Configuration

Configuration is managed via `.env` file. Copy the example and customize:

```bash
cp .env.example .env
```

Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `PLF_CONTAINER_RUNTIME` | (auto-detect) | `podman` or `docker`; auto-detected during `init` based on what's running |
| `PLF_PODMAN_MACHINE` | `k3d` | Podman machine name (macOS only) |
| `K3D_CLUSTER_NAME` | `polaris-local-forge` | Cluster name |
| `K3S_VERSION` | `v1.31.5-k3s1` | K3S version |
| `AWS_ENDPOINT_URL` | `http://localhost:19000` | RustFS S3 endpoint |
| `POLARIS_URL` | `http://localhost:18181` | Polaris API endpoint |

> **Note:** `PLF_CONTAINER_RUNTIME` is auto-detected during `init`. It prefers running runtimes over installed ones. Set it manually in `.env` only to override auto-detection.

View current configuration:

```bash
task config
# or
uv run polaris-local-forge config
```

## Troubleshooting

### Quick Diagnostics

```bash
task status              # Check deployment status
task troubleshoot:events # View recent events
task logs:polaris        # Stream Polaris logs
```

### Common Issues

**Polaris pod stuck in ContainerCreating:**
```bash
kubectl get events -n polaris --sort-by='.lastTimestamp'
task polaris:deploy  # Re-apply deployment
```

**RustFS not accessible:**
```bash
kubectl get pods -n rustfs
task troubleshoot:rustfs
```

**Bootstrap job fails:**
```bash
task logs:bootstrap
task polaris:reset  # Reset Polaris
```

**Port 19000 blocked by gvproxy (Podman):**

When using Podman, the `gvproxy` network proxy may occupy port 19000 (needed by RustFS). This happens when a previous Podman machine session didn't clean up properly.

```bash
# Check what's using port 19000
lsof -i :19000

# Option 1: Let doctor fix it
task doctor -- --fix

# Option 2: Stop the Podman machine
podman machine stop k3d

# Option 3: Switch to Docker
# Edit .env and set PLF_CONTAINER_RUNTIME=docker
```

### Manual kubectl Commands

```bash
kubectl get all -n polaris
kubectl get all -n rustfs
kubectl logs -f -n polaris deployment/polaris
kubectl describe pod -n polaris -l app=polaris
```

## Cleanup

```bash
# Cleanup catalog only (keep cluster)
task catalog:cleanup

# Reset catalog (cleanup + setup)
task catalog:reset

# Complete teardown (prompts to stop Podman machine on macOS)
task teardown

# Or just delete cluster (prompts to stop Podman machine on macOS)
task clean:all

# Delete cluster and stop Podman machine without prompts
polaris-local-forge cluster delete --yes --stop-podman
```

## Project Structure

```
polaris-local-forge/
├── .env.example              # Environment configuration template
├── .kube/config              # Cluster kubeconfig (generated)
├── config/
│   └── cluster-config.yaml   # k3d cluster configuration
├── k8s/
│   ├── features/             # Helm chart manifests
│   │   ├── rustfs.yaml
│   │   ├── polaris.yaml      # Generated
│   │   └── postgresql.yaml   # Generated
│   └── polaris/
│       ├── kustomization.yaml
│       ├── .bootstrap-credentials.env  # Generated
│       └── rsa_key*          # Generated RSA keys
├── notebooks/
│   └── verify_polaris.ipynb  # Verification notebook
├── polaris-forge-setup/      # Ansible playbooks
├── scripts/
│   └── explore_catalog.sql   # Generated DuckDB script
├── src/polaris_local_forge/  # Python CLI
├── work/
│   └── principal.txt         # Catalog credentials (generated)
├── pyproject.toml
└── Taskfile.yml
```

## Using with Cortex Code

This repo includes a [Cortex Code](https://docs.snowflake.com/en/developer-guide/cortex-code/overview) skill that automates the full setup interactively.

**Install the skill:**

```bash
cortex skill add https://github.com/kameshsampath/polaris-local-forge
```

**Then say:** "get started with apache polaris" or "setup from example manifest"

The skill wraps all CLI commands, manages a `.snow-utils/` manifest for tracking resources, and supports catalog-only resets without rebuilding the cluster.

The CLI supports `--work-dir` to keep the skill repo pristine -- generated files (k8s manifests, credentials, kubeconfig, notebooks) go to a user project directory. A lightweight `user-project/pyproject.toml` template is included for query-only workspaces.

See [SKILL_README.md](SKILL_README.md) for full details on triggers, example manifests, S3/RustFS configuration, and consuming project setup.

## Development

### Isolated Testing

For development and testing without polluting the source tree, use isolated test environments:

```bash
# Create an isolated test environment in /tmp
task test:isolated

# This creates /tmp/plf-test-<pid>/ with:
# - Symlinked Taskfile.yml pointing to source
# - Fresh .env with auto-detected runtime
# - Isolated .kube/, k8s/, work/ directories

# Run full setup in the isolated environment
cd /tmp/plf-test-*
task setup:all

# Clean up all isolated test folders
task test:isolated:clean

# List existing test folders
task test:isolated:list
```

The isolated environment protects the source directory from accidental initialization. Commands like `init`, `doctor`, `prepare`, and `cluster create` will refuse to run in the source directory without `--work-dir`.

### Project Structure (Python CLI)

```
src/polaris_local_forge/
├── __init__.py           # Package init
├── cli.py                # Main entry point, init, doctor, prepare, teardown
├── common.py             # Shared utilities (config, ansible, templates)
├── container_runtime.py  # Runtime detection and management
├── cluster.py            # Cluster commands (create, delete, status, etc.)
├── polaris_ops.py        # Polaris commands (deploy, purge, bootstrap)
└── catalog.py            # Catalog commands (setup, cleanup, verify-sql)
```

## Related Projects

- [Apache Polaris](https://polaris.apache.org/) - Iceberg REST Catalog
- [Apache Iceberg](https://iceberg.apache.org/) - Open table format
- [RustFS](https://rustfs.com/) - S3-compatible object storage
- [k3d](https://k3d.io/) - k3s in Docker
- [PyIceberg](https://py.iceberg.apache.org/) - Python Iceberg library
- [DuckDB](https://duckdb.org/) - In-process SQL database

## Acknowledgments

Thanks to the contributors and reviewers who provided feedback, testing, and ideas that helped shape this project.

## License

Copyright (c) Snowflake Inc. All rights reserved. Licensed under the Apache 2.0 license.

## Contributing

Contributions welcome! Please submit a Pull Request.
