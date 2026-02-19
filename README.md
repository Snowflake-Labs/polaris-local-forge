# Polaris Local Forge

![k3d](https://img.shields.io/badge/k3d-v5.8.0-427cc9)
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
| Docker | >= 4.27 | [Docker Desktop](https://www.docker.com/products/docker-desktop/) |
| k3d | >= 5.0.0 | `brew install k3d` or [k3d.io](https://k3d.io/) |
| Python | >= 3.12 | [python.org](https://www.python.org/downloads/) |
| uv | latest | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Task | latest | `brew install go-task` or [taskfile.dev](https://taskfile.dev) |

### Optional Tools

| Tool | Purpose | Install |
|------|---------|---------|
| DuckDB CLI | SQL verification | `brew install duckdb` |
| direnv | Auto-load env vars | `brew install direnv` |

### Verify Prerequisites

```bash
docker --version && docker info > /dev/null && echo "Docker: OK"
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
task setup:all
```

After setup completes, you'll see:

| Service | URL | Credentials |
|---------|-----|-------------|
| Polaris API | http://localhost:18181 | See `k8s/polaris/.bootstrap-credentials.env` |
| RustFS S3 | http://localhost:9000 | `admin` / `password` |
| RustFS Console | http://localhost:9001 | `admin` / `password` |

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

### Setup & Teardown

| Command | Description |
|---------|-------------|
| `task setup:all` | Complete setup (cluster + Polaris + catalog) |
| `task teardown` | Complete teardown (cleanup + delete cluster) |
| `task reset:all` | Teardown and setup fresh |

### Status & Config

| Command | Description |
|---------|-------------|
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
| `polaris-local-forge config` | Show current configuration |
| `polaris-local-forge config --output json` | Configuration as JSON |
| `polaris-local-forge setup --dry-run` | Preview setup plan |
| `polaris-local-forge setup --yes` | Execute setup |
| `polaris-local-forge teardown --yes` | Execute teardown |
| `polaris-local-forge cluster status` | Cluster status |
| `polaris-local-forge cluster status --output json` | Cluster status as JSON |
| `polaris-local-forge polaris status` | Polaris status |
| `polaris-local-forge polaris status --output json` | Polaris status as JSON |
| `polaris-local-forge catalog list` | List catalogs |
| `polaris-local-forge catalog list --output json` | Catalogs as JSON |
| `polaris-local-forge polaris bump-version` | Update Polaris version |
| `polaris-local-forge cluster bump-k3s` | Update K3S version |

All destructive commands support `--dry-run` to preview and `--yes` to skip confirmation.

## Configuration

Configuration is managed via `.env` file. Copy the example and customize:

```bash
cp .env.example .env
```

Key settings:

| Variable | Default | Description |
|----------|---------|-------------|
| `K3D_CLUSTER_NAME` | `polaris-local-forge` | Cluster name |
| `K3S_VERSION` | `v1.31.5-k3s1` | K3S version |
| `AWS_ENDPOINT_URL` | `http://localhost:9000` | RustFS S3 endpoint |
| `POLARIS_URL` | `http://localhost:18181` | Polaris API endpoint |

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

# Complete teardown
task teardown

# Or just delete cluster
task clean:all
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

## Related Projects

- [Apache Polaris](https://polaris.apache.org/) - Iceberg REST Catalog
- [Apache Iceberg](https://iceberg.apache.org/) - Open table format
- [RustFS](https://rustfs.com/) - S3-compatible object storage
- [k3d](https://k3d.io/) - k3s in Docker
- [PyIceberg](https://py.iceberg.apache.org/) - Python Iceberg library
- [DuckDB](https://duckdb.org/) - In-process SQL database

## License

Apache 2.0

## Contributing

Contributions welcome! Please submit a Pull Request.
