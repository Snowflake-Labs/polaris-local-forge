# Apache Polaris(Incubating) Starter Kit with RustFS on k3s

![k3d](https://img.shields.io/badge/k3d-v5.6.0-427cc9)
![Docker Desktop](https://img.shields.io/badge/Docker%20Desktop-v4.27-0db7ed)
![Apache Polaris(Incubating)](https://img.shields.io/badge/Apache%20Polaris-1.2.0-incubating)
![RustFS](https://img.shields.io/badge/RustFS-1.0.0-orange)

This starter kit provides a complete development environment for Apache Polaris with [RustFS](https://rustfs.com/) S3-compatible storage running on k3s Kubernetes. It includes automated setup of PostgreSQL metastore, S3 integration via RustFS, and all necessary configurations for immediate development use.

**Key features:**

- 🚀 Automated k3s cluster setup with k3d
- ☁️ Integrated RustFS for S3-compatible object storage (high-performance, Rust-based)
- 🗄️ PostgreSQL metastore configuration
- 🤖 Task-based automation for easy management
- 🦆 DuckDB CLI + SQL-first catalog exploration
- 📓 Jupyter notebook for verification

> **Note**: Looking for LocalStack instead? Check out the [`localstack`](https://github.com/snowflake-labs/polaris-local-forge/tree/localstack) branch.

## 🚀 Quick Start

Get Apache Polaris running locally in 3 steps:

### 1. Clone the Repository

```bash
git clone https://github.com/snowflake-labs/polaris-local-forge
cd polaris-local-forge
```

### 2. Install Prerequisites

Install [Task](https://taskfile.dev) (if not already installed):

```bash
# macOS
brew install go-task/tap/go-task

# Linux
curl -sL https://taskfile.dev/install.sh | sh
sudo mv bin/task /usr/local/bin/

# Windows (Scoop)
scoop install task

# Windows (Chocolatey)
choco install go-task
```

Setup Python environment:

```bash
# Install uv and setup Python environment
task setup:python
```

> **Note**: Task commands automatically use the virtual environment. You only need to manually activate it if running Python/Jupyter commands directly:
>
> ```bash
> source .venv/bin/activate  # On Unix-like systems
> .venv\Scripts\activate     # On Windows
> ```

### 3. Deploy Everything

```bash
task setup:all
```

This single command will:

- ✅ Generate required configuration files
- ✅ Create the k3s cluster with k3d
- ✅ Deploy PostgreSQL and RustFS
- ✅ Deploy Apache Polaris
- ✅ Create a demo catalog

**That's it!** ✨

Verify your setup:

```bash
# Option 1: DuckDB CLI (if installed)
task catalog:verify:sql

# Option 2: Jupyter notebook (Python + PyIceberg)
jupyter notebook notebooks/verify_setup.ipynb
```

## 📍 What You Get

Once setup completes, you'll have the following services running:

| Service    | URL                      | Credentials/Details                                          |
| ---------- | ------------------------ | ------------------------------------------------------------ |
| 🌟 **Polaris API** | <http://localhost:18181> | See `k8s/polaris/.bootstrap-credentials.env` for login       |
| ☁️ **RustFS S3 API** | <http://localhost:9000> | S3-compatible storage - Use `admin/password` for credentials |
| 🖥️ **RustFS Console** | <http://localhost:9001> | Web UI for managing buckets and objects                      |

**Quick access:**

```bash
task urls  # Display all service URLs
task status  # Check deployment status
```

## 📋 Task Commands Reference

The project uses [Task](https://taskfile.dev) to automate common workflows. Here are the most useful commands:

### Installation & Setup

```bash
task install:uv      # Install uv Python package manager
task setup:python    # Setup Python environment (installs uv + creates venv)
task prepare         # Generate required configuration files
```

### Essential Commands

```bash
task help            # List all available tasks
task setup:all       # Complete setup (prepare → cluster → deploy → catalog)
task reset:all       # Complete reset (delete cluster → recreate everything)
task urls            # Show all service URLs and credentials
task status          # Check deployment status
task clean:all       # Delete cluster and all resources
```

### Cluster Management

```bash
task cluster:create           # Create k3d cluster
task cluster:bootstrap-check  # Wait for bootstrap deployments
task cluster:polaris-check    # Wait for Polaris deployment
task cluster:delete           # Delete the cluster
task cluster:reset            # Delete and recreate cluster with fresh catalog
```

### Polaris Operations

```bash
task polaris:deploy     # Deploy Polaris to the cluster
task polaris:reset      # Purge and re-bootstrap Polaris
task polaris:purge      # Purge Polaris data
task polaris:bootstrap  # Bootstrap Polaris (run after purge)
```

### Catalog Management

```bash
task catalog:setup        # Setup demo catalog (bucket, catalog, principal, roles)
task catalog:verify:sql   # Verify catalog using DuckDB CLI (non-interactive)
task catalog:explore:sql  # Explore catalog using DuckDB CLI (interactive)
task catalog:cleanup      # Cleanup catalog resources
task catalog:reset        # Cleanup and recreate catalog (keeps cluster running)
```

### Logging & Troubleshooting

```bash
# View logs
task logs:polaris      # Stream Polaris server logs
task logs:postgresql   # Stream PostgreSQL logs
task logs:rustfs       # Stream RustFS logs
task logs:bootstrap    # View bootstrap job logs
task logs:purge        # View purge job logs

# Troubleshooting
task troubleshoot:polaris     # Diagnose Polaris issues
task troubleshoot:postgresql  # Check database connectivity
task troubleshoot:rustfs      # Verify RustFS connectivity
task troubleshoot:events      # Show recent events in polaris namespace
```

## 📦 Prerequisites

Before you begin, ensure you have the following tools installed:

### Required Tools

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (>= 4.27) or [Docker Engine](https://docs.docker.com/engine/install/)
- [kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl) - Kubernetes command-line tool
- [k3d](https://k3d.io/) (>= 5.0.0) - Lightweight wrapper to run k3s in Docker
- [Python](https://www.python.org/downloads/) (>= 3.11)
- [uv](https://github.com/astral-sh/uv) - Python packaging tool
- [Task](https://taskfile.dev) - Task runner (see installation above)

### Optional Tools

- [DuckDB CLI](https://duckdb.org/docs/installation/) - For SQL-first verification (`task catalog:verify:sql`)
- [direnv](https://direnv.net) - Automatic environment variable loading

**Install DuckDB CLI:**

```bash
# macOS
brew install duckdb

# Linux (or download from https://duckdb.org/docs/installation/)
curl -LO https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
sudo mv duckdb /usr/local/bin/
```

> **Important**
> Ensure all required tools are installed and on your PATH before running `task setup:all`.

### Verify Prerequisites

```bash
# Check required tools
docker --version
kubectl version --client
k3d version
python3 --version
uv --version
task --version

# Check Docker is running
docker ps
```

## 🔧 Configuration

### Environment Variables

The Taskfile automatically manages most environment variables. If you need to customize them, create a `.env` file:

```bash
# Optional: Override default values
export PROJECT_HOME="$PWD"
export KUBECONFIG="$PWD/.kube/config"
export K3D_CLUSTER_NAME=polaris-local-forge
export K3S_VERSION=v1.32.1-k3s1
export FEATURES_DIR="$PWD/k8s"
```

> **Tip**: Use [direnv](https://direnv.net) to automatically load environment variables when entering the project directory.

### Custom Python Version

```bash
# Pin a different Python version
uv python pin 3.11  # or 3.13

# Recreate virtual environment
uv venv --force
source .venv/bin/activate
uv sync
```

## ✅ Verification

After running `task setup:all`, verify your setup:

### 1. Run the Verification Notebook

Open the notebook:

```bash
jupyter notebook notebooks/verify_setup.ipynb
```

> **Note**: If running Jupyter directly (not via Task), ensure your virtual environment is activated first: `source .venv/bin/activate`

The notebook will:

- Create a test namespace
- Create a test table
- Insert sample data
- Query the data back

### 2. DuckDB CLI Verification (SQL-first)

If you prefer SQL-first workflows, use DuckDB CLI to verify and explore the catalog:

```bash
# Non-interactive: runs SQL script and exits
task catalog:verify:sql

# Interactive: runs SQL script then drops into DuckDB shell
task catalog:explore:sql
```

The SQL script (`scripts/explore_catalog.sql`) is auto-generated during `task catalog:setup` with your principal credentials pre-configured. It will:

- Attach DuckDB to Polaris REST catalog
- Create a `wildlife` schema and `penguins` Iceberg table
- Load the Palmer Penguins dataset (333 rows)
- Run analytics queries (species stats, correlations)
- Explore Iceberg metadata and snapshots

In interactive mode, you can continue exploring:

```sql
-- Query the penguins table
SELECT * FROM polaris_catalog.wildlife.penguins LIMIT 10;

-- Custom analytics
SELECT species, AVG(body_mass_g) as avg_mass
FROM polaris_catalog.wildlife.penguins
GROUP BY species;

-- Explore Iceberg metadata
SELECT * FROM iceberg_snapshots('polaris_catalog.wildlife.penguins');
```

### 3. Check RustFS Storage

Open the RustFS Console at <http://localhost:9001> to view your Iceberg files. Login with `admin/password`.

You should see the catalog structure with metadata and data files in the `polardb` bucket.

### 4. Verify Deployments

```bash
# Check all deployments
task status

# Or manually
kubectl get all -n polaris
kubectl get all -n rustfs
```

Expected output in `polaris` namespace:

```text
NAME                           READY   STATUS      RESTARTS   AGE
pod/polaris-694ddbb476-m2trm   1/1     Running     0          13m
pod/polaris-bootstrap-xxxxx    0/1     Completed   0          13m
pod/postgresql-0               1/1     Running     0          15m

NAME                    TYPE           CLUSTER-IP     EXTERNAL-IP             PORT(S)          AGE
service/polaris         LoadBalancer   10.43.202.93   172.19.0.3,172.19.0.4   8181:32181/TCP   13m
service/postgresql      ClusterIP      10.43.182.31   <none>                  5432/TCP         15m
service/postgresql-hl   ClusterIP      None           <none>                  5432/TCP         15m
```

## 🔍 Troubleshooting

### Quick Diagnostics

```bash
# Check deployment status
task status

# View events
task troubleshoot:events

# Check specific component
task troubleshoot:polaris
task troubleshoot:postgresql
task troubleshoot:rustfs
```

### Common Issues

#### 1. Polaris Server Fails to Start

```bash
# Check Polaris logs
task logs:polaris

# Check pod status and events
task troubleshoot:polaris
```

#### 2. RustFS Not Accessible

```bash
# Verify RustFS is running
kubectl get pods -n rustfs

# Check connectivity
task troubleshoot:rustfs
```

#### 3. PostgreSQL Connection Issues

```bash
# Check PostgreSQL logs
task logs:postgresql

# Verify connectivity
task troubleshoot:postgresql
```

#### 4. Bootstrap Job Fails

```bash
# View bootstrap logs
task logs:bootstrap

# Reset Polaris
task polaris:reset
```

### Manual Troubleshooting Commands

If Task commands don't help, you can use these manual commands:

```bash
# Check events
kubectl get events -n polaris --sort-by='.lastTimestamp'

# Describe pods
kubectl describe pod -n polaris -l app=polaris

# Check logs
kubectl logs -f -n polaris deployment/polaris
kubectl logs -f -n polaris jobs/polaris-bootstrap
kubectl logs -f -n polaris statefulset/postgresql
kubectl logs -f -n rustfs deployment/rustfs

# Check services
kubectl get svc -n polaris
kubectl get svc -n rustfs

# Verify PostgreSQL
kubectl exec -it -n polaris postgresql-0 -- pg_isready -h localhost

# Verify RustFS
kubectl exec -it -n rustfs deployment/rustfs -- mc ls local
```

## 🧹 Cleanup & Reset

### Reset Catalog Only (Keep Cluster Running)

Clean and recreate the catalog with fresh data:

```bash
task catalog:reset
```

Or just cleanup without recreating:

```bash
task catalog:cleanup
```

### Reset Everything (Delete and Recreate Cluster)

Complete reset - deletes cluster and recreates everything with fresh catalog:

```bash
task reset:all
# Same as: task cluster:reset
```

### Delete Everything

Delete the k3d cluster and all resources:

```bash
task clean:all
```

This removes:

- k3d cluster
- All Kubernetes resources
- Catalog data in RustFS
- PostgreSQL data

> **Note**: Your configuration files in `k8s/polaris/` (credentials, secrets, keys) are preserved. Run `task prepare` to regenerate them if needed.

## 🛠️ What's Next?

Now that you have Apache Polaris running locally, you can:

- **Connect query engines**: Use with Apache Spark, Trino, or Risingwave
- **Explore the API**: Check the [Polaris API documentation](https://polaris.apache.org/)
- **Create more catalogs**: Run `task catalog:setup` with custom parameters
- **Develop integrations**: Use the RustFS S3 endpoint for testing
- **Experiment with Iceberg**: Create tables, partitions, and time-travel queries

## 📚 Related Projects and Tools

### Core Components

- [Apache Polaris](https://github.com/apache/polaris) - Data Catalog and Governance Platform
- [Apache Iceberg](https://iceberg.apache.org/) - Open table format for data lakes
- [PyIceberg](https://py.iceberg.apache.org/) - Python library to interact with Apache Iceberg
- [RustFS](https://rustfs.com/) - High-performance S3-compatible object storage built with Rust
- [k3d](https://k3d.io) - k3s in Docker
- [k3s](https://k3s.io) - Lightweight Kubernetes Distribution

### Development Tools

- [Docker](https://www.docker.com/) - Container Platform
- [Kubernetes](https://kubernetes.io/) - Container Orchestration
- [kubectl](https://kubernetes.io/docs/reference/kubectl/) - Kubernetes CLI
- [Task](https://taskfile.dev) - Modern task runner and build tool
- [uv](https://github.com/astral-sh/uv) - Fast Python packaging tool
- [Ansible](https://www.ansible.com/) - Automation and configuration management

### Documentation

- [Polaris Documentation](https://polaris.apache.org/)
- [Iceberg Documentation](https://iceberg.apache.org/docs/latest/)
- [RustFS Documentation](https://docs.rustfs.com/)
- [k3d Documentation](https://k3d.io/v5.5.1/)
- [Kubernetes Documentation](https://kubernetes.io/docs/home/)
- [Task Documentation](https://taskfile.dev/usage/)

---

## 📄 License

Copyright (c) Snowflake Inc. All rights reserved.  
Licensed under the Apache 2.0 license.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

<details>
<summary>🔧 Advanced: Manual Setup (Click to expand)</summary>

If you prefer to run commands manually instead of using Task, here's the step-by-step process:

### 1. Prepare Configuration Files

Generate required sensitive files from templates:

```bash
ansible-playbook polaris-forge-setup/prepare.yml
```

### 2. Create the Cluster

```bash
bin/setup.sh
```

Wait for bootstrap deployments:

```bash
ansible-playbook polaris-forge-setup/cluster_checks.yml --tags=bootstrap
```

### 3. Verify Base Components

PostgreSQL:

```bash
kubectl get pods,svc -n polaris
```

RustFS:

```bash
kubectl get pods,svc -n rustfs
```

### 4. Deploy Polaris

```bash
kubectl apply -k k8s/polaris
```

Wait for Polaris deployment:

```bash
ansible-playbook polaris-forge-setup/cluster_checks.yml --tags=polaris
```

### 5. Setup Catalog

Export AWS environment variables:

```bash
unset AWS_PROFILE
export AWS_ENDPOINT_URL=http://localhost:9000  # Use localhost for local machine
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
export AWS_REGION=us-east-1
```

> **Note**: Use `http://localhost:9000` when running from your local machine. The `http://rustfs.rustfs:9000` endpoint only works from inside the cluster.

Create catalog:

```bash
ansible-playbook polaris-forge-setup/catalog_setup.yml
```

### 6. Generate Verification Notebook

```bash
ansible-playbook polaris-forge-setup/catalog_setup.yml --tags=verify
```

### 7. Purge and Re-bootstrap (if needed)

Purge:

```bash
kubectl patch job polaris-purge -n polaris -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete --timeout=300s job/polaris-purge -n polaris
kubectl logs -n polaris jobs/polaris-purge
```

Re-bootstrap:

```bash
kubectl delete -k k8s/polaris/job
kubectl apply -k k8s/polaris/job
kubectl wait --for=condition=complete --timeout=300s job/polaris-bootstrap -n polaris
kubectl logs -n polaris jobs/polaris-bootstrap
```

### 8. Cleanup

Cleanup catalog:

```bash
ansible-playbook polaris-forge-setup/catalog_cleanup.yml
```

Delete cluster:

```bash
bin/cleanup.sh
```

</details>
