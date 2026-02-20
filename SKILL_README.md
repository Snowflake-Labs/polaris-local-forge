# Polaris Local Forge -- Cortex Code Skills

This document describes how to use **polaris-local-forge** as a [Cortex Code](https://docs.snowflake.com/en/developer-guide/cortex-code/overview) skill to set up a local [Apache Polaris](https://polaris.apache.org/releases/1.3.0/) (Incubating) development environment.

## Overview

Polaris Local Forge provides a complete local data lakehouse stack:

| Component | Role | Local URL |
|-----------|------|-----------|
| [Apache Polaris (Incubating)](https://polaris.apache.org/releases/1.3.0/) | Iceberg REST Catalog | `http://localhost:18181` |
| [RustFS](https://docs.rustfs.com/) | S3-compatible object storage | `http://localhost:19000` (API), `:19001` (console) |
| PostgreSQL | Polaris metastore backend | Internal (via k3d) |
| [Podman](https://podman.io/) (default) / [Docker](https://www.docker.com/) | Container runtime | Podman preferred (OSS, shipped with Cortex Code) |
| k3d/k3s | Local Kubernetes cluster | `kubectl` via kubeconfig |
| [Apache Iceberg](https://iceberg.apache.org/) | Open table format | Via Polaris catalog |

## Available Skills

| Skill | Description |
|-------|-------------|
| `polaris-local-forge` | Full local environment setup: k3d cluster + RustFS + PostgreSQL + Polaris + catalog |

## Installation

```bash
cortex skill add https://github.com/kameshsampath/polaris-local-forge
```

Verify installation:

```bash
cortex skill list
```

## Usage Triggers

Say any of these to activate the skill:

### Getting Started

- "try polaris locally"
- "get started with polaris"
- "get started with apache polaris"
- "apache polaris quickstart"
- "run polaris on my machine"

### Developer / Builder

- "polaris dev environment"
- "local data lakehouse"
- "local iceberg development"
- "local iceberg catalog"
- "local polaris setup"

### Technology-Specific

- "duckdb polaris local"
- "local s3 iceberg"
- "polaris docker setup"
- "rustfs setup"
- "local s3 setup"

### Manifest-Driven

- "get started with apache polaris using example manifest"
- "setup from example manifest"
- "replay polaris local forge"
- "replay from manifest"

### Catalog Operations

- "reset polaris catalog"
- "recreate catalog"
- "cleanup catalog"
- "setup catalog only"

### Status & Teardown

- "polaris status"
- "verify polaris setup"
- "teardown polaris"
- "delete polaris cluster"

## Example Manifest Workflow

An example manifest is included at `example-manifests/polaris-local-forge-manifest.md` with sane defaults for a quick start.

### Quick Start with Example Manifest

1. Install the skill:

```bash
cortex skill add https://github.com/kameshsampath/polaris-local-forge
```

2. Say: **"get started with apache polaris using example manifest"**

3. The skill will:
   - Copy the example manifest to `.snow-utils/snow-utils-manifest.md`
   - Show the pre-configured defaults (cluster name, S3 bucket, catalog, etc.)
   - Walk you through the full setup interactively
   - Update the manifest status as resources are created

### Manual Setup

Say **"get started with apache polaris"** and the skill will guide you step by step:

1. Workspace init (copy `pyproject.toml` + `.env.example` to your project directory)
2. Configuration review and confirmation
3. Prerequisites check (`doctor`)
4. Environment setup (Python venv with query deps)
5. Configuration generation (RSA keys, bootstrap creds, k8s manifests)
6. Cluster creation (k3d with RustFS + PostgreSQL)
7. Polaris deployment
8. S3/RustFS configuration
9. Catalog setup (bucket, catalog, principal, roles)
10. Verification (DuckDB or notebook)

### CLI Global Options

The CLI supports `--work-dir` to keep the skill directory pristine:

```bash
uv run --project <SKILL_DIR> polaris-local-forge --work-dir <PROJECT_DIR> <command>
```

| Option | Description |
|--------|-------------|
| `--work-dir PATH` | Working directory for generated files (default: skill directory) |
| `--env-file PATH` | Path to .env file (default: `<work-dir>/.env`) |

When `--work-dir` is specified, all generated files (k8s manifests, credentials,
kubeconfig, kubectl, notebooks, scripts) are written to the work directory.
The skill directory remains read-only.

For a **second cluster**, create a new directory and re-run:

```bash
mkdir ~/polaris-staging && cd ~/polaris-staging
# The skill creates a fully independent environment here
```

## Container Runtime

Podman is the preferred and default container runtime. It is fully open source and ships pre-installed with Cortex Code. The CLI auto-detects the available runtime (Podman first, then Docker) and stores the result in `PLF_CONTAINER_RUNTIME`.

| Setting | Default | Description |
|---------|---------|-------------|
| `PLF_CONTAINER_RUNTIME` | (auto-detect) | `podman` or `docker` |
| `PLF_PODMAN_MACHINE` | `k3d` | Dedicated Podman machine name (macOS only) |

On macOS, a dedicated Podman machine named `k3d` is created with 4 CPUs and 16GB RAM, keeping the user's default machine untouched. All k3d operations are pinned to this machine's socket.

For detailed setup instructions (machine creation, cgroup delegation, network), see [docs/podman-setup.md](docs/podman-setup.md).

To use Docker instead, set `PLF_CONTAINER_RUNTIME=docker` in `.env`.

## S3 / RustFS -- Local External Volume Equivalent

In Snowflake environments, [snow-utils-volumes](https://github.com/kameshsampath/snow-utils-skills) creates:

1. AWS S3 bucket
2. IAM role and policy
3. Snowflake `CREATE EXTERNAL VOLUME`

In polaris-local-forge, the local equivalent is:

| Snowflake | Local (polaris-local-forge) |
|-----------|---------------------------|
| AWS S3 bucket | RustFS bucket (`aws s3 mb s3://bucket --endpoint-url http://localhost:19000`) |
| IAM role/policy | Static credentials: `admin` / `password` |
| External Volume SQL | Not needed -- Polaris catalog config points directly to `s3://bucket` with RustFS endpoint |

### Configuring AWS CLI for RustFS

```bash
export AWS_ENDPOINT_URL=http://localhost:19000
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=password
```

Then use standard AWS CLI commands:

```bash
aws s3 ls                          # List buckets
aws s3 mb s3://my-bucket           # Create bucket
aws s3 ls s3://polaris/            # List objects in polaris bucket
```

No `--endpoint-url` flag needed when `AWS_ENDPOINT_URL` is set.

### S3 Dependencies

Both the skill's `pyproject.toml` and the lightweight `user-project/pyproject.toml` include `boto3>=1.35.0` and `pyiceberg[s3fs]>=0.8.1`. No additional Python dependencies are needed. The `aws` CLI must be installed separately if you want to use it directly.

## Consuming Projects -- Minimal Setup

A project that uses polaris-local-forge as infrastructure needs only these files in its own directory:

### Required Files

**`.env`:**

```bash
POLARIS_URL=http://localhost:18181
AWS_ENDPOINT_URL=http://localhost:19000
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_REGION=us-east-1
# From work/principal.txt in the polaris workspace:
POLARIS_REALM=POLARIS
CLIENT_ID=<from principal.txt>
CLIENT_SECRET=<from principal.txt>
```

**`pyproject.toml`** -- copy from `user-project/pyproject.toml` in the skill repo.
It includes `duckdb`, `pyiceberg[s3fs]`, `boto3`, `pandas`, `pyarrow`, and
optional `[notebook]` extras for Jupyter.

**Notebook or SQL scripts** for querying.

### Not Required in Consuming Projects

- k8s manifests, Ansible playbooks, cluster config
- The `polaris-local-forge` CLI or source code
- The Taskfile
- RustFS/PostgreSQL configuration

Infrastructure lives in the `polaris-local-forge` repo. Your project only needs connection details.

## Relationship to Other Skill Repos

| Repo | Purpose | Relationship |
|------|---------|-------------|
| [polaris-local-forge](https://github.com/kameshsampath/polaris-local-forge) | Local Polaris infra | **This repo** -- provides the local environment |
| [snow-utils-skills](https://github.com/kameshsampath/snow-utils-skills) | Snowflake PAT, External Volumes | Provides real AWS S3 equivalent (Phase 2 interop) |
| [kamesh-demo-skills](https://github.com/kameshsampath/kamesh-demo-skills) | Demo applications | Can consume polaris-local-forge as infrastructure |

### Future Interoperability

The `.snow-utils/` directory convention and manifest format are shared across all skill repos. A demo skill from `kamesh-demo-skills` could be adapted to target local Polaris by pointing to the `polaris-local-forge` manifest for connection details.

## Generated Files

With `--work-dir`, generated files live directly in the user's project directory:

```
my-polaris-project/                    # --work-dir
├── .env                               # Configuration
├── pyproject.toml                     # Lightweight query deps
├── .snow-utils/
│   └── snow-utils-manifest.md         # Resource tracking manifest
├── .kube/config                       # Cluster kubeconfig (chmod 600)
├── bin/kubectl                        # Version-matched kubectl
├── k8s/                               # Generated + copied k8s manifests
│   ├── features/                      # RustFS, Polaris, PostgreSQL manifests
│   └── polaris/                       # Secrets, credentials, RSA keys
├── work/
│   └── principal.txt                  # Catalog credentials (chmod 600)
├── notebooks/
│   └── verify_polaris.ipynb           # Verification notebook
└── scripts/
    └── explore_catalog.sql            # SQL verification
```

Sensitive directories (`.kube/`, `work/`, `.snow-utils/`) are set to `0700`.
Sensitive files (`.env`, `principal.txt`, `rsa_key`, credentials) are set to `0600`.

### File Lifecycle

**Cluster-level files** (survive catalog reset, reused on cluster recreation):

- RSA keys, bootstrap credentials, k8s manifests, kubectl, kubeconfig

**Catalog-level files** (regenerated on catalog reset):

- `work/principal.txt` (new credentials on each catalog setup)
- `scripts/explore_catalog.sql` (re-templated with new credentials)

### User Project Template

The skill repo includes `user-project/pyproject.toml` with lightweight dependencies
for querying and exploration:

| Package | Purpose |
|---------|---------|
| `duckdb` | SQL engine with Iceberg extension |
| `pyiceberg[s3fs]` | PyIceberg with S3 filesystem support |
| `boto3` | AWS SDK for S3/RustFS operations |
| `pandas` | Data manipulation |
| `pyarrow` | Arrow columnar format support |
| `python-dotenv` | Load `.env` configuration |
| `ipykernel` + `jupyter` | Optional: notebook support (`[notebook]` extra) |

## Phase 2: Real AWS S3 Support

Currently polaris-local-forge is fully local (RustFS). Real AWS S3 support would require:

- Real AWS credentials (IAM/SSO instead of static admin/password)
- Real S3 endpoint (remove `AWS_ENDPOINT_URL` override)
- IAM role ARN for Polaris catalog `storageConfigInfo`
- Optional: skip RustFS deployment

This is tracked as a future enhancement. Contributions welcome.

## References

- [Apache Polaris (Incubating) 1.3.0 Documentation](https://polaris.apache.org/releases/1.3.0/)
- [Polaris Management API Spec](https://polaris.apache.org/releases/1.3.0/polaris-api-specs/polaris-management-api/)
- [Polaris Catalog API Spec (Swagger)](https://editor.swagger.io/?url=https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/generated/bundled-polaris-catalog-service.yaml)
- [RustFS Documentation](https://docs.rustfs.com/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [k3d Documentation](https://k3d.io/)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg.html)
