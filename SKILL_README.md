# Apache Polaris Local Forge — Cortex Code Skills

This document describes how to use **polaris-local-forge** as a [Cortex Code](https://docs.snowflake.com/en/developer-guide/cortex-code/overview) skill to set up a local [Apache Polaris](https://polaris.apache.org/) development environment.

> [!TIP]
> This skill embodies **Infrastructure-as-Intent** — instead of remembering complex parameter chains, just say what you want.
> The skill handles the plumbing. You focus on the value.
>
> Read more: [Infrastructure-as-Intent: The Field Velocity Blueprint](https://blogs.kamesh.dev/infrastructure-as-intent-the-field-velocity-blueprint-e6217ef30f14)

## Overview

Apache Polaris Local Forge provides a complete local data lakehouse stack:

| Component | Role | Local URL |
|-----------|------|-----------|
| [Apache Polaris](https://polaris.apache.org/) | Iceberg REST Catalog | `http://localhost:18181` |
| [RustFS](https://docs.rustfs.com/) | S3-compatible object storage | `http://localhost:19000` (API), `:19001` (console) |
| PostgreSQL | Apache Polaris metastore backend | Internal (via k3d) |
| [Podman](https://podman.io/) (default) / [Docker](https://www.docker.com/) | Container runtime | Podman preferred (OSS, shipped with Cortex Code) |
| k3d/k3s | Local Kubernetes cluster | `kubectl` via kubeconfig |
| [Apache Iceberg](https://iceberg.apache.org/) | Open table format | Via Apache Polaris catalog |

## Available Skills

| Skill | Description |
|-------|-------------|
| `polaris-local-forge` | Full local environment setup: k3d cluster + RustFS + PostgreSQL + Apache Polaris + catalog |

## Installation

```bash
cortex skill add https://github.com/Snowflake-Labs/polaris-local-forge
```

Verify installation:

```bash
cortex skill list
```

## Usage Triggers

> [!TIP]
> **Shorthand invocation:** You can use `$plf` instead of `$polaris-local-forge` when explicitly invoking the skill. Cortex Code supports partial matching on skill names.

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

- "get started with apache polaris using "<https://github.com/Snowflake-Labs/polaris-local-forge/blob/main/example-manifests/polaris-local-forge-manifest.md>""
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

## Apache Polaris API Queries via Natural Language

> [!WARNING]
> **Experimental Feature — Read-Only:** REST API query support via natural language is experimental.
> Currently supports **read-only** operations only (GET requests).
>
> **Not supported yet:** create, update, delete operations (POST/PUT/DELETE).
> If you ask to create a catalog, namespace, or modify resources, the skill will inform you this isn't available yet.

The skill can query the Apache Polaris REST API directly using natural language. No need to construct curl commands or manage OAuth tokens — the skill handles authentication automatically.

### Auto-Activation (Project Context Detection)

When you're in a project directory with polaris-local-forge infrastructure set up, the skill **auto-activates** for API queries. No need to say "using polaris" or "polaris show namespaces" — just say "show namespaces".

**Context signals that trigger auto-activation:**

| Context Signal | Check | Meaning |
|----------------|-------|---------|
| `.snow-utils/snow-utils-manifest.md` with `polaris-local-forge:` | `grep -q "polaris-local-forge:" ...` | This skill is installed (primary) |
| `.env` with `PLF_POLARIS_CATALOG_NAME` | `grep -q "PLF_POLARIS_CATALOG_NAME" .env` | Polaris config exists (secondary) |
| `bin/plf` wrapper script | `[ -x bin/plf ]` | CLI initialized (tertiary) |

When any of these exist in your current directory:
- "show namespaces" → queries Polaris (not Snowflake, not Kubernetes)
- "list catalogs" → calls Management API
- "describe table X" → fetches table metadata

> [!CAUTION]
> **Forbidden commands — these will NOT work:**
> - `polaris` CLI — does not exist (`polaris namespaces list`, `polaris --help`)
> - `curl http://localhost:8181/...` or `curl http://localhost:18181/...` — needs OAuth token
> - `docker ps --filter "name=polaris"` — don't check, assume infra is running
> - `ansible-playbook` directly — causes asdf/tool-versions conflict
>
> **Correct pattern** — use `plf api query`:
> ```bash
> ./bin/plf api query /api/catalog/v1/polardb/namespaces
> ./bin/plf api query /api/management/v1/catalogs
> ```

No disambiguation prompts, no Docker checks — the skill assumes infrastructure is running and uses `plf api query` directly.

### Catalog Operations

| Say this... | API Endpoint | Description |
|-------------|--------------|-------------|
| "list catalogs" | `GET /api/management/v1/catalogs` | Show all available catalogs |
| "show catalog polardb" | `GET /api/management/v1/catalogs/{catalog}` | Get catalog configuration |
| "describe catalog polardb" | `GET /api/management/v1/catalogs/{catalog}` | Get catalog details |

### Namespace Operations

| Say this... | API Endpoint | Description |
|-------------|--------------|-------------|
| "show namespaces" | `GET /api/catalog/v1/{catalog}/namespaces` | List namespaces in default catalog |
| "list namespaces in polardb" | `GET /api/catalog/v1/polardb/namespaces` | List namespaces in specific catalog |
| "describe namespace default" | `GET /api/catalog/v1/{catalog}/namespaces/default` | Get namespace details |

### Table Operations

| Say this... | API Endpoint | Description |
|-------------|--------------|-------------|
| "list tables in default" | `GET /api/catalog/v1/{catalog}/namespaces/default/tables` | List tables in namespace |
| "show table penguins in default" | `GET /api/catalog/v1/{catalog}/namespaces/default/tables/penguins` | Get table metadata |
| "describe table penguins" | `GET /api/catalog/v1/{catalog}/namespaces/{ns}/tables/{table}` | Full table details |

### View Operations

| Say this... | API Endpoint | Description |
|-------------|--------------|-------------|
| "list views in default" | `GET /api/catalog/v1/{catalog}/namespaces/default/views` | List views in namespace |
| "show view myview in default" | `GET /api/catalog/v1/{catalog}/namespaces/default/views/myview` | Get view details |

### Principal & Role Operations

| Say this... | API Endpoint | Description |
|-------------|--------------|-------------|
| "list principals" | `GET /api/management/v1/principals` | Show all principals |
| "show principal alice" | `GET /api/management/v1/principals/alice` | Get principal details |
| "list principal roles" | `GET /api/management/v1/principal-roles` | List principal roles |
| "show principal role admin" | `GET /api/management/v1/principal-roles/admin` | Get principal role details |
| "list roles for principal alice" | `GET /api/management/v1/principals/alice/principal-roles` | Roles assigned to principal |
| "list catalog roles for polardb" | `GET /api/management/v1/catalogs/{catalog}/catalog-roles` | List catalog roles |
| "show catalog role admin in polardb" | `GET /api/management/v1/catalogs/{catalog}/catalog-roles/admin` | Get catalog role details |
| "list grants for role admin in polardb" | `GET /api/management/v1/catalogs/{catalog}/catalog-roles/admin/grants` | List grants for role |

> [!NOTE]
> The skill handles OAuth token retrieval and credential management automatically.
> You never need to manually construct curl commands or manage tokens.

## Example Manifest Workflow

An example manifest is included at `example-manifests/polaris-local-forge-manifest.md` with sane defaults for a quick start.

> [!NOTE]
> **Container runtime is auto-detected.** The example manifest leaves `container_runtime` empty — the CLI detects whether Docker or Podman is running on your system. No configuration needed.

### Quick Start with Example Manifest

1. Install the skill:

```bash
cortex skill add https://github.com/Snowflake-Labs/polaris-local-forge
```

1. Say: **"get started with apache polaris using example manifest"**

2. The skill will:
   - Copy the example manifest to `.snow-utils/snow-utils-manifest.md`
   - Show the pre-configured defaults (cluster name, S3 bucket, catalog, etc.)
   - Walk you through the full setup interactively
   - Update the manifest status as resources are created

### Export Manifest for Sharing

After completing setup, export your manifest to share with colleagues:

```
export manifest for sharing
```

The skill will:

- Create `{project_name}-manifest.md` in project root
- Ask whether to keep your runtime (`podman`/`docker`) or clear for auto-detection
- Set status to `REMOVED` so recipient can replay from scratch
- Add `# ADAPT:` markers on customizable values

**Recipient usage:**

```
setup from shared manifest
```

Or from a URL:

```
setup from manifest URL https://example.com/my-manifest.md
```

> [!TIP]
> Default is **clear runtime for auto-detection** — maximizes compatibility across Docker and Podman users.

### Cross-Workflow Compatibility with Task Commands

The manifest created by Cortex Code (this skill) is fully compatible with the Task workflow and vice versa. Both use the same manifest format at `.snow-utils/snow-utils-manifest.md`.

**This means you can:**

- Start with `task setup:all` → later use Cortex Code for queries and operations
- Start with Cortex Code → later use `task setup:replay` to resume
- Mix and match based on preference

**Task commands that work with the skill manifest:**

| Task Command | Description |
|--------------|-------------|
| `task setup:replay WORK_DIR=...` | Resume/replay from manifest (equivalent to "replay from manifest") |
| `task teardown WORK_DIR=...` | Teardown with confirmation (equivalent to "teardown polaris") |
| `task status` | Check cluster status (equivalent to "polaris status") |

For detailed Task workflow documentation, see [README.md](README.md#manifest-workflow).

### Manual Setup

Say **"get started with apache polaris"** and the skill will guide you step by step:

1. Workspace init (copy `pyproject.toml` + `.env.example` to your project directory)
2. Configuration review and confirmation
3. Prerequisites check (`doctor`)
4. Environment setup (Python venv with query deps)
5. Configuration generation (RSA keys, bootstrap creds, k8s manifests)
6. Cluster creation (k3d with RustFS + PostgreSQL)
7. Apache Polaris deployment
8. S3/RustFS configuration
9. Catalog setup (bucket, catalog, principal, roles)
10. Verification (DuckDB or notebook)

> [!NOTE]
> For CLI commands, container runtime setup, environment variables, and troubleshooting, see [README.md](README.md).

## S3 / RustFS — Local External Volume Equivalent

In Snowflake environments, [snow-utils-volumes](https://github.com/kameshsampath/snow-utils-skills) creates:

1. AWS S3 bucket
2. IAM role and policy
3. Snowflake `CREATE EXTERNAL VOLUME`

In polaris-local-forge, the local equivalent is:

| Snowflake | Local (polaris-local-forge) |
|-----------|---------------------------|
| AWS S3 bucket | RustFS bucket (`aws s3 mb s3://bucket --endpoint-url http://localhost:19000`) |
| IAM role/policy | Static credentials: `admin` / `password` |
| External Volume SQL | Not needed — Apache Polaris catalog config points directly to `s3://bucket` with RustFS endpoint |

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

## Consuming Projects — Minimal Setup

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

**`pyproject.toml`** — copy from `user-project/pyproject.toml` in the skill repo.
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
| [polaris-local-forge](https://github.com/Snowflake-Labs/polaris-local-forge) | Local Apache Polaris infra | **This repo** — provides the local environment |
| [snow-utils-skills](https://github.com/kameshsampath/snow-utils-skills) | Snowflake PAT, External Volumes | Provides real AWS S3 equivalent (Phase 2 interop) |
| [kamesh-demo-skills](https://github.com/kameshsampath/kamesh-demo-skills) | Demo applications | Can consume polaris-local-forge as infrastructure |

### Future Interoperability

The `.snow-utils/` directory convention and manifest format are shared across all skill repos. A demo skill from `kamesh-demo-skills` could be adapted to target local Apache Polaris by pointing to the `polaris-local-forge` manifest for connection details.

## Phase 2: Real AWS S3 Support

Currently polaris-local-forge is fully local (RustFS). Real AWS S3 support would require:

- Real AWS credentials (IAM/SSO instead of static admin/password)
- Real S3 endpoint (remove `AWS_ENDPOINT_URL` override)
- IAM role ARN for Apache Polaris catalog `storageConfigInfo`
- Optional: skip RustFS deployment

This is tracked as a future enhancement. Contributions welcome.

## References

See [README.md](README.md#related-projects) for the full list of related projects and documentation links.
