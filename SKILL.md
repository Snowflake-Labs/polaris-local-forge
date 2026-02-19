---
name: polaris-local-forge
description: "Set up a local Apache Polaris (Incubating) development environment with RustFS S3-compatible storage, PostgreSQL, and k3d. Triggers: polaris local, local iceberg catalog, local polaris setup, rustfs setup, local s3 setup, create polaris cluster, setup local forge, verify polaris, try polaris locally, get started with polaris, get started with apache polaris, apache polaris quickstart, local iceberg development, polaris dev environment, local data lakehouse, try iceberg locally, set up local catalog, run polaris on my machine, polaris docker setup, local s3 iceberg, duckdb polaris local, replay polaris local forge, get started with apache polaris using example manifest, setup from example manifest, replay from manifest, reset polaris catalog, recreate catalog, cleanup catalog, setup catalog only, catalog reset, catalog cleanup, teardown polaris, delete polaris cluster, polaris status, verify polaris setup."
location: user
---

# Polaris Local Forge

Set up a complete local [Apache Polaris](https://polaris.apache.org/releases/1.3.0/) (Incubating) development environment with [RustFS](https://docs.rustfs.com/) S3-compatible storage, PostgreSQL metastore, and k3d/k3s Kubernetes -- all on your machine.

Query [Apache Iceberg](https://iceberg.apache.org/) tables locally with DuckDB, PyIceberg, or any Iceberg REST-compatible engine.

**MANIFEST FILE:** `.snow-utils/snow-utils-manifest.md` (exact path, always .md)

**PREREQUISITE:** NO PREREQUISITE -- this skill is self-contained infrastructure.

**AUTH MODEL:**
- **Bootstrap credentials:** Auto-generated admin credentials for Polaris realm setup (in `.bootstrap-credentials.env`)
- **Principal credentials:** API-generated client_id/client_secret for catalog access (in `work/principal.txt`)
- **RustFS credentials:** Static `admin`/`password` for S3-compatible storage (no IAM)

**SENSITIVE DATA:** `principal.txt` contains `realm,client_id,client_secret`. When displaying to user, show ONLY the realm. Mask credentials: `client_id: ****xxxx` (last 4 chars). NEVER show `client_secret` at all.

## Prerequisites

This skill requires the following tools installed on your machine:

| Tool | Purpose | Install |
|------|---------|---------|
| Docker | Container runtime | [Docker Desktop](https://www.docker.com/products/docker-desktop/) (>= 4.27) |
| k3d | k3s-in-Docker | `brew install k3d` or [k3d.io](https://k3d.io/) |
| Python | >= 3.12 | [python.org](https://www.python.org/downloads/) |
| uv | Python package manager | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |

**Optional:**

| Tool | Purpose | Install |
|------|---------|---------|
| DuckDB CLI | SQL verification | `brew install duckdb` |
| AWS CLI | S3 bucket operations on RustFS | `brew install awscli` |
| direnv | Auto-load env vars | `brew install direnv` |

## Workflow

**FORBIDDEN ACTIONS -- NEVER DO THESE:**

- NEVER skip prerequisite checks -- always run `doctor` first
- NEVER delete `.snow-utils/` directory or manifest -- preserve for audit/cleanup/replay
- NEVER hardcode credentials in scripts -- always read from `.env` or `.snow-utils/`
- NEVER assume the cluster is running -- always check status before catalog operations
- NEVER use `sed/awk/bash` to edit manifest files -- use the file editing tool (Edit/StrReplace)
- NEVER run destructive commands (`teardown`, `cluster:delete`, `polaris:purge`) without explicit user confirmation
- NEVER expose `principal.txt` contents in output -- show only the realm. Mask client_id: show `****` + last 4 chars. NEVER show client_secret at all. Example: `realm: default-realm, client_id: ****a1b2, client_secret: ********`
- NEVER modify files under `k8s/`, `polaris-forge-setup/`, or `src/` -- those are infrastructure code, not skill artifacts
- NEVER guess or invent CLI options -- ONLY use options from the CLI Reference tables below. If a command fails with "No such option", run `uv run polaris-local-forge <command> --help` and use ONLY the options shown there

**INTERACTIVE PRINCIPLE:** This skill is designed to be interactive. At every decision point, ASK the user and WAIT for their response before proceeding.

**DISPLAY PRINCIPLE:** When showing configuration or status, substitute actual values from `.env` and the manifest. The user should see real values, not raw `${...}` placeholders.

**RESILIENCE PRINCIPLE:** Always update the manifest IMMEDIATELY after each resource creation step, not in batches. This ensures Resume Flow can recover from any interruption.

Pattern:

```
1. Set overall Status: IN_PROGRESS at START of resource creation
2. Update each resource row to DONE immediately after creation
3. Set Status: COMPLETE only at the END when ALL resources verified
```

If user aborts mid-flow, the manifest preserves progress:
- Overall Status stays IN_PROGRESS
- Completed resources show DONE
- Pending resources show PENDING/REMOVED
- Resume Flow picks up from first non-DONE resource

**IDEMPOTENCY PRINCIPLE:** Before editing any file, CHECK if the change is already applied.

Pattern for manifest updates:

```bash
grep -q "Status.*COMPLETE" .snow-utils/snow-utils-manifest.md && echo "Already complete" || echo "Needs update"
```

Pattern for file edits:

```
1. Read current file state
2. Check if desired content already exists
3. Only edit if change is needed
4. Skip with message: "Already applied: [description]"
```

**ENVIRONMENT REQUIREMENT:** The skill uses local RustFS for S3-compatible storage. AWS CLI commands target `http://localhost:9000` with static credentials. No real AWS account is needed.

> **Note:** This skill configures Polaris with local RustFS (S3-compatible) storage only.
> For real AWS S3 support, see Phase 2 in [SKILL_README.md](SKILL_README.md).

### Step 0: Detect or Create Project Directory

**First, check if already in the polaris-local-forge project directory:**

```bash
if [ -f pyproject.toml ] && grep -q "polaris-local-forge" pyproject.toml 2>/dev/null; then
  echo "Detected polaris-local-forge project: $(pwd)"
  [ -f .env ] && echo "  Found: .env"
  [ -d .snow-utils ] && echo "  Found: .snow-utils/"
fi
```

**If existing project detected -> go to Step 0a (Prerequisites Check).**

**If NOT in polaris-local-forge project, ask user:**

```
polaris-local-forge project not detected in current directory.

Options:
  1. Clone to ./polaris-local-forge (recommended)
  2. Clone to a custom directory name
  3. I already have it cloned -- let me navigate there
```

**STOP**: Wait for user input.

**If clone requested:**

```bash
PROJECT_DIR="${PROJECT_DIR:-polaris-local-forge}"
git clone https://github.com/kameshsampath/polaris-local-forge "${PROJECT_DIR}"
cd "${PROJECT_DIR}"
```

**Infer PROJECT_NAME from directory:**

```bash
PROJECT_NAME=$(basename $(pwd))
echo "Project: ${PROJECT_NAME}"
```

> **IMPORTANT:** All subsequent steps run within the `polaris-local-forge` project directory. The manifest, `.env`, and all artifacts live here.

### Step 0a: Prerequisites Check

**Check manifest for cached tool verification:**

```bash
grep "^tools_verified:" .snow-utils/snow-utils-manifest.md 2>/dev/null
```

**If `tools_verified:` exists with a date:** Skip tool checks, continue to Step 0b.

**Otherwise, run prerequisite check:**

```bash
uv run polaris-local-forge doctor
```

If any tool is missing, stop and provide installation instructions from the Prerequisites table above.

**STOP**: Do not proceed until all prerequisites pass.

**After all tools verified, update manifest:**

```bash
grep -q "^tools_verified:" .snow-utils/snow-utils-manifest.md 2>/dev/null || \
  echo "tools_verified: $(date +%Y-%m-%d)" >> .snow-utils/snow-utils-manifest.md 2>/dev/null || true
```

### Step 0b: Detect or Initialize Manifest

#### Remote Manifest URL Detection

If the user provides a URL (in their prompt or pasted), detect and normalize it **before** local manifest detection:

**Supported URL patterns and translation rules:**

- **GitHub blob:** `https://github.com/{owner}/{repo}/blob/{branch}/{path}` -> replace host with `raw.githubusercontent.com` and remove `/blob/` segment
- **GitHub raw:** `https://raw.githubusercontent.com/...` -> use as-is
- **GitHub gist:** `https://gist.github.com/{user}/{id}` -> append `/raw` if not already present
- **Any other HTTPS URL ending in `.md`** -> use as-is

**After translating, show user and confirm:**

```
Found manifest URL. Download URL:
  <translated_raw_url>

Download to current directory as <filename>? [yes/no]
```

**STOP**: Wait for user confirmation.

**If yes:**

```bash
curl -fSL -o <filename> "<translated_raw_url>"
```

After successful download, continue with local manifest detection below.

---

**Check for existing manifest:**

```bash
WORKING_MANIFEST=""
SHARED_MANIFEST=""

if [ -f .snow-utils/snow-utils-manifest.md ]; then
  WORKING_MANIFEST="EXISTS"
  WORKING_STATUS=$(grep "^\*\*Status:\*\*" .snow-utils/snow-utils-manifest.md | head -1)
  echo "Working manifest: .snow-utils/snow-utils-manifest.md (${WORKING_STATUS})"
fi

for f in *-manifest.md; do
  [ -f "$f" ] && grep -q "## shared_info\|COCO_INSTRUCTION" "$f" 2>/dev/null && {
    SHARED_MANIFEST="$f"
    echo "Shared manifest: $f"
  }
done
```

**Decision matrix:**

| Working Manifest | Shared Manifest | Action |
|-----------------|-----------------|--------|
| None | None | Fresh start -> Step 0d |
| None | Exists | Copy shared to `.snow-utils/` -> Step 0c |
| Exists (REMOVED) | None | Replay Flow (reuse existing config) |
| Exists (COMPLETE) | None | Ask user: re-run, reset, or skip |
| Exists (IN_PROGRESS) | None | Resume Flow (continue from last step) |
| Exists | Exists | **Conflict** -- ask user which to use |

**If BOTH manifests exist, show:**

```
Found two manifests:

  1. Working manifest: .snow-utils/snow-utils-manifest.md
     Status: <WORKING_STATUS>

  2. Shared manifest: <SHARED_MANIFEST>
     (contains resource definitions from another setup)

Which manifest should we use?
  A. Resume working manifest (continue where you left off)
  B. Start fresh from shared manifest (backup working, adapt values)
  C. Cancel
```

**STOP**: Wait for user choice.

**If using example manifest** (user says "get started with apache polaris using example manifest"):

```bash
mkdir -p .snow-utils && chmod 700 .snow-utils
cp example-manifests/polaris-local-forge-manifest.md .snow-utils/snow-utils-manifest.md
chmod 600 .snow-utils/snow-utils-manifest.md
```

Then proceed to **Step 0c** to check for adaptive markers.

### Step 0c: Shared Manifest Adapt-Check

**ALWAYS run this step when using a shared or example manifest. Prompt user ONLY if `# ADAPT:` markers are found.**

```bash
ADAPT_COUNT=$(grep -c "# ADAPT:" .snow-utils/snow-utils-manifest.md 2>/dev/null)
echo "ADAPT markers found: ${ADAPT_COUNT}"
```

**If `ADAPT_COUNT` > 0 (markers found):**

Extract all values with `# ADAPT:` markers and present to user:

```
Manifest Value Review
─────────────────────
The following values can be customized for your environment:

  Setting                       Default Value          Marker
  ────────────────────────────  ─────────────────────  ──────────────────────
  K3D_CLUSTER_NAME:             polaris-local-forge    # ADAPT: customizable
  PLF_POLARIS_S3_BUCKET:        polaris                # ADAPT: customizable
  PLF_POLARIS_CATALOG_NAME:     polardb                # ADAPT: customizable
  PLF_POLARIS_PRINCIPAL_NAME:   iceberg                # ADAPT: customizable
  KUBECONFIG:                   (derived from cluster name)
  KUBECTL_PATH:                 (derived from cluster name)

Options:
  1. Accept all defaults (recommended for first-time setup)
  2. Edit a specific value
  3. Cancel
```

**STOP**: Wait for user choice.

| Choice | Action |
|--------|--------|
| **1 -- Accept all** | Proceed with defaults |
| **2 -- Edit specific** | Ask which value, update manifest in-place, re-display |
| **3 -- Cancel** | Stop |

**If user changes `K3D_CLUSTER_NAME`:** Automatically update derived values (`KUBECONFIG`, `KUBECTL_PATH`, resource table row 1) in the manifest. Also update `.env` with the new cluster name.

**If `ADAPT_COUNT` = 0 (no markers):** Proceed silently with values as-is.

### Step 0d: Initialize Manifest

```bash
mkdir -p .snow-utils && chmod 700 .snow-utils
if [ ! -f .snow-utils/snow-utils-manifest.md ]; then
cat > .snow-utils/snow-utils-manifest.md << 'EOF'
# Snow-Utils Manifest

This manifest tracks resources created by polaris-local-forge.

---

## project_recipe
project_name: polaris-local-forge

## prereqs

## required_skills
polaris-local-forge: https://github.com/kameshsampath/polaris-local-forge
EOF
fi
chmod 600 .snow-utils/snow-utils-manifest.md
```

### Step 1: Environment Setup

**SHOW -- what we're about to do:**

> Set up Python environment and install all dependencies.
> This creates a virtual environment, pins Python 3.12, and installs
> packages defined in `pyproject.toml` (including `boto3`, `pyiceberg[s3fs]`, `duckdb`).

**STOP**: Wait for user confirmation before proceeding.

**DO:**

```bash
[ -f .env ] && echo "Existing .env found -- keeping it." || cp .env.example .env
uv python pin 3.12
uv venv
uv sync
```

**SUMMARIZE:**

> Environment ready. Python venv created, all dependencies installed.
> Configuration loaded from `.env`.

### Step 2: Generate Configuration Files

**SHOW -- what we're about to do:**

> Generate configuration files from templates using Ansible:
>
> - RSA key pair for Polaris token authentication
> - Bootstrap credentials for initial Polaris realm setup
> - PostgreSQL Helm chart values
> - Polaris Helm chart values with S3 endpoint configuration
> - Kubernetes secrets manifest

**STOP**: Wait for user confirmation before proceeding.

**DO:**

```bash
uv run polaris-local-forge prepare
```

**Post-step -- copy cluster-level files to scoped directory:**

```bash
CLUSTER_NAME=$(grep K3D_CLUSTER_NAME .env | cut -d= -f2)
CLUSTER_NAME=${CLUSTER_NAME:-polaris-local-forge}
SCOPE_DIR=".snow-utils/${CLUSTER_NAME}"

mkdir -p "${SCOPE_DIR}/work" "${SCOPE_DIR}/bin" "${SCOPE_DIR}/.kube"
chmod 700 "${SCOPE_DIR}"

cp k8s/polaris/.bootstrap-credentials.env "${SCOPE_DIR}/"
cp k8s/polaris/polaris-secrets.yaml "${SCOPE_DIR}/" 2>/dev/null || true
cp k8s/polaris/rsa_key "${SCOPE_DIR}/" 2>/dev/null || true
cp k8s/polaris/rsa_key.pub "${SCOPE_DIR}/" 2>/dev/null || true
cp k8s/features/polaris.yaml "${SCOPE_DIR}/" 2>/dev/null || true
cp k8s/features/postgresql.yaml "${SCOPE_DIR}/" 2>/dev/null || true
cp k8s/polaris/.polaris.env "${SCOPE_DIR}/" 2>/dev/null || true
cp bin/kubectl "${SCOPE_DIR}/bin/" 2>/dev/null || true

chmod 600 "${SCOPE_DIR}"/*
```

**Update manifest:** Record tools_verified date and K3S/kubectl versions.

**SUMMARIZE:**

> Configuration files generated and copied to `.snow-utils/${CLUSTER_NAME}/`.
> RSA keys, bootstrap credentials, and Helm chart values are ready.

### Step 3: Create Cluster

**SHOW -- what we're about to do:**

> Create a k3d cluster with the following configuration:
>
> - **Cluster name:** ${K3D_CLUSTER_NAME}
> - **K3S version:** ${K3S_VERSION}
> - **RustFS S3:** localhost:9000
> - **RustFS Console:** localhost:9001
> - **PostgreSQL:** internal metastore
> - **Polaris API:** localhost:18181
>
> This will create a local Kubernetes cluster running in Docker.

**STOP**: Wait for user confirmation before proceeding.

**DO:**

```bash
uv run polaris-local-forge cluster create
```

**Post-step -- copy kubeconfig and activate scoped environment:**

```bash
cp .kube/config "${SCOPE_DIR}/.kube/config"
chmod 600 "${SCOPE_DIR}/.kube/config"
```

**Set the scoped cluster environment for this session:**

```bash
export KUBECONFIG="${SCOPE_DIR}/.kube/config"
export PATH="${SCOPE_DIR}/bin:$PATH"
```

This ensures:
- `kubectl` resolves to the version-matched binary in `.snow-utils/<cluster-name>/bin/`
- `KUBECONFIG` points to the scoped kubeconfig in `.snow-utils/<cluster-name>/.kube/config`
- All subsequent `kubectl` and `uv run polaris-local-forge` commands use the correct cluster

**Verify scoped kubectl works:**

```bash
kubectl version --client
kubectl get nodes
```

**SUMMARIZE:**

> k3d cluster `${K3D_CLUSTER_NAME}` created. Kubeconfig and kubectl
> scoped to `.snow-utils/${CLUSTER_NAME}/`. Cluster is starting up.

### Step 4: Wait for Bootstrap

**SHOW -- what we're about to do:**

> Wait for the bootstrap deployments (RustFS, PostgreSQL) to be ready.
> This may take 1-3 minutes while containers pull images and start.

**DO:**

```bash
uv run polaris-local-forge cluster bootstrap-check
```

**SUMMARIZE:**

> Bootstrap complete. RustFS and PostgreSQL are running.

### Step 5: Deploy Polaris

**SHOW -- what we're about to do:**

> Deploy Apache Polaris (Incubating) to the cluster. This installs the Polaris server
> and runs the bootstrap job to initialize the default realm.

**DO:**

```bash
uv run polaris-local-forge polaris deploy
```

**SUMMARIZE:**

> Polaris deployment submitted. Waiting for it to become ready.

### Step 6: Wait for Polaris

**DO:**

```bash
uv run polaris-local-forge cluster polaris-check
```

**SUMMARIZE:**

> Polaris is running at `http://localhost:18181`. Realm `${POLARIS_REALM}` is initialized.

### Step 7: S3/RustFS Configuration

**SHOW -- Local AWS settings for RustFS:**

> RustFS provides S3-compatible storage at `http://localhost:9000`.
> These settings let you use `aws` CLI and any S3-compatible SDK against RustFS:
>
> ```
> AWS_ENDPOINT_URL=http://localhost:9000
> AWS_REGION=us-east-1
> AWS_ACCESS_KEY_ID=admin
> AWS_SECRET_ACCESS_KEY=password
> ```
>
> **Local equivalent of `snow-utils-volumes`:** In Snowflake environments,
> `snow-utils-volumes` creates real AWS S3 buckets + IAM roles + Snowflake
> External Volumes. Here, RustFS provides the S3 layer directly -- no IAM
> roles needed, no External Volume SQL. Polaris catalog config points to
> `s3://bucket` with the RustFS endpoint.

**DO -- verify RustFS is accessible (if AWS CLI is installed):**

```bash
aws s3 ls --endpoint-url http://localhost:9000
```

**Create additional S3 buckets (optional):**

```bash
aws s3 mb s3://my-bucket --endpoint-url http://localhost:9000
```

**SUMMARIZE:**

> RustFS S3 is accessible at `http://localhost:9000`. Credentials: `admin`/`password`.
> AWS CLI and S3 SDKs work with these local settings.

### Step 8: Catalog Setup

**SHOW -- what we're about to do:**

> Set up the Polaris catalog on RustFS:
>
> - **S3 bucket:** `${PLF_POLARIS_S3_BUCKET}` (on RustFS at localhost:9000)
> - **Catalog:** `${PLF_POLARIS_CATALOG_NAME}` (REST catalog backed by S3)
> - **Principal:** `${PLF_POLARIS_PRINCIPAL_NAME}` (with client_id/client_secret)
> - **Roles and grants:** catalog admin + principal role assignment
>
> This creates the S3 bucket, registers the catalog with Polaris,
> creates a principal for external access, and configures RBAC grants.

**STOP**: Wait for user confirmation before proceeding.

**DO:**

```bash
uv run polaris-local-forge catalog setup
```

**Post-step -- copy catalog-level files to scoped directory:**

```bash
cp work/principal.txt "${SCOPE_DIR}/work/principal.txt"
chmod 600 "${SCOPE_DIR}/work/principal.txt"
cp scripts/explore_catalog.sql "${SCOPE_DIR}/" 2>/dev/null || true
```

**Update manifest -- set Status: IN_PROGRESS and mark each resource DONE as created:**

```markdown
<!-- START -- polaris-local-forge:${CLUSTER_NAME} -->
## Polaris Local Forge: ${CLUSTER_NAME}

**Created:** {TIMESTAMP}
**Status:** IN_PROGRESS

### Cluster
**K3D_CLUSTER_NAME:** ${K3D_CLUSTER_NAME}
**K3S_VERSION:** ${K3S_VERSION}

### RustFS S3
**AWS_ENDPOINT_URL:** http://localhost:9000

### Polaris
**POLARIS_URL:** http://localhost:18181
**POLARIS_REALM:** ${POLARIS_REALM}

### Catalog
**PLF_POLARIS_S3_BUCKET:** ${PLF_POLARIS_S3_BUCKET}
**PLF_POLARIS_CATALOG_NAME:** ${PLF_POLARIS_CATALOG_NAME}
**PLF_POLARIS_PRINCIPAL_NAME:** ${PLF_POLARIS_PRINCIPAL_NAME}

### Resources

| # | Type | Name | Status |
|---|------|------|--------|
| 1 | k3d Cluster | ${K3D_CLUSTER_NAME} | DONE |
| 2 | RustFS | S3-compatible storage | DONE |
| 3 | PostgreSQL | Polaris metastore | DONE |
| 4 | Polaris | REST Catalog server | DONE |
| 5 | S3 Bucket | ${PLF_POLARIS_S3_BUCKET} | DONE |
| 6 | Catalog | ${PLF_POLARIS_CATALOG_NAME} | DONE |
| 7 | Principal | ${PLF_POLARIS_PRINCIPAL_NAME} | DONE |
<!-- END -- polaris-local-forge:${CLUSTER_NAME} -->
```

**SUMMARIZE:**

> Catalog `${PLF_POLARIS_CATALOG_NAME}` created with principal `${PLF_POLARIS_PRINCIPAL_NAME}`.
> Credentials saved to `.snow-utils/${CLUSTER_NAME}/work/principal.txt`
> (realm shown, client_id/secret masked).

### Step 9: Verification

**SHOW -- what we're about to do:**

> Verify the entire setup by running a DuckDB SQL script that:
>
> 1. Connects to Polaris REST catalog at `http://localhost:18181`
> 2. Authenticates using the principal credentials from `work/principal.txt`
> 3. Creates a test Iceberg table on RustFS
> 4. Queries the table to confirm read/write works
>
> This confirms: Polaris API, RustFS S3, catalog, principal, and RBAC grants are all working.

**DO:**

```bash
uv run polaris-local-forge catalog verify-sql
```

**Check the result:**

- **If SUCCEEDED:** Continue to Step 9a (generate notebook)
- **If FAILED:** Check Troubleshooting section. Common issues: Polaris not ready, RustFS not accessible, principal credentials invalid.

**SUMMARIZE (on success):**

> Verification passed! DuckDB successfully queried Iceberg tables via Polaris REST catalog on RustFS.

### Step 9a: Generate Notebook

**SHOW -- what we're about to do:**

> Generate a Jupyter notebook for interactive Polaris and Iceberg exploration.
> The notebook is pre-configured with your catalog connection details and
> principal credentials, ready to run.

**DO:**

```bash
uv run polaris-local-forge catalog generate-notebook
```

**Post-step -- copy notebook to scoped directory:**

```bash
cp notebooks/verify_polaris.ipynb "${SCOPE_DIR}/" 2>/dev/null || true
```

**SUMMARIZE:**

> Notebook generated at `notebooks/verify_polaris.ipynb`.
> Open it with `jupyter notebook notebooks/verify_polaris.ipynb` for interactive exploration.

### Step 10: Summary

**SUMMARIZE -- Setup Complete:**

```
Polaris Local Forge -- Setup Complete!

Service URLs:
  Polaris API:     http://localhost:18181
  RustFS S3:       http://localhost:9000
  RustFS Console:  http://localhost:9001

Catalog:
  Name:      ${PLF_POLARIS_CATALOG_NAME}
  S3 Bucket: ${PLF_POLARIS_S3_BUCKET}
  Principal: ${PLF_POLARIS_PRINCIPAL_NAME}

Credentials:
  RustFS:    admin / password
  Polaris:   See .snow-utils/${CLUSTER_NAME}/work/principal.txt
  Bootstrap: See .snow-utils/${CLUSTER_NAME}/bootstrap-credentials.env

Next steps:
  jupyter notebook notebooks/verify_polaris.ipynb   # Interactive exploration
  uv run polaris-local-forge catalog verify-sql      # Re-run verification
  uv run polaris-local-forge catalog explore-sql     # Interactive DuckDB SQL

Scoped kubectl (for direct cluster queries):
  export KUBECONFIG=.snow-utils/${CLUSTER_NAME}/.kube/config
  export PATH=.snow-utils/${CLUSTER_NAME}/bin:$PATH
  kubectl get pods -n polaris

Manifest: .snow-utils/snow-utils-manifest.md
```

**Update manifest status to COMPLETE.**

## Catalog-Only Flows

These flows operate on the catalog without rebuilding the cluster.

### Catalog Setup (cluster must be running)

**Trigger:** "setup catalog only", "create catalog"

```bash
uv run polaris-local-forge catalog setup
```

### Catalog Cleanup

**Trigger:** "cleanup catalog", "remove catalog"

**STOP**: Confirm with user before executing.

```bash
uv run polaris-local-forge catalog cleanup --yes
```

Updates manifest status to REMOVED. Generated files in `.snow-utils/<cluster-name>/` are preserved.

### Catalog Reset

**Trigger:** "reset polaris catalog", "recreate catalog"

**STOP**: Confirm with user before executing.

```bash
uv run polaris-local-forge catalog cleanup --yes
uv run polaris-local-forge catalog setup
```

Runs cleanup + setup. Generates new `principal.txt` (new credentials). Copies fresh catalog-level files to `.snow-utils/<cluster-name>/work/`.

### Full Catalog Reset

**Trigger:** "full catalog reset", "purge polaris database"

**STOP**: This is destructive. Confirm with user.

```bash
uv run polaris-local-forge polaris reset --yes
uv run polaris-local-forge catalog setup
```

Purges the entire Polaris database and recreates from scratch.

## Teardown Flow

**Trigger:** "teardown polaris", "delete everything", "clean up"

**STOP**: This is destructive. Confirm with user.

```bash
uv run polaris-local-forge teardown --yes
```

Updates manifest status to REMOVED. Files in `.snow-utils/<cluster-name>/` are **preserved** for future replay.

### Explicit Purge

**Trigger:** "purge all generated files", "start completely fresh"

**STOP**: This is irreversible. Confirm with user.

```bash
uv run polaris-local-forge teardown --yes
rm -rf .snow-utils/${CLUSTER_NAME}
```

Only offered when user explicitly asks to wipe everything.

## Replay Flow

**Trigger:** "replay polaris local forge", "recreate environment"

When manifest has `Status: REMOVED`:

1. Read config values from manifest
2. Show replay plan to user
3. On confirmation, execute Steps 1-10
4. Reuse cluster-level files from `.snow-utils/<cluster-name>/` (RSA keys, manifests)
5. Re-activate scoped environment (`KUBECONFIG`, `PATH` for kubectl)
6. Regenerate catalog-level files (new principal credentials)
7. Update manifest to COMPLETE

## Consuming Projects: Minimal Setup

A separate project using polaris-local-forge as infrastructure needs only:

**In the project directory:**

- `.env` with:

```bash
POLARIS_URL=http://localhost:18181
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_REGION=us-east-1
# From .snow-utils/<cluster-name>/work/principal.txt:
POLARIS_REALM=POLARIS
CLIENT_ID=<from principal.txt>
CLIENT_SECRET=<from principal.txt>
```

- A notebook (`.ipynb`) or SQL scripts for querying
- `pyproject.toml` with query deps (`duckdb`, `pyiceberg[s3fs]`)

**NOT needed:** k8s manifests, ansible, polaris-local-forge CLI source.

## Stopping Points

1. Step 0: Ask for project directory (if not detected)
2. Step 0a: If prerequisites missing
3. Step 0b: Manifest detection (ask which to use if conflict)
4. Step 0c: Adapt-check (if shared manifest has `# ADAPT:` markers)
5. Step 1: Before environment setup
6. Step 2: Before generating config files
7. Step 3: Before cluster creation
8. Step 8: Before catalog setup
9. Catalog-only flows: Before cleanup/reset
10. Teardown: Before destructive operations

## CLI Reference

All commands use `uv run polaris-local-forge` (aliased as `plf` below for brevity).

**OPTION NAMES (NEVER guess or invent options):**

> ONLY use options listed in the tables below. If a command fails with "No such option",
> run `uv run polaris-local-forge <command> --help` to see actual available options and
> use ONLY those. NEVER invent, abbreviate, or rename options.

**`--yes` is REQUIRED** when executing destructive commands after user has approved (CLIs prompt interactively which does not work in Cortex Code's non-interactive shell). All destructive commands support `--dry-run` to preview and `--yes` to skip interactive confirmation.

### Setup & Teardown

| Command | Description |
|---------|-------------|
| `plf setup --yes` | Complete setup (cluster + Polaris + catalog) |
| `plf setup --dry-run` | Preview setup plan without executing |
| `plf teardown --yes` | Complete teardown (cleanup + delete cluster) |
| `plf teardown --dry-run` | Preview teardown plan without executing |

### Status & Config

| Command | Description |
|---------|-------------|
| `plf doctor` | Check system prerequisites |
| `plf doctor --output json` | Prerequisites as JSON (for automation) |
| `plf cluster status` | Show cluster status |
| `plf polaris status` | Show Polaris status |
| `plf config` | Show current configuration |
| `plf config --output json` | Configuration as JSON |

### Cluster

| Command | Description |
|---------|-------------|
| `plf cluster create` | Create k3d cluster |
| `plf cluster delete --yes` | Delete cluster |
| `plf cluster bootstrap-check` | Wait for bootstrap deployments |
| `plf cluster polaris-check` | Wait for Polaris deployment |

### Polaris

| Command | Description |
|---------|-------------|
| `plf polaris deploy` | Deploy Polaris to cluster |
| `plf polaris check` | Verify Polaris deployment |
| `plf polaris reset --yes` | Purge and re-bootstrap Polaris |

### Catalog

| Command | Description |
|---------|-------------|
| `plf catalog setup` | Setup demo catalog |
| `plf catalog cleanup --yes` | Cleanup catalog resources |
| `plf catalog verify-sql` | Verify with DuckDB (non-interactive) |
| `plf catalog explore-sql` | Explore with DuckDB (interactive) |
| `plf catalog list` | List catalogs |
| `plf catalog generate-notebook` | Generate verification notebook |

### Logs & Troubleshooting (via scoped kubectl)

| Command | Description |
|---------|-------------|
| `kubectl logs -f -n polaris deployment/polaris` | Stream Polaris logs |
| `kubectl logs -f -n polaris statefulset/postgresql` | Stream PostgreSQL logs |
| `kubectl logs -f -n rustfs deployment/rustfs` | Stream RustFS logs |
| `kubectl get events -n polaris --sort-by='.lastTimestamp'` | Recent Polaris events |
| `kubectl describe pod -n polaris -l app=polaris` | Diagnose Polaris pod |

## Troubleshooting

**Polaris pod stuck in ContainerCreating:**

```bash
kubectl get events -n polaris --sort-by='.lastTimestamp'
kubectl describe pod -n polaris -l app=polaris
uv run polaris-local-forge polaris deploy
```

**RustFS not accessible:**

```bash
kubectl get svc -n rustfs
aws s3 ls --endpoint-url http://localhost:9000
```

**Bootstrap job fails:**

```bash
kubectl logs -f -n polaris jobs/polaris-bootstrap
uv run polaris-local-forge polaris reset --yes
```

**Catalog setup fails (S3 bucket error):**

```bash
aws s3 ls --endpoint-url http://localhost:9000
uv run polaris-local-forge catalog cleanup --yes
uv run polaris-local-forge catalog setup
```

**DuckDB verification fails:**

Ensure the catalog is set up and principal credentials are valid:

```bash
uv run polaris-local-forge catalog list
cat work/principal.txt
```

**Official documentation:**

- [Apache Polaris (Incubating) 1.3.0](https://polaris.apache.org/releases/1.3.0/)
- [Polaris Management API Spec](https://polaris.apache.org/releases/1.3.0/polaris-api-specs/polaris-management-api/)
- [Polaris Catalog API Spec (Swagger)](https://editor.swagger.io/?url=https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/generated/bundled-polaris-catalog-service.yaml)
- [RustFS Documentation](https://docs.rustfs.com/)
- [Apache Iceberg](https://iceberg.apache.org/)

## Directory Structure

After skill-based setup:

```
polaris-local-forge/
├── .env                              # Environment configuration
├── .snow-utils/
│   ├── snow-utils-manifest.md        # Resource tracking manifest
│   └── polaris-local-forge/          # Scoped by cluster name
│       ├── bin/kubectl               # Version-matched kubectl
│       ├── .kube/config              # Cluster kubeconfig
│       ├── work/
│       │   └── principal.txt         # Catalog credentials (chmod 600)
│       ├── bootstrap-credentials.env
│       ├── polaris-secrets.yaml
│       ├── polaris.yaml
│       ├── postgresql.yaml
│       ├── .polaris.env
│       ├── rsa_key / rsa_key.pub
│       └── explore_catalog.sql
├── config/cluster-config.yaml
├── k8s/                              # Kubernetes manifests (infra)
├── notebooks/verify_polaris.ipynb
├── polaris-forge-setup/              # Ansible playbooks (infra)
├── scripts/explore_catalog.sql
├── src/polaris_local_forge/          # CLI source (infra)
├── pyproject.toml
├── Taskfile.yml                      # Optional: task runner (not required by skill)
├── SKILL.md                          # This file
├── SKILL_README.md                   # Skills documentation
└── example-manifests/
    └── polaris-local-forge-manifest.md
```
