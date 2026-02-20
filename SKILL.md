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
| Podman (default) | Container runtime (OSS) | `brew install podman` or [podman.io](https://podman.io/) |
| Docker (alternative) | Container runtime | [Docker Desktop](https://www.docker.com/products/docker-desktop/) (>= 4.27) |
| k3d | k3s-in-Docker/Podman | `brew install k3d` or [k3d.io](https://k3d.io/) |
| Python | >= 3.12 | [python.org](https://www.python.org/downloads/) |
| uv | Python package manager | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |

**Container Runtime:** Podman is the preferred runtime (fully OSS, shipped with Cortex Code). Auto-detection checks for Podman first, then Docker. Set `PLF_CONTAINER_RUNTIME=docker` in `.env` to use Docker instead. See [docs/podman-setup.md](docs/podman-setup.md) for Podman machine setup.

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
- NEVER modify files in the skill directory (`<SKILL_DIR>`) -- `k8s/`, `polaris-forge-setup/`, `src/` are read-only source. Only the user's `--work-dir` is writable
- NEVER guess or invent CLI options -- ONLY use options from the CLI Reference tables below. If a command fails with "No such option", run `${PLF} <command> --help` and use ONLY the options shown there

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

**Pre-Check Rules (Fail Fast):**

| Command | Pre-Check | If Fails |
|---------|-----------|----------|
| `setup` | Docker running, k3d installed | Stop: "Prerequisites missing. Run `doctor` first." |
| `setup --fresh` | Docker running, k3d installed | Stop: "Prerequisites missing. Run `doctor` first." |
| `cluster create` | Cluster doesn't exist | Stop: "Cluster already exists. Use `cluster delete` first or `setup` to resume." |
| `catalog setup` | Cluster running, Polaris ready | Stop: "Cluster not ready. Run `setup` first." |
| `polaris deploy` | Cluster running | Stop: "Cluster not running. Run `cluster create` first." |
| `teardown` | Any resources exist | Proceed gracefully (idempotent with `--yes`) |
| `catalog cleanup` | Catalog exists | Proceed gracefully (idempotent with `--yes`) |

### Step 0: Initialize Project Directory

**Detect if user already has a workspace set up:**

```bash
if [ -f .env ] && [ -f pyproject.toml ]; then
  echo "Existing workspace detected: $(pwd)"
  [ -d .snow-utils ] && echo "  Found: .snow-utils/"
fi
```

**If existing workspace detected -> go to Step 0a (Prerequisites Check).**

**If NOT in an existing workspace, ask user:**

```
Where would you like to create your Polaris workspace?

Options:
  1. Use current directory: $(pwd)
  2. Create a new directory (e.g., polaris-dev)
```

**STOP**: Wait for user input.

**Initialize the workspace with lightweight project files:**

```bash
cp <SKILL_DIR>/user-project/pyproject.toml .
cp <SKILL_DIR>/.env.example .env
```

**Infer PROJECT_NAME from directory and write it into `.env`:**

```bash
PROJECT_NAME=$(basename $(pwd))
echo "Project: ${PROJECT_NAME}"
```

If `K3D_CLUSTER_NAME` is not already set in `.env`, append the inferred value:

```bash
grep -q "^K3D_CLUSTER_NAME=" .env || echo "K3D_CLUSTER_NAME=${PROJECT_NAME}" >> .env
```

> **IMPORTANT:** All subsequent CLI commands use `--work-dir` to point generated files here.
> The skill directory (`<SKILL_DIR>`) stays read-only. For a second cluster, create another
> directory and re-run the skill.

**Define the CLI shorthand used throughout this skill:**

```bash
PLF="uv run --project <SKILL_DIR> polaris-local-forge --work-dir ."
```

All subsequent `${PLF} <command>` invocations use this pattern.

### Step 0a: Configuration Review and Confirmation

**Display current configuration for user review:**

```
Configuration Review
────────────────────
  Config file:                   .env
  Work directory:                $(pwd)

  PLF_CONTAINER_RUNTIME:         ${PLF_CONTAINER_RUNTIME}  # ADAPT: podman (default) or docker
  PLF_PODMAN_MACHINE:            ${PLF_PODMAN_MACHINE}     # macOS only (default: k3d)

  K3D_CLUSTER_NAME:              ${K3D_CLUSTER_NAME}      # ADAPT: defaults to project directory name
  K3S_VERSION:                   ${K3S_VERSION}
  KUBECONFIG:                    .kube/config

  AWS_ENDPOINT_URL:              http://localhost:9000
  AWS_REGION:                    us-east-1
  AWS_ACCESS_KEY_ID:             admin

  POLARIS_URL:                   http://localhost:18181
  POLARIS_REALM:                 ${POLARIS_REALM}           # ADAPT: customizable

  PLF_POLARIS_S3_BUCKET:         ${PLF_POLARIS_S3_BUCKET}    # ADAPT: customizable
  PLF_POLARIS_CATALOG_NAME:      ${PLF_POLARIS_CATALOG_NAME} # ADAPT: customizable
  PLF_POLARIS_PRINCIPAL_NAME:    ${PLF_POLARIS_PRINCIPAL_NAME} # ADAPT: customizable

Review the configuration above. Would you like to change any values?
  1. Accept all (recommended for first-time setup)
  2. Edit a specific value
  3. Cancel
```

**STOP**: Wait for user input.

**If user edits values:** Update `.env`, re-display, and confirm again.

**After confirmation, proceed to prerequisites check.**

### Step 0b: Prerequisites Check

**Check manifest for cached tool verification:**

```bash
grep "^tools_verified:" .snow-utils/snow-utils-manifest.md 2>/dev/null
```

**If `tools_verified:` exists with a date:** Skip tool checks, continue to Step 0c.

**Otherwise, run prerequisite check:**

```bash
${PLF} doctor
```

The doctor command checks:
- Container runtime detection (Podman preferred, Docker fallback)
- If Podman: machine state, CPUs, memory, cgroup v2, k3d network
- Required tools: k3d, Python, uv
- Environment: .env, venv, cluster

If Podman is detected but the machine is missing or under-provisioned, the user should run `task podman:setup` before proceeding.

If any tool is missing, stop and provide installation instructions from the Prerequisites table above.

**STOP**: Do not proceed until all prerequisites pass.

**After all tools verified, update manifest:**

```bash
grep -q "^tools_verified:" .snow-utils/snow-utils-manifest.md 2>/dev/null || \
  echo "tools_verified: $(date +%Y-%m-%d)" >> .snow-utils/snow-utils-manifest.md 2>/dev/null || true
```

### Step 0c: Detect or Initialize Manifest

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
  [ -f "$f" ] && grep -q "## shared_info\|CORTEX_CODE_INSTRUCTION" "$f" 2>/dev/null && {
    SHARED_MANIFEST="$f"
    echo "Shared manifest: $f"
  }
done
```

**Decision matrix:**

| Working Manifest | Shared Manifest | Action |
|-----------------|-----------------|--------|
| None | None | Fresh start -> Step 0e |
| None | Exists | Copy shared to `.snow-utils/` -> Step 0d |
| Exists (REMOVED) | None | Replay Flow (reuse existing config) |
| Exists (COMPLETE) | None | Ask user: re-run, reset, or skip |
| Exists (IN_PROGRESS) | None | Resume Flow -- `${PLF} setup --yes` auto-resumes from first PENDING resource |
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

Then proceed to **Step 0d** to check for adaptive markers.

### Step 0d: Shared Manifest Adapt-Check

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

  Setting                       Default Value              Marker
  ────────────────────────────  ─────────────────────────  ──────────────────────
  PLF_CONTAINER_RUNTIME:        podman                     # ADAPT: podman or docker
  K3D_CLUSTER_NAME:             (project directory name)   # ADAPT: customizable
  POLARIS_REALM:                default-realm              # ADAPT: customizable
  PLF_POLARIS_S3_BUCKET:        polaris                    # ADAPT: customizable
  PLF_POLARIS_CATALOG_NAME:     polardb                    # ADAPT: customizable
  PLF_POLARIS_PRINCIPAL_NAME:   iceberg                    # ADAPT: customizable
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
| **2 -- Edit specific** | Ask which value, update manifest and `.env` in-place, re-display |
| **3 -- Cancel** | Stop |

**If user changes `PLF_CONTAINER_RUNTIME`:** Update `.env` with the new value. If switching to Docker, clear `PLF_PODMAN_MACHINE` from `.env`.

**If user changes `K3D_CLUSTER_NAME`:** Automatically update derived values (`KUBECONFIG`, `KUBECTL_PATH`, resource table row 1) in the manifest. Also update `.env` with the new cluster name.

**If user changes `POLARIS_REALM` or any `PLF_POLARIS_*` value:** Update both the manifest and `.env`.

**If `ADAPT_COUNT` = 0 (no markers):** Proceed silently with values as-is.

### Step 0e: Initialize Manifest

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

> Set up the lightweight Python environment for querying and exploration.
> This creates a virtual environment and installs query dependencies
> (`duckdb`, `pyiceberg`, `boto3`, `pandas`) from the workspace `pyproject.toml`.
> The infrastructure CLI runs from the skill directory separately.

**STOP**: Wait for user confirmation before proceeding.

**DO:**

```bash
uv python pin 3.12
uv venv
uv sync --all-extras
```

**SUMMARIZE:**

> Environment ready. Python venv created with query/notebook dependencies.
> Configuration loaded from `.env`.

### Step 2: Full Setup

**SHOW -- what we're about to do:**

> Run the complete setup pipeline. This single command handles everything:
>
> 1. **Prepare** -- Generate RSA keys, bootstrap credentials, Helm charts, k8s manifests
> 2. **Create cluster** -- Download kubectl, create k3d cluster (`${K3D_CLUSTER_NAME}`)
>    using ${PLF_CONTAINER_RUNTIME}, wait for bootstrap (RustFS, PostgreSQL)
> 3. **Deploy Polaris** -- Install Polaris server, run bootstrap job, wait until ready
> 4. **Setup catalog** -- Create S3 bucket (`${PLF_POLARIS_S3_BUCKET}`),
>    register catalog (`${PLF_POLARIS_CATALOG_NAME}`),
>    create principal (`${PLF_POLARIS_PRINCIPAL_NAME}`), configure RBAC grants
>
> Services will be available at:
> - **Polaris API:** localhost:18181
> - **RustFS S3:** localhost:9000 (credentials: admin/password)
> - **RustFS Console:** localhost:9001
>
> Live progress is printed as each resource comes up. Takes 2-5 minutes.

**DO:**

```bash
${PLF} setup --yes
```

**After setup completes, set the scoped cluster environment for this session:**

```bash
export KUBECONFIG="$(pwd)/.kube/config"
export PATH="$(pwd)/bin:$PATH"
set -a && source .env && set +a
```

The CLI manages the manifest automatically during setup:

1. Creates `.snow-utils/snow-utils-manifest.md` with all resources PENDING and Status: IN_PROGRESS
2. Updates each resource row to DONE immediately after creation (resilience pattern)
3. Sets Status: COMPLETE and appends Cleanup Instructions when all resources are verified
4. If interrupted, re-running `setup --yes` resumes from the first non-DONE resource

**After setup, verify manifest was written:**

```bash
cat .snow-utils/snow-utils-manifest.md
```

Expected: all 7 resource rows show `DONE`, Status shows `COMPLETE`.

**SUMMARIZE:**

> Setup complete. Cluster `${K3D_CLUSTER_NAME}` running with Polaris, RustFS, and PostgreSQL.
> Catalog `${PLF_POLARIS_CATALOG_NAME}` created with principal `${PLF_POLARIS_PRINCIPAL_NAME}`.
> Credentials saved to `work/principal.txt`.
> Manifest: `.snow-utils/snow-utils-manifest.md`

### Step 3: Verification

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
${PLF} catalog verify-sql
```

**Check the result:**

- **If SUCCEEDED:** Continue to Step 9a (generate notebook)
- **If FAILED:** Check Troubleshooting section. Common issues: Polaris not ready, RustFS not accessible, principal credentials invalid.

**SUMMARIZE (on success):**

> Verification passed! DuckDB successfully queried Iceberg tables via Polaris REST catalog on RustFS.

### Step 3a: Generate Notebook

**SHOW -- what we're about to do:**

> Generate a Jupyter notebook for interactive Polaris and Iceberg exploration.
> The notebook is pre-configured with your catalog connection details and
> principal credentials, ready to run.

**DO:**

```bash
${PLF} catalog generate-notebook
```

**SUMMARIZE:**

> Notebook generated at `notebooks/verify_polaris.ipynb`.
> Open it with `jupyter notebook notebooks/verify_polaris.ipynb` for interactive exploration.

### Step 4: Summary

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
  Polaris:   See work/principal.txt
  Bootstrap: See k8s/polaris/.bootstrap-credentials.env

Next steps:
  jupyter notebook notebooks/verify_polaris.ipynb  # Interactive exploration
  ${PLF} catalog verify-sql                        # Re-run verification
  ${PLF} catalog explore-sql                       # Interactive DuckDB SQL

Shell setup:
  export KUBECONFIG=$(pwd)/.kube/config
  export PATH=$(pwd)/bin:$PATH
  set -a && source .env && set +a   # AWS config for RustFS
  kubectl get pods -n polaris
  aws s3 ls

Manifest: .snow-utils/snow-utils-manifest.md
```

**Update manifest status to COMPLETE.**

## Catalog-Only Flows

These flows operate on the catalog without rebuilding the cluster.

### Catalog Setup (cluster must be running)

**Trigger:** "setup catalog only", "create catalog"

```bash
${PLF} catalog setup
```

### Catalog Cleanup

**Trigger:** "cleanup catalog", "remove catalog"

**STOP**: Confirm with user before executing.

```bash
${PLF} catalog cleanup --yes
```

Updates manifest status to REMOVED. Generated files in `work/` are preserved.

### Catalog Reset

**Trigger:** "reset polaris catalog", "recreate catalog"

**STOP**: Confirm with user before executing.

```bash
${PLF} catalog cleanup --yes
${PLF} catalog setup
```

Runs cleanup + setup. Generates new `principal.txt` (new credentials).

### Full Catalog Reset

**Trigger:** "full catalog reset", "purge polaris database"

**STOP**: This is destructive. Confirm with user.

```bash
${PLF} polaris reset --yes
${PLF} catalog setup
```

Purges the entire Polaris database and recreates from scratch.

## Teardown Flow

**Trigger:** "teardown polaris", "delete everything", "clean up"

**STOP**: This is destructive. Confirm with user.

```bash
${PLF} teardown --yes
```

Updates manifest status to REMOVED. Generated files are preserved in the work directory for future replay.

### Explicit Purge

**Trigger:** "purge all generated files", "start completely fresh"

**STOP**: This is irreversible. Confirm with user.

```bash
${PLF} teardown --yes
rm -rf work/ k8s/ bin/ .kube/ notebooks/ scripts/
```

Only offered when user explicitly asks to wipe everything.

## Replay Flow

**Trigger:** "replay polaris local forge", "recreate environment"

When manifest has `Status: REMOVED`:

1. Read config values from manifest
2. Show replay plan to user
3. On confirmation, execute Steps 1-10
4. Reuse existing `.env` and work directory layout
5. Re-activate scoped environment (`KUBECONFIG`, `PATH` for kubectl)
6. Regenerate catalog-level files (new principal credentials)
7. Update manifest to COMPLETE

## Consuming Projects: Minimal Setup

A separate project that wants to query the Polaris catalog needs only:

**In the project directory:**

- `.env` with:

```bash
POLARIS_URL=http://localhost:18181
AWS_ENDPOINT_URL=http://localhost:9000
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=password
AWS_REGION=us-east-1
# From work/principal.txt in the polaris workspace:
POLARIS_REALM=POLARIS
CLIENT_ID=<from principal.txt>
CLIENT_SECRET=<from principal.txt>
```

- A notebook (`.ipynb`) or SQL scripts for querying
- `pyproject.toml` with query deps (copy `user-project/pyproject.toml` from the skill repo)

**NOT needed:** k8s manifests, ansible, polaris-local-forge CLI source.

## Stopping Points

1. Step 0: Ask for workspace directory (if not detected)
2. Step 0a: Configuration review -- wait for user confirmation
3. Step 0b: If prerequisites missing
4. Step 0c: Manifest detection (ask which to use if conflict)
5. Step 0d: Adapt-check (if shared manifest has `# ADAPT:` markers)
6. Step 1: Before environment setup
7. Step 2: Before generating config files
8. Step 3: Before cluster creation
9. Step 8: Before catalog setup
10. Catalog-only flows: Before cleanup/reset
11. Teardown: Before destructive operations

## CLI Reference

All commands use:

```bash
PLF="uv run --project <SKILL_DIR> polaris-local-forge --work-dir <PROJECT_DIR>"
```

Aliased as `plf` in tables below for brevity.

**Global options (before any subcommand):**

| Option | Description |
|--------|-------------|
| `--work-dir PATH` | Working directory for generated files (default: skill directory) |
| `--env-file PATH` | Path to .env file (default: `<work-dir>/.env`) |

**OPTION NAMES (NEVER guess or invent options):**

> ONLY use options listed in the tables below. If a command fails with "No such option",
> run `${PLF} <command> --help` to see actual available options and
> use ONLY those. NEVER invent, abbreviate, or rename options.

**`--yes` is REQUIRED** when executing destructive commands after user has approved (CLIs prompt interactively which does not work in Cortex Code's non-interactive shell). All destructive commands support `--dry-run` to preview and `--yes` to skip interactive confirmation.

**COMMAND NAMES (exact -- do NOT substitute):**

- `setup` -- NOT "install", "create", "provision", "init"
- `teardown` -- NOT "cleanup", "destroy", "remove", "delete"
- `doctor` -- NOT "check", "verify", "health", "prereqs"
- `catalog setup` -- NOT "catalog create", "catalog init"
- `catalog cleanup` -- NOT "catalog delete", "catalog remove"
- `cluster create` -- NOT "cluster setup", "cluster init"
- `cluster delete` -- NOT "cluster remove", "cluster destroy"

### Setup & Teardown

| Command | Description |
|---------|-------------|
| `plf setup --yes` | Complete setup (cluster + Polaris + catalog); resumes from manifest if interrupted |
| `plf setup --yes --fresh` | Complete setup ignoring saved manifest progress (start from scratch) |
| `plf setup --dry-run` | Preview setup plan; shows which steps are done (from manifest) |
| `plf teardown --yes` | Complete teardown (cleanup + delete cluster); marks manifest resources as REMOVED |
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
${PLF} polaris deploy
```

**RustFS not accessible:**

```bash
kubectl get svc -n rustfs
aws s3 ls --endpoint-url http://localhost:9000
```

**Bootstrap job fails:**

```bash
kubectl logs -f -n polaris jobs/polaris-bootstrap
${PLF} polaris reset --yes
```

**Catalog setup fails (S3 bucket error):**

```bash
aws s3 ls --endpoint-url http://localhost:9000
${PLF} catalog cleanup --yes
${PLF} catalog setup
```

**DuckDB verification fails:**

Ensure the catalog is set up and principal credentials are valid:

```bash
${PLF} catalog list
cat work/principal.txt
```

**Official documentation:**

- [Apache Polaris (Incubating) 1.3.0](https://polaris.apache.org/releases/1.3.0/)
- [Polaris Management API Spec](https://polaris.apache.org/releases/1.3.0/polaris-api-specs/polaris-management-api/)
- [Polaris Catalog API Spec (Swagger)](https://editor.swagger.io/?url=https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/generated/bundled-polaris-catalog-service.yaml)
- [RustFS Documentation](https://docs.rustfs.com/)
- [Apache Iceberg](https://iceberg.apache.org/)

## Security Notes

- **Bootstrap credentials:** Generated RSA keys and admin credentials are stored in `k8s/polaris/` with restricted permissions (chmod 600)
- **Principal credentials:** `work/principal.txt` contains sensitive `client_id` and `client_secret` -- NEVER display full values in logs or output; mask as `****` + last 4 chars for client_id, never show client_secret
- **RustFS credentials:** Static `admin`/`password` for local development only -- not suitable for production use
- **KUBECONFIG:** Scoped to project directory (`.kube/config`) to isolate from system kubeconfig
- **kubectl binary:** Downloaded to project `bin/` directory to ensure version compatibility with the cluster
- **.env file:** Contains configuration but no secrets by default -- add to `.gitignore` if you add sensitive values
- **Manifest directory:** `.snow-utils/` directory uses chmod 700; manifest files use chmod 600
- **Network isolation:** All services run on localhost ports (18181, 9000, 9001) -- not exposed externally by default

## Directory Structure

### User Workspace (--work-dir)

After skill-based setup, the user's project directory contains:

```
my-polaris-project/                   # User's --work-dir
├── .env                              # Environment configuration (from .env.example)
├── pyproject.toml                    # Lightweight query deps (from user-project/)
├── .venv/                            # Python virtual environment (uv sync)
├── .snow-utils/
│   └── snow-utils-manifest.md        # Resource tracking manifest
├── .kube/
│   └── config                        # Cluster kubeconfig (chmod 600)
├── bin/
│   └── kubectl                       # Version-matched kubectl binary
├── k8s/                              # Generated + copied k8s manifests
│   ├── features/
│   │   ├── rustfs.yaml               # RustFS deployment (copied from skill)
│   │   ├── polaris.yaml              # Generated Polaris Helm values
│   │   └── postgresql.yaml           # Generated PostgreSQL Helm values
│   └── polaris/
│       ├── kustomization.yaml        # Copied from skill
│       ├── polaris-secrets.yaml      # Generated secrets
│       ├── .bootstrap-credentials.env
│       ├── .polaris.env
│       ├── rsa_key / rsa_key.pub     # RSA key pair
│       └── jobs/                     # Bootstrap/purge jobs (copied from skill)
├── work/
│   └── principal.txt                 # Catalog credentials (chmod 600)
├── notebooks/
│   └── verify_polaris.ipynb          # Generated verification notebook
└── scripts/
    └── explore_catalog.sql           # Generated SQL verification script
```

### Skill Repository (read-only source)

```
polaris-local-forge/                  # SKILL_DIR -- read-only
├── .env.example                      # Template copied to user workspace
├── config/cluster-config.yaml        # k3d cluster configuration
├── k8s/                              # Static k8s manifests (source of truth)
├── polaris-forge-setup/              # Ansible playbooks + templates
├── scripts/explore_catalog.py        # Python verification script
├── src/polaris_local_forge/          # CLI source
├── user-project/
│   └── pyproject.toml                # Lightweight deps template
├── pyproject.toml                    # Full CLI + infrastructure deps
├── SKILL.md                          # This file
├── SKILL_README.md                   # Skills documentation
└── example-manifests/
    └── polaris-local-forge-manifest.md
```
