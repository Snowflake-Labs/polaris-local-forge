<!-- CORTEX_CODE_INSTRUCTION: This is a polaris-local-forge manifest.
     project_name: polaris-local-forge
     setup_steps:
       1. Check if required skills are installed: cortex skill list
       2. For each skill in ## required_skills NOT in the list, run: cortex skill add <url>
       3. Once all skills installed, ask "setup from example manifest"
     To trigger: ask "get started with apache polaris" or "setup from example manifest" -->

# Polaris Local Forge Manifest

## shared_info
shared_by: POLARIS_LOCAL_FORGE
shared_date: 2026-02-19
notes: |
  Polaris Local Forge - local Apache Polaris (Incubating) dev environment
  with RustFS S3-compatible storage, PostgreSQL metastore, and k3d.
  To replay: ask "get started with apache polaris using example manifest"

## prereqs
- Podman (default) or Docker Desktop (>= 4.27) running
- k3d installed
- Python >= 3.12
- uv package manager

## required_skills
polaris-local-forge: https://github.com/kameshsampath/polaris-local-forge

<!-- START -- polaris-local-forge:polaris-local-forge -->
## Polaris Local Forge: polaris-local-forge

**Created:** 2026-02-19
**Status:** REMOVED

### Container Runtime
**PLF_CONTAINER_RUNTIME:** podman
**PLF_PODMAN_MACHINE:** k3d

### Cluster
**K3D_CLUSTER_NAME:** polaris-local-forge  # ADAPT: customizable
**K3S_VERSION:** v1.35.1-k3s1
**KUBECTL_VERSION:** v1.35.1
**KUBECONFIG:** .kube/config
**KUBECTL_PATH:** bin/kubectl

### RustFS S3
**AWS_ENDPOINT_URL:** http://localhost:9000
**RUSTFS_CONSOLE_URL:** http://localhost:9001
**AWS_ACCESS_KEY_ID:** admin
**AWS_REGION:** us-east-1

### Polaris
**POLARIS_URL:** http://localhost:18181
**POLARIS_REALM:** default-realm

### Catalog
**PLF_POLARIS_S3_BUCKET:** polaris  # ADAPT: customizable
**PLF_POLARIS_CATALOG_NAME:** polardb  # ADAPT: customizable
**PLF_POLARIS_PRINCIPAL_NAME:** iceberg  # ADAPT: customizable

### Resources

| # | Type | Name | Status |
|---|------|------|--------|
| 1 | k3d Cluster | polaris-local-forge | REMOVED |
| 2 | RustFS | S3-compatible storage | REMOVED |
| 3 | PostgreSQL | Polaris metastore | REMOVED |
| 4 | Polaris | REST Catalog server | REMOVED |
| 5 | S3 Bucket | polaris | REMOVED |
| 6 | Catalog | polardb | REMOVED |
| 7 | Principal | iceberg | REMOVED |
<!-- END -- polaris-local-forge:polaris-local-forge -->
