# Snow-Utils Manifest

This manifest tracks resources created by polaris-local-forge.

---

**Status:** REMOVED

## shared_info

shared_by: User
shared_to: ALICE
shared_date: 2026-02-20

## CORTEX_CODE_INSTRUCTION

ALICE, to set up this Polaris Local Forge environment on your machine:

1. Save this file to your target directory
2. Open Cortex Code and say: "setup from shared manifest" or "replay from manifest"
3. Cortex Code will read this manifest and guide you through setup
4. Values marked with `# ADAPT:` can be customized for your environment

The setup will create a local Apache Polaris environment with:

- k3d Kubernetes cluster
- RustFS S3-compatible storage
- PostgreSQL metastore
- Polaris Iceberg REST catalog
- Demo penguins dataset

## project_recipe

project_name: polaris-dev  # ADAPT: customizable

## configuration

container_runtime: podman  # ADAPT: podman or docker
podman_machine: k3d  # ADAPT: your podman machine name (macOS only)
cluster_name: polaris-dev  # ADAPT: customizable

## resources

| # | Resource | Type | Status |
|---|----------|------|--------|
| 1 | k3d cluster | infrastructure | REMOVED |
| 2 | RustFS | storage | REMOVED |
| 3 | PostgreSQL | database | REMOVED |
| 4 | Polaris | service | REMOVED |
| 5 | Catalog | data | REMOVED |
| 6 | Principal | auth | REMOVED |
| 7 | Demo data | data | REMOVED |

## prereqs

## installed_skills

polaris-local-forge: <https://github.com/kameshsampath/polaris-local-forge>
