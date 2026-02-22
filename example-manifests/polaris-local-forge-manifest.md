<!-- CORTEX_CODE_INSTRUCTION: This is a shared polaris-local-forge manifest.
     project_name: polaris-dev
     setup_steps:
       1. Check if required skills are installed: cortex skill list
       2. For each skill in ## installed_skills NOT in the list, run: cortex skill add <url>
       3. Once all skills installed, ask "setup from shared manifest" or "replay from manifest"
     To trigger: ask "setup from shared manifest" -->

# Snow-Utils Manifest

This manifest tracks resources created by polaris-local-forge.

---

## prereqs

## installed_skills

polaris-local-forge: <https://github.com/Snowflake-Labs/polaris-local-forge>

---

**Status:** REMOVED

## shared_info

shared_by: your-username
shared_date: 2026-02-21
original_project_dir: polaris-dev
notes: |
  Polaris Local Forge - Local Apache Polaris with RustFS and k3d.
  Container runtime is auto-detected (Docker or Podman).
  To replay: open in Cortex Code, ask "setup from shared manifest"

## project_recipe

project_name: polaris-dev  # ADAPT: customizable

## configuration

container_runtime:  # Auto-detected by CLI
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
