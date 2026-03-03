# Iceberg Metadata Path Rewrite: Technical Decision Document

**Status:** Implemented (pure Python rewriter)
**Author:** polaris-local-forge team
**Date:** 2026-02-25
**Audience:** Iceberg committers, Snowflake engineers, L2C reviewers

---

## 1. Problem Statement

When migrating an Iceberg table from one S3 location to another (e.g.,
`s3://polardb/` to `s3://kameshs-polaris-dev-polardb/`), Snowflake's
`CREATE ICEBERG TABLE ... METADATA_FILE_PATH` fails because the metadata
files contain **absolute S3 paths** referencing the original source bucket.

Snowflake enforces that all files referenced by the metadata must reside
as strict subpaths under the external volume's `STORAGE_BASE_URL`. Since
the metadata still points to `s3://polardb/...`, Snowflake rejects the
registration:

```
091557 (22000): One of the specified Iceberg metadata files does not conform
to the required directory hierarchy. All files must reside as a strict
subpath under the defined base directory.
Current base directory: s3://kameshs-polaris-dev-polardb/.
Conflicting file path: s3://polardb/wildlife/penguins/data/snap-xxx.avro.
```

This is inherent to how Iceberg stores paths in its metadata -- all paths
are fully-qualified URIs per the spec. After a bulk copy of data files to
a new bucket, the metadata must be updated to reflect the new locations.

## 2. Iceberg Spec References

The Iceberg Table Spec (https://iceberg.apache.org/spec/) defines the
following fields that store absolute paths. All of these must be rewritten
when migrating table data to a new location.

### 2.1 Table Metadata (metadata.json)

| Field | Spec Section | Description |
|-------|-------------|-------------|
| `location` | Table Metadata Fields | The table's base location (used by writers for new files) |
| `snapshots[].manifest-list` | Snapshots | Location of the manifest list Avro file for each snapshot |
| `metadata-log[].metadata-file` | Table Metadata Fields | Locations of previous metadata.json files |

### 2.2 Manifest List (Avro)

| Field ID | Field Name | Spec Section | Description |
|----------|------------|-------------|-------------|
| 500 | `manifest_path` | Manifest Lists | Location of each manifest file |

### 2.3 Manifest Files (Avro)

| Field ID | Field Name | Spec Section | Description |
|----------|------------|-------------|-------------|
| 100 | `data_file.file_path` | Data File Fields | Full URI for each data file |
| 143 | `data_file.referenced_data_file` | Data File Fields | Fully qualified location of a data file that deletes reference (optional, for delete files) |

### 2.4 Exhaustive Path Summary

For a typical table with 1 snapshot, 1 manifest list, and 1 manifest file:

```
metadata.json
  ├── location:                           "s3://old-bucket/ns/table"
  ├── snapshots[0].manifest-list:         "s3://old-bucket/ns/table/metadata/snap-xxx.avro"
  └── metadata-log[0].metadata-file:      "s3://old-bucket/ns/table/metadata/v1.metadata.json"

manifest-list.avro (snap-xxx.avro)
  └── records[0].manifest_path:           "s3://old-bucket/ns/table/metadata/xxx-manifest.avro"

manifest.avro (xxx-manifest.avro)
  └── records[0].data_file.file_path:     "s3://old-bucket/ns/table/data/xxx.parquet"
  └── records[0].data_file.referenced_data_file: (null or "s3://old-bucket/...")
```

## 3. Approaches Evaluated

### 3.1 Approach A: Match Source and Destination Bucket Names

**Idea:** Name the migration bucket identically to the source bucket so
paths don't need rewriting.

**Verdict:** Rejected.

- S3 bucket names are globally unique -- cannot reuse `polardb`.
- User-scoped naming (`kameshs-polaris-dev-polardb`) is a requirement for
  multi-user isolation and project scoping.
- Fundamentally doesn't solve the problem for any real migration scenario.

### 3.2 Approach B: PySpark `rewrite_table_path` Procedure

**Idea:** Use the official Iceberg Spark procedure
(`system.rewrite_table_path`) which was purpose-built for this exact use case.

Reference: https://iceberg.apache.org/docs/nightly/spark-procedures/#rewrite_table_path

**How it works:**
- Takes `source_prefix` and `target_prefix` as arguments
- Stages rewritten metadata files at a configurable location
- Returns a file listing all source/target pairs for file copy
- Handles all spec-defined path fields comprehensively

**Verdict:** Rejected for this project.

- Requires a running Spark cluster or local PySpark installation
- Adds ~500MB+ of JVM dependencies
- Counter to the project goal of minimizing Spark/JVM ecosystem usage
- L2C targets lightweight CLI-driven migration, not Spark-heavy workflows

**This is the recommended approach** for organizations already running Spark.

### 3.3 Approach C: Snowflake `CATALOG = 'SNOWFLAKE'` with Explicit Columns

**Idea:** Skip `METADATA_FILE_PATH` entirely. Use `CATALOG = 'SNOWFLAKE'`
with explicit column definitions, which doesn't read from Iceberg metadata.

**Verdict:** Rejected.

- Requires generating correct Snowflake column definitions from Iceberg
  schemas, including complex type mapping (Iceberg -> Snowflake types).
- Loses schema evolution history embedded in Iceberg metadata.
- Snowflake still needs to read the data files, and the external volume
  validates that data file paths are under `STORAGE_BASE_URL`. If the
  Parquet/Avro files internally reference other paths, this can still fail.
- Does not solve the fundamental problem of path references in data.

### 3.4 Approach D: Wait for PyIceberg `rewrite_table_path`

**Idea:** Wait for the community to implement
[apache/iceberg-python#2014](https://github.com/apache/iceberg-python/issues/2014)
which would add a native Python `rewrite_table_path` to PyIceberg.

**Status of the upstream issue (as of 2026-02-25):**
- Opened: 2025-05-18 by @abfisher0417
- Assigned to @abfisher0417
- Supported by maintainers @corleyma and @sungwy
- Was marked stale (180 days no activity) on 2025-12-09
- Revived by community interest on 2025-12-09
- **No PR submitted as of 2026-02-25**

**Verdict:** Not viable as a blocking dependency.

- No implementation timeline available
- L2C needs this capability now for the migration workflow
- When available, this would be the ideal replacement (see Section 6:
  Deprecation Plan)

### 3.5 Approach E: Pure Python Metadata Rewriter (Chosen)

**Idea:** Implement a targeted Python rewriter using `json` (for
metadata.json) and `fastavro` (for Avro manifest files) that performs
simple string prefix replacement on all known path fields.

**Verdict:** Chosen.

**Rationale:**
- Zero JVM dependencies
- Handles the exact fields defined in the Iceberg spec (Section 2)
- Simple string prefix replacement (`s3://old/` -> `s3://new/`) is
  deterministic and easily auditable
- `fastavro` preserves Avro schemas exactly (no schema mutation)
- Aligned with the Spark `RewriteTablePathSparkAction` algorithm:
  both perform prefix-based string replacement on the same set of fields
- Minimal code (~200 lines) with clear deprecation path

## 4. Chosen Approach: Implementation Details

### 4.1 Algorithm

```
For each migrated table:
  1. Find latest metadata.json in s3://<dst>/ns/table/metadata/
  2. Read metadata.json, collect manifest-list URIs from snapshots
  3. Rewrite paths in metadata.json (location, manifest-list, metadata-log)
  4. For each manifest-list URI:
     a. Read manifest list (Avro), collect manifest URIs
     b. Rewrite manifest_path fields, write back
     c. For each manifest URI:
        i.  Read manifest (Avro)
        ii. Rewrite data_file.file_path and referenced_data_file fields
        iii. Write back
  5. Write updated metadata.json back to S3
```

### 4.2 Key Design Decisions

1. **In-place rewrite on destination bucket:** No staging location needed
   because we own the destination bucket and the original source is preserved.

2. **Prefix replacement (not full URI parsing):** Matches the Spark
   `RewriteTablePathUtil.replacePath()` approach. Simple, predictable,
   and handles both `s3://` and `s3a://` schemes.

3. **Post-sync hook:** Rewrite runs after each table's data sync completes
   successfully, ensuring data files exist before metadata references them.

4. **`--skip-rewrite` flag:** Escape hatch for debugging or when metadata
   paths are already correct (e.g., if source and destination happen to
   match, or for incremental re-syncs where rewrite was already done).

5. **Non-fatal rewrite failure:** If rewrite fails, the table remains
   marked as `synced` but with a `rewrite_error` field in state. The user
   can re-run sync to retry.

### 4.3 Files Modified

| File | Changes |
|------|---------|
| `src/polaris_local_forge/l2c/rewrite.py` | New module with `rewrite_table_paths()` and helpers for JSON/Avro rewriting |
| `src/polaris_local_forge/l2c/sync.py` | Calls `rewrite_table_paths()` after successful sync; adds `--skip-rewrite` flag |
| `src/polaris_local_forge/l2c/common.py` | `find_latest_metadata()` moved here for shared use by rewrite and register |
| `pyproject.toml` | Added `fastavro>=1.9.0` dependency |

### 4.4 Dependencies

- **`fastavro`** (pure Python Avro library): Reads and writes Avro files
  with schema preservation. Used for manifest lists and manifest files.
  No JVM required. Well-maintained, 1000+ GitHub stars.
- **`json`** (stdlib): For metadata.json read/write.

## 5. Upstream References and Tracking

### 5.1 Iceberg Spec

- Full spec: https://iceberg.apache.org/spec/
- Relevant sections: Table Metadata Fields, Snapshots, Manifest Lists,
  Data File Fields (field IDs 100, 143, 500)

### 5.2 Iceberg Core (JVM)

- `RewriteTablePathSparkAction`:
  https://iceberg.apache.org/javadoc/1.9.0/org/apache/iceberg/spark/actions/RewriteTablePathSparkAction.html
- Spark procedure docs:
  https://iceberg.apache.org/docs/nightly/spark-procedures/#rewrite_table_path
- Related issue (local filesystem paths):
  https://github.com/apache/iceberg/issues/12277

### 5.3 PyIceberg

- Feature request: https://github.com/apache/iceberg-python/issues/2014
  - Status: Open, assigned, **no PR as of 2026-02-25**
  - Community interest confirmed; maintainers supportive
- Related discussion on table migration:
  https://github.com/apache/iceberg-python/issues/1254

### 5.4 Snowflake

- Snowflake `CREATE ICEBERG TABLE` with `METADATA_FILE_PATH`:
  https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table-snowflake
- Requires all referenced files to be under `STORAGE_BASE_URL` of the
  external volume -- no cross-bucket references allowed.

## 6. Deprecation Plan

This pure Python rewriter is a **bridge implementation** until PyIceberg
natively supports `rewrite_table_path`.

### When to deprecate:

1. **PyIceberg releases `rewrite_table_path`** (tracking issue #2014)
2. The released API supports:
   - S3 source/target prefixes
   - In-place rewrite or staging to a specified location
   - All spec-defined path fields (at minimum: 100, 143, 500, plus
     metadata.json `location`, `manifest-list`, `metadata-log`)

### Deprecation steps:

1. Add a `pyiceberg` version check in `rewrite.py`
2. If PyIceberg >= N.M (the version with `rewrite_table_path`):
   - Log a deprecation warning: "Using native PyIceberg rewriter"
   - Delegate to PyIceberg's implementation
3. After one release cycle, remove the custom rewriter code
4. Update this document's status to "Deprecated"

### What to preserve:

- The `--skip-rewrite` flag on `sync` (useful regardless of implementation)
- The state tracking (`rewrite_count`, `rewrite_error` fields)

## 7. Open Questions for Reviewers

1. **Partition statistics files:** Iceberg v3 adds `partition-statistics`
   with file locations. Our rewriter currently does not handle these since
   the tables being migrated are v2. Should we add support proactively?

2. **Schema evolution across snapshots:** If the Avro schema changes between
   manifest versions (e.g., after a schema evolution), does `fastavro`
   handle reading old-schema manifests correctly? Our testing shows it does,
   but confirmation from Iceberg experts would be valuable.

3. **Puffin files (statistics):** Iceberg tables can have associated Puffin
   statistics files. These files are referenced from `statistics` entries in
   metadata.json. We currently don't rewrite these references since the L2C
   source tables don't have statistics files. Should we add support?

4. **Is there a better upstream path?** If PyIceberg's `rewrite_table_path`
   (issue #2014) is not progressing, should we contribute our Python
   implementation upstream? The code is ~200 lines and covers the core
   use case.

5. **`s3a://` vs `s3://` scheme handling:** Our rewriter handles both
   schemes via `_key_from_uri()`. Is there a canonical scheme preference
   for Iceberg metadata on AWS?

---

*This document is intended for sharing with Apache Iceberg committers and
Snowflake engineers to validate the approach and identify any spec
compliance gaps. Feedback welcome.*
