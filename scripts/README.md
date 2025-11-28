# Scripts

This directory contains utility scripts for the Polaris Local Forge project.

## explore_catalog.py

A DuckDB-based exploration and verification script that demonstrates Polaris Iceberg REST Catalog functionality using real-world data (Palmer Penguins dataset).

### Features

- ✅ Connects to Polaris Iceberg REST Catalog using DuckDB
- ✅ Verifies OAuth2 authentication
- ✅ Creates schemas and Iceberg tables
- ✅ Loads real-world CSV data (Palmer Penguins dataset)
- ✅ Performs analytics queries on Iceberg tables
- ✅ Tests Iceberg metadata operations (snapshots, metadata)
- ✅ Automatic cleanup of resources

### Requirements

Install dependencies using uv:

```bash
uv sync
```

This will install DuckDB and other required packages from `pyproject.toml`.

**Port Forwarding Required:**

The script needs access to both Polaris and LocalStack from your local machine:

```bash
# Terminal 1: Polaris API
kubectl port-forward -n polaris svc/polaris 18181:8181

# Terminal 2: LocalStack S3 (required for data writes and reads)
kubectl port-forward -n localstack svc/localstack 4566:4566
```

**Important:** Due to a [bug in DuckDB Iceberg extension](https://github.com/duckdb/duckdb-iceberg/issues/594), the script uses `ACCESS_DELEGATION_MODE='none'` which means it uses static S3 credentials instead of Polaris vended credentials. This requires direct access to LocalStack S3.

### Usage

#### Basic Exploration

The script will auto-detect credentials from `work/principal.txt` (or fallback to `k8s/polaris/.bootstrap-credentials.env`) and load the Palmer Penguins dataset into a `wildlife.penguins` table:

```bash
python scripts/explore_catalog.py
```

Or use the convenient task alias:

```bash
task verify
```

Or with verbose output:

```bash
task verify -- --verbose
```

#### Custom Configuration

Specify custom catalog name and endpoint:

```bash
python scripts/explore_catalog.py \
  --catalog my_catalog \
  --endpoint http://localhost:8080/api/catalog
```

Provide credentials explicitly:

```bash
python scripts/explore_catalog.py \
  --client-id <your-client-id> \
  --client-secret <your-client-secret>
```

#### Advanced Options

Keep resources for further exploration:

```bash
python scripts/explore_catalog.py --skip-cleanup
```

This will create a `wildlife.penguins` table that you can explore further.

Custom schema and table names:

```bash
python scripts/explore_catalog.py \
  --schema my_animals \
  --table birds
```

Use a different CSV dataset:

```bash
python scripts/explore_catalog.py \
  --penguins-url https://example.com/my-data.csv
```

Full help:

```bash
python scripts/explore_catalog.py --help
```

### What Gets Demonstrated

The script performs a complete workflow with real data:

1. **DuckDB Connection**: Installs and loads `iceberg` and `httpfs` extensions
2. **OAuth2 Authentication**: Creates a secret for Polaris authentication
3. **Catalog Attachment**: Attaches the Polaris Iceberg REST catalog
4. **Catalog Verification**: Lists tables to verify catalog accessibility
5. **Schema Creation**: Creates a `wildlife` schema (default)
6. **Table Creation**: Creates a `penguins` table with appropriate columns
7. **Data Loading**: 
   - Downloads Palmer Penguins CSV dataset (333 rows) from GitHub
   - Loads into in-memory temporary table
   - Inserts from temp table to Polaris Iceberg table
   - **Uses S3 secret directly** (workaround for [DuckDB bug #594](https://github.com/duckdb/duckdb-iceberg/issues/594))
   - Writes Parquet files to LocalStack S3
8. **Data Analytics**: Performs aggregation queries (species statistics)
9. **Metadata Operations**: Tests Iceberg-specific functions:
   - `iceberg_metadata()` - retrieves table metadata
   - `iceberg_snapshots()` - retrieves snapshot information
10. **Cleanup**: Drops resources (optional)

### Dataset Information

**Palmer Penguins Dataset**  
Source: <https://github.com/dataprofessor/data/blob/master/penguins_cleaned.csv>

This dataset contains measurements for 333 penguins across 3 species:

- Adelie
- Chinstrap
- Gentoo

Columns:

- `species` (VARCHAR): Penguin species
- `island` (VARCHAR): Island in Palmer Archipelago
- `bill_length_mm` (DOUBLE): Bill length in millimeters
- `bill_depth_mm` (DOUBLE): Bill depth in millimeters
- `flipper_length_mm` (DOUBLE): Flipper length in millimeters
- `body_mass_g` (DOUBLE): Body mass in grams
- `sex` (VARCHAR): Penguin sex

### Example Output

```
======================================================================
Polaris Catalog Explorer - DuckDB Iceberg Extension Demo
======================================================================
Dataset: Palmer Penguins (https://raw.githubusercontent.com/...penguins_cleaned.csv)
Schema: wildlife | Table: penguins
======================================================================

Connect to DuckDB...
ℹ️  Creating DuckDB connection...
✅ DuckDB connection established

Create Polaris secret...
ℹ️  Creating Polaris secret for OAuth2 authentication...
✅ Polaris secret created successfully

Attach Polaris catalog...
ℹ️  Attaching Polaris catalog 'polardb'...
✅ Polaris catalog attached successfully

Verify catalog...
ℹ️  Verifying catalog accessibility...
✅ Catalog verified successfully

Create schema...
ℹ️  Creating schema 'wildlife'...
✅ Schema 'wildlife' created successfully

Create table...
ℹ️  Creating table 'wildlife.penguins'...
✅ Table 'wildlife.penguins' created successfully

Load penguins dataset...
ℹ️  Loading penguins dataset from https://raw.githubusercontent.com/...
✅ Successfully loaded 333 penguin records

Query penguin data...
ℹ️  Querying penguin data...
✅ Query returned 3 species

Test metadata operations...
ℹ️  Testing Iceberg metadata operations...
✅ Retrieved snapshots: 1 entries

Cleaning up resources...
ℹ️  Cleaning up resources...
✅ Cleanup completed successfully

======================================================================
✅ All steps completed successfully!
======================================================================
```

### Comparing Approaches

The Polaris Local Forge project provides two complementary verification approaches:

| Feature | explore_catalog.py (DuckDB) | verify_setup.ipynb (PyIceberg) |
|---------|----------------------------|--------------------------------|
| **Interface** | CLI script | Jupyter notebook |
| **Language** | SQL-first | Python-first |
| **Data** | Real dataset (Penguins) | Test data |
| **Use Case** | Quick verification, CI/CD | Interactive exploration |
| **Setup** | Lightweight, single script | Notebook environment |
| **Output** | Terminal with emojis | Notebook cells |
| **Speed** | Very fast | Fast |
| **Schema** | `wildlife.penguins` | `demo_db.fruits` |

**Both are complementary** and test different aspects of the setup!

### Integration with Taskfile

You can use the convenient `verify` task alias:

```bash
# Basic verification
task verify

# With options
task verify -- --verbose
task verify -- --skip-cleanup
task verify -- --schema my_schema --table my_table
```

The `setup:all` task automatically runs verification after setup:

```bash
task setup:all
```

### Reference

This script follows the DuckDB Iceberg REST Catalog documentation:
<https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs>

### Troubleshooting

**Error: "Client ID or secret not provided"**

- Ensure the credentials file exists at `work/principal.txt` (format: `REALM,CLIENT_ID,CLIENT_SECRET`)
- Or at `k8s/polaris/.bootstrap-credentials.env` (fallback location)
- Or provide credentials explicitly using `--client-id` and `--client-secret`

**Error: "Failed to attach catalog"**

- Check that Polaris is running and accessible
- Verify the endpoint URL is correct
- Check port forwarding if running in Kubernetes

**Error: "Failed to create schema/table"**

- Ensure the principal has appropriate permissions
- Check Polaris logs for detailed error messages

**Error: "Failed to load CSV data"**

- Ensure you have internet connectivity to download the dataset
- Check if the penguins CSV URL is accessible
- Try using a different dataset with `--penguins-url`

**Network connection issues**

- For local development, ensure both port forwardings are active:

```bash
# Polaris API
kubectl port-forward -n polaris svc/polaris 18181:8181

# LocalStack S3 (required due to ACCESS_DELEGATION_MODE='none')
kubectl port-forward -n localstack svc/localstack 4566:4566
```
