# Dataset Configurations

This directory contains TOML configuration files for loading datasets into Iceberg tables via PyIceberg. Each file represents a namespace and can contain multiple related tables.

## Available Datasets

### `wildlife.toml` - Wildlife Namespace
- **`penguins`**: Palmer Penguins dataset (333 records)
  - Source: CSV from GitHub (https://raw.githubusercontent.com/dataprofessor/data/master/penguins_cleaned.csv)
  - Schema: species, island, bill measurements, body mass, sex
  - Use case: Biological research, data science demos

### `plantae.toml` - Plantae Namespace  
- **`fruits`**: Simple fruits dataset (5 records)
  - Source: Inline data (no external dependencies)
  - Schema: id, name, season
  - Use case: Quick testing, schema examples, multi-namespace demos

## Usage

### Load Individual Namespaces

```bash
# Load wildlife datasets (penguins)
python scripts/pyiceberg_data_loader.py --config-file datasets/wildlife.toml

# Load plantae datasets (fruits)
python scripts/pyiceberg_data_loader.py --config-file datasets/plantae.toml

# Preview operations (dry-run)
python scripts/pyiceberg_data_loader.py --config-file datasets/wildlife.toml --dry-run
```

### Load All Datasets

```bash
# Load all namespaces
for config in datasets/*.toml; do
    python scripts/pyiceberg_data_loader.py --config-file "$config"
done
```

## Configuration Format

Each TOML file follows this structure:

```toml
# Namespace-level metadata
[metadata]
name = "namespace_name"
description = "Description of this namespace"
namespace = "default_namespace"
version = "1.0"

# Individual table configuration
[datasets.table_name.source]
type = "csv"  # or "inline"
url = "https://..."  # for CSV
# data = [...]  # for inline

[datasets.table_name.table]
namespace = "namespace_name"  # can override metadata.namespace
name = "table_name"

# PyIceberg Schema Types: https://py.iceberg.apache.org/api/#types
[datasets.table_name.table.schema]
column1 = "string"
column2 = "int"
column3 = "double"
# ... more columns
```

## Supported Data Types

The loader supports these PyIceberg type mappings:

- `"string"` → StringType()
- `"int"` → IntegerType()
- `"long"` → LongType()
- `"float"` → FloatType()
- `"double"` → DoubleType()
- `"boolean"` → BooleanType()
- `"date"` → DateType()
- `"timestamp"` → TimestampType()

For advanced types (decimal, list, map), see the [PyIceberg documentation](https://py.iceberg.apache.org/api/#types).

## Integration with L2C

After loading data with PyIceberg:

1. **Sync to S3**: `plf l2c sync --force --yes`
2. **Register in Snowflake**: `plf l2c register --yes`
3. **Full Migration**: `plf l2c migrate --force --yes`

Result in Snowflake:
- `wildlife.penguins` → `WILDLIFE_PENGUINS`
- `plantae.fruits` → `PLANTAE_FRUITS`

## Adding New Datasets

1. Create a new `.toml` file for your namespace
2. Define your table configurations
3. Test with `--dry-run` first
4. Load data and run L2C migration

Example for a new `geology` namespace:

```toml
# datasets/geology.toml
[metadata]
name = "geology"
namespace = "geology"

[datasets.minerals.source]
type = "csv"
url = "https://example.com/minerals.csv"

[datasets.minerals.table]
namespace = "geology"
name = "minerals"

[datasets.minerals.table.schema]
name = "string"
hardness = "double"
crystal_system = "string"
```