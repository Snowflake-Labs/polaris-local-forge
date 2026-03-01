#!/usr/bin/env python3
"""
Generic PyIceberg Data Loader

A flexible data loader that uses PyIceberg to properly load data into Iceberg tables
while maintaining correct metadata. Supports multiple data sources and formats via
TOML configuration files.

This loader fixes the DuckDB UUID corruption issue by using PyIceberg's proper
Iceberg metadata handling instead of DuckDB's direct file manipulation.

Usage:
    python scripts/pyiceberg_data_loader.py --config-file datasets/wildlife.toml
    python scripts/pyiceberg_data_loader.py --config-file datasets/plantae.toml --dry-run
"""

import os
import sys
from pathlib import Path
import click
import tomllib  # Python 3.11+, or use tomli for older versions
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    StringType, IntegerType, LongType, FloatType, DoubleType,
    BooleanType, DateType, TimestampType, DecimalType, NestedField
)
from typing import Dict, Any


# Type mapping from TOML string to PyIceberg types
TYPE_MAPPING = {
    "string": StringType(),
    "int": IntegerType(),
    "integer": IntegerType(),
    "long": LongType(),
    "float": FloatType(),
    "double": DoubleType(),
    "boolean": BooleanType(),
    "date": DateType(),
    "timestamp": TimestampType(),
}

# Parallel PyArrow type mapping for casting DataFrames to match declared schema
ARROW_TYPE_MAPPING = {
    "string": pa.string(),
    "int": pa.int32(),
    "integer": pa.int32(),
    "long": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "date": pa.date32(),
    "timestamp": pa.timestamp("us"),
}


def load_env_file(env_path: Path) -> Dict[str, str]:
    """Load .env file into a dict (simple key=value parser)."""
    env_vars = {}
    if not env_path.exists():
        return env_vars
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, _, value = line.partition('=')
                env_vars[key.strip()] = value.strip()
    return env_vars


def load_config(config_file: Path) -> Dict[str, Any]:
    """Load TOML configuration file."""
    try:
        with open(config_file, 'rb') as f:
            return tomllib.load(f)
    except FileNotFoundError:
        click.echo(f"Config file not found: {config_file}", err=True)
        sys.exit(1)
    except tomllib.TOMLDecodeError as e:
        click.echo(f"Invalid TOML syntax in {config_file}: {e}", err=True)
        sys.exit(1)


def connect_to_catalog(catalog_config: Path = None) -> Any:
    """Connect to Polaris catalog via PyIceberg.

    Reads connection settings from .env in the current working directory,
    and credentials from work/principal.txt (or the provided catalog_config path).

    Uses direct S3 credentials (not vended-credentials) because the local
    RustFS/MinIO store doesn't support AWS STS. FsspecFileIO is used instead
    of PyArrow's native S3 to avoid RustFS multipart upload HeadObject 403 bug.
    """
    env = load_env_file(Path.cwd() / ".env")

    polaris_url = env.get("POLARIS_URL", os.environ.get("POLARIS_URL", "http://localhost:18181"))
    catalog_name = env.get("PLF_POLARIS_CATALOG_NAME", os.environ.get("PLF_POLARIS_CATALOG_NAME", "polardb"))
    realm = env.get("POLARIS_REALM", os.environ.get("POLARIS_REALM", "POLARIS"))
    s3_endpoint = env.get("AWS_ENDPOINT_URL", os.environ.get("AWS_ENDPOINT_URL", "http://localhost:19000"))
    s3_access_key = env.get("AWS_ACCESS_KEY_ID", os.environ.get("AWS_ACCESS_KEY_ID", "admin"))
    s3_secret_key = env.get("AWS_SECRET_ACCESS_KEY", os.environ.get("AWS_SECRET_ACCESS_KEY", "password"))
    s3_region = env.get("AWS_REGION", os.environ.get("AWS_REGION", "us-east-1"))

    if catalog_config is None:
        catalog_config = Path("work/principal.txt")
    resolved_path = catalog_config if catalog_config.is_absolute() else (Path.cwd() / catalog_config).resolve()

    if not resolved_path.exists():
        click.echo(f"Catalog config not found at: {resolved_path}", err=True)
        click.echo("Run 'plf catalog setup' first to create the principal configuration.", err=True)
        sys.exit(1)

    content = resolved_path.read_text().strip()
    parts = content.split(',')
    if len(parts) != 3:
        click.echo(f"Invalid principal.txt format at {resolved_path}", err=True)
        click.echo("Expected: REALM,CLIENT_ID,CLIENT_SECRET", err=True)
        sys.exit(1)

    _, client_id, client_secret = parts

    click.echo(f"  Polaris URL:  {polaris_url}")
    click.echo(f"  Catalog:      {catalog_name}")
    click.echo(f"  Realm:        {realm}")
    click.echo(f"  S3 endpoint:  {s3_endpoint}")
    click.echo(f"  Principal:    {resolved_path}")

    try:
        catalog = RestCatalog(
            name=catalog_name,
            **{
                "uri": f"{polaris_url}/api/catalog",
                "credential": f"{client_id}:{client_secret}",
                "header.content-type": "application/vnd.api+json",
                "header.X-Iceberg-Access-Delegation": "",
                "header.Polaris-Realm": realm,
                "warehouse": catalog_name,
                "scope": "PRINCIPAL_ROLE:ALL",
                "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": s3_access_key,
                "s3.secret-access-key": s3_secret_key,
                "s3.region": s3_region,
                "s3.path-style-access": "true",
            },
        )
        click.echo("Connected to Polaris catalog")
        return catalog

    except Exception as e:
        click.echo(f"Failed to connect to Polaris catalog: {e}", err=True)
        sys.exit(1)


def load_data_source(source_config: Dict[str, Any]) -> pd.DataFrame:
    """Load data from various sources (CSV, inline, etc.)."""
    source_type = source_config.get("type")

    if source_type == "csv":
        url = source_config.get("url")
        if not url:
            raise ValueError("CSV source requires 'url' field")

        click.echo(f"  Loading CSV data from {url}")
        try:
            df = pd.read_csv(url)
            click.echo(f"  Loaded {len(df)} rows from CSV")
            return df
        except Exception as e:
            raise ValueError(f"Failed to load CSV from {url}: {e}")

    elif source_type == "inline":
        data = source_config.get("data")
        if not data:
            raise ValueError("Inline source requires 'data' field")

        click.echo(f"  Loading inline data ({len(data)} records)")
        df = pd.DataFrame(data)
        click.echo(f"  Loaded {len(df)} rows from inline data")
        return df

    else:
        raise ValueError(f"Unsupported source type: {source_type}")


def create_pyiceberg_schema(schema_config: Dict[str, str]) -> Schema:
    """Create PyIceberg schema from TOML configuration."""
    fields = []
    for i, (name, type_str) in enumerate(schema_config.items(), 1):
        if type_str not in TYPE_MAPPING:
            raise ValueError(f"Unsupported type '{type_str}' for field '{name}'. "
                           f"Supported types: {list(TYPE_MAPPING.keys())}")
        fields.append(NestedField(
            field_id=i,
            name=name,
            field_type=TYPE_MAPPING[type_str],
            required=False,
        ))
    return Schema(*fields)


def get_or_create_table(catalog: Any, table_config: Dict[str, Any], df: pd.DataFrame) -> Any:
    """Get existing table or create new one with proper schema."""
    namespace = table_config["namespace"]
    table_name = table_config["table_name"]
    schema_config = table_config["schema"]

    try:
        catalog.create_namespace(namespace)
        click.echo(f"  Created namespace: {namespace}")
    except Exception:
        pass

    schema = create_pyiceberg_schema(schema_config)
    full_table_name = f"{namespace}.{table_name}"

    try:
        table = catalog.load_table(full_table_name)
        click.echo(f"  Using existing table: {full_table_name}")
        return table
    except Exception:
        click.echo(f"  Creating new table: {full_table_name}")
        table = catalog.create_table(identifier=full_table_name, schema=schema)
        click.echo(f"  Created table: {full_table_name}")
        return table


def _cast_to_schema(arrow_table: pa.Table, schema_config: Dict[str, str]) -> pa.Table:
    """Cast Arrow table columns to match the declared TOML schema types.

    Pandas may infer int64 for columns declared as double (e.g., whole-number
    measurements like flipper_length_mm=181). This cast ensures the Arrow table
    matches what PyIceberg expects from the Iceberg table schema.
    """
    for col_name, type_str in schema_config.items():
        if col_name in arrow_table.column_names:
            target_type = ARROW_TYPE_MAPPING.get(type_str)
            if target_type and arrow_table.schema.field(col_name).type != target_type:
                col_idx = arrow_table.schema.get_field_index(col_name)
                arrow_table = arrow_table.set_column(
                    col_idx, col_name, arrow_table.column(col_name).cast(target_type)
                )
    return arrow_table


def load_dataset(catalog: Any, dataset_name: str, dataset_config: Dict[str, Any], dry_run: bool = False) -> None:
    """Load a single dataset into Iceberg table via PyIceberg."""
    click.echo(f"\nProcessing dataset: {dataset_name}")

    try:
        source_config = dataset_config["source"]
        df = load_data_source(source_config)

        table_config = dataset_config["table"]
        table_config["table_name"] = table_config["name"]

        if dry_run:
            click.echo(f"  [DRY-RUN] Would load {len(df)} rows into {table_config['namespace']}.{table_config['name']}")
            click.echo(f"  [DRY-RUN] Schema: {list(df.columns)}")
            return

        table = get_or_create_table(catalog, table_config, df)

        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        arrow_table = _cast_to_schema(arrow_table, table_config.get("schema", {}))
        table.overwrite(arrow_table)
        click.echo(f"  Loaded {len(df)} rows into {table_config['namespace']}.{table_config['name']}")

    except Exception as e:
        import traceback
        click.echo(f"Failed to load dataset {dataset_name}: {e}", err=True)
        traceback.print_exc()
        raise


@click.command()
@click.option('--config-file', required=True, type=click.Path(exists=True, path_type=Path),
              help='TOML config file with data loading specification')
@click.option('--catalog-config', type=click.Path(path_type=Path),
              help='Catalog configuration (defaults to work/principal.txt)')
@click.option('--dry-run', is_flag=True, help='Preview operations without executing')
def load_data(config_file: Path, catalog_config: Path, dry_run: bool):
    """Generic PyIceberg data loader supporting multiple data sources and formats."""

    click.echo("PyIceberg Data Loader")
    click.echo("=" * 50)

    click.echo(f"Loading configuration from {config_file}")
    config = load_config(config_file)

    catalog = None
    if not dry_run:
        click.echo("Connecting to Polaris catalog...")
        catalog = connect_to_catalog(catalog_config)

    metadata = config.get("metadata", {})
    datasets = config.get("datasets", {})

    click.echo(f"\nProcessing {len(datasets)} dataset(s) from namespace: {metadata.get('namespace', 'default')}")

    for dataset_name, dataset_config in datasets.items():
        try:
            load_dataset(catalog, dataset_name, dataset_config, dry_run)
        except Exception:
            click.echo(f"Stopping due to error in dataset {dataset_name}", err=True)
            sys.exit(1)

    click.echo(f"\nSuccessfully processed {len(datasets)} dataset(s)!")

    if dry_run:
        click.echo("\nRun without --dry-run to actually load the data")


if __name__ == "__main__":
    load_data()
