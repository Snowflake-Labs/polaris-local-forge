#!/usr/bin/env python3
# Copyright 2025 Snowflake Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
DuckDB Iceberg Extension - Polaris REST Catalog Explorer

This script demonstrates using DuckDB's Iceberg extension to interact with
Apache Polaris REST Catalog, following the pattern documented at:
https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs

Workflow:
1. Load Penguins CSV data into DuckDB memory
2. Connect to Polaris REST Catalog via DuckDB Iceberg extension
3. Create schema and Iceberg table
4. INSERT data from memory to Iceberg table (via Polaris)
5. Query and explore Iceberg metadata

NOTE: All data writes go through Polaris REST API. Polaris vends S3 credentials
and manages metadata. We do NOT use DuckDB's S3 extension directly.
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

import duckdb


class PolarisExplorer:
    """Explores Polaris Iceberg REST Catalog using DuckDB's Iceberg extension."""

    def __init__(
        self,
        catalog_name: str = "polardb",
        polaris_endpoint: str = "http://localhost:18181/api/catalog",
        oauth_server: str = "http://localhost:18181/api/catalog/v1/oauth/tokens",
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        penguins_url: str = "https://raw.githubusercontent.com/dataprofessor/data/master/penguins_cleaned.csv",
        verbose: bool = False,
    ):
        """Initialize the Polaris explorer."""
        self.catalog_name = catalog_name
        self.polaris_endpoint = polaris_endpoint
        self.oauth_server = oauth_server
        self.client_id = client_id
        self.client_secret = client_secret
        self.penguins_url = penguins_url
        self.verbose = verbose
        self.conn: duckdb.DuckDBPyConnection | None = None

    def log(self, message: str, level: str = "INFO"):
        """Log a message with formatting."""
        prefixes = {
            "INFO": "  ",
            "SUCCESS": "OK ",
            "ERROR": "ERROR ",
            "WARNING": "WARN ",
            "DEBUG": "DEBUG ",
        }
        if level == "DEBUG" and not self.verbose:
            return
        print(f"{prefixes.get(level, '  ')}{message}")

    def setup_duckdb(self) -> bool:
        """
        Setup DuckDB with Iceberg and httpfs extensions.

        The httpfs extension is needed to download the CSV from GitHub.
        The iceberg extension provides Polaris REST Catalog support.
        """
        try:
            self.log("Initializing DuckDB...")
            self.conn = duckdb.connect(":memory:")

            self.log("Loading iceberg extension...", "DEBUG")
            self.conn.execute("INSTALL iceberg")
            self.conn.execute("LOAD iceberg")

            self.log("Loading httpfs extension...", "DEBUG")
            self.conn.execute("INSTALL httpfs")
            self.conn.execute("LOAD httpfs")

            self.log("DuckDB setup complete", "SUCCESS")
            return True

        except Exception as e:
            self.log(f"DuckDB setup failed: {e}", "ERROR")
            return False

    def load_penguins_data(self) -> bool:
        """
        Load Penguins CSV data into DuckDB in-memory table.

        This creates a temporary table with the CSV data that we'll later
        insert into the Iceberg table via Polaris.
        """
        try:
            self.log(f"Downloading Penguins dataset from {self.penguins_url}...")

            if not self.conn:
                raise RuntimeError("DuckDB connection not established")

            # Load CSV into temporary in-memory table
            self.conn.execute(f"""
                CREATE TEMP TABLE penguins_staging AS 
                SELECT * FROM read_csv_auto('{self.penguins_url}')
            """)

            # Get row count
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM penguins_staging"
            ).fetchone()
            count = count_result[0] if count_result else 0
            self.log(f"Loaded {count} penguin records into memory", "SUCCESS")

            # Show schema in verbose mode
            if self.verbose:
                schema = self.conn.execute("DESCRIBE penguins_staging").fetchall()
                self.log("\nPenguins dataset schema:", "DEBUG")
                for col in schema:
                    self.log(f"  {col[0]}: {col[1]}", "DEBUG")

            return True

        except Exception as e:
            self.log(f"Failed to load CSV data: {e}", "ERROR")
            return False

    def connect_to_polaris(self) -> bool:
        """
        Connect to Polaris REST Catalog using DuckDB Iceberg extension.

        This uses DuckDB's ATTACH statement with TYPE iceberg to establish
        connection to the Polaris REST Catalog with OAuth2 authentication.

        Reference: https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs
        """
        try:
            self.log("Connecting to Polaris REST Catalog...")

            if not self.conn:
                raise RuntimeError("DuckDB connection not established")

            # Create secret for OAuth2 authentication
            if self.client_id and self.client_secret:
                self.log("Creating Polaris OAuth2 secret...", "DEBUG")
                self.conn.execute(f"""
                    CREATE OR REPLACE SECRET polaris_secret (
                        TYPE iceberg,
                        CLIENT_ID '{self.client_id}',
                        CLIENT_SECRET '{self.client_secret}',
                        OAUTH2_SERVER_URI '{self.oauth_server}'
                    )
                """)

                # Attach catalog
                self.log(f"Attaching catalog '{self.catalog_name}'...", "DEBUG")
                self.conn.execute(f"""
                    ATTACH '{self.catalog_name}' AS polaris_catalog (
                        TYPE iceberg,
                        SECRET polaris_secret,
                        ENDPOINT '{self.polaris_endpoint}'
                    )
                """)
            else:
                self.log("No credentials provided, using no-auth mode", "WARNING")
                self.conn.execute(f"""
                    ATTACH '{self.catalog_name}' AS polaris_catalog (
                        TYPE iceberg,
                        ENDPOINT '{self.polaris_endpoint}'
                    )
                """)

            # Verify connection by listing tables
            tables = self.conn.execute("SHOW ALL TABLES").fetchall()
            self.log(
                f"Connected successfully ({len(tables)} existing tables)", "SUCCESS"
            )

            return True

        except Exception as e:
            self.log(f"Failed to connect to Polaris: {e}", "ERROR")
            return False

    def create_schema_and_table(self) -> bool:
        """
        Create wildlife schema and penguins Iceberg table in Polaris.

        The table schema matches the Penguins CSV structure.
        DuckDB communicates with Polaris to create these resources.
        """
        try:
            if not self.conn:
                raise RuntimeError("DuckDB connection not established")

            # Create schema
            self.log("Creating 'wildlife' schema...")
            self.conn.execute("CREATE SCHEMA IF NOT EXISTS polaris_catalog.wildlife")
            self.log("Schema created", "SUCCESS")

            # Create table with schema matching the CSV
            self.log("Creating 'penguins' Iceberg table...")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS polaris_catalog.wildlife.penguins (
                    species VARCHAR,
                    island VARCHAR,
                    bill_length_mm DOUBLE,
                    bill_depth_mm DOUBLE,
                    flipper_length_mm DOUBLE,
                    body_mass_g DOUBLE,
                    sex VARCHAR
                )
            """)
            self.log("Iceberg table created via Polaris", "SUCCESS")

            return True

        except Exception as e:
            self.log(f"Failed to create schema/table: {e}", "ERROR")
            return False

    def insert_data_to_iceberg(self) -> bool:
        """
        Insert data from staging table to Polaris Iceberg table.

        Process:
        1. Communicates with Polaris REST API for metadata
        2. Writes Parquet files to S3 storage
        3. Updates Iceberg metadata via Polaris
        """
        try:
            if not self.conn:
                raise RuntimeError("DuckDB connection not established")

            self.log("Inserting data into Iceberg table...")

            # INSERT from staging to Iceberg table
            self.conn.execute("""
                INSERT INTO polaris_catalog.wildlife.penguins
                SELECT * FROM penguins_staging
            """)

            # Verify data was written
            count_result = self.conn.execute(
                "SELECT COUNT(*) FROM polaris_catalog.wildlife.penguins"
            ).fetchone()
            count = count_result[0] if count_result else 0

            self.log(f"Successfully inserted {count} records", "SUCCESS")

            # Clean up staging table
            self.conn.execute("DROP TABLE penguins_staging")

            return True

        except Exception as e:
            self.log(f"Failed to insert data: {e}", "ERROR")
            return False

    def query_data(self) -> bool:
        """Query and display data from the Iceberg table."""
        try:
            if not self.conn:
                raise RuntimeError("DuckDB connection not established")

            self.log("Querying penguin data...")

            # Get species statistics
            result = self.conn.execute("""
                SELECT 
                    species,
                    COUNT(*) as count,
                    ROUND(AVG(bill_length_mm), 2) as avg_bill_length_mm,
                    ROUND(AVG(body_mass_g), 2) as avg_body_mass_g
                FROM polaris_catalog.wildlife.penguins
                GROUP BY species
                ORDER BY species
            """).fetchall()

            self.log(f"\nFound {len(result)} species:", "SUCCESS")
            for row in result:
                self.log(
                    f"  {row[0]}: {row[1]} penguins, "
                    f"avg bill {row[2]}mm, avg mass {row[3]}g",
                    "INFO",
                )

            return True

        except Exception as e:
            self.log(f"Failed to query data: {e}", "ERROR")
            return False

    def explore_metadata(self) -> bool:
        """
        Explore Iceberg metadata using DuckDB functions.

        DuckDB provides iceberg_metadata() and iceberg_snapshots() functions
        to inspect Iceberg table metadata and snapshot history.

        Reference: https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs
        """
        try:
            if not self.conn:
                raise RuntimeError("DuckDB connection not established")

            self.log("\nExploring Iceberg metadata...")

            # Get table metadata
            self.log("\nTable Metadata Files:", "INFO")
            metadata = self.conn.execute("""
                SELECT * FROM iceberg_metadata('polaris_catalog.wildlife.penguins')
            """).fetchall()

            if metadata:
                for row in metadata:
                    self.log(f"  {row}", "INFO")
            else:
                self.log("  No metadata entries found", "INFO")

            self.log(f"\nTotal: {len(metadata)} metadata file(s)", "DEBUG")

            # Get snapshots
            self.log("\nIceberg Snapshots:", "INFO")
            snapshots = self.conn.execute("""
                SELECT 
                    snapshot_id,
                    sequence_number,
                    timestamp_ms,
                    manifest_list
                FROM iceberg_snapshots('polaris_catalog.wildlife.penguins')
            """).fetchall()

            if snapshots:
                for snap in snapshots:
                    self.log(f"\n  Snapshot ID: {snap[0]}", "INFO")
                    self.log(f"  Sequence #: {snap[1]}", "INFO")
                    self.log(f"  Timestamp: {snap[2]}", "INFO")
                    if self.verbose:
                        self.log(f"  Manifest: {snap[3]}", "DEBUG")
            else:
                self.log("  No snapshots found", "INFO")

            self.log(f"\nTotal: {len(snapshots)} snapshot(s)", "SUCCESS")

            return True

        except Exception as e:
            self.log(f"Failed to explore metadata: {e}", "ERROR")
            return False

    def cleanup(self) -> bool:
        """Drop the test table and schema."""
        try:
            if not self.conn:
                raise RuntimeError("DuckDB connection not established")

            self.log("Cleaning up resources...")
            self.conn.execute("DROP TABLE IF EXISTS polaris_catalog.wildlife.penguins")
            self.conn.execute("DROP SCHEMA IF EXISTS polaris_catalog.wildlife")
            self.log("Cleanup complete", "SUCCESS")
            return True

        except Exception as e:
            self.log(f"Failed to cleanup: {e}", "ERROR")
            return False

    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            self.log("Connection closed", "DEBUG")

    def run(self, skip_cleanup: bool = False) -> bool:
        """
        Run the complete exploration workflow.

        Steps:
        1. Setup DuckDB with extensions
        2. Load Penguins CSV into memory
        3. Connect to Polaris REST Catalog
        4. Create schema and Iceberg table
        5. Insert data into Iceberg table
        6. Query data
        7. Explore Iceberg metadata
        8. Cleanup (optional)
        """
        self.log("=" * 70)
        self.log("DuckDB Iceberg Extension - Polaris REST Catalog Explorer")
        self.log("=" * 70)

        steps = [
            ("Setup DuckDB", self.setup_duckdb),
            ("Load Penguins CSV data", self.load_penguins_data),
            ("Connect to Polaris", self.connect_to_polaris),
            ("Create schema and table", self.create_schema_and_table),
            ("Insert data to Iceberg", self.insert_data_to_iceberg),
            ("Query data", self.query_data),
            ("Explore metadata", self.explore_metadata),
        ]

        try:
            for step_name, step_func in steps:
                self.log(f"\n{step_name}...")
                if not step_func():
                    self.log(f"Step failed: {step_name}", "ERROR")
                    return False

            if not skip_cleanup:
                self.log("\nCleaning up...")
                self.cleanup()
            else:
                self.log(
                    "\nSkipping cleanup - wildlife.penguins table preserved", "WARNING"
                )

            self.log("\n" + "=" * 70)
            self.log("All steps completed successfully!", "SUCCESS")
            self.log("=" * 70)
            return True

        except KeyboardInterrupt:
            self.log("\nInterrupted by user", "WARNING")
            return False
        except Exception as e:
            self.log(f"\nUnexpected error: {e}", "ERROR")
            return False
        finally:
            self.close()


def load_credentials(credentials_file: Path) -> tuple[Optional[str], Optional[str]]:
    """
    Load Polaris principal credentials from work/principal.txt.

    Format: REALM,CLIENT_ID,CLIENT_SECRET
    Example: POLARIS,3457ce369dea54c0,38284a6e9226c4653ed8e69b190ed63a
    """
    if not credentials_file.exists():
        return None, None

    content = credentials_file.read_text().strip()

    # Parse CSV format: REALM,CLIENT_ID,CLIENT_SECRET
    if "," in content:
        parts = content.split(",")
        if len(parts) >= 3:
            return parts[1].strip(), parts[2].strip()

    return None, None


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Explore Polaris Iceberg REST Catalog using DuckDB",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage (auto-detect credentials from work/principal.txt)
  python explore_catalog.py

  # Verbose output
  python explore_catalog.py --verbose

  # Keep table for further exploration
  python explore_catalog.py --skip-cleanup

  # Custom credentials
  python explore_catalog.py --client-id ID --client-secret SECRET

Reference:
  https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs
        """,
    )

    parser.add_argument("--catalog", default="polardb", help="Polaris catalog name")
    parser.add_argument(
        "--endpoint",
        default="http://localhost:18181/api/catalog",
        help="Polaris REST endpoint",
    )
    parser.add_argument(
        "--oauth-server",
        default="http://localhost:18181/api/catalog/v1/oauth/tokens",
        help="OAuth2 token server",
    )
    parser.add_argument("--client-id", help="OAuth2 client ID")
    parser.add_argument("--client-secret", help="OAuth2 client secret")
    parser.add_argument("--credentials-file", type=Path, help="Credentials file path")
    parser.add_argument(
        "--penguins-url",
        default="https://raw.githubusercontent.com/dataprofessor/data/master/penguins_cleaned.csv",
        help="Penguins CSV URL",
    )
    parser.add_argument(
        "--skip-cleanup", action="store_true", help="Keep test resources"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    args = parser.parse_args()

    # Load credentials
    client_id = args.client_id
    client_secret = args.client_secret

    if not client_id or not client_secret:
        if args.credentials_file:
            creds_file = args.credentials_file
        else:
            # Use principal.txt - this contains catalog principal credentials
            creds_file = Path(__file__).parent.parent / "work" / "principal.txt"

        if creds_file.exists():
            print(f"Loading credentials from: {creds_file}")
            client_id, client_secret = load_credentials(creds_file)
        else:
            print(f"Warning: Credentials not found: {creds_file}")
            print("Run 'task catalog:setup' to generate principal credentials")

    # Run explorer
    explorer = PolarisExplorer(
        catalog_name=args.catalog,
        polaris_endpoint=args.endpoint,
        oauth_server=args.oauth_server,
        client_id=client_id,
        client_secret=client_secret,
        penguins_url=args.penguins_url,
        verbose=args.verbose,
    )

    success = explorer.run(skip_cleanup=args.skip_cleanup)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
