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

"""L2C inventory command -- discover local Polaris tables via Iceberg REST API.

Uses the Polaris REST catalog API directly (no pyiceberg dependency for this
command) to avoid version-compatibility issues with the /v1/config endpoint.
"""

import json
from pathlib import Path

import click
import requests
from dotenv import dotenv_values

from polaris_local_forge.l2c.common import (
    get_local_catalog_name,
    get_local_polaris_url,
    read_principal,
)


class PolarisRestClient:
    """Thin wrapper around the Iceberg REST Catalog API."""

    def __init__(self, base_url: str, catalog_name: str,
                 realm: str, client_id: str, client_secret: str):
        self.base_url = f"{base_url}/api/catalog"
        self.prefix = catalog_name
        self._token = self._get_token(base_url, realm, client_id, client_secret)
        self._headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    @staticmethod
    def _get_token(base_url: str, realm: str,
                   client_id: str, client_secret: str) -> str:
        resp = requests.post(
            f"{base_url}/api/catalog/v1/oauth/tokens",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
            headers={"Polaris-Realm": realm},
        )
        resp.raise_for_status()
        return resp.json()["access_token"]

    def list_namespaces(self) -> list[list[str]]:
        resp = requests.get(
            f"{self.base_url}/v1/{self.prefix}/namespaces",
            headers=self._headers,
        )
        resp.raise_for_status()
        return resp.json().get("namespaces", [])

    def list_tables(self, namespace: str) -> list[dict]:
        resp = requests.get(
            f"{self.base_url}/v1/{self.prefix}/namespaces/{namespace}/tables",
            headers=self._headers,
        )
        resp.raise_for_status()
        return resp.json().get("identifiers", [])

    def load_table(self, namespace: str, table: str) -> dict:
        resp = requests.get(
            f"{self.base_url}/v1/{self.prefix}/namespaces/{namespace}/tables/{table}",
            headers=self._headers,
        )
        resp.raise_for_status()
        return resp.json()


def _parse_schema(metadata: dict) -> list[dict]:
    """Extract column info from Iceberg table metadata."""
    schemas = metadata.get("metadata", {}).get("schemas", [])
    if not schemas:
        return []
    current_id = metadata.get("metadata", {}).get("current-schema-id", 0)
    schema = next((s for s in schemas if s.get("schema-id") == current_id), schemas[-1])
    return [
        {
            "name": f["name"],
            "type": f.get("type", "unknown") if isinstance(f.get("type"), str)
                    else f.get("type", {}).get("type", "unknown"),
            "required": f.get("required", False),
        }
        for f in schema.get("fields", [])
    ]


def _discover_tables(client: PolarisRestClient) -> list[dict]:
    """Walk all namespaces and list tables with schema info."""
    results = []
    for ns_parts in client.list_namespaces():
        ns_name = ".".join(ns_parts)
        for ident in client.list_tables(ns_name):
            table_name = ident["name"]
            fqn = f"{ns_name}.{table_name}"
            try:
                meta = client.load_table(ns_name, table_name)
                location = meta.get("metadata", {}).get("location", "")
                results.append({
                    "namespace": ns_name,
                    "table": table_name,
                    "fqn": fqn,
                    "schema": _parse_schema(meta),
                    "location": location,
                })
            except Exception as e:
                results.append({
                    "namespace": ns_name,
                    "table": table_name,
                    "fqn": fqn,
                    "error": str(e),
                })
    return results


@click.command("inventory")
@click.option("--output", "-o", type=click.Choice(["text", "json"]), default="text",
              help="Output format")
@click.pass_context
def inventory(ctx, output: str):
    """List local Polaris namespaces and tables available for migration."""
    work_dir = ctx.obj["WORK_DIR"]
    env_file = work_dir / ".env"
    cfg = dotenv_values(env_file) if env_file.exists() else {}

    realm, client_id, client_secret = read_principal(work_dir)
    polaris_url = get_local_polaris_url(cfg)
    catalog_name = get_local_catalog_name(cfg)

    try:
        client = PolarisRestClient(
            polaris_url, catalog_name, realm, client_id, client_secret,
        )
    except requests.ConnectionError:
        raise click.ClickException(
            f"Cannot connect to Polaris at {polaris_url}.\n"
            "Is the cluster running? Try: plf doctor"
        )
    except requests.HTTPError as e:
        raise click.ClickException(f"Polaris auth failed: {e}")

    tables = _discover_tables(client)

    if not tables:
        click.echo("No tables found in local Polaris catalog.")
        return

    if output == "json":
        click.echo(json.dumps(tables, indent=2))
        return

    click.echo(f"Found {len(tables)} table(s) in local Polaris:\n")
    for t in tables:
        if "error" in t:
            click.echo(f"  {t['fqn']}  [ERROR: {t['error']}]")
            continue
        cols = ", ".join(f"{f['name']}:{f['type']}" for f in t["schema"])
        click.echo(f"  {t['fqn']}")
        click.echo(f"    location: {t['location']}")
        click.echo(f"    columns:  {cols}")
        click.echo()
