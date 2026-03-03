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

"""Pure Python Iceberg metadata path rewriter.

Rewrites absolute S3 paths in Iceberg metadata after migrating table data
from one bucket to another. Handles all three file types per the Iceberg spec:

  1. metadata.json (JSON) -- location, snapshot manifest-list, metadata-log
  2. Manifest lists (Avro) -- manifest_path (field 500)
  3. Manifest files (Avro) -- data_file.file_path (field 100),
     data_file.referenced_data_file (field 143)

This is a pure Python equivalent of Iceberg's Spark `rewrite-table-path`
procedure (RewriteTablePathSparkAction), using json + fastavro instead of
the JVM. See docs/iceberg-metadata-rewrite.md for rationale and alternatives.

DEPRECATION: Replace with pyiceberg rewrite_table_path when available.
Tracking: https://github.com/apache/iceberg-python/issues/2014
"""

import json
from io import BytesIO

import click
import fastavro

from polaris_local_forge.l2c.common import find_latest_metadata


def _s3_get_json(s3_client, bucket: str, key: str) -> dict:
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read())


def _s3_put_json(s3_client, bucket: str, key: str, data: dict) -> None:
    body = json.dumps(data, indent=2).encode()
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)


def _s3_get_avro(s3_client, bucket: str, key: str) -> tuple[dict, list[dict]]:
    """Download an Avro file, return (writer_schema, records)."""
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    buf = BytesIO(resp["Body"].read())
    reader = fastavro.reader(buf)
    schema = reader.writer_schema
    records = list(reader)
    return schema, records


def _s3_put_avro(
    s3_client, bucket: str, key: str, schema: dict, records: list[dict],
) -> None:
    """Write records back to S3 as Avro with the original schema."""
    buf = BytesIO()
    fastavro.writer(buf, schema, records)
    buf.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buf.read())


def _replace_prefix(value: str, src: str, dst: str) -> str:
    if value and value.startswith(src):
        return dst + value[len(src):]
    return value


# ---------------------------------------------------------------------------
# metadata.json rewriter
# ---------------------------------------------------------------------------

def _rewrite_metadata_json(data: dict, src: str, dst: str) -> dict:
    """Rewrite path fields in an Iceberg metadata.json dict.

    Fields rewritten:
      - location (table base path)
      - snapshots[].manifest-list (manifest list URIs)
      - metadata-log[].metadata-file (previous metadata URIs)
    """
    if "location" in data:
        data["location"] = _replace_prefix(data["location"], src, dst)

    for snapshot in data.get("snapshots", []):
        if "manifest-list" in snapshot:
            snapshot["manifest-list"] = _replace_prefix(
                snapshot["manifest-list"], src, dst,
            )

    for entry in data.get("metadata-log", []):
        if "metadata-file" in entry:
            entry["metadata-file"] = _replace_prefix(
                entry["metadata-file"], src, dst,
            )

    return data


# ---------------------------------------------------------------------------
# Avro rewriters
# ---------------------------------------------------------------------------

def _rewrite_manifest_list(records: list[dict], src: str, dst: str) -> list[dict]:
    """Rewrite manifest_path in manifest list records (field 500)."""
    for rec in records:
        if "manifest_path" in rec:
            rec["manifest_path"] = _replace_prefix(rec["manifest_path"], src, dst)
    return records


def _rewrite_manifest(records: list[dict], src: str, dst: str) -> list[dict]:
    """Rewrite data_file paths in manifest records (fields 100, 143)."""
    for rec in records:
        df = rec.get("data_file")
        if not df:
            continue
        if "file_path" in df:
            df["file_path"] = _replace_prefix(df["file_path"], src, dst)
        if df.get("referenced_data_file"):
            df["referenced_data_file"] = _replace_prefix(
                df["referenced_data_file"], src, dst,
            )
    return records


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def _key_from_uri(uri: str, bucket: str) -> str:
    """Extract S3 key from a full s3://bucket/key URI."""
    prefix = f"s3://{bucket}/"
    if uri.startswith(prefix):
        return uri[len(prefix):]
    alt_prefix = f"s3a://{bucket}/"
    if uri.startswith(alt_prefix):
        return uri[len(alt_prefix):]
    return uri


def rewrite_table_paths(
    cloud_s3,
    bucket: str,
    namespace: str,
    table: str,
    source_prefix: str,
    target_prefix: str,
) -> int:
    """Rewrite all Iceberg metadata paths for a table after sync.

    Performs an in-place rewrite on the destination bucket, updating all
    absolute path references from source_prefix to target_prefix.

    Args:
        cloud_s3: boto3 S3 client for the destination bucket
        bucket: destination S3 bucket name
        namespace: Iceberg namespace (e.g., "wildlife")
        table: Iceberg table name (e.g., "penguins")
        source_prefix: original path prefix (e.g., "s3://polardb/")
        target_prefix: new path prefix (e.g., "s3://kameshs-polaris-dev-polardb/")

    Returns:
        Number of files rewritten
    """
    metadata_key = find_latest_metadata(cloud_s3, bucket, namespace, table)
    if not metadata_key:
        click.secho(
            f"    No metadata.json found under {namespace}/{table}/metadata/",
            fg="yellow",
        )
        return 0

    click.echo(f"    Rewriting metadata: {metadata_key}")
    metadata = _s3_get_json(cloud_s3, bucket, metadata_key)

    rewritten_count = 1  # metadata.json itself

    manifest_list_uris = [
        s["manifest-list"] for s in metadata.get("snapshots", [])
        if "manifest-list" in s
    ]

    metadata = _rewrite_metadata_json(metadata, source_prefix, target_prefix)

    for ml_uri in manifest_list_uris:
        ml_key = _key_from_uri(
            _replace_prefix(ml_uri, source_prefix, target_prefix), bucket,
        )
        try:
            schema, records = _s3_get_avro(cloud_s3, bucket, ml_key)
        except Exception as e:
            click.secho(f"    Warning: cannot read manifest list {ml_key}: {e}", fg="yellow")
            continue

        manifest_uris = [r["manifest_path"] for r in records if "manifest_path" in r]

        records = _rewrite_manifest_list(records, source_prefix, target_prefix)
        _s3_put_avro(cloud_s3, bucket, ml_key, schema, records)
        rewritten_count += 1

        for m_uri in manifest_uris:
            m_key = _key_from_uri(
                _replace_prefix(m_uri, source_prefix, target_prefix), bucket,
            )
            try:
                m_schema, m_records = _s3_get_avro(cloud_s3, bucket, m_key)
            except Exception as e:
                click.secho(f"    Warning: cannot read manifest {m_key}: {e}", fg="yellow")
                continue

            m_records = _rewrite_manifest(m_records, source_prefix, target_prefix)
            _s3_put_avro(cloud_s3, bucket, m_key, m_schema, m_records)
            rewritten_count += 1

    _s3_put_json(cloud_s3, bucket, metadata_key, metadata)

    click.echo(f"    Rewrote {rewritten_count} file(s): "
               f"{source_prefix} -> {target_prefix}")
    return rewritten_count
