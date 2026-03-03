# Configuring Apache Polaris with S3-Compatible Object Storage

When using Apache Polaris as an Iceberg REST catalog with an S3-compatible object
store like RustFS or MinIO (instead of real AWS S3), three configuration layers
must be aligned: server-side catalog, client-side PyIceberg, and access control.

## 1. Polaris Catalog (Server-Side via Management API)

When creating the catalog via `POST /api/management/v1/catalogs`, set
`stsUnavailable: true` in `storageConfigInfo`. This tells Polaris not to call
AWS STS to generate temporary credentials -- S3-compatible stores don't support STS.

```json
{
  "catalog": {
    "name": "my_catalog",
    "type": "INTERNAL",
    "properties": {
      "default-base-location": "s3://my-bucket"
    },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://my-bucket"],
      "region": "us-east-1",
      "endpoint": "http://localhost:19000",
      "endpointInternal": "http://rustfs-svc.rustfs:9000",
      "pathStyleAccess": true,
      "stsUnavailable": true
    }
  }
}
```

| Field | Purpose |
|-------|---------|
| `endpoint` | External endpoint for clients outside the cluster |
| `endpointInternal` | Internal endpoint for Polaris pod (inside k8s) |
| `pathStyleAccess` | Required for RustFS/MinIO (no virtual-hosted buckets) |
| `stsUnavailable` | Disables STS credential vending |

## 2. PyIceberg Client Configuration

Two critical settings beyond the standard connection parameters:

### Disable vended credentials

```python
"header.X-Iceberg-Access-Delegation": ""
```

Set to **empty string** to disable. Without this, PyIceberg auto-requests
`vended-credentials` and the server errors with *"no credentials are available"*
since STS is unavailable.

### Use FsspecFileIO

```python
"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"
```

Use fsspec/s3fs instead of PyArrow's native S3 filesystem. PyArrow always uses
multipart upload, and RustFS has a bug where `HeadObject` returns 403 on
multipart-uploaded objects. fsspec uses simple PUT for small files, avoiding this.

### Provide S3 credentials directly

Since the server won't vend credentials, the client must supply them:

```python
catalog = RestCatalog(
    name="my_catalog",
    **{
        "uri": "http://localhost:18181/api/catalog",
        "credential": f"{client_id}:{client_secret}",
        "header.content-type": "application/vnd.api+json",
        "header.Polaris-Realm": "POLARIS",
        "header.X-Iceberg-Access-Delegation": "",
        "warehouse": "my_catalog",
        "scope": "PRINCIPAL_ROLE:ALL",
        "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
        "s3.endpoint": "http://localhost:19000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "us-east-1",
        "s3.path-style-access": "true",
    },
)
```

## 3. Access Control Setup

Catalog creation requires **root/bootstrap credentials** (not regular principal
credentials). After creating the catalog, set up access for your principal:

1. Create a catalog role in the new catalog
2. Grant `CATALOG_MANAGE_CONTENT` privilege to the catalog role
3. Assign the catalog role to the principal's principal role

## 4. DuckDB Iceberg Extension

DuckDB's Iceberg extension also requests credential vending by default when
connecting to a REST catalog. Two things are needed:

### Disable credential vending

Add `ACCESS_DELEGATION_MODE 'none'` to the `ATTACH` statement:

```sql
ATTACH 'my_catalog' AS polaris_catalog (
    TYPE iceberg,
    SECRET polaris_secret,
    ENDPOINT 'http://localhost:18181/api/catalog',
    ACCESS_DELEGATION_MODE 'none'
);
```

### Provide direct S3 credentials

Create a separate S3 secret so DuckDB can read Parquet files directly:

```sql
CREATE OR REPLACE SECRET rustfs_s3 (
    TYPE s3,
    KEY_ID 'admin',
    SECRET 'password',
    ENDPOINT 'localhost:19000',
    URL_STYLE 'path',
    USE_SSL false,
    REGION 'us-east-1'
);
```

Note: The `ENDPOINT` value for DuckDB's S3 secret must be **host:port only**
(no `http://` scheme prefix).

## Issues Reference

| Issue | Root Cause | Fix |
|-------|-----------|-----|
| `S3Exception: Access Denied` on table creation | Polaris tries STS to vend credentials; S3-compatible stores don't support STS | `stsUnavailable: true` on catalog |
| `Credential vending was requested but no credentials are available` (PyIceberg) | PyIceberg auto-sends `X-Iceberg-Access-Delegation: vended-credentials` | Set header to empty string `""` |
| `Credential vending was requested but no credentials are available` (DuckDB) | DuckDB Iceberg extension requests vended credentials by default | `ACCESS_DELEGATION_MODE 'none'` + S3 secret |
| `AWS Error ACCESS_DENIED during HeadObject` | RustFS returns 403 on HeadObject for multipart-uploaded objects | Use `py-io-impl: pyiceberg.io.fsspec.FsspecFileIO` |
| `Could not get token from .../oauth/tokens` (DuckDB) | Incorrect OAuth URL or missing credentials in DuckDB secret | Use `/api/catalog/v1/oauth/tokens` path |

## References

- [Polaris Management API Spec](https://editor.swagger.io/?url=https://raw.githubusercontent.com/apache/polaris/refs/heads/main/spec/polaris-management-service.yml) -- `AwsStorageConfigInfo.stsUnavailable` field
- [PyIceberg Configuration](https://py.iceberg.apache.org/configuration/#fileio) -- FileIO, S3 settings, and Apache Polaris example
- [DuckDB Iceberg REST Catalogs](https://duckdb.org/docs/stable/core_extensions/iceberg/iceberg_rest_catalogs) -- ATTACH syntax, `ACCESS_DELEGATION_MODE`

## Where This Is Applied in polaris-local-forge

| Layer | File | Setting |
|-------|------|---------|
| Server catalog | `polaris-forge-setup/catalog_setup.yml` | `stsUnavailable: true` |
| Polaris pod env | `polaris-forge-setup/templates/polaris.yaml.j2` | `AWS_ENDPOINT_URL` uses internal service endpoint |
| PyIceberg data loader | `scripts/pyiceberg_data_loader.py` | `X-Iceberg-Access-Delegation: ""` + `FsspecFileIO` + direct S3 creds |
| Verify notebook | `polaris-forge-setup/templates/notebooks/verify_setup.ipynb.j2` | Same PyIceberg client config |
| DuckDB analyze template | `polaris-forge-setup/templates/scripts/analyze_catalog.sql.j2` | `ACCESS_DELEGATION_MODE 'none'` + S3 secret |
| DuckDB explore template | `polaris-forge-setup/templates/scripts/explore_catalog.sql.j2` | Same DuckDB config |
| CLI catalog query | `src/polaris_local_forge/catalog.py` | S3 secret + `ACCESS_DELEGATION_MODE 'none'` |
| Python DuckDB explorer | `scripts/explore_catalog.py` | S3 secret + `ACCESS_DELEGATION_MODE 'none'` |
| L2C workbook | `user-project/notebooks/l2c_workbook.ipynb` | Both DuckDB and PyIceberg fixes |
