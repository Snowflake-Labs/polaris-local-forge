--!jinja
-- L2C Setup: Catalog Integration (account-level object)
-- Creates an object-store catalog integration for Iceberg tables.
--
-- Prerequisites:
-- - admin_role must have CREATE INTEGRATION privilege
--
-- Usage:
--   snow sql -f sql/setup_catalog_integration.sql \
--     --enable-templating ALL \
--     --variable admin_role=$ADMIN_ROLE \
--     --variable catalog_integration=$CATALOG_INTEGRATION

USE ROLE {{admin_role}};

CREATE CATALOG INTEGRATION IF NOT EXISTS {{catalog_integration}}
  CATALOG_SOURCE = OBJECT_STORE
  TABLE_FORMAT = ICEBERG
  ENABLED = TRUE;

SELECT 'Catalog integration ' || '{{catalog_integration}}' || ' created' AS status;
