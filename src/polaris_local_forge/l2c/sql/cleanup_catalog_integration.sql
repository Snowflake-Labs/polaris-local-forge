--!jinja
-- L2C Cleanup: Drop Catalog Integration
--
-- Usage:
--   snow sql -f sql/cleanup_catalog_integration.sql \
--     --enable-templating ALL \
--     --variable admin_role=$ADMIN_ROLE \
--     --variable catalog_integration=$CATALOG_INTEGRATION

USE ROLE {{admin_role}};

DROP CATALOG INTEGRATION IF EXISTS {{catalog_integration}};

SELECT 'Catalog integration ' || '{{catalog_integration}}' || ' dropped' AS status;
