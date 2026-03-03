--!jinja
-- L2C Cleanup: Revoke grants and drop SA_ROLE
-- Reverses setup_role.sql grants before dropping the role.
--
-- Usage:
--   snow sql -f sql/cleanup_role.sql \
--     --enable-templating ALL \
--     --variable admin_role=$ADMIN_ROLE \
--     --variable sa_role=$SA_ROLE \
--     --variable database=$DATABASE \
--     --variable schema=$SCHEMA \
--     --variable volume_name=$VOLUME_NAME \
--     --variable catalog_integration=$CATALOG_INTEGRATION

USE ROLE {{admin_role}};

-- Revoke grants (reverse order of setup_role.sql)
REVOKE USAGE ON INTEGRATION {{catalog_integration}} FROM ROLE {{sa_role}};
REVOKE USAGE ON EXTERNAL VOLUME {{volume_name}} FROM ROLE {{sa_role}};
REVOKE CREATE ICEBERG TABLE ON SCHEMA {{database}}.{{schema}} FROM ROLE {{sa_role}};
REVOKE USAGE ON SCHEMA {{database}}.{{schema}} FROM ROLE {{sa_role}};
REVOKE USAGE ON DATABASE {{database}} FROM ROLE {{sa_role}};

DROP ROLE IF EXISTS {{sa_role}};

SELECT 'SA_ROLE ' || '{{sa_role}}' || ' dropped' AS status;
