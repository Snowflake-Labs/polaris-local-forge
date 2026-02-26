--!jinja
-- L2C Setup: Create SA_ROLE with least-privilege grants
-- Creates a migration role restricted to the target DB/Schema.
-- SA_ROLE has NO account-level privileges.
--
-- Prerequisites:
-- - admin_role must be able to CREATE ROLE, CREATE DATABASE, and GRANT
-- - External volume and catalog integration must already exist
--
-- Usage:
--   snow sql -f sql/setup_role.sql \
--     --enable-templating ALL \
--     --variable admin_role=$ADMIN_ROLE \
--     --variable sa_role=$SA_ROLE \
--     --variable snowflake_user=$SNOWFLAKE_USER \
--     --variable database=$DATABASE \
--     --variable schema=$SCHEMA \
--     --variable volume_name=$VOLUME_NAME \
--     --variable catalog_integration=$CATALOG_INTEGRATION

USE ROLE {{admin_role}};

-- ============================================================================
-- Create Least-Privilege Migration Role
-- ============================================================================

CREATE ROLE IF NOT EXISTS {{sa_role}};
GRANT ROLE {{sa_role}} TO USER {{snowflake_user}};

-- ============================================================================
-- Create Target Database and Schema
-- ============================================================================

CREATE DATABASE IF NOT EXISTS {{database}};
CREATE SCHEMA IF NOT EXISTS {{database}}.{{schema}};

-- ============================================================================
-- Least-Privilege Grants to SA_ROLE
-- ============================================================================
-- SA_ROLE can only:
--   1. See the target DB/Schema (USAGE)
--   2. Create Iceberg tables in that schema
--   3. Use the external volume and catalog integration
-- No OWNERSHIP, no ALL PRIVILEGES, no account-level grants.

GRANT USAGE ON DATABASE {{database}} TO ROLE {{sa_role}};
GRANT USAGE ON SCHEMA {{database}}.{{schema}} TO ROLE {{sa_role}};
GRANT CREATE ICEBERG TABLE ON SCHEMA {{database}}.{{schema}} TO ROLE {{sa_role}};
GRANT USAGE ON EXTERNAL VOLUME {{volume_name}} TO ROLE {{sa_role}};
GRANT USAGE ON INTEGRATION {{catalog_integration}} TO ROLE {{sa_role}};

SELECT 'SA_ROLE ' || '{{sa_role}}' || ' created with least-privilege grants on ' || '{{database}}' || '.' || '{{schema}}' AS status;
