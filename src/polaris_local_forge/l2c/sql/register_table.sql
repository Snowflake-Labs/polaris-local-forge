--!jinja
-- L2C Register: Create Snowflake External Iceberg Table
-- Schema is inferred from the existing Iceberg metadata file.
-- Table is READ-ONLY (catalog integration = object store).
--
-- Usage:
--   snow sql -f sql/register_table.sql \
--     --enable-templating ALL \
--     --variable sa_role=$SA_ROLE \
--     --variable database=$DATABASE \
--     --variable schema=$SCHEMA \
--     --variable table_name=$TABLE_NAME \
--     --variable external_volume=$EXTERNAL_VOLUME \
--     --variable catalog_integration=$CATALOG_INTEGRATION \
--     --variable metadata_file_path=$METADATA_FILE_PATH

USE ROLE {{sa_role}};
USE DATABASE {{database}};
USE SCHEMA {{schema}};

CREATE ICEBERG TABLE IF NOT EXISTS {{table_name}}
    EXTERNAL_VOLUME = '{{external_volume}}'
    CATALOG = '{{catalog_integration}}'
    METADATA_FILE_PATH = '{{metadata_file_path}}';

SELECT 'Registered ' || '{{table_name}}' || ' from ' || '{{metadata_file_path}}' AS status;
