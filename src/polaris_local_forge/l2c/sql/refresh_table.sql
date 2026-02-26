--!jinja
-- L2C Refresh: Update Snowflake External Iceberg Table metadata pointer
-- Points the table at a newer Iceberg metadata snapshot (zero-downtime).
--
-- Usage:
--   snow sql -f sql/refresh_table.sql \
--     --enable-templating ALL \
--     --variable sa_role=$SA_ROLE \
--     --variable database=$DATABASE \
--     --variable schema=$SCHEMA \
--     --variable table_name=$TABLE_NAME \
--     --variable metadata_file_path=$METADATA_FILE_PATH

USE ROLE {{sa_role}};
USE DATABASE {{database}};
USE SCHEMA {{schema}};

ALTER ICEBERG TABLE {{table_name}}
    REFRESH '{{metadata_file_path}}';

SELECT 'Refreshed ' || '{{table_name}}' || ' -> ' || '{{metadata_file_path}}' AS status;
