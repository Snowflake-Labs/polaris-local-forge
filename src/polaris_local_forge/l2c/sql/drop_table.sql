--!jinja
-- L2C Clear: Drop a single Iceberg table
--
-- Usage:
--   snow sql -f sql/drop_table.sql \
--     --enable-templating ALL \
--     --variable sa_role=$SA_ROLE \
--     --variable database=$DATABASE \
--     --variable schema=$SCHEMA \
--     --variable table_name=$TABLE_NAME

USE ROLE {{sa_role}};
USE DATABASE {{database}};
USE SCHEMA {{schema}};

DROP ICEBERG TABLE IF EXISTS {{table_name}};

SELECT 'Dropped ' || '{{table_name}}' AS status;
