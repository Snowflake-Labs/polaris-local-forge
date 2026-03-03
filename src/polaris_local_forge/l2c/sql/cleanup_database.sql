--!jinja
-- L2C Cleanup: Drop target database (optional, prompted separately)
--
-- Usage:
--   snow sql -f sql/cleanup_database.sql \
--     --enable-templating ALL \
--     --variable admin_role=$ADMIN_ROLE \
--     --variable database=$DATABASE

USE ROLE {{admin_role}};

DROP DATABASE IF EXISTS {{database}};

SELECT 'Database ' || '{{database}}' || ' dropped' AS status;
