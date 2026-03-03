--!jinja
-- L2C Cleanup: Drop External Volume
--
-- Usage:
--   snow sql -f sql/cleanup_external_volume.sql \
--     --enable-templating ALL \
--     --variable admin_role=$ADMIN_ROLE \
--     --variable volume_name=$VOLUME_NAME

USE ROLE {{admin_role}};

DROP EXTERNAL VOLUME IF EXISTS {{volume_name}};

SELECT 'External volume ' || '{{volume_name}}' || ' dropped' AS status;
