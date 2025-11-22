{% macro s3_setup() %}
LOAD httpfs;
LOAD delta;

CREATE OR REPLACE SECRET minio_secret (
    TYPE S3,
    KEY_ID '{{ env_var("AWS_ACCESS_KEY_ID") }}',
    SECRET '{{ env_var("AWS_SECRET_ACCESS_KEY") }}',
    ENDPOINT '{{ env_var("MINIO_ENDPOINT") | replace("http://", "") }}',
    URL_STYLE 'path',
    USE_SSL false
);

{% endmacro %}
