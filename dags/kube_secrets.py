from airflow.contrib.kubernetes.secret import Secret

# BambooHR
BAMBOOHR_API_TOKEN = Secret(
    "env", "BAMBOOHR_API_TOKEN", "airflow", "BAMBOOHR_API_TOKEN"
)

# gitlab_analytics user private token
GITLAB_ANALYTICS_PRIVATE_TOKEN = Secret(
    "env", "GITLAB_ANALYTICS_PRIVATE_TOKEN", "airflow", "GITLAB_ANALYTICS_PRIVATE_TOKEN"
)

# GCP Related
GCP_SERVICE_CREDS = Secret(
    "env", "GCP_SERVICE_CREDS", "airflow", "cloudsql-credentials"
)
GCP_PROJECT = Secret("env", "GCP_PROJECT", "airflow", "GCP_PROJECT")
GCP_REGION = Secret("env", "GCP_REGION", "airflow", "GCP_REGION")
GCP_PRODUCTION_INSTANCE_NAME = Secret(
    "env", "GCP_PRODUCTION_INSTANCE_NAME", "airflow", "GCP_PRODUCTION_INSTANCE"
)

GCP_BILLING_ACCOUNT_CREDENTIALS = Secret(
    "env",
    "GCP_BILLING_ACCOUNT_CREDENTIALS",
    "airflow",
    "GCP_BILLING_ACCOUNT_CREDENTIALS",
)

# Stitch
STITCH_CONFIG = Secret("volume", "/secrets", "airflow", "STITCH_CONFIG")

# Greenhouse S3 Bucket
GREENHOUSE_ACCESS_KEY_ID = Secret(
    "env", "GREENHOUSE_ACCESS_KEY_ID", "airflow", "GREENHOUSE_ACCESS_KEY_ID"
)
GREENHOUSE_SECRET_ACCESS_KEY = Secret(
    "env", "GREENHOUSE_SECRET_ACCESS_KEY", "airflow", "GREENHOUSE_SECRET_ACCESS_KEY"
)

# Gainsight S3 Bucket
GAINSIGHT_ACCESS_KEY_ID = Secret(
    "env", "GAINSIGHT_ACCESS_KEY_ID", "airflow", "GAINSIGHT_ACCESS_KEY_ID"
)
GAINSIGHT_SECRET_ACCESS_KEY = Secret(
    "env", "GAINSIGHT_SECRET_ACCESS_KEY", "airflow", "GAINSIGHT_SECRET_ACCESS_KEY"
)
GAINSIGHT_BUCKET_NAME = Secret(
    "env", "GAINSIGHT_BUCKET_NAME", "airflow", "GAINSIGHT_BUCKET_NAME"
)

# Postgres
PG_USERNAME = Secret("env", "PG_USERNAME", "airflow", "PG_USERNAME")
PG_ADDRESS = Secret("env", "PG_ADDRESS", "airflow", "PG_ADDRESS")
PG_PASSWORD = Secret("env", "PG_PASSWORD", "airflow", "PG_PASSWORD")
PG_DATABASE = Secret("env", "PG_DATABASE", "airflow", "PG_DATABASE")
PG_PORT = Secret("env", "PG_PORT", "airflow", "PG_PORT")

# Customers DB
CUSTOMERS_DB_USER = Secret("env", "CUSTOMERS_DB_USER", "airflow", "CUSTOMERS_DB_USER")
CUSTOMERS_DB_PASS = Secret("env", "CUSTOMERS_DB_PASS", "airflow", "CUSTOMERS_DB_PASS")
CUSTOMERS_DB_HOST = Secret("env", "CUSTOMERS_DB_HOST", "airflow", "CUSTOMERS_DB_HOST")
CUSTOMERS_DB_NAME = Secret("env", "CUSTOMERS_DB_NAME", "airflow", "CUSTOMERS_DB_NAME")


# GitLab Profiler DB
GITLAB_COM_DB_USER = Secret(
    "env", "GITLAB_COM_DB_USER", "airflow", "GITLAB_COM_DB_USER"
)
GITLAB_COM_DB_PASS = Secret(
    "env", "GITLAB_COM_DB_PASS", "airflow", "GITLAB_COM_DB_PASS"
)
GITLAB_COM_DB_HOST = Secret(
    "env", "GITLAB_COM_DB_HOST", "airflow", "GITLAB_COM_DB_HOST"
)
GITLAB_COM_DB_NAME = Secret(
    "env", "GITLAB_COM_DB_NAME", "airflow", "GITLAB_COM_DB_NAME"
)

GITLAB_OPS_DB_USER = Secret(
    "env", "GITLAB_OPS_DB_USER", "airflow", "GITLAB_OPS_DB_USER"
)
GITLAB_OPS_DB_PASS = Secret(
    "env", "GITLAB_OPS_DB_PASS", "airflow", "GITLAB_OPS_DB_PASS"
)
GITLAB_OPS_DB_HOST = Secret(
    "env", "GITLAB_OPS_DB_HOST", "airflow", "GITLAB_OPS_DB_HOST"
)
GITLAB_OPS_DB_NAME = Secret(
    "env", "GITLAB_OPS_DB_NAME", "airflow", "GITLAB_OPS_DB_NAME"
)

# GitLab Profiler DB
GITLAB_PROFILER_DB_USER = Secret(
    "env", "GITLAB_PROFILER_DB_USER", "airflow", "GITLAB_PROFILER_DB_USER"
)
GITLAB_PROFILER_DB_PASS = Secret(
    "env", "GITLAB_PROFILER_DB_PASS", "airflow", "GITLAB_PROFILER_DB_PASS"
)
GITLAB_PROFILER_DB_HOST = Secret(
    "env", "GITLAB_PROFILER_DB_HOST", "airflow", "GITLAB_PROFILER_DB_HOST"
)
GITLAB_PROFILER_DB_NAME = Secret(
    "env", "GITLAB_PROFILER_DB_NAME", "airflow", "GITLAB_PROFILER_DB_NAME"
)

# dbt
GIT_DATA_TESTS_PRIVATE_KEY = Secret(
    "env", "GIT_DATA_TESTS_PRIVATE_KEY", "airflow", "GIT_DATA_TESTS_PRIVATE_KEY"
)
GIT_DATA_TESTS_CONFIG = Secret(
    "env", "GIT_DATA_TESTS_CONFIG", "airflow", "GIT_DATA_TESTS_CONFIG"
)

# graphite
GRAPHITE_HOST = Secret("env", "GRAPHITE_HOST", "airflow", "GRAPHITE_HOST")
GRAPHITE_PASSWORD = Secret("env", "GRAPHITE_PASSWORD", "airflow", "GRAPHITE_PASSWORD")
GRAPHITE_USERNAME = Secret("env", "GRAPHITE_USERNAME", "airflow", "GRAPHITE_USERNAME")

# Snowflake Generic
SNOWFLAKE_ACCOUNT = Secret("env", "SNOWFLAKE_ACCOUNT", "airflow", "SNOWFLAKE_ACCOUNT")
SNOWFLAKE_PASSWORD = Secret(
    "env", "SNOWFLAKE_PASSWORD", "airflow", "SNOWFLAKE_PASSWORD"
)

# Snowflake Load
SNOWFLAKE_LOAD_DATABASE = Secret(
    "env", "SNOWFLAKE_LOAD_DATABASE", "airflow", "SNOWFLAKE_LOAD_DATABASE"
)
SNOWFLAKE_LOAD_ROLE = Secret(
    "env", "SNOWFLAKE_LOAD_ROLE", "airflow", "SNOWFLAKE_LOAD_ROLE"
)
SNOWFLAKE_LOAD_PASSWORD = Secret(
    "env", "SNOWFLAKE_LOAD_PASSWORD", "airflow", "SNOWFLAKE_LOAD_PASSWORD"
)
SNOWFLAKE_LOAD_USER = Secret(
    "env", "SNOWFLAKE_LOAD_USER", "airflow", "SNOWFLAKE_LOAD_USER"
)
SNOWFLAKE_LOAD_WAREHOUSE = Secret(
    "env", "SNOWFLAKE_LOAD_WAREHOUSE", "airflow", "SNOWFLAKE_LOAD_WAREHOUSE"
)

# Snowflake Transform
SNOWFLAKE_TRANSFORM_ROLE = Secret(
    "env", "SNOWFLAKE_TRANSFORM_ROLE", "airflow", "SNOWFLAKE_TRANSFORM_ROLE"
)
SNOWFLAKE_TRANSFORM_SCHEMA = Secret(
    "env", "SNOWFLAKE_TRANSFORM_SCHEMA", "airflow", "SNOWFLAKE_TRANSFORM_SCHEMA"
)
SNOWFLAKE_TRANSFORM_USER = Secret(
    "env", "SNOWFLAKE_TRANSFORM_USER", "airflow", "SNOWFLAKE_TRANSFORM_USER"
)
SNOWFLAKE_TRANSFORM_WAREHOUSE = Secret(
    "env", "SNOWFLAKE_TRANSFORM_WAREHOUSE", "airflow", "SNOWFLAKE_TRANSFORM_WAREHOUSE"
)
SNOWFLAKE_PREP_WAREHOUSE = Secret(
    "env", "SNOWFLAKE_PREP_WAREHOUSE", "airflow", "SNOWFLAKE_PREP_WAREHOUSE"
)
SNOWFLAKE_PROD_WAREHOUSE = Secret(
    "env", "SNOWFLAKE_PROD_WAREHOUSE", "airflow", "SNOWFLAKE_PROD_WAREHOUSE"
)
SNOWFLAKE_USER = Secret("env", "SNOWFLAKE_USER", "airflow", "SNOWFLAKE_USER")

# Permission Bot
PERMISSION_BOT_USER = Secret(
    "env", "PERMISSION_BOT_USER", "airflow", "SNOWFLAKE_PERMISSION_USER"
)
PERMISSION_BOT_PASSWORD = Secret(
    "env", "PERMISSION_BOT_PASSWORD", "airflow", "SNOWFLAKE_PERMISSION_PASSWORD"
)
PERMISSION_BOT_ACCOUNT = Secret(
    "env", "PERMISSION_BOT_ACCOUNT", "airflow", "SNOWFLAKE_ACCOUNT"
)
PERMISSION_BOT_DATABASE = Secret(
    "env", "PERMISSION_BOT_DATABASE", "airflow", "SNOWFLAKE_PERMISSION_DATABASE"
)
PERMISSION_BOT_ROLE = Secret(
    "env", "PERMISSION_BOT_ROLE", "airflow", "SNOWFLAKE_PERMISSION_ROLE"
)
PERMISSION_BOT_WAREHOUSE = Secret(
    "env", "PERMISSION_BOT_WAREHOUSE", "airflow", "SNOWFLAKE_PERMISSION_WAREHOUSE"
)

# Domains
DORG_API_KEY = Secret("env", "DORG_API_KEY", "airflow", "DORG_API_KEY")
DORG_USERNAME = Secret("env", "DORG_USERNAME", "airflow", "DORG_USERNAME")
DORG_PASSWORD = Secret("env", "DORG_PASSWORD", "airflow", "DORG_PASSWORD")
GMAPS_API_KEY = Secret("env", "GMAPS_API_KEY", "airflow", "GMAPS_API_KEY")
CLEARBIT_API_KEY = Secret("env", "CLEARBIT_API_KEY", "airflow", "CLEARBIT_API_KEY")

# GitLab API
GITLAB_COM_API_TOKEN = Secret(
    "env", "GITLAB_COM_API_TOKEN", "airflow", "GITLAB_COM_API_TOKEN"
)

QUALTRICS_API_TOKEN = Secret(
    "env", "QUALTRICS_API_TOKEN", "airflow", "QUALTRICS_API_TOKEN"
)

QUALTRICS_GROUP_ID = Secret(
    "env", "QUALTRICS_GROUP_ID", "airflow", "QUALTRICS_GROUP_ID"
)

QUALTRICS_POOL_ID = Secret("env", "QUALTRICS_POOL_ID", "airflow", "QUALTRICS_POOL_ID")

QUALTRICS_NPS_ID = Secret("env", "QUALTRICS_NPS_ID", "airflow", "QUALTRICS_NPS_ID")

QUALTRICS_POST_PURCHASE_ID = Secret(
    "env", "QUALTRICS_POST_PURCHASE_ID", "airflow", "QUALTRICS_POST_PURCHASE_ID"
)

SALT = Secret("env", "SALT", "airflow", "SALT")

SALT_EMAIL = Secret("env", "SALT_EMAIL", "airflow", "SALT_EMAIL")

SALT_IP = Secret("env", "SALT_IP", "airflow", "SALT_IP")

SALT_NAME = Secret("env", "SALT_NAME", "airflow", "SALT_NAME")

SALT_PASSWORD = Secret("env", "SALT_PASSWORD", "airflow", "SALT_PASSWORD")

DISCOURSE_API_TOKEN = Secret(
    "env", "DISCOURSE_API_TOKEN", "airflow", "DISCOURSE_API_TOKEN"
)

#Zuora Revenue 
ZUORA_REVENUE_GCS_NAME=Secret(
    "env", "ZUORA_REVENUE_GCS_NAME", "airflow", "ZUORA_REVENUE_GCS_NAME"
)
ZUORA_REVENUE_API_URL=Secret(
    "env", "ZUORA_REVENUE_API_URL", "airflow", "ZUORA_REVENUE_API_URL"
)
ZUORA_REVENUE_AUTH_CODE=Secret(
    "env", "ZUORA_REVENUE_AUTH_CODE", "airflow", "ZUORA_REVENUE_AUTH_CODE"
)
