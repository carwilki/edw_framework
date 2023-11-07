from Datalake.utils import secrets


sf_other_user = secrets.get("SVC_BD_SNOWFLAKE_NP", "username")
sf_other_key = secrets.get("SVC_BD_SNOWFLAKE_NP", "pkey")
sf_prod_user = secrets.get("databricks_service_account", "username")
sf_prod_password = secrets.get("databricks_service_account", "password")
sf_url = "petsmart.us-central1.gcp.snowflakecomputing.com"
sf_warehouse = "IT_WH"
sf_prod_role = "ROLE_BIGDATA"
sf_dev_role = "ROLE_DATABRICKS_NONPRD"
