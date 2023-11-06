from Datalake.utils import secrets


sf_username = secrets.get("databricks_service_account", "username")
sf_password = secrets.get("databricks_service_account", "password")
sf_url = "petsmart.us-central1.gcp.snowflakecomputing.com"
sf_wharehouse = "IT_WH"
sf_prod_role = "ROLE_BIGDATA"
sf_dev_role = "ROLE_DATABRICKS_NONPRD"
