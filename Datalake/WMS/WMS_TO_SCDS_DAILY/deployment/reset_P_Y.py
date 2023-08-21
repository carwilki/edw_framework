# Databricks notebook source
import requests
import json

token = dbutils.secrets.get(scope="db-token-jobsapi", key="password")
google_service_account = (
    "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com"
)
instance_id = dbutils.secrets.get(scope="db-token-jobsapi", key="instance_id")


# COMMAND ----------
def create_job(payload):
    api_version = "/api/2.1"
    api_command = "/jobs/reset"
    url = f"https://{instance_id}{api_version}{api_command}"
    job_id = 182576057205935
    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    payload = {"job_id": job_id, "new_settings": payload}
    response = requests.post(url=url, headers=params, data=payload)

    return response.text


# COMMAND ----------
with open("./create_wf_WMS_TO_SCDS_DAILY_A_I.json") as file:
    payload = file.read()

# COMMAND ----------
response = create_job(payload)
print(response)
