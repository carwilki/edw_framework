# Databricks notebook source
import requests
import json
import re

token = dbutils.secrets.get(scope="db-token-jobsapi", key="password")
google_service_account = (
    "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com"
)
instance_id = dbutils.secrets.get(scope="db-token-jobsapi", key="instance_id")

# COMMAND ----------


def reset_job(payload):
    api_version = "/api/2.1"
    api_command = "/jobs/reset"
    url = f"https://{instance_id}{api_version}{api_command}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.post(url=url, headers=params, data=payload)

    return response.text


def getJobId(json_response):
    id = json_response.split(":")[1]
    return re.findall(r"\d+", id)[0]


def set_permission(payload, job_id):
    api_version = "/api/2.0"
    api_command = f"/permissions/jobs/{job_id}"
    url = f"https://{instance_id}{api_version}{api_command}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.patch(url=url, headers=params, data=payload)

    return response.text


# list of all the jobs to reset
job_json = [
    "prd_reset_wf_WMS_TO_SCDS_DAILY_A_I.json",
    "prd_reset_wf_WMS_TO_SCDS_DAILY_I_P.json",
    "prd_reset_wf_WMS_TO_SCDS_DAILY_P_Y.json",
]

# static job perms
permission_json = {
    "access_control_list": [
        {
            "group_name": "App_Databricks_DE_Prod_BigDataOperations",
            "permission_level": "CAN_MANAGE",
        },
        {"group_name": "App_Databricks_DE_Prod_Viewer", "permission_level": "CAN_VIEW"},
    ]
}

# Loop through all the jobs to reset
for job in job_json:
    with open(job) as json_file:
        job_payload = json.load(json_file)
        payload = json.dumps(job_payload)

    response = reset_job(payload)
    print(response)
