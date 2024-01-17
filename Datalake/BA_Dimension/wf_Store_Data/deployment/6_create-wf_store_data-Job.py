# Databricks notebook source
import requests
import json
from Datalake.utils import secrets
from Datalake.utils.files.driver_installer import (
    get_file_driver_payload,
    get_file_driver_cluster_payload,
)

token = secrets.get(scope="db-token-jobsapi", key="password")
google_service_account = (
    "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com"
)
instance_id = secrets.get(scope="db-token-jobsapi", key="instance_id")


def create_job(payload):
    api_version = "/api/2.1"
    api_command = "/jobs/create"
    url = f"https://{instance_id}{api_version}{api_command}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.post(url=url, headers=params, data=payload)

    return response.text


def create_cluster(payload):
    path = "/api/2.0/clusters/create"
    url = f"https://{instance_id}{path}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.post(url=url, headers=params, data=payload)

    return response.text


def getJobId(json_response):
    json_response = json.loads(json_response)
    return json_response.id


def set_permission(payload, job_id):
    api_version = "/api/2.0"
    api_command = f"/permissions/jobs/{job_id}"
    url = f"https://{instance_id}{api_version}{api_command}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.patch(url=url, headers=params, data=payload)

    return response.text


# COMMAND ----------
# create the file driver cluster to run the file utils on
# this is required for the file utils to execute.
create_cluster(get_file_driver_cluster_payload())

job_json = "prod_wf_store_data.json"
# create the workflow first. we need the id of the job so we can set the permissions
# and set up the file driver
with open(job_json) as json_file:
    job_payload = json.load(json_file)

payload = json.dumps(job_payload)
response = create_job(payload)

job_id = getJobId(response)

# set up the file driver parmeters
env = "prod"
pf = "wf_store_data"
dc = "FileDriverCluster"
rau = "gcpdatajobs-shared@petsmart.com"
to = "2h"
# creat the file driver job
file_driver = get_file_driver_payload(env, job_id, pf, dc, rau, to)
driver_id = getJobId(create_cluster(file_driver))

permission_json = {
    "access_control_list": [
        {
            "group_name": "App_Databricks_DE_Prod_BigDataOperations",
            "permission_level": "CAN_MANAGE",
        },
        {"group_name": "App_Databricks_DE_Prod_Viewer", "permission_level": "CAN_VIEW"},
    ]
}

payload = json.dumps(permission_json)
# set the permissions on the workflow
response = set_permission(payload, job_id)
print(response)
# set the permissions on the file driver
response = set_permission(payload, file_driver)
print(response)
