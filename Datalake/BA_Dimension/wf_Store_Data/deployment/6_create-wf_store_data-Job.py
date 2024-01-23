# Databricks notebook source
import requests
import json
from Datalake.utils import secrets
from Datalake.utils.files.driver_installer import (
    get_file_driver_payload,
    get_file_driver_cluster_payload,
)

from databricks.sdk import WorkspaceClient

token = secrets.get(scope="db-token-jobsapi", key="password")
google_service_account = (
    "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com"
)
instance_id = secrets.get(scope="db-token-jobsapi", key="instance_id")
ws = WorkspaceClient(host=f"https://{instance_id}", token=token)


def create_job(payload):
    api_version = "/api/2.1"
    api_command = "/jobs/create"
    url = f"https://{instance_id}{api_version}{api_command}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.post(url=url, headers=params, data=payload)

    if response.status_code == 200:
        return response
    else:
        raise Exception(response.text)

def create_cluster(payload) -> str | None:
    path = "/api/2.0/clusters/create"
    url = f"https://{instance_id}{path}"
    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.post(url=url, headers=params, data=payload)

    if response.status_code == 200:
        return response.json()["cluster_id"]
    else:
        raise Exception(response.text)


def getJobId(json_response):
    return json_response["job_id"]


def set_permission(payload, job_id):
    api_version = "/api/2.0"
    api_command = f"/permissions/jobs/{job_id}"
    url = f"https://{instance_id}{api_version}{api_command}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.patch(url=url, headers=params, data=payload)

    if response.status_code == 200:
        return response
    else:
        raise Exception(response.text)


def file_driver_cluster_exists(dc) -> str | None:
    clusters = ws.clusters.list()
    for c in clusters:
        if c.cluster_name == dc:
            return c.cluster_id

    return None


# COMMAND ----------

job_json = "prod_wf_store_data.json"
env = "prod"
dc = "FileDriverCluster"
pf = "wf_store_data"
name = f"{pf}_driver"
dc_id = None
rau = "gcpdatajobs-shared@petsmart.com"
to = "2h"
driver_sa = "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com"
# create the file driver cluster to run the file utils on
# this is required for the file utils to execute.
# Check first to see if the file driver cluster already exists.
dc_id = file_driver_cluster_exists(dc)
if dc_id is None:
    print(f"Creating file driver cluster: {dc}")
    dc_id = create_cluster(get_file_driver_cluster_payload(dc,driver_sa))
else:
    print(f"File driver cluster already exists: {dc}")

# create the workflow first. we need the id of the job so we can set the permissions
# and set up the file driver
with open(job_json) as json_file:
    job_payload = json.load(json_file)

payload = json.dumps(job_payload)
print(f"create job payload:{payload}")
response = create_job(payload)
job_id = response.json().get("job_id")
print(f"job_id:{job_id}")
# set up the file driver parmeters

# creat the file driver job
file_driver = get_file_driver_payload(name, env, job_id, pf, dc_id, rau, to)
print(f"file_driver payload:{file_driver}")
dr = create_job(file_driver)
driver_id = dr.json().get("job_id")
print(f"driver_id:{driver_id}")

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
print(response.text)
# set the permissions on the file driver
response = set_permission(payload, driver_id)
print(response.text)
print(
    f"""
      Driver Id: {driver_id}
      Job Id: {job_id}
      Driver Cluster: {dc}
      ############################################
      IMPORTANT::
      The Driver Id is the Id of the job that needs
      to be run in tidal. the driver controls all
      of the file setup.
      ############################################
      """
)
