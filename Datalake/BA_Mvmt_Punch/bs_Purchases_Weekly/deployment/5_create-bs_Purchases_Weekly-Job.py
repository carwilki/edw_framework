# Databricks notebook source
import requests
import json
token = dbutils.secrets.get(scope = "db-token-jobsapi", key = "password")
google_service_account = "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com"
instance_id = dbutils.secrets.get(scope = "db-token-jobsapi", key = "instance_id")

# COMMAND ----------

def create_job(payload):
  api_version = '/api/2.1'
  api_command = '/jobs/create'
  url = f"https://{instance_id}{api_version}{api_command}"

  params = {
    "Authorization" : "Bearer " + token,
    "Content-Type" : "application/json"
  }

  response = requests.post(
    url = url,
    headers = params,
    data = payload
  )

  return response.text

def getJobId(json_response):
  import re
  id=json_response.split(":")[1]
  return re.findall(r'\d+',id )[0]

# COMMAND ----------

import json
job_json = 'bs_Purchases_Weekly_deployment.json'

with open(job_json) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)


# COMMAND ----------

response = create_job(payload)
job_id=getJobId(response)
print(response)

# COMMAND ----------

def set_permission(payload,job_id):
  api_version = '/api/2.0'
  api_command = f'/permissions/jobs/{job_id}'
  url = f"https://{instance_id}{api_version}{api_command}"

  params = {
    "Authorization" : "Bearer " + token,
    "Content-Type" : "application/json"
  }

  response = requests.patch(
    url = url,
    headers = params,
    data = payload
  )

  return response.text





# COMMAND ----------

permission_json= {
      "access_control_list": [
        {
          "group_name": "App_Databricks_DE_Prod_BigDataOperations",
          "permission_level": "CAN_MANAGE"
        },
        {
          "group_name": "App_Databricks_DE_Prod_Viewer",
          "permission_level": "CAN_VIEW"
        }
      ]
    }


# COMMAND ----------



payload = json.dumps(permission_json)

response=set_permission(payload,job_id)
print(response)
