# Databricks notebook source
import requests
import json
token = dbutils.secrets.get(scope = "db-token-jobsapi", key = "password")
google_service_account = "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com"
instance_id = dbutils.secrets.get(scope = "db-token-jobsapi", key = "instance_id")


# COMMAND ----------

def reset_job(payload):
  api_version = '/api/2.1'
  api_command = '/jobs/reset'
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

# COMMAND ----------

import json
child_json_2 = 'WF_Workforce_Analytics_dim_fact_for_next_run_json2_reset.json'

with open(child_json_2) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = reset_job(payload)
print(response)

# COMMAND ----------

import json
child_json_3 = 'WF_Workforce_Analytics_SF_json3_reset.json'

with open(child_json_3) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = reset_job(payload)
print(response)