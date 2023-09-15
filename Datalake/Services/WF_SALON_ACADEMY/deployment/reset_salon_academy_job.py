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
job_json = 'wf_salon_academy_job_reset.json'

with open(job_json) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

#print(payload)

# COMMAND ----------

response = reset_job(payload)
print(response)

