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

# COMMAND ----------


def getJobId(json_response):
  import re
  id=json_response.split(":")[1]
  return re.findall(r'\d+',id )[0]


# COMMAND ----------

import json
child_json_1 = 'wf_pet_training_job1.json'

with open(child_json_1) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

print(payload)

response = create_job(payload)
print(response)
# COMMAND ----------

wf_pet_training_pre_job_id=getJobId(response)



# COMMAND ----------

child_json_2 = 'wf_pet_training_job2.json'

with open(child_json_2) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

print(payload)

response = create_job(payload)
print(response)
# COMMAND ----------

wf_pet_training_refine_job_id=getJobId(response)



# COMMAND ----------

child_json_3 = 'wf_pet_training_job3.json'

with open(child_json_3) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

print(payload)

response = create_job(payload)
print(response)
# COMMAND ----------

wf_pet_training_snowflake_job_id=getJobId(response)


# COMMAND ----------

parentJson = f"""{{
    "run_as": {{
        "user_name": "gcpdatajobs-shared@petsmart.com"
    }},
    "name": "wf_pet_training",
    "email_notifications": {{
        "no_alert_for_skipped_runs": false
    }},
    "webhook_notifications": {{}},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {{
            "task_key": "wf_pet_training_pre",
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {wf_pet_training_pre_job_id}
            }},
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "wf_pet_training_refine",
            "depends_on": [
                {{
                    "task_key": "wf_pet_training_pre"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {wf_pet_training_refine_job_id}
            }},
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "wf_pet_training_snowflake",
            "depends_on": [
                {{
                    "task_key": "wf_pet_training_refine"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {wf_pet_training_snowflake_job_id}
            }},
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }}
    ],
    "format": "MULTI_TASK"
}}"""
# COMMAND ----------

response = create_job(parentJson)
print(response)
