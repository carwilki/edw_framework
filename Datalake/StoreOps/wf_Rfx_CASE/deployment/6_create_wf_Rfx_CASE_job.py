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


import json
child_json_1 = 'wf_Rfx_CASE_pre_deployment_1.json'

with open(child_json_1) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = create_job(payload)
print(response)

# COMMAND ----------

wf_Rfx_CASE_pre=getJobId(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response=set_permission(payload,wf_Rfx_CASE_pre)
print(response)

# COMMAND ----------

import json
child_json_2 = 'wf_Rfx_CASE_refine_deployment_2.json'

with open(child_json_2) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = create_job(payload)
print(response)

# COMMAND ----------

wf_Rfx_CASE_refine=getJobId(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response=set_permission(payload,wf_Rfx_CASE_refine)
print(response)

# COMMAND ----------

import json
child_json_3 = 'wf_Rfx_CASE_SF_deployment_3.json'

with open(child_json_3) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = create_job(payload)
print(response)

# COMMAND ----------

wf_Rfx_CASE_SF=getJobId(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response=set_permission(payload,wf_Rfx_CASE_SF)
print(response)


# COMMAND ----------

parentJson = f"""
{{
    "run_as": {{
        "user_name": "gcpdatajobs-shared@petsmart.com"
    }},
    "name": "WF_Rfx_CASE",
    "email_notifications": {{
        "no_alert_for_skipped_runs": false
    }},
    "webhook_notifications": {{}},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {{
            "task_key": "wf_Rfx_CASE_Pre",
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {wf_Rfx_CASE_Pre},
                "python_params": [
                    "prod"
                ]
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
            "task_key": "wf_Rfx_CASE_refine",
            "depends_on": [
                {{
                    "task_key": "wf_Rfx_CASE_Pre"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {wf_Rfx_CASE_refine}
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
            "task_key": "wf_Rfx_CASE_SF",
            "depends_on": [
                {{
                    "task_key": "wf_Rfx_CASE_refine"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {wf_Rfx_CASE_SF}
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
}}

"""

# COMMAND ----------

response = create_job(parentJson)
job_id=getJobId(response)
print(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response=set_permission(payload,job_id)
print(response)

# COMMAND ----------


