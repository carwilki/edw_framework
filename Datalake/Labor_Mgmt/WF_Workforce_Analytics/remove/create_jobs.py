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
    api_command = "/jobs/create"
    url = f"https://{instance_id}{api_version}{api_command}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.post(url=url, headers=params, data=payload)

    return response.text


# COMMAND ----------


def getJobId(json_response):
    import re

    id = json_response.split(":")[1]
    return re.findall(r"\d+", id)[0]


# COMMAND ----------


def set_permission(payload, job_id):
    api_version = "/api/2.0"
    api_command = f"/permissions/jobs/{job_id}"
    url = f"https://{instance_id}{api_version}{api_command}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.patch(url=url, headers=params, data=payload)

    return response.text


# COMMAND ----------

permission_json = {
    "access_control_list": [
        {
            "group_name": "NZ_Migration_Team",
            "permission_level": "CAN_MANAGE",
        }
    ]
}


# COMMAND ----------


import json

child_json_1 = "job1.json"

with open(child_json_1) as json_file:
    job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = create_job(payload)
print(response)

# COMMAND ----------

WF_Workforce_Analytics_dim_fact_pre = getJobId(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response = set_permission(payload, WF_Workforce_Analytics_dim_fact_pre)
print(response)

# COMMAND ----------

import json

child_json_2 = "job2.json"

with open(child_json_2) as json_file:
    job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = create_job(payload)
print(response)

# COMMAND ----------

WF_Workforce_Analytics_dim_fact_for_next_run = getJobId(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response = set_permission(payload, WF_Workforce_Analytics_dim_fact_for_next_run)
print(response)

# COMMAND ----------

import json

child_json_3 = "job3.json"

with open(child_json_3) as json_file:
    job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = create_job(payload)
print(response)

# COMMAND ----------

WF_Workforce_Analytics_SF = getJobId(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response = set_permission(payload, WF_Workforce_Analytics_SF)
print(response)


# COMMAND ----------

parentJson = f"""
{{
    "run_as": {{
        "user_name": "rsingha@petsmart.com"
    }},
    "name": "WF_Workforce_Analytics",
    "email_notifications": {{
        "no_alert_for_skipped_runs": false
    }},
    "webhook_notifications": {{}},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {{
            "task_key": "WF_Workforce_Analytics_dim_fact_pre",
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {WF_Workforce_Analytics_dim_fact_pre},
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
            "task_key": "WF_Workforce_Analytics_dim_fact_for_next_run",
            "depends_on": [
                {{
                    "task_key": "WF_Workforce_Analytics_dim_fact_pre"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {WF_Workforce_Analytics_dim_fact_for_next_run}
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
            "task_key": "SF_Workforce_Analytics",
            "depends_on": [
                {{
                    "task_key": "WF_Workforce_Analytics_dim_fact_for_next_run"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {WF_Workforce_Analytics_SF}
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
    "format": "MULTI_TASK",
    "tags": {{
        "Department": "NZ-Migration"
    }},
    "run_as": {{"user_name": "rsingha@petsmart.com"}}
}}

"""

# COMMAND ----------

response = create_job(parentJson)
job_id = getJobId(response)
print(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response = set_permission(payload, job_id)
print(response)

# COMMAND ----------


