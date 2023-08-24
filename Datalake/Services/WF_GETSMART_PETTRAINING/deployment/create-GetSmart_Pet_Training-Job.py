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

import json
job_json = 'wf_GetSmart_Pet_Training_deployment.json'

with open(job_json) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

#print(payload)

# COMMAND ----------

# payload = """{
#     "run_as": {
#         "user_name": "gcpdatajobs-shared@petsmart.com"
#     },
#     "name": "wf_GetSmart_Pet_Training",
#     "email_notifications": {
#         "no_alert_for_skipped_runs": false
#     },
#     "webhook_notifications": {},
#     "timeout_seconds": 0,
#     "max_concurrent_runs": 1,
#     "tasks": [
#         {
#             "task_key": "s_gs_pt_training_type_pre",
#             "run_if": "ALL_SUCCESS",
#             "spark_python_task": {
#                 "python_file": "Datalake/Services/WF_GETSMART_PETTRAINING/notebooks/m_gs_pt_training_type_pre.py",
#                 "parameters": [
#                     "prod"
#                 ],
#                 "source": "GIT"
#             },
#             "job_cluster_key": "GetSmart_Pet_Training",
#             "libraries": [
#                 {
#                     "pypi": {
#                         "package": "retry"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "deepdiff"
#                     }
#                 },
#                 {
#                     "maven": {
#                         "coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.3.0.jre8-preview"
#                     }
#                 }
#             ],
#             "timeout_seconds": 0,
#             "email_notifications": {},
#             "notification_settings": {
#                 "no_alert_for_skipped_runs": false,
#                 "no_alert_for_canceled_runs": false,
#                 "alert_on_last_attempt": false
#             }
#         },
#         {
#             "task_key": "s_gs_pt_training_pre",
#             "depends_on": [
#                 {
#                     "task_key": "s_gs_pt_training_type_pre"
#                 }
#             ],
#             "run_if": "ALL_SUCCESS",
#             "spark_python_task": {
#                 "python_file": "Datalake/Services/WF_GETSMART_PETTRAINING/notebooks/m_gs_pt_training_pre.py",
#                 "parameters": [
#                     "prod"
#                 ],
#                 "source": "GIT"
#             },
#             "job_cluster_key": "GetSmart_Pet_Training",
#             "libraries": [
#                 {
#                     "maven": {
#                         "coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.3.0.jre8-preview"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "retry"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "deepdiff"
#                     }
#                 }
#             ],
#             "timeout_seconds": 0,
#             "email_notifications": {},
#             "notification_settings": {
#                 "no_alert_for_skipped_runs": false,
#                 "no_alert_for_canceled_runs": false,
#                 "alert_on_last_attempt": false
#             }
#         },
#         {
#             "task_key": "s_gs_pt_travel_pre",
#             "depends_on": [
#                 {
#                     "task_key": "s_gs_pt_training_pre"
#                 }
#             ],
#             "run_if": "ALL_SUCCESS",
#             "spark_python_task": {
#                 "python_file": "Datalake/Services/WF_GETSMART_PETTRAINING/notebooks/m_gs_pt_travel_pre.py",
#                 "parameters": [
#                     "prod"
#                 ],
#                 "source": "GIT"
#             },
#             "job_cluster_key": "GetSmart_Pet_Training",
#             "libraries": [
#                 {
#                     "pypi": {
#                         "package": "deepdiff"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "retry"
#                     }
#                 },
#                 {
#                     "maven": {
#                         "coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.3.0.jre8-preview"
#                     }
#                 }
#             ],
#             "timeout_seconds": 0,
#             "email_notifications": {},
#             "notification_settings": {
#                 "no_alert_for_skipped_runs": false,
#                 "no_alert_for_canceled_runs": false,
#                 "alert_on_last_attempt": false
#             }
#         },
#         {
#             "task_key": "s_gs_pt_history_pre",
#             "depends_on": [
#                 {
#                     "task_key": "s_gs_pt_travel_pre"
#                 }
#             ],
#             "run_if": "ALL_SUCCESS",
#             "spark_python_task": {
#                 "python_file": "Datalake/Services/WF_GETSMART_PETTRAINING/notebooks/m_gs_pt_history_pre.py",
#                 "parameters": [
#                     "prod"
#                 ],
#                 "source": "GIT"
#             },
#             "job_cluster_key": "GetSmart_Pet_Training",
#             "libraries": [
#                 {
#                     "pypi": {
#                         "package": "deepdiff"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "retry"
#                     }
#                 },
#                 {
#                     "maven": {
#                         "coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.3.0.jre8-preview"
#                     }
#                 }
#             ],
#             "timeout_seconds": 0,
#             "email_notifications": {},
#             "notification_settings": {
#                 "no_alert_for_skipped_runs": false,
#                 "no_alert_for_canceled_runs": false,
#                 "alert_on_last_attempt": false
#             }
#         },
#         {
#             "task_key": "s_gs_pt_training_type",
#             "depends_on": [
#                 {
#                     "task_key": "s_gs_pt_history_pre"
#                 }
#             ],
#             "run_if": "ALL_SUCCESS",
#             "spark_python_task": {
#                 "python_file": "Datalake/Services/WF_GETSMART_PETTRAINING/notebooks/m_gs_pt_training_type.py",
#                 "parameters": [
#                     "prod"
#                 ],
#                 "source": "GIT"
#             },
#             "job_cluster_key": "GetSmart_Pet_Training",
#             "libraries": [
#                 {
#                     "pypi": {
#                         "package": "retry"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "deepdiff"
#                     }
#                 },
#                 {
#                     "maven": {
#                         "coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.3.0.jre8-preview"
#                     }
#                 }
#             ],
#             "timeout_seconds": 0,
#             "email_notifications": {},
#             "notification_settings": {
#                 "no_alert_for_skipped_runs": false,
#                 "no_alert_for_canceled_runs": false,
#                 "alert_on_last_attempt": false
#             }
#         },
#         {
#             "task_key": "s_gs_pt_training",
#             "depends_on": [
#                 {
#                     "task_key": "s_gs_pt_training_type"
#                 }
#             ],
#             "run_if": "ALL_SUCCESS",
#             "spark_python_task": {
#                 "python_file": "Datalake/Services/WF_GETSMART_PETTRAINING/notebooks/m_gs_pt_training.py",
#                 "parameters": [
#                     "prod"
#                 ],
#                 "source": "GIT"
#             },
#             "job_cluster_key": "GetSmart_Pet_Training",
#             "libraries": [
#                 {
#                     "maven": {
#                         "coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.3.0.jre8-preview"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "retry"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "deepdiff"
#                     }
#                 }
#             ],
#             "timeout_seconds": 0,
#             "email_notifications": {},
#             "notification_settings": {
#                 "no_alert_for_skipped_runs": false,
#                 "no_alert_for_canceled_runs": false,
#                 "alert_on_last_attempt": false
#             }
#         },
#         {
#             "task_key": "s_gs_pt_travel",
#             "depends_on": [
#                 {
#                     "task_key": "s_gs_pt_training"
#                 }
#             ],
#             "run_if": "ALL_SUCCESS",
#             "spark_python_task": {
#                 "python_file": "Datalake/Services/WF_GETSMART_PETTRAINING/notebooks/m_gs_pt_travel.py",
#                 "parameters": [
#                     "prod"
#                 ],
#                 "source": "GIT"
#             },
#             "job_cluster_key": "GetSmart_Pet_Training",
#             "libraries": [
#                 {
#                     "pypi": {
#                         "package": "deepdiff"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "retry"
#                     }
#                 },
#                 {
#                     "maven": {
#                         "coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.3.0.jre8-preview"
#                     }
#                 }
#             ],
#             "timeout_seconds": 0,
#             "email_notifications": {},
#             "notification_settings": {
#                 "no_alert_for_skipped_runs": false,
#                 "no_alert_for_canceled_runs": false,
#                 "alert_on_last_attempt": false
#             }
#         },
#         {
#             "task_key": "s_gs_pt_history",
#             "depends_on": [
#                 {
#                     "task_key": "s_gs_pt_travel"
#                 }
#             ],
#             "run_if": "ALL_SUCCESS",
#             "spark_python_task": {
#                 "python_file": "Datalake/Services/WF_GETSMART_PETTRAINING/notebooks/m_gs_pt_history.py",
#                 "parameters": [
#                     "prod"
#                 ],
#                 "source": "GIT"
#             },
#             "job_cluster_key": "GetSmart_Pet_Training",
#             "libraries": [
#                 {
#                     "pypi": {
#                         "package": "deepdiff"
#                     }
#                 },
#                 {
#                     "pypi": {
#                         "package": "retry"
#                     }
#                 },
#                 {
#                     "maven": {
#                         "coordinates": "com.microsoft.sqlserver:mssql-jdbc:12.3.0.jre8-preview"
#                     }
#                 }
#             ],
#             "timeout_seconds": 0,
#             "email_notifications": {},
#             "notification_settings": {
#                 "no_alert_for_skipped_runs": false,
#                 "no_alert_for_canceled_runs": false,
#                 "alert_on_last_attempt": false
#             }
#         }
#     ],
#     "job_clusters": [
#         {
#             "job_cluster_key": "GetSmart_Pet_Training",
#             "new_cluster": {
#                 "cluster_name": "",
#                 "spark_version": "13.0.x-scala2.12",
#                 "spark_conf": {
# 					"spark.driver.maxResultSize": "64g",
#                     "spark.databricks.delta.preview.enabled": "true",
#                     "spark.sql.hive.metastore.jars": "maven",
#                     "spark.hadoop.javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
#                     "spark.hadoop.javax.jdo.option.ConnectionPassword": "{{secrets/metastore/password}}",
#                     "spark.hadoop.javax.jdo.option.ConnectionURL": "{{secrets/metastore/connectionuri}}",
#                     "spark.hadoop.javax.jdo.option.ConnectionUserName": "{{secrets/metastore/userid}}",
#                     "spark.sql.hive.metastore.version": "3.1.0"
#                 },
#                 "gcp_attributes": {
#                     "use_preemptible_executors": false,
#                     "google_service_account": "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com",
#                     "availability": "ON_DEMAND_GCP",
#                     "zone_id": "auto"
#                 },
#                 "node_type_id": "n2-standard-8",
#                 "driver_node_type_id": "n2-standard-8",
#                 "custom_tags": {
#                     "Department": "Netezza-Migration"
#                 },
#                 "enable_elastic_disk": false,
#                 "num_workers": 8
#             }
#         }
#     ],
#     "git_source": {
#         "git_url": "https://github.ssg.petsmart.com/BigData-AdvancedAnalytics/nz-databricks-migration.git",
#         "git_provider": "gitHubEnterprise",
#         "git_branch": "main"
#     },
#     "tags": {
#         "Department": "Netezza-Migration"
#     },
#     "format": "MULTI_TASK"
# }
# """

# COMMAND ----------

response = create_job(payload)
print(response)
