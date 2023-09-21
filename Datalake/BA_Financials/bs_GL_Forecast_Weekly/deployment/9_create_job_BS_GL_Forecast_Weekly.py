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
child_json_1 = 'SF_BS_GL_Forecast_Weekly_json1.json'

with open(child_json_1) as json_file:
  job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = create_job(payload)
print(response)


# COMMAND ----------

SF_BS_GL_Forecast_Weekly=getJobId(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response=set_permission(payload,SF_BS_GL_Forecast_Weekly)
print(response)

# COMMAND ----------

parentJson = f"""
{{
    "run_as": {{
        "user_name": "gcpdatajobs-shared@petsmart.com"
    }},
    "name": "bs_GL_Forecast_Weekly",
    "email_notifications": {{
        "no_alert_for_skipped_runs": false
    }},
    "webhook_notifications": {{}},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {{
            "task_key": "m_bpc_to_gl_plan_pre_AND_gl_forecast_pre",
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_bpc_to_gl_plan_pre_AND_gl_forecast_pre",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_bpc_to_gl_exchange_rate_FORECAST",
            "depends_on": [
                {{
                    "task_key": "m_bpc_to_gl_plan_pre_AND_gl_forecast_pre"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_bpc_to_gl_exchange_rate_FORECAST",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_gl_profit_ctr_GL_FORECAST_PRE",
            "depends_on": [
                {{
                    "task_key": "m_bpc_to_gl_exchange_rate_FORECAST"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_gl_profit_ctr_GL_FORECAST_PRE",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_gl_forecast_month_pre_SQL",
            "depends_on": [
                {{
                    "task_key": "m_gl_profit_ctr_GL_FORECAST_PRE"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_gl_forecast_month_pre_SQL",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_gl_plan_forecast_month_forecast",
            "depends_on": [
                {{
                    "task_key": "m_gl_forecast_month_pre_SQL"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_gl_plan_forecast_month_forecast",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_bpc_to_gl_exchange_rate_PLAN",
            "depends_on": [
                {{
                    "task_key": "m_gl_plan_forecast_month_forecast"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_bpc_to_gl_exchange_rate_PLAN",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_gl_profit_ctr_GL_PLAN_PRE",
            "depends_on": [
                {{
                    "task_key": "m_bpc_to_gl_exchange_rate_PLAN"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_gl_profit_ctr_GL_PLAN_PRE",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_gl_plan_month_pre_SQL",
            "depends_on": [
                {{
                    "task_key": "m_gl_profit_ctr_GL_PLAN_PRE"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_gl_plan_month_pre_SQL",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_gl_plan_forecast_month_delete",
            "depends_on": [
                {{
                    "task_key": "m_gl_plan_month_pre_SQL"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_gl_plan_forecast_month_delete",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_gl_plan_forecast_month_F1_Adj",
            "depends_on": [
                {{
                    "task_key": "m_gl_plan_forecast_month_delete"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_gl_plan_forecast_month_F1_Adj",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_gl_plan_forecast_month_F1_Adj_closed_store_UPD",
            "depends_on": [
                {{
                    "task_key": "m_gl_plan_forecast_month_F1_Adj"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_gl_plan_forecast_month_F1_Adj_closed_store_UPD",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "m_gl_plan_forecast_day_SQL",
            "depends_on": [
                {{
                    "task_key": "m_gl_plan_forecast_month_F1_Adj_closed_store_UPD"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "notebook_task": {{
                "notebook_path": "Datalake/BA_Financials/bs_GL_Forecast_Weekly/notebooks/m_gl_plan_forecast_day_SQL",
                "base_parameters": {{
                    "env": "prod"
                }},
                "source": "GIT"
            }},
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "libraries": [
                {{
                    "pypi": {{
                        "package": "retry"
                    }}
                }},
                {{
                    "pypi": {{
                        "package": "deepdiff"
                    }}
                }}
            ],
            "timeout_seconds": 0,
            "email_notifications": {{}},
            "notification_settings": {{
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }}
        }},
        {{
            "task_key": "sf_gl_plan_forecast_weekly",
            "depends_on": [
                {{
                    "task_key": "m_gl_plan_forecast_day_SQL"
                }}
            ],
            "run_if": "ALL_SUCCESS",
            "run_job_task": {{
                "job_id": {SF_BS_GL_Forecast_Weekly}
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
    "job_clusters": [
        {{
            "job_cluster_key": "GL_Forecast_Weekly_cluster",
            "new_cluster": {{
                "cluster_name": "",
                "spark_version": "13.0.x-scala2.12",
                "spark_conf": {{
					"spark.driver.maxResultSize": "64g",
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.sql.hive.metastore.jars": "maven",
                    "spark.hadoop.javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
                    "spark.hadoop.javax.jdo.option.ConnectionPassword": "{{{{secrets/metastore/password}}}}",
                    "spark.hadoop.javax.jdo.option.ConnectionURL": "{{{{secrets/metastore/connectionuri}}}}",
                    "spark.hadoop.javax.jdo.option.ConnectionUserName": "{{{{secrets/metastore/userid}}}}",
                    "spark.sql.hive.metastore.version": "3.1.0"
                }},
                "gcp_attributes": {{
                    "use_preemptible_executors": false,
                    "google_service_account": "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com",
                    "availability": "ON_DEMAND_GCP",
                    "zone_id": "auto"
                }},
                "node_type_id": "n2-standard-8",
                "driver_node_type_id": "n2-standard-8",
                "custom_tags": {{
                    "Department": "Netezza-Migration"
                }},
                "enable_elastic_disk": false,
																		
																			   
											 
                "num_workers": 8
            }}
        }}
    ],
    "git_source": {{
        "git_url": "https://github.ssg.petsmart.com/BigData-AdvancedAnalytics/nz-databricks-migration.git",
        "git_provider": "gitHubEnterprise",
        "git_branch": "main"
    }},
    "tags": {{
        "Department": "NZ_Migration"
    }},
    "format": "MULTI_TASK"
}}"""

# COMMAND ----------

response = create_job(parentJson)
job_id=getJobId(response)
print(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response=set_permission(payload,job_id)
print(response)
