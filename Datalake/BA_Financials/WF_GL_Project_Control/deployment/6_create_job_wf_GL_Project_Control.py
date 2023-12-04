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
            "group_name": "App_Databricks_DE_Prod_BigDataOperations",
            "permission_level": "CAN_MANAGE",
        },
        {"group_name": "App_Databricks_DE_Prod_Viewer", "permission_level": "CAN_VIEW"},
    ]
}


# COMMAND ----------


import json

child_json_1 = "wf_gl_project_control_sap_pre_json1.json"

with open(child_json_1) as json_file:
    job_payload = json.load(json_file)

payload = json.dumps(job_payload)

response = create_job(payload)
print(response)

# COMMAND ----------

wf_gl_project_control_sap_pre = getJobId(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response = set_permission(payload, wf_gl_project_control_sap_pre)
print(response)






# COMMAND ----------

parentJson = f"""
{{

  "name": "wf_GL_Project_Control",

  "email_notifications": {{

    "no_alert_for_skipped_runs": false

  }},

  "webhook_notifications": {{}},

  "timeout_seconds": 0,

  "max_concurrent_runs": 1,

  "tasks": [

    {{

      "task_key": "gl_project_control_sap_pre",

      "run_if": "ALL_SUCCESS",

      "run_job_task": {{

        "job_id": {wf_gl_project_control_sap_pre}

      }},

      "timeout_seconds": 0,

      "email_notifications": {{}},

      "notification_settings": {{

        "no_alert_for_skipped_runs": false,

        "no_alert_for_canceled_runs": false,

        "alert_on_last_attempt": false

      }},

      "webhook_notifications": {{}}

    }},

    {{

      "task_key": "s_gl_project_UPD",

      "depends_on": [

        {{

          "task_key": "gl_project_control_sap_pre"

        }}

      ],

      "run_if": "ALL_SUCCESS",

      "notebook_task": {{

        "notebook_path": "Datalake/BA_Financials/WF_GL_Project_Control/notebooks/m_gl_project_UPD",

        "base_parameters": {{

          "env": "prod"

        }},

        "source": "GIT"

      }},

      "job_cluster_key": "GL_Project_Control_Job_cluster",

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

      }},

      "webhook_notifications": {{}}

    }},

    {{

      "task_key": "s_gl_project_INS",

      "depends_on": [

        {{

          "task_key": "s_gl_project_UPD"

        }}

      ],

      "run_if": "ALL_SUCCESS",

      "notebook_task": {{

        "notebook_path": "Datalake/BA_Financials/WF_GL_Project_Control/notebooks/m_gl_project_INS",

        "base_parameters": {{

          "env": "prod"

        }},

        "source": "GIT"

      }},

      "job_cluster_key": "GL_Project_Control_Job_cluster",

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

      }},

      "webhook_notifications": {{}}

    }},

    {{

      "task_key": "s_gl_project_detail",

      "depends_on": [

        {{

          "task_key": "s_gl_project_INS"

        }}

      ],

      "run_if": "ALL_SUCCESS",

      "notebook_task": {{

        "notebook_path": "Datalake/BA_Financials/WF_GL_Project_Control/notebooks/m_gl_project_detail",

        "base_parameters": {{

          "env": "prod"

        }},

        "source": "GIT"

      }},

      "job_cluster_key": "GL_Project_Control_Job_cluster",

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

      }},

      "webhook_notifications": {{}}

    }},

    {{

      "task_key": "s_gl_project_cost_detail",

      "depends_on": [

        {{

          "task_key": "s_gl_project_detail"

        }}

      ],

      "run_if": "ALL_SUCCESS",

      "notebook_task": {{

        "notebook_path": "Datalake/BA_Financials/WF_GL_Project_Control/notebooks/m_gl_project_cost_detail",

        "base_parameters": {{

          "env": "prod"

        }},

        "source": "GIT"

      }},

      "job_cluster_key": "GL_Project_Control_Job_cluster",

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

      }},

      "webhook_notifications": {{}}

    }},

    {{

      "task_key": "s_gl_project_detail_prev_pre",

      "depends_on": [

        {{

          "task_key": "s_gl_project_cost_detail"

        }}

      ],

      "run_if": "ALL_SUCCESS",

      "notebook_task": {{

        "notebook_path": "Datalake/BA_Financials/WF_GL_Project_Control/notebooks/m_gl_project_detail_prev_pre",

        "base_parameters": {{

          "env": "prod"

        }},

        "source": "GIT"

      }},

      "job_cluster_key": "GL_Project_Control_Job_cluster",

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

      }},

      "webhook_notifications": {{}}

    }}

  ],

  "job_clusters": [

    {{

      "job_cluster_key": "GL_Project_Control_Job_cluster",

      "new_cluster": {{

        "cluster_name": "",

        "spark_version": "13.3.x-scala2.12",

        "spark_conf": {{

          "spark.databricks.dataLineage.enabled": "true",

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

        "custom_tags": {{

          "Department": "NZ-Migration"

        }},

        "spark_env_vars": {{

          "PYSPARK_PYTHON": "/databricks/python3/bin/python3",
          "SNOWFLAKE_SPARK_CONNECTOR_VERSION":"2.12"

        }},

        "enable_elastic_disk": false,
        "autoscale": {{

          "min_workers": 2,

          "max_workers": 8

        }}

      }}

    }}

  ],

  "git_source": {{

    "git_url": "https://github.ssg.petsmart.com/BigData-AdvancedAnalytics/nz-databricks-migration.git",

    "git_provider": "gitHubEnterprise",

    "git_branch": "main"

  }},

  "tags": {{

    "Department": "NZ-Migration"

  }},

  "run_as": {{

    "user_name": "gcpdatajobs-shared@petsmart.com"

  }}

}}
"""

# COMMAND ----------





# COMMAND ----------

response = create_job(parentJson)
job_id = getJobId(response)
print(response)

# COMMAND ----------

payload = json.dumps(permission_json)

response = set_permission(payload, job_id)
print(response)

# COMMAND ----------
