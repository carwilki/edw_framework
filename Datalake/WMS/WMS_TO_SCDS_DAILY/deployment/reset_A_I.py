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
    api_command = "/jobs/reset"
    url = f"https://{instance_id}{api_version}{api_command}"
    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}
    response = requests.post(url=url, headers=params, data=payload)

    return response.text


# COMMAND ----------
payload = """
{
    "job_id":194887990419371,
    "new_settings":{
    "run_as": {
        "user_name": "gcpdatajobs-shared@petsmart.com"
    },
    "name": "WMS_TO_SCDS_DAILY_A_I",
    "email_notifications": {
        "no_alert_for_skipped_runs": false
    },
    "webhook_notifications": {},
    "timeout_seconds": 0,
    "max_concurrent_runs": 100,
    "tasks": [
        {
            "task_key": "DC_10_PRE_set1",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc10",
                    "prod",
                    "set_A_I_1"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_10_PRE_set2",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc10",
                    "prod",
                    "set_A_I_2"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_12_PRE_set1",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc12",
                    "prod",
                    "set_A_I_1"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_12_PRE_set2",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc12",
                    "prod",
                    "set_A_I_2"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_14_PRE_set1",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc14",
                    "prod",
                    "set_A_I_1"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_14_PRE_set2",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc14",
                    "prod",
                    "set_A_I_2"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_22_PRE_set1",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc22",
                    "prod",
                    "set_A_I_1"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_22_PRE_set2",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc22",
                    "prod",
                    "set_A_I_2"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_36_PRE_set1",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc36",
                    "prod",
                    "set_A_I_1"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_36_PRE_set2",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc36",
                    "prod",
                    "set_A_I_2"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_38_PRE_set1",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc38",
                    "prod",
                    "set_A_I_1"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_38_PRE_set2",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc38",
                    "prod",
                    "set_A_I_2"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_41_PRE_set1",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc41",
                    "prod",
                    "set_A_I_1"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "DC_41_PRE_set2",
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                "parameters": [
                    "dc41",
                    "prod",
                    "set_A_I_2"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 30000,
            "retry_on_timeout": false,
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "s_WM_Asn",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Asn.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Asn_Detail",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Asn_Detail.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Asn_Detail_Status",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Asn_Detail_Status.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Asn_Status",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Asn_Status.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Business_Partner",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Business_Partner.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_C_Leader_Audit",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_C_Leader_Audit.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_C_Tms_Plan",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_C_TMS_Plan.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Carrier_Code",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Carrier_Code.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Commodity_Code",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Commodity_Code.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Do_Status",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Do_Status.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Dock_Door",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Dock_Door.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Act",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Act.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Act_Elm",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Act_Elm.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Act_Elm_Crit",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Act_Elm_Crit.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Aud_Log",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Aud_Log.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Crit_Val",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Crit_Val.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Elm",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Elm.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Elm_Crit",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Elm_Crit.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Emp_Dtl",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Emp_Dtl.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Emp_Stat_Code",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Emp_Stat_Code.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Evnt_Smry_Hdr",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_Wm_E_Evnt_Smry_Hdr.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Job_Function",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_Wm_E_Job_Function.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Labor_Type_Code",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Labor_Type_Code.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Msrmnt",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Msrmnt.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Msrmnt_Rule",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Msrmnt_Rule.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Msrmnt_Rule_Calc",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Msrmnt_Rule_Calc.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Msrmnt_Rule_Condition",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Msrmnt_Rule_Condition.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_E_Shift",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_E_Shift.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Equipment",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Equipment.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Equipment_Instance",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Equipment_Instance.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Facility",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Facility.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Ilm_Appointment_Objects",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Ilm_Appointment_Objects.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Ilm_Appointment_Status",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Ilm_Appointment_Status.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Ilm_Appointment_Type",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Ilm_Appointment_Type.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Ilm_Appointments",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_Wm_Ilm_Appointments.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "s_WM_Ilm_Appt_Equipments",
            "depends_on": [
                {
                    "task_key": "DC_22_PRE_set1"
                },
                {
                    "task_key": "DC_12_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set1"
                },
                {
                    "task_key": "DC_14_PRE_set1"
                },
                {
                    "task_key": "DC_36_PRE_set1"
                },
                {
                    "task_key": "DC_10_PRE_set2"
                },
                {
                    "task_key": "DC_12_PRE_set2"
                },
                {
                    "task_key": "DC_14_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set1"
                },
                {
                    "task_key": "DC_38_PRE_set2"
                },
                {
                    "task_key": "DC_36_PRE_set2"
                },
                {
                    "task_key": "DC_41_PRE_set2"
                },
                {
                    "task_key": "DC_22_PRE_set2"
                },
                {
                    "task_key": "DC_38_PRE_set1"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Ilm_Appt_Equipments.py",
                "parameters": [
                    "prod"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "max_retries": 3,
            "min_retry_interval_millis": 2000,
            "retry_on_timeout": false,
            "timeout_seconds": 86400,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            },
            "description": ""
        },
        {
            "task_key": "SF_Asn",
            "depends_on": [
                {
                    "task_key": "s_WM_Asn"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_ASN",
                    "LOCATION_ID,WM_ASN_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_Asn_Detail",
            "depends_on": [
                {
                    "task_key": "s_WM_Asn_Detail"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_ASN_DETAIL",
                    "LOCATION_ID,WM_ASN_DETAIL_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_Asn_Status",
            "depends_on": [
                {
                    "task_key": "s_WM_Asn_Status"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_ASN_STATUS",
                    "LOCATION_ID,WM_ASN_STATUS",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_Business_Partner",
            "depends_on": [
                {
                    "task_key": "s_WM_Business_Partner"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_BUSINESS_PARTNER",
                    "LOCATION_ID,WM_BUSINESS_PARTNER_ID,WM_TC_COMPANY_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_C_Leader_Audit",
            "depends_on": [
                {
                    "task_key": "s_WM_C_Leader_Audit"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_C_LEADER_AUDIT",
                    "LOCATION_ID,WM_C_LEADER_AUDIT_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_C_TMS_Plan",
            "depends_on": [
                {
                    "task_key": "s_WM_C_Tms_Plan"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_C_TMS_PLAN",
                    "LOCATION_ID,WM_C_TMS_PLAN_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_Commodity_Code",
            "depends_on": [
                {
                    "task_key": "s_WM_Commodity_Code"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_COMMODITY_CODE",
                    "LOCATION_ID,WM_COMMODITY_CD_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_WM_Do_Status",
            "depends_on": [
                {
                    "task_key": "s_WM_Do_Status"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_DO_STATUS",
                    "LOCATION_ID,WM_ORDER_STATUS_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_Dock_Door",
            "depends_on": [
                {
                    "task_key": "s_WM_Dock_Door"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_DOCK_DOOR",
                    "LOCATION_ID,WM_DOCK_DOOR_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Act",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Act"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_ACT",
                    "LOCATION_ID,WM_ACT_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Act_Elm",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Act_Elm"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_ACT_ELM",
                    "LOCATION_ID,WM_ACT_ID,WM_ELM_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Act_Elm_Crit",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Act_Elm_Crit"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_ACT_ELM_CRIT",
                    "LOCATION_ID,WM_ACT_ID,WM_ELM_ID,WM_CRIT_ID,WM_CRIT_VAL_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Aud_Log",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Aud_Log"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_AUD_LOG",
                    "LOCATION_ID,WM_AUD_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Crit_Val",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Crit_Val"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_CRIT_VAL",
                    "LOCATION_ID,WM_CRIT_ID,WM_CRIT_VAL_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Elm",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Elm"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_ELM",
                    "LOCATION_ID,WM_ELM_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Emp_Dtl",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Emp_Dtl"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_EMP_DTL",
                    "LOCATION_ID,WM_EMP_DTL_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_WM_E_Emp_Stat_Code",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Emp_Stat_Code"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_EMP_STAT_CODE",
                    "LOCATION_ID,WM_EMP_STAT_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Evnt_Smry_Hdr",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Evnt_Smry_Hdr"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_EVNT_SMRY_HDR",
                    "LOCATION_ID,WM_ELS_TRAN_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Job_Function",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Job_Function"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_JOB_FUNCTION",
                    "LOCATION_ID,WM_JOB_FUNCTION_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Labor_Type_Code",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Labor_Type_Code"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_LABOR_TYPE_CODE",
                    "LOCATION_ID,WM_LABOR_TYPE_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Msrmnt",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Msrmnt"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_MSRMNT",
                    "LOCATION_ID,WM_MSRMNT_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Msrmnt_Rule",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Msrmnt_Rule"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_MSRMNT_RULE",
                    "LOCATION_ID,WM_MSRMNT_ID,WM_RULE_NBR",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Msrmnt_Rule_Calc",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Msrmnt_Rule_Calc"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_MSRMNT_RULE_CALC",
                    "LOCATION_ID,WM_MSRMNT_ID,WM_RULE_NBR,CALC_SEQ_NBR",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Msrmnt_Rule_Condition",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Msrmnt_Rule_Condition"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_MSRMNT_RULE_CONDITION",
                    "LOCATION_ID,WM_MSRMNT_ID,WM_RULE_NBR,RULE_SEQ_NBR",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_E_Shift",
            "depends_on": [
                {
                    "task_key": "s_WM_E_Shift"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_E_SHIFT",
                    "LOCATION_ID,WM_SHIFT_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        },
        {
            "task_key": "SF_Facility",
            "depends_on": [
                {
                    "task_key": "s_WM_Facility"
                }
            ],
            "run_if": "ALL_SUCCESS",
            "spark_python_task": {
                "python_file": "Datalake/utils/SF_Ingest_v2.py",
                "parameters": [
                    "prod",
                    "WM_FACILITY",
                    "LOCATION_ID,WM_FACILITY_ID",
                    "UPDATE_TSTMP,LOAD_TSTMP"
                ],
                "source": "GIT"
            },
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "libraries": [
                {
                    "maven": {
                        "coordinates": "com.oracle.ojdbc:ojdbc8:19.3.0.0"
                    }
                },
                {
                    "pypi": {
                        "package": "deepdiff"
                    }
                },
                {
                    "pypi": {
                        "package": "retry"
                    }
                }
            ],
            "timeout_seconds": 0,
            "email_notifications": {},
            "notification_settings": {
                "no_alert_for_skipped_runs": false,
                "no_alert_for_canceled_runs": false,
                "alert_on_last_attempt": false
            }
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "wms_to_scds_daily_cluster",
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "13.0.x-scala2.12",
                "spark_conf": {
                    "spark.driver.maxResultSize": "64g",
                    "spark.databricks.delta.preview.enabled": "true",
                    "spark.sql.hive.metastore.jars": "maven",
                    "spark.hadoop.javax.jdo.option.ConnectionDriverName": "org.mariadb.jdbc.Driver",
                    "spark.hadoop.javax.jdo.option.ConnectionPassword": "{{secrets/metastore/password}}",
                    "spark.hadoop.javax.jdo.option.ConnectionURL": "{{secrets/metastore/connectionuri}}",
                    "spark.hadoop.javax.jdo.option.ConnectionUserName": "{{secrets/metastore/userid}}",
                    "spark.sql.hive.metastore.version": "3.1.0"
                },
                "gcp_attributes": {
                    "use_preemptible_executors": false,
                    "google_service_account": "petm-bdpl-bricksengprd-p-sa@petm-prj-bricksengprd-p-2f96.iam.gserviceaccount.com",
                    "availability": "ON_DEMAND_GCP",
                    "zone_id": "auto"
                },
                "node_type_id": "n2-standard-8",
                "driver_node_type_id": "n2-standard-8",
                "custom_tags": {
                    "Department": "Netezza-Migration"
                },
                "enable_elastic_disk": false,
                "num_workers": 8
            }
        }
    ],
    "git_source": {
        "git_url": "https://github.ssg.petsmart.com/BigData-AdvancedAnalytics/nz-databricks-migration.git",
        "git_provider": "gitHubEnterprise",
        "git_branch": "DNM-5882"
    },
    "tags": {
        "Department": "Netezza-Migration"
    },
    "format": "MULTI_TASK"
}
}"""

# COMMAND ----------
response = create_job(payload)
print(response)
