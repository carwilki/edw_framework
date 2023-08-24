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
    "job_id":68695068834633,
    "new_settings": {
        "run_as": {
            "user_name": "gcpdatajobs-shared@petsmart.com"
        },
        "name": "WMS_TO_SCDS_DAILY_I_P",
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
                        "set_I_P_1"
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
                        "set_I_P_2"
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
                        "set_I_P_1"
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
                        "set_I_P_2"
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
                "min_retry_interval_millis": 30,
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
                        "set_I_P_1"
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
                        "set_I_P_2"
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
                        "set_I_P_1"
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
                        "set_I_P_2"
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
                        "set_I_P_1"
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
                "task_key": "DC_36_PRE_set2",
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/WL_SCDS_STG_PRE_LOAD_DC.py",
                    "parameters": [
                        "dc36",
                        "prod",
                        "set_I_P_2"
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
                        "set_I_P_1"
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
                        "set_I_P_2"
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
                        "set_I_P_1"
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
                        "set_I_P_2"
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
                "task_key": "s_WM_ILM_TASK_STATUS",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Ilm_Task_Status.py",
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
                "task_key": "s_WM_Ilm_Yard_Activity",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Ilm_Yard_Activity.py",
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
                "task_key": "s_WM_Item_Cbo",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Item_Cbo.py",
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
                "task_key": "s_WM_Item_Facility_Mapping_Wms",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Item_Facility_Mapping_Wms.py",
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
                "task_key": "s_WM_Item_Facility_Slotting",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Item_Facility_Slotting.py",
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
                "task_key": "s_WM_Item_Group_Wms",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Item_Group_Wms.py",
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
                "task_key": "s_WM_Item_Package_Cbo",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Item_Package_Cbo.py",
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
                "task_key": "s_WM_Item_Wms",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Item_Wms.py",
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
                "task_key": "s_WM_Labor_Activity",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Labor_Activity.py",
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
                "task_key": "s_WM_Labor_Criteria",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Labor_Criteria.py",
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
                "task_key": "s_WM_Labor_Msg",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Labor_Msg.py",
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
                "task_key": "s_WM_Labor_Msg_Crit",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Labor_Msg_Crit.py",
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
                "task_key": "s_WM_Labor_Msg_Dtl",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Labor_Msg_Dtl.py",
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
                "task_key": "s_WM_Labor_Msg_Dtl_Crit",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Labor_Msg_Dtl_Crit.py",
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
                "task_key": "s_WM_Labor_Tran_Dtl_Crit",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Labor_Tran_Dtl_Crit.py",
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
                "task_key": "s_WM_Locn_Grp",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Locn_Grp.py",
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
                "task_key": "s_WM_Locn_Hdr",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Locn_Hdr.py",
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
                "task_key": "s_WM_Lpn",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Lpn.py",
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
                "task_key": "s_WM_Lpn_Audit_Results",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Lpn_Audit_Results.py",
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
                "task_key": "s_WM_Lpn_Detail",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Lpn_Detail.py",
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
                "task_key": "s_WM_Lpn_Facility_Status",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Lpn_Facility_Status.py",
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
                "task_key": "s_WM_Lpn_Lock",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Lpn_Lock.py",
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
                "task_key": "s_WM_Lpn_Size_Type",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Lpn_Size_Type.py",
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
                "task_key": "s_WM_Lpn_Status",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Lpn_Status.py",
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
                "task_key": "s_WM_Lpn_Type",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Lpn_Type.py",
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
                "task_key": "s_WM_Order_Line_Item",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Order_Line_Item.py",
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
                "task_key": "s_WM_Order_Status",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Order_Status.py",
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
                "task_key": "s_WM_Orders",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Orders.py",
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
                "task_key": "s_WM_Outpt_Lpn_Detail",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Outpt_Lpn_Detail.py",
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
                "task_key": "s_WM_Outpt_Order_Line_Item",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Outpt_Order_Line_Item.py",
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
                "task_key": "s_WM_Output_Orders",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Output_Orders.py",
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
                "task_key": "s_WM_Pick_Locn_Dtl",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Pick_Locn_Dtl.py",
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
                "task_key": "s_WM_Pick_Locn_Dtl_Slotting",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Pick_Locn_Dtl_Slotting.py",
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
                "task_key": "s_WM_Pick_Locn_Hdr",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Pick_Locn_Hdr.py",
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
                "task_key": "s_WM_Pick_Locn_Hdr_Slotting",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Pick_Locn_Hdr_Slotting.py",
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
                "task_key": "s_WM_Outpt_Lpn",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Outpt_Lpn.py",
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
                "task_key": "SF_Order_Line_Item",
                "depends_on": [
                    {
                        "task_key": "s_WM_Order_Line_Item"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_ORDER_LINE_ITEM",
                        "LOCATION_ID,WM_ORDER_ID,WM_LINE_ITEM_ID",
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
                "task_key": "SF_Order_Status",
                "depends_on": [
                    {
                        "task_key": "s_WM_Order_Status"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_ORDER_STATUS",
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
                "task_key": "SF_Orders",
                "depends_on": [
                    {
                        "task_key": "s_WM_Orders"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_ORDERS",
                        "LOCATION_ID,WM_ORDER_ID",
                        "LOAD_TSTMP"
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
                "task_key": "SF_Output_Lpn",
                "depends_on": [
                    {
                        "task_key": "s_WM_Outpt_Lpn"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_OUTPT_LPN",
                        "LOCATION_ID,WM_OUTPT_LPN_ID",
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
                "task_key": "SF_Output_Lpn_Detail",
                "depends_on": [
                    {
                        "task_key": "s_WM_Outpt_Lpn_Detail"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_OUTPT_LPN_DETAIL",
                        "LOCATION_ID,WM_OUTPT_LPN_DETAIL_ID",
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
                "task_key": "SF_Pick_locn_Dtl",
                "depends_on": [
                    {
                        "task_key": "s_WM_Pick_Locn_Dtl"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_PICK_LOCN_DTL",
                        "LOCATION_ID,WM_PICK_LOCN_DTL_ID",
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
                "task_key": "SF_Pick_locn_Hdr",
                "depends_on": [
                    {
                        "task_key": "s_WM_Pick_Locn_Hdr"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_PICK_LOCN_HDR",
                        "LOCATION_ID,WM_PICK_LOCN_HDR_ID",
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
                "task_key": "SF_Pick_Locn_Hdr_Slotting",
                "depends_on": [
                    {
                        "task_key": "s_WM_Pick_Locn_Hdr_Slotting"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_PICK_LOCN_HDR_SLOTTING",
                        "LOCATION_ID,WM_PICK_LOCN_HDR_ID",
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
                "task_key": "SF_Item_Cbo",
                "depends_on": [
                    {
                        "task_key": "s_WM_Item_Cbo"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_ITEM_CBO",
                        "LOCATION_ID,WM_ITEM_ID",
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
                "task_key": "SF_Item_Facility_Mapping_Wms",
                "depends_on": [
                    {
                        "task_key": "s_WM_Item_Facility_Mapping_Wms"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_ITEM_FACILITY_MAPPING_WMS",
                        "LOCATION_ID,WM_ITEM_FACILITY_MAPPING_ID",
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
                "task_key": "SF_Item_Package_Cbo",
                "depends_on": [
                    {
                        "task_key": "s_WM_Item_Package_Cbo"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_ITEM_PACKAGE_CBO",
                        "LOCATION_ID,WM_ITEM_PACKAGE_ID",
                        "LOAD_TSTMP"
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
                "task_key": "SF_Item_WMS",
                "depends_on": [
                    {
                        "task_key": "s_WM_Item_Wms"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_ITEM_WMS",
                        "LOCATION_ID,WM_ITEM_ID",
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
                "task_key": "SF_Labor_Activity",
                "depends_on": [
                    {
                        "task_key": "s_WM_Labor_Activity"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LABOR_ACTIVITY",
                        "LOCATION_ID,WM_LABOR_ACTIVITY_ID",
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
                "task_key": "SF_Labor_Criteria",
                "depends_on": [
                    {
                        "task_key": "s_WM_Labor_Criteria"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LABOR_CRITERIA",
                        "LOCATION_ID,WM_CRIT_ID",
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
                "task_key": "SF_Labor_Msg",
                "depends_on": [
                    {
                        "task_key": "s_WM_Labor_Msg"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LABOR_MSG",
                        "LOCATION_ID,WM_LABOR_MSG_ID",
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
                "task_key": "SF_Labor_Msg_Dtl",
                "depends_on": [
                    {
                        "task_key": "s_WM_Labor_Msg_Dtl"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LABOR_MSG_DTL",
                        "LOCATION_ID,WM_LABOR_MSG_DTL_ID",
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
                "task_key": "SF_Labor_Msg_Dtl_Crit",
                "depends_on": [
                    {
                        "task_key": "s_WM_Labor_Msg_Dtl_Crit"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LABOR_MSG_DTL_CRIT",
                        "LOCATION_ID,WM_LABOR_MSG_DTL_CRIT_ID",
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
                "task_key": "SF_Locn_Hdr",
                "depends_on": [
                    {
                        "task_key": "s_WM_Locn_Hdr"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LOCN_HDR",
                        "LOCATION_ID,WM_LOCN_HDR_ID",
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
                "task_key": "SF_Lpn",
                "depends_on": [
                    {
                        "task_key": "s_WM_Lpn"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LPN",
                        "LOCATION_ID,WM_LPN_ID",
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
                "task_key": "SF_lpn_Audit_Results",
                "depends_on": [
                    {
                        "task_key": "s_WM_Lpn_Audit_Results"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LPN_AUDIT_RESULTS",
                        "LOCATION_ID,WM_LPN_AUDIT_RESULTS_ID",
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
                "task_key": "SF_Lpn_Detail",
                "depends_on": [
                    {
                        "task_key": "s_WM_Lpn_Detail"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LPN_DETAIL",
                        "LOCATION_ID,WM_LPN_ID,WM_LPN_DETAIL_ID",
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
                "task_key": "SF_Lpn_Facility_Status",
                "depends_on": [
                    {
                        "task_key": "s_WM_Lpn_Facility_Status"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LPN_FACILITY_STATUS",
                        "LOCATION_ID,WM_LPN_FACILITY_STATUS,INBOUND_OUTBOUND_IND",
                        "LOAD_TSTMP"
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
                "task_key": "SF_Lpn_Lock",
                "depends_on": [
                    {
                        "task_key": "s_WM_Lpn_Lock"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LPN_LOCK",
                        "LOCATION_ID,WM_LPN_LOCK_ID",
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
                "task_key": "SF_Lpn_Size_Type",
                "depends_on": [
                    {
                        "task_key": "s_WM_Lpn_Size_Type"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LPN_SIZE_TYPE",
                        "LOCATION_ID,WM_LPN_SIZE_TYPE_ID",
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
                "task_key": "SF_Lpn_Status",
                "depends_on": [
                    {
                        "task_key": "s_WM_Lpn_Status"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LPN_STATUS",
                        "LOCATION_ID,WM_LPN_STATUS",
                        "LOAD_TSTMP"
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
                "task_key": "SF_Lpn_Type",
                "depends_on": [
                    {
                        "task_key": "s_WM_Lpn_Type"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_LPN_TYPE",
                        "LOCATION_ID,WM_LPN_TYPE",
                        "LOAD_TSTMP"
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
}
"""

# COMMAND ----------
response = create_job(payload)
print(response)
