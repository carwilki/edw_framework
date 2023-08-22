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
payload ="""
    {
    "job_id":<paste job id to reset>,
    "new_settings": {
        "run_as": {
            "user_name": "gcpdatajobs-shared@petsmart.com"
        },
        "name": "WMS_TO_SCDS_DAILY_P_Y",
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
                        "set_P_Y_1"
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
                        "set_P_Y_2"
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
                        "set_P_Y_1"
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
                        "set_P_Y_2"
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
                        "set_P_Y_1"
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
                        "set_P_Y_2"
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
                        "set_P_Y_1"
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
                        "set_P_Y_2"
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
                        "set_P_Y_1"
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
                        "set_P_Y_2"
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
                        "set_P_Y_1"
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
                        "set_P_Y_2"
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
                        "set_P_Y_1"
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
                        "set_P_Y_2"
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
                "task_key": "s_WM_Picking_Short_Item",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Picking_Short_Item.py",
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
                "task_key": "s_WM_Pix_Tran",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Pix_Tran.py",
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
                "task_key": "s_WM_Product_Class",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Product_Class.py",
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
                "task_key": "s_WM_Purchase_Orders",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Purchase_Orders.py",
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
                "task_key": "s_WM_Purchase_Orders_Line_Item",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Purchase_Orders_Line_Item.py",
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
                "task_key": "s_WM_Purchase_Orders_Line_Status",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Purchase_Orders_Line_Status.py",
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
                "task_key": "s_WM_Purchase_Orders_Status",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Purchase_Orders_Status.py",
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
                "task_key": "s_WM_Putaway_Lock",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Putaway_Lock.py",
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
                "task_key": "s_WM_Rack_Type",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Rack_Type.py",
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
                "task_key": "s_WM_Rack_Type_Level",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Rack_Type_Level.py",
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
                "task_key": "s_WM_Resv_Locn_Hdr",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Resv_Locn_Hdr.py",
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
                "task_key": "s_WM_Sec_User",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Sec_User.py",
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
                "task_key": "s_WM_Ship_Via",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Ship_Via.py",
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
                "task_key": "s_WM_Shipment",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Shipment.py",
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
                "task_key": "s_WM_Shipment_Status",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Shipment_Status.py",
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
                "task_key": "s_WM_Size_Uom",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Size_Uom.py",
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
                "task_key": "s_WM_Slot_Item",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Slot_Item.py",
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
                "task_key": "s_WM_Slot_Item_Score",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Slot_Item_Score.py",
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
                "task_key": "s_WM_Standard_UOM",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Standard_UOM.py",
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
                "task_key": "s_WM_Stop",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Stop.py",
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
                "task_key": "s_WM_Stop_Status",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Stop_Status.py",
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
                "task_key": "s_WM_Sys_Code",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Sys_Code.py",
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
                "task_key": "s_WM_Task_Dtl",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Task_Dtl.py",
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
                "task_key": "s_WM_Task_Hdr",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Task_Hdr.py",
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
                "task_key": "s_WM_Trailer_Contents",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Trailer_Contents.py",
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
                "task_key": "s_WM_Trailer_Ref",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Trailer_Ref.py",
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
                "task_key": "s_WM_Trailer_Type",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Trailer_Type.py",
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
                "task_key": "s_WM_Trailer_Visit",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Trailer_Visit.py",
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
                "task_key": "s_WM_Trailer_Visit_Dtl",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Trailer_Visit_Dtl.py",
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
                "task_key": "s_WM_UN_Number",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_UN_Number.py",
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
                "task_key": "s_WM_User_Profile",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_User_Profile.py",
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
                "task_key": "s_WM_Vend_Perf_Tran",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Vend_Perf_Tran.py",
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
                "task_key": "s_WM_Wave_Parm",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Wave_Parm.py",
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
                "task_key": "s_WM_Yard",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Yard.py",
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
                "task_key": "s_WM_Yard_Zone",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Yard_Zone.py",
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
                "task_key": "s_WM_Yard_Zone_Slot",
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
                    "python_file": "Datalake/WMS/WMS_TO_SCDS_DAILY/notebooks/m_WM_Yard_Zone_Slot.py",
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
                "task_key": "SF_Picking_Short_Item",
                "depends_on": [
                    {
                        "task_key": "s_WM_Picking_Short_Item"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_PICKING_SHORT_ITEM",
                        "LOCATION_ID,WM_PICKING_SHORT_ITEM_ID",
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
                "task_key": "SF_Purchase_Orders",
                "depends_on": [
                    {
                        "task_key": "s_WM_Purchase_Orders"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_PURCHASE_ORDERS",
                        "LOCATION_ID,WM_PURCHASE_ORDERS_ID",
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
                "task_key": "SF_Product_Class",
                "depends_on": [
                    {
                        "task_key": "s_WM_Product_Class"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_PRODUCT_CLASS",
                        "LOCATION_ID,WM_PRODUCT_CLASS_ID",
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
                "task_key": "SF_Purchase_Orders_Line_Item",
                "depends_on": [
                    {
                        "task_key": "s_WM_Purchase_Orders_Line_Item"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_PURCHASE_ORDERS_LINE_ITEM",
                        "LOCATION_ID,WM_PURCHASE_ORDERS_ID,WM_PURCHASE_ORDERS_LINE_ITEM_ID",
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
                "task_key": "SF_Purchase_Orders_Status",
                "depends_on": [
                    {
                        "task_key": "s_WM_Purchase_Orders_Status"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_PURCHASE_ORDERS_STATUS",
                        "WM_PURCHASE_ORDERS_STATUS",
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
                "task_key": "SF_Putaway_Lock",
                "depends_on": [
                    {
                        "task_key": "s_WM_Putaway_Lock"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_PUTAWAY_LOCK",
                        "LOCATION_ID,WM_LOCN_ID",
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
                "task_key": "SF_Rack_Type",
                "depends_on": [
                    {
                        "task_key": "s_WM_Rack_Type"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_RACK_TYPE",
                        "LOCATION_ID,WM_RACK_TYPE_ID",
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
                "task_key": "SF_Rack_Type_Level",
                "depends_on": [
                    {
                        "task_key": "s_WM_Rack_Type_Level"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_RACK_TYPE_LEVEL",
                        "LOCATION_ID,WM_RACK_LEVEL_ID",
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
                "task_key": "SF_Resv_Locn_Hdr",
                "depends_on": [
                    {
                        "task_key": "s_WM_Resv_Locn_Hdr"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_RESV_LOCN_HDR",
                        "LOCATION_ID,WM_RESV_LOCN_HDR_ID",
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
                "task_key": "SF_Ship_Via",
                "depends_on": [
                    {
                        "task_key": "s_WM_Ship_Via"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_SHIP_VIA",
                        "LOCATION_ID,WM_SHIP_VIA_ID",
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
                "task_key": "SF_Shipment",
                "depends_on": [
                    {
                        "task_key": "s_WM_Shipment"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_SHIPMENT",
                        "LOCATION_ID,WM_SHIPMENT_ID",
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
                "task_key": "SF_Shipment_Status",
                "depends_on": [
                    {
                        "task_key": "s_WM_Shipment_Status"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_SHIPMENT_STATUS",
                        "LOCATION_ID,WM_SHIPMENT_STATUS",
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
                "task_key": "SF_Size_Uom",
                "depends_on": [
                    {
                        "task_key": "s_WM_Size_Uom"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_SIZE_UOM",
                        "LOCATION_ID,WM_SIZE_UOM_ID",
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
                "task_key": "SF_Standard_Uom",
                "depends_on": [
                    {
                        "task_key": "s_WM_Standard_UOM"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_STANDARD_UOM",
                        "LOCATION_ID,WM_STANDARD_UOM_ID",
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
                "task_key": "SF_Stop",
                "depends_on": [
                    {
                        "task_key": "s_WM_Stop"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_STOP",
                        "LOCATION_ID,WM_SHIPMENT_ID,WM_STOP_SEQ",
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
                "task_key": "SF_Stop_Status",
                "depends_on": [
                    {
                        "task_key": "s_WM_Stop_Status"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_STOP_STATUS",
                        "LOCATION_ID,WM_STOP_STATUS",
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
                "task_key": "SF_Task_Dtl",
                "depends_on": [
                    {
                        "task_key": "s_WM_Task_Dtl"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_TASK_DTL",
                        "LOCATION_ID,WM_TASK_DTL_ID",
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
                "task_key": "SF_Task_Hdr",
                "depends_on": [
                    {
                        "task_key": "s_WM_Task_Hdr"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_TASK_HDR",
                        "LOCATION_ID,WM_TASK_HDR_ID",
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
                "task_key": "SF_Un_Number",
                "depends_on": [
                    {
                        "task_key": "s_WM_UN_Number"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_UN_NUMBER",
                        "LOCATION_ID,WM_UN_NBR_ID",
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
                "task_key": "SF_User_Profile",
                "depends_on": [
                    {
                        "task_key": "s_WM_User_Profile"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_USER_PROFILE",
                        "LOCATION_ID,WM_USER_PROFILE_ID",
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
                "task_key": "SF_Yard",
                "depends_on": [
                    {
                        "task_key": "s_WM_Yard"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_YARD",
                        "LOCATION_ID,WM_YARD_ID",
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
                "task_key": "SF_Yard_Zone",
                "depends_on": [
                    {
                        "task_key": "s_WM_Yard_Zone"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_YARD_ZONE",
                        "LOCATION_ID,WM_YARD_ID,WM_YARD_ZONE_ID",
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
                "task_key": "SF_Yard_Zone_Slot",
                "depends_on": [
                    {
                        "task_key": "s_WM_Yard_Zone_Slot"
                    }
                ],
                "run_if": "ALL_SUCCESS",
                "spark_python_task": {
                    "python_file": "Datalake/utils/SF_Ingest_v2.py",
                    "parameters": [
                        "prod",
                        "WM_YARD_ZONE_SLOT",
                        "LOCATION_ID,WM_YARD_ID,WM_YARD_ZONE_SLOT_ID,WM_YARD_ID",
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
}
"""

# COMMAND ----------
response = create_job(payload)
print(response)
