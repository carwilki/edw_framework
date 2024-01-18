# Databricks notebook source
# from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
# from pyspark.sql.functions import *

# COMMAND ----------


import json
import requests

env = "prod"
if env == "prod":
    work_db = "work"
    platform_db = "stranger_things"
    token = dbutils.secrets.get(scope="db-token-jobsapi", key="password")
    instance_id = "3609071286715921.1.gcp.databricks.com"
else:
    work_db = "qa_work"
    platform_db = "qa_stranger_things"
    instance_id = "3986616729757273.3.gcp.databricks.com"

# COMMAND ----------

def trigger_rocky_job(payload):
    api_version = "/api/2.1"
    api_command = "/jobs/run-now"
    url = f"https://{instance_id}{api_version}{api_command}"

    params = {"Authorization": "Bearer " + token, "Content-Type": "application/json"}

    response = requests.post(url=url, headers=params, data=payload)

    return response.text

# COMMAND ----------

import csv
import time

tables = ['EP_PLAN_CLASS_WK']

for table in tables:    
  print(table)
  query = f"""select job_id from {work_db}.rocky_ingestion_metadata where source_table='{table}' and table_group='NZ_Migration'"""
  print(query)
  job_id = spark.sql(query).collect()[0][0]
  print(job_id)
  try:
      run_info = trigger_rocky_job(json.dumps({"job_id": job_id}))
      print("response:", run_info)
      #time.sleep(240)
      # run_id = json.loads(run_info)['run_id']
      # print(run_id)
  except Exception as e:
      print("failed for", table, e)

# COMMAND ----------


# trigger_rocky(tables)


# COMMAND ----------



# COMMAND ----------

