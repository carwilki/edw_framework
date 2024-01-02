# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
from pyspark.sql.functions import *
# COMMAND ----------
legacy_tables = ['WMS_INV_NEED_TYPE', 'WM_E_JOB_FUNCTION', 'WMS_JOB_FUNCTION']
copy_hist_to_legacy(legacy_tables)
# COMMAND ----------
table_list = ['WMS_INV_NEED_TYPE', 'WM_E_JOB_FUNCTION', 'WMS_JOB_FUNCTION'] 
# COMMAND ----------  
for table in table_list:
    try:
      spark.sql(f"drop table  refine.{table}_history")
      print(f"refine.{table}_history deleted")
    except Exception as e:
      print("failed for ", table, e)