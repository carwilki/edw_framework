# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
from pyspark.sql.functions import *

# COMMAND ----------

legacy_tables = ['EDU_CERT_SITE_DAILY']
copy_hist_to_legacy(legacy_tables)

# COMMAND ----------

table_list = ['EDU_CERT_SITE_DAILY'] 

# COMMAND ----------

for table in table_list:
    try:
      spark.sql(f"drop table  refine.{table}_history")
      print(f"refine.{table}_history deleted")
    except Exception as e:
      print("failed for ", table, e)
