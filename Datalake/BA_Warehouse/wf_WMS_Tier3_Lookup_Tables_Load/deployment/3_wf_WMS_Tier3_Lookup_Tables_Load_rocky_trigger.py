# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

tables = ['WMS_INV_NEED_TYPE', 'WM_E_JOB_FUNCTION', 'WMS_JOB_FUNCTION']

# COMMAND ----------

trigger_rocky(tables)

