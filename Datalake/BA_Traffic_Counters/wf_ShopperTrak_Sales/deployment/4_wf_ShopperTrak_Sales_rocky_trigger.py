# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

tables = ['ST_FILES_CTRL']

# COMMAND ----------

trigger_rocky(tables)

