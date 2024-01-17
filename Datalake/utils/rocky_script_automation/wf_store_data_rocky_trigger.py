# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
from pyspark.sql.functions import *

# COMMAND ----------


# COMMAND ----------

tables = ['STORE_DATA']

# COMMAND ----------
trigger_rocky(tables)

