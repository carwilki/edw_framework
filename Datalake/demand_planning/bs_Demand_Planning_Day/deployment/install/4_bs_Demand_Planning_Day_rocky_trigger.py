# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils.py import *
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

tables = ['SAP_SKU_LINK_TYPE']

# COMMAND ----------

trigger_rocky(tables)

