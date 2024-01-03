# Databricks notebook source
from Datalake.utils.rocky_script_automation.RockyAutomationUtils import *
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

tables = ['EDU_CERT_SITE_DAILY']

# COMMAND ----------

trigger_rocky(tables)

