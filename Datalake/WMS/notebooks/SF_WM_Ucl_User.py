# Databricks notebook source
# MAGIC %run ./utils/genericUtilities

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue='')
env = dbutils.widgets.get('env')

# COMMAND ----------

deltaTable='dev_refine.WM_E_CONSOL_PERF_SMRY'
SFTable='WM_E_CONSOL_PERF_SMRY_LGCY'

# COMMAND ----------

try:
    ingestToSF(env,deltaTable,SFTable)
    logger.info('Data write to SF completed')
except Exception as e:
    raise e
