# Databricks notebook source
# MAGIC %run ./utils/genericUtilities

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue='')
env = dbutils.widgets.get('env')

# COMMAND ----------

deltaTable=env+'_refine.WM_UCL_USER'
SFTable='WM_UCL_USER_LGCY'

# COMMAND ----------

try:
    ingestToSF(env,deltaTable,SFTable)
    logger.info('Data write to SF completed')
except Exception as e:
    raise e
