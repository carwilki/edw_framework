# Databricks notebook source
from pyspark.dbutils import DBUtils
from logging import getLogger

# COMMAND ----------

# MAGIC %run ./utils/genericUtilities

# COMMAND ----------

dbutils:DBUtils=dbutils
dbutils.widgets.text(name='env', defaultValue='')
env = getEnvPrefix(dbutils.widgets.get('env'))
logger = getLogger()

# COMMAND ----------

deltaTable=env+'_refine.WM_UCL_USER'
SFTable='WM_UCL_USER_LGCY'

# COMMAND ----------

try:
    ingestToSF(env,deltaTable,SFTable)
    logger.info('Data write to SF completed')
except Exception as e:
    raise e
