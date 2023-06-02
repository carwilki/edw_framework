# Databricks notebook source
# MAGIC %run ./utils/genericUtilities

# COMMAND ----------

from pyspark.dbutils import DBUtils
from logging import getLogger

# COMMAND ----------

dbutils: DBUtils = dbutils
dbutils.widgets.text(name='env', defaultValue='')
env = dbutils.widgets.get('env')
logger = getLogger()
refine = getEnvPrefix(env)+'refine'
raw = getEnvPrefix(env)+'raw'
legacy = getEnvPrefix(env)+'legacy'

# COMMAND ----------

deltaTable=refine+'.WM_E_CONSOL_PERF_SMRY'
SFTable='WM_E_CONSOL_PERF_SMRY_LGCY'

# COMMAND ----------

try:
    ingestToSF(raw,deltaTable,SFTable)
    logger.info('Data write to SF completed')
except Exception as e:
    raise e
