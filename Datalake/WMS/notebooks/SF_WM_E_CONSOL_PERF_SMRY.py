# Databricks notebook source

from pyspark.dbutils import DBUtils
from utils.genericUtilities import ingestToSF
from logging import getLogger

# COMMAND ----------

# MAGIC %run ./utils/genericUtilities

# COMMAND ----------
dbutils: DBUtils = dbutils
dbutils.widgets.text(name='env', defaultValue='')
env = dbutils.widgets.get('env')
logger = getLogger()
# COMMAND ----------

deltaTable=env+'_refine.WM_E_CONSOL_PERF_SMRY'
SFTable='WM_E_CONSOL_PERF_SMRY_LGCY'

# COMMAND ----------

try:
    ingestToSF(env,deltaTable,SFTable)
    logger.info('Data write to SF completed')
except Exception as e:
    raise e

# COMMAND ----------


