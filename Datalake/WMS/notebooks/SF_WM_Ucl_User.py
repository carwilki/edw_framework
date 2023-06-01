# Databricks notebook source
from pyspark.dbutils import DBUtils
from utils.genericUtilities import ingestToSF
from logging import getLogger
# COMMAND ----------

dbutils:DBUtils=dbutils
dbutils.widgets.text(name='env', defaultValue='')
env = dbutils.widgets.get('env')
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
