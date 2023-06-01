# Databricks notebook source
from pyspark.dbutils import DBUtils
from logging import getLogger
# MAGIC %run ./utils/genericUtilities

# COMMAND ----------
dbutils: DBUtils = dbutils
dbutils.widgets.text(name='env', defaultValue='')
env = dbutils.widgets.get('env')
logger = getLogger()
# COMMAND ----------

deltaTable=env+"_refine.WM_E_DEPT"
SFTable="WM_E_DEPT_LGCY"

# COMMAND ----------

try:
    ingestToSF(env,deltaTable,SFTable)
    logger.info("Data write to SF completed succesfully")
except Exception as e:
    raise e
