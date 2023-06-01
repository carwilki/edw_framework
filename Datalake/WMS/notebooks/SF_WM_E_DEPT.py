# Databricks notebook source
# MAGIC %run ./utils/genericUtilities

# COMMAND ----------

from pyspark.dbutils import DBUtils
from logging import getLogger

# COMMAND ----------

dbutils: DBUtils = dbutils
dbutils.widgets.text(name='env', defaultValue='')
env = getEnvPrefix(dbutils.widgets.get('env'))
logger = getLogger()

# COMMAND ----------

deltaTable=env+"refine.WM_E_DEPT"
SFTable="WM_E_DEPT_LGCY"

# COMMAND ----------

try:
    ingestToSF(env,deltaTable,SFTable)
    logger.info("Data write to SF completed succesfully")
except Exception as e:
    raise e
