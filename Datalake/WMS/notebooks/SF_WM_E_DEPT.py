# Databricks notebook source
# MAGIC %run ./utils/genericUtilities

# COMMAND ----------

dbutils.widgets.text(name='env', defaultValue='')
env = dbutils.widgets.get('env')

# COMMAND ----------

deltaTable=env+"_refine.WM_E_DEPT"
SFTable="WM_E_DEPT_LGCY"

# COMMAND ----------

try:
    ingestToSF(env,deltaTable,SFTable)
    logger.info("Data write to SF completed succesfully")
except Exception as e:
    raise e
