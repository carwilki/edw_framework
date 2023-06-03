from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from logging import getLogger
from Datalake.WMS.notebooks.utils.genericUtilities import getEnvPrefix,ingestToSF

dbutils: DBUtils = DBUtils(SparkSession.getActiveSession())

env = dbutils.jobs.taskValues.get(key='env', default='')

if env is None or env == "":
    raise ValueError("env is not set")

logger = getLogger()

refine = getEnvPrefix(env)+'refine'
raw = getEnvPrefix(env)+'raw'

deltaTable=f"{refine}.WM_E_DEPT"
SFTable="WM_E_DEPT_LGCY"

try:
    ingestToSF(raw,deltaTable,SFTable)
    logger.info("Data write to SF completed succesfully")
except Exception as e:
    raise e
