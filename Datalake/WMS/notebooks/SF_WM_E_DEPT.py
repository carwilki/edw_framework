from pyspark.dbutils import DBUtils
from logging import getLogger
from Datalake.WMS.notebooks.utils.genericUtilities import getEnvPrefix,ingestToSF

dbutils: DBUtils = dbutils
dbutils.widgets.text(name='env', defaultValue='')
env =dbutils.widgets.get('env')
logger = getLogger()
refine = getEnvPrefix(env)+'refine'
raw = getEnvPrefix(env)+'raw'
legacy = getEnvPrefix(env)+'legacy'

deltaTable=f"{refine}.WM_E_DEPT"
SFTable="WM_E_DEPT_LGCY"

try:
    ingestToSF(raw,deltaTable,SFTable)
    logger.info("Data write to SF completed succesfully")
except Exception as e:
    raise e
