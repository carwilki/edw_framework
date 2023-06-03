#
from pyspark.dbutils import DBUtils
from logging import getLogger


dbutils:DBUtils=dbutils
dbutils.widgets.text(name='env', defaultValue='')
env = dbutils.widgets.get('env')
logger = getLogger()
refine = getEnvPrefix(env)+'refine'
raw = getEnvPrefix(env)+'raw'
legacy = getEnvPrefix(env)+'legacy'

deltaTable=f'{refine}.WM_UCL_USER'
SFTable='WM_UCL_USER_LGCY'

try:
    ingestToSF(raw,deltaTable,SFTable)
    logger.info('Data write to SF completed')
except Exception as e:
    raise e
