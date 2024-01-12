# Databricks notebook source
#Code converted on 2023-10-17 15:29:06
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQL_INS_and_DUPS_CHECK, type SQL_TRANSFORM 
# COLUMN COUNT: 9

_sql = f"""
INSERT OVERWRITE {legacy}.EDU_ASSESSMENTS_CONS
SELECT * FROM {legacy}.EDU_ASSESSMENTS
UNION ALL
-- Create additional records based on specific conditions
SELECT DISTINCT
  COURSEID AS ASSESSMENT_MID,
  -1 AS ASSESSMENT_LID,
  0 AS REVISION_NBR,
  FIRST_VALUE(COURSE) OVER (PARTITION BY COURSEID ORDER BY UPDATE_DT DESC) AS ASSESSMENT_NAME,
  NULL AS ASSESSMENT_AUTHOR,
  NULL AS MODIFY_TSTMP,
  0 AS TIME_LIMIT_FLAG,
  0 AS TIME_LIMIT_NBR,
  0 AS SECTIONS_CNT,
  NULL AS LAST_UPDATE_TSTMP,
  NULL AS ASSESSMENT_TYPE_ID,
  FIRST_VALUE(COURSE) OVER (PARTITION BY COURSEID ORDER BY UPDATE_DT DESC) AS COURSE_NAME,
  NULL AS ASSESSMENT_DESC,
  0 AS PETSHOTEL_ASSESSMENT_FLAG,
  0 AS SALON_ASSESSMENT_FLAG,
  NULL AS LAST_UPDT_USER,
  NULL AS LAST_UPDT_TSTMP,
  CURRENT_DATE AS LOAD_DT
FROM {legacy}.SKILLSOFT_ACM_ASSET_ACTIVITY
WHERE UPPER(COURSE) LIKE '%[COMPLIANCE]%'
"""

spark.sql(_sql)

# COMMAND ----------


