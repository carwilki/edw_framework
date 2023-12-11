# Databricks notebook source
#Code converted on 2023-10-17 15:29:07
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
empl_protected = getEnvPrefix(env) + 'empl_protected'

# COMMAND ----------

# Processing node SQ_Shortcut_to_Skillsoft_Acm_Asset_Activity, type SOURCE 
# COLUMN COUNT: 3

_sql = f"""
INSERT OVERWRITE {legacy}.EDU_RESULT_CONS
SELECT
  ROW_NUMBER() OVER(ORDER BY DUE_DATE, USERID, COURSEID) * -1 AS RESULT_ID,
  COALESCE(A.DATE_COMPLETED, date'1900-01-01') AS TEST_TAKEN_DT,
  A.BEGIN_DATE AS TEST_TAKEN_START_TSTMP,
  A.COURSEID AS ASSESSMENT_MID,
  -1 AS ASSESSMENT_LID,
  A.LAST_VISIT_DATE AS LAST_MODIFIED_TSTMP,
  NULL AS WRITE_ANSWER_FLAG,
  A.USERID AS EMPLOYEE_ID,
  A.GROUP_FROM AS MEMBER_GROUP,
  NULL AS PARTICIPANT_DETAILS,
  NULL AS HOSTNAME,
  NULL AS IP_ADDRESS,
  NULL AS SIGNATURE,
  NULL AS STILL_GOING_FLAG,
  2 AS STATUS_ID,
  NULL AS SECTIONS_CNT,
  MAX(CASE WHEN LENGTH(REGEXP_REPLACE(A.SCORE, '[0-9]', '')) = 0 THEN CAST(A.SCORE AS INT) ELSE NULL END) OVER(PARTITION BY A.COURSEID) AS MAX_SCORE_NBR,
  CASE WHEN LENGTH(REGEXP_REPLACE(A.SCORE, '[0-9]', '')) = 0 THEN CAST(A.SCORE AS INT) ELSE NULL END AS TOTAL_SCORE_NBR,
  NULL AS SPECIAL_1,
  NULL AS SPECIAL_2,
  NULL AS SPECIAL_3,
  NULL AS SPECIAL_4,
  NULL AS SPECIAL_5,
  NULL AS SPECIAL_6,
  NULL AS SPECIAL_7,
  NULL AS SPECIAL_8,
  NULL AS SPECIAL_9,
  NULL AS SPECIAL_10,
  NULL AS TIME_TAKEN_NBR,
  NULL AS SCORE_RESULT,
  NULL AS SCORE_RESULT_NBR,
  CASE WHEN A.STATUS IN ('C', 'EQ') THEN 1 ELSE 0 END AS PASSED_FLAG,
  CASE WHEN MAX_SCORE_NBR > 0 THEN CAST(((TOTAL_SCORE_NBR / CAST(MAX_SCORE_NBR AS NUMERIC(10, 2))) * 100) AS INTEGER) ELSE NULL END AS PERCENTAGE_SCORE_NBR,
  NULL AS SCHEDULE_NAME,
  NULL AS MONITORED_FLAG,
  NULL AS MONITOR_NAME,
  NULL AS TIME_LIMIT_DISABLED_FLAG,
  NULL AS DISABLED_BY,
  NULL AS IMAGE_REF,
  -1 AS SCOREBAND_ID,
  EP.EMPL_FIRST_NAME AS FIRST_NAME,
  EP.EMPL_LAST_NAME AS LAST_NAME,
  USER_EMAIL AS PRIMARY_EMAIL,
  NULL AS RESTRICT_PART_FLAG,
  NULL AS RESTRICT_ADMIN_FLAG,
  NULL AS R_PART_FROM_DT,
  NULL AS R_PART_TO_DT,
  NULL AS R_ADMIN_FROM_DT,
  NULL AS R_ADMIN_TO_DT,
  FIRST_VALUE(COURSE) OVER(PARTITION BY COURSEID ORDER BY UPDATE_DT DESC) AS COURSE_NAME,
  NULL AS MEMBER_SUB_GROUP_1,
  NULL AS MEMBER_SUB_GROUP_2,
  NULL AS MEMBER_SUB_GROUP_3,
  NULL AS MEMBER_SUB_GROUP_4,
  NULL AS MEMBER_SUB_GROUP_5,
  NULL AS MEMBER_SUB_GROUP_6,
  NULL AS MEMBER_SUB_GROUP_7,
  NULL AS MEMBER_SUB_GROUP_8,
  NULL AS MEMBER_SUB_GROUP_9,
  NULL AS TEST_CENTER,
  CURRENT_DATE AS LOAD_DT,
  EP.LOCATION_ID
FROM {legacy}.SKILLSOFT_ACM_ASSET_ACTIVITY A
JOIN {empl_protected}.LEGACY_EMPLOYEE_PROFILE EP ON A.USERID = EP.EMPLOYEE_ID
WHERE UPPER(A.COURSE) LIKE '%[COMPLIANCE]%'
UNION ALL
SELECT
  ER.* EXCEPT(bd_create_dt_tm,source_file_name),
  EP.LOCATION_ID
FROM {legacy}.EDU_RESULT ER
JOIN {empl_protected}.LEGACY_EMPLOYEE_PROFILE EP ON ER.EMPLOYEE_ID = EP.EMPLOYEE_ID
"""

spark.sql(_sql)

# COMMAND ----------


