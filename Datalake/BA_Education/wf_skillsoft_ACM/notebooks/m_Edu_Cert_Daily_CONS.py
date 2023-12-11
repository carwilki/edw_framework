# Databricks notebook source
#Code converted on 2023-10-17 15:29:09
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

# Processing node SQ_Shortcut_to_Skill_Soft_ACM, type SOURCE 
# COLUMN COUNT: 3

_sql = f"""
INSERT INTO {legacy}.EDU_CERT_DAILY_CONS
SELECT
  CURRENT_DATE - INTERVAL 1 DAY AS DAY_DT,
  ACM.USERID AS EMPLOYEE_ID,
  ACM.COURSEID AS ASSESSMENT_MID,
  -1 AS ASSESSMENT_LID,
  ACM.DATE_COMPLETED AS TEST_TAKEN_DT,
  ACM.ASSESSMENT_NAME,
  ACM.EP_JOB_CODE AS JOB_CD,
  ACM.EP_LOCATION_ID AS LOCATION_ID,
  regexp_replace(ACM.SCORE, r'\.','') AS LAST_TEST_SCORE_NBR,
  CASE WHEN ACM.STATUS IN ('C', 'EQ') THEN 1 ELSE 0 END AS LAST_TEST_PASSED_FLAG,
  ACM.DATE_COMPLETED AS COMPLIANT_START_DT,
  ACM.COMPLIANT_EXPIRATION_DT,
  CASE WHEN ACM.STATUS IN ('OD') THEN 0 ELSE 1 END AS CURR_COMPLIANCE_FLAG,
  CASE WHEN ACM.STATUS IN ('OD') THEN 1 ELSE 0 END AS CURR_MISSING_FLAG,
  ACM.CURR_PERIOD_ATTEMPTS_NBR,
  CURRENT_DATE AS LOAD_DT
FROM (
  SELECT
    A.*,
    FIRST_VALUE(COURSE) OVER (PARTITION BY COURSEID ORDER BY UPDATE_DT DESC) AS ASSESSMENT_NAME,
    CASE WHEN DATE_COMPLETED IS NOT NULL THEN DATE_ADD(DATE_COMPLETED, VALIDITYINDAYS) ELSE NULL END AS COMPLIANT_EXPIRATION_DT,
    CASE
      WHEN A.ACTUAL_ASSESSMENT_ATTEMPTS IS NULL AND A.STATUS IN ('C', 'EQ') THEN 1
      WHEN A.ACTUAL_ASSESSMENT_ATTEMPTS IS NULL THEN 0
      WHEN LENGTH(TRANSLATE(A.ACTUAL_ASSESSMENT_ATTEMPTS, '0123456789', '')) = 0 THEN CAST(A.ACTUAL_ASSESSMENT_ATTEMPTS AS INTEGER)
      ELSE 0
    END AS CURR_PERIOD_ATTEMPTS_NBR,
    EP.LOCATION_ID AS EP_LOCATION_ID,
    EP.JOB_CODE AS EP_JOB_CODE,
    ROW_NUMBER() OVER (PARTITION BY A.USERID, A.COURSEID ORDER BY A.DUE_DATE DESC, A.STATUS) AS RN
  FROM {legacy}.SKILLSOFT_ACM_ASSET_ACTIVITY A
  JOIN {empl_protected}.legacy_employee_profile EP ON A.USERID = EP.EMPLOYEE_ID
  WHERE UPPER(A.COURSE) LIKE '%[COMPLIANCE]%'
    AND EP.EMPL_STATUS_CD = 'A'
    AND (
      A.STATUS IN ('OD', 'I', 'NS')
      OR (
        A.STATUS IN ('C', 'EQ')
        AND CURRENT_DATE - INTERVAL 1 DAY BETWEEN A.DATE_COMPLETED AND 
                                                  CASE WHEN DATE_COMPLETED IS NOT NULL THEN DATE_ADD(DATE_COMPLETED, VALIDITYINDAYS) ELSE NULL END --COMPLIANT_EXPIRATION_DT
      )
    )
) ACM
WHERE ACM.RN = 1
UNION ALL
SELECT * EXCEPT(bd_create_dt_tm,source_file_name) FROM {legacy}.EDU_CERT_DAILY
WHERE DAY_DT = CURRENT_DATE - INTERVAL 1 DAY
"""

spark.sql(_sql)

# COMMAND ----------


