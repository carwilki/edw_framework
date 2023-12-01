# Databricks notebook source
#Code converted on 2023-10-17 15:29:10
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

# Processing node SQ_Shortcut_to_Snapshot_ACM_Feed, type SOURCE 
# COLUMN COUNT: 3

_sql = f"""
INSERT INTO {legacy}.EDU_CERT_SUMMARY_CONS
WITH ACM_PRE AS
(
  SELECT
    CURRENT_DATE - INTERVAL 1 DAY AS DAY_DT,
    ACM.USERID AS EMPLOYEE_ID,
    ACM.COURSEID AS MISSED_ASSESS_MID,
    -1 AS MISSED_ASSESS_LID,
    ACM.ASSESSMENT_NAME AS MISSED_ASSESS_NAME,
    ACM.EP_JOB_CODE AS JOB_CD,
    ACM.EP_LOCATION_ID AS LOCATION_ID,
    ACM.CURR_COMPLIANCE_FLAG,
    CURRENT_DATE AS LOAD_DT
  FROM
  (
    SELECT
      A.*,
      FIRST_VALUE(COURSE) OVER(PARTITION BY COURSEID ORDER BY UPDATE_DT DESC) ASSESSMENT_NAME,
      CASE
        WHEN A.ACTUAL_ASSESSMENT_ATTEMPTS IS NULL THEN 0
        WHEN LENGTH(TRANSLATE(A.ACTUAL_ASSESSMENT_ATTEMPTS, '0123456789', '')) = 0 THEN CAST(A.ACTUAL_ASSESSMENT_ATTEMPTS AS INTEGER)
        ELSE 0
      END CURR_PERIOD_ATTEMPTS_NBR,
      CASE
        WHEN A.STATUS IN ('I', 'NS') THEN 1
        ELSE 0
      END CURR_COMPLIANCE_FLAG,
      EP.LOCATION_ID AS EP_LOCATION_ID,
      EP.JOB_CODE AS EP_JOB_CODE,
      ROW_NUMBER() OVER(PARTITION BY A.USERID, A.COURSEID ORDER BY A.DUE_DATE DESC, A.STATUS) RN
    FROM {legacy}.SKILLSOFT_ACM_ASSET_ACTIVITY A
    JOIN {empl_protected}.legacy_employee_profile EP
    ON A.USERID = EP.EMPLOYEE_ID
    WHERE UPPER(A.COURSE) LIKE('%[COMPLIANCE]%')
    AND EP.EMPL_STATUS_CD = 'A'
    AND A.STATUS IN ('OD', 'I', 'NS')
  ) ACM
  WHERE ACM.RN = 1
)
SELECT *
FROM ACM_PRE
UNION ALL
SELECT
  DAY_DT,
  EMPLOYEE_ID,
  MISSED_ASSESS_MID,
  MISSED_ASSESS_LID,
  MISSED_ASSESS_NAME,
  JOB_CD,
  LOCATION_ID,
  CURR_COMPLIANCE_FLAG,
  CURRENT_DATE AS LOAD_DT
FROM {legacy}.EDU_CERT_SUMMARY
-- THIS FILTER WILL EXCLUDE ROWS FROM PTS WHERE EMPLOYEES ARE IN COMPLIANCE BUT OUT OF COMPLIANCE FOR ACM
WHERE DAY_DT = CURRENT_DATE - INTERVAL 1 DAY
AND (
  CURR_COMPLIANCE_FLAG = 0
  OR EMPLOYEE_ID NOT IN (
    SELECT DISTINCT EMPLOYEE_ID
    FROM ACM_PRE
  )
)
"""

spark.sql(_sql)

# COMMAND ----------


