# Databricks notebook source
# Code converted on 2023-09-06 09:48:16
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

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
empl_protected = getEnvPrefix(env) + 'empl_protected'

# COMMAND ----------

#Testing script
# Processing node SQ_Shortcut_to_EMPLOYEE_PROFILE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_EMPLOYEE_PROFILE = spark.sql(f"""SELECT DISTINCT

((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE  )-1)) - 7) PERIOD_DT

,A.EMPLOYEE_ID

,B.LOCATION_ID

,SUM(HOURS_WORKED) OVER (PARTITION BY((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE  )-1)) - 7), B.LOCATION_ID, A.EMPLOYEE_ID) HOURS_WORKED

,SUM(HOURS_WORKED) OVER (PARTITION BY((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE  )-1)) - 7), A.EMPLOYEE_ID) TOT_HOURS_WORKED

FROM

(SELECT *

  FROM   (SELECT *

            FROM {empl_protected}.legacy_EMPLOYEE_PROFILE_DAY

/*  UNION */
/*  SELECT * */
/*    FROM {legacy}.EMPLOYEE_PROFILE_DAY_PSOFT -- USE EMPLOYEE_PROFILE_DAY_PSOFT UNTIL 1/11/2011 */
/*   WHERE DAY_DT > CAST('20101128' AS TIMESTAMP) */
) T /*  THIS WILL BE THE CONVERSION DATE. */
  WHERE  DAY_DT = ((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE  )-1)) - 7)) A

,(SELECT *

    FROM {empl_protected}.legacy_EMPL_EMPL_LOC_WK

/*  UNION */
/* SELECT * */
/*   FROM {legacy}.EMPL_EMPL_LOC_WK_PSOFT */
) B /*  USE EMPL_EMPL_LOC_WK_PSOFT UNTIL AFTER 4/15/2011, THEN IT CAN BE REMOVED. */
,{legacy}.EARNINGS_ID E

WHERE A.FULLPT_FLAG    IN ('P')

  AND A.EMPL_TYPE_CD   IN ('H')

  AND A.EMPL_STATUS_CD IN ('A')

  AND A.JOB_CODE       NOT IN (9000)

/* /* 13 week no change*/ */
  AND A.EMPL_HIRE_DT    < ((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE)-1)) - 91 - 7)

  AND A.FULLPT_CHG_DT   < ((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE)-1)) - 91 - 7)

  AND A.STATUS_CHG_DT   < ((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE)-1)) - 91 - 7)

  AND (A.LOCATION_CHG_DT < ((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE)-1)) - 91 - 7) OR A.LOCATION_CHG_DT IS NULL)

  AND A.EMPLOYEE_ID     = B.EMPLOYEE_ID

  AND B.EARN_ID         = E.EARN_ID

  AND B.EARN_ID NOT LIKE ('LJP%')

  AND SUBSTR(B.EARN_ID,4,4) NOT IN ('2031','2032','2033','31','32','33')

  AND HOURS_WORKED      > 0



  AND B.WEEK_DT         > ((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE )-1)) - 94 - 0)

  AND B.WEEK_DT         <= ((to_date('CURRENT_DATE) - (DATE_PART('DOW', 'CURRENT_DATE )-1)) - 7)""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EMPLOYEE_PROFILE = SQ_Shortcut_to_EMPLOYEE_PROFILE \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[0],'PERIOD_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[1],'EMPLOYEE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[2],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[3],'HOURS_WORKED') \
	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[4],'TOT_HOURS_WORKED')

# COMMAND ----------

# # Processing node SQ_Shortcut_to_EMPLOYEE_PROFILE, type SOURCE 
# # COLUMN COUNT: 5

# SQ_Shortcut_to_EMPLOYEE_PROFILE = spark.sql(f"""SELECT DISTINCT

# ((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE  )-1)) - 7) PERIOD_DT

# ,A.EMPLOYEE_ID

# ,B.LOCATION_ID

# ,SUM(HOURS_WORKED) OVER (PARTITION BY((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE  )-1)) - 7), B.LOCATION_ID, A.EMPLOYEE_ID) HOURS_WORKED

# ,SUM(HOURS_WORKED) OVER (PARTITION BY((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE  )-1)) - 7), A.EMPLOYEE_ID) TOT_HOURS_WORKED

# FROM

# (SELECT *

#   FROM   (SELECT *

#             FROM {empl_protected}.legacy_EMPLOYEE_PROFILE_DAY

# /*  UNION */
# /*  SELECT * */
# /*    FROM {legacy}.EMPLOYEE_PROFILE_DAY_PSOFT -- USE EMPLOYEE_PROFILE_DAY_PSOFT UNTIL 1/11/2011 */
# /*   WHERE DAY_DT > CAST('20101128' AS TIMESTAMP) */
# ) T /*  THIS WILL BE THE CONVERSION DATE. */
#   WHERE  DAY_DT = ((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE  )-1)) - 7)) A

# ,(SELECT *

#     FROM {empl_protected}.legacy_EMPL_EMPL_LOC_WK

# /*  UNION */
# /* SELECT * */
# /*   FROM {legacy}.EMPL_EMPL_LOC_WK_PSOFT */
# ) B /*  USE EMPL_EMPL_LOC_WK_PSOFT UNTIL AFTER 4/15/2011, THEN IT CAN BE REMOVED. */
# ,{legacy}.EARNINGS_ID E

# WHERE A.FULLPT_FLAG    IN ('P')

#   AND A.EMPL_TYPE_CD   IN ('H')

#   AND A.EMPL_STATUS_CD IN ('A')

#   AND A.JOB_CODE       NOT IN (9000)

# /* /* 13 week no change*/ */
#   AND A.EMPL_HIRE_DT    < ((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE)-1)) - 91 - 7)

#   AND A.FULLPT_CHG_DT   < ((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE)-1)) - 91 - 7)

#   AND A.STATUS_CHG_DT   < ((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE)-1)) - 91 - 7)

#   AND (A.LOCATION_CHG_DT < ((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE)-1)) - 91 - 7) OR A.LOCATION_CHG_DT IS NULL)

#   AND A.EMPLOYEE_ID     = B.EMPLOYEE_ID

#   AND B.EARN_ID         = E.EARN_ID

#   AND B.EARN_ID NOT LIKE ('LJP%')

#   AND SUBSTR(B.EARN_ID,4,4) NOT IN ('2031','2032','2033','31','32','33')

#   AND HOURS_WORKED      > 0



#   AND B.WEEK_DT         > ((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE )-1)) - 94 - 0)

#   AND B.WEEK_DT         <= ((CURRENT_DATE - (DATE_PART('DOW', CURRENT_DATE )-1)) - 7)""").withColumn("sys_row_id", monotonically_increasing_id())
# # Conforming fields names to the component layout
# SQ_Shortcut_to_EMPLOYEE_PROFILE = SQ_Shortcut_to_EMPLOYEE_PROFILE \
# 	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[0],'PERIOD_DT') \
# 	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[1],'EMPLOYEE_ID') \
# 	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[2],'LOCATION_ID') \
# 	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[3],'HOURS_WORKED') \
# 	.withColumnRenamed(SQ_Shortcut_to_EMPLOYEE_PROFILE.columns[4],'TOT_HOURS_WORKED')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 5

SQ_Shortcut_to_EMPLOYEE_PROFILE_temp = SQ_Shortcut_to_EMPLOYEE_PROFILE.toDF(*["SQ_Shortcut_to_EMPLOYEE_PROFILE___" + col for col in SQ_Shortcut_to_EMPLOYEE_PROFILE.columns])

EXPTRANS = SQ_Shortcut_to_EMPLOYEE_PROFILE_temp.withColumn("HOURS_WORKED1", col('SQ_Shortcut_to_EMPLOYEE_PROFILE___HOURS_WORKED') / 13) \
	.withColumn("var_HOURS_WORKED1", round(col('HOURS_WORKED1'), 4)) \
	.withColumn("var_POINT_POS", instr ( col('HOURS_WORKED1') , '.' )) \
	.withColumn("var_3RD_DIGIT", expr("""substring ( HOURS_WORKED1 , var_POINT_POS + 3 , 1 )""")) \
	.withColumn("var_HOURS_WORKED_CALC", expr("""CASE WHEN  (var_3RD_DIGIT = 5  AND  var_HOURS_WORKED1 > 0) THEN var_HOURS_WORKED1 + 0.005 WHEN  (var_3RD_DIGIT = 5  AND  var_HOURS_WORKED1 < 0) THEN var_HOURS_WORKED1 - 0.005 ELSE var_HOURS_WORKED1 END""")) \
	.withColumn("var_AVG_HOURS_WORKED", col('SQ_Shortcut_to_EMPLOYEE_PROFILE___TOT_HOURS_WORKED') / 13).selectExpr(
	"SQ_Shortcut_to_EMPLOYEE_PROFILE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_EMPLOYEE_PROFILE___PERIOD_DT as PERIOD_DT",
	"SQ_Shortcut_to_EMPLOYEE_PROFILE___EMPLOYEE_ID as EMPLOYEE_ID",
	"SQ_Shortcut_to_EMPLOYEE_PROFILE___LOCATION_ID as LOCATION_ID",
	"var_HOURS_WORKED_CALC as var_HOURS_WORKED_CALC",
	"IF (var_AVG_HOURS_WORKED > 35.99, 1, 0) as Flag"
)

# COMMAND ----------

# Processing node Shortcut_to_EMPL_PT_36HR_FLAG, type TARGET 
# COLUMN COUNT: 6

Shortcut_to_EMPL_PT_36HR_FLAG = EXPTRANS.selectExpr(
	"CAST(PERIOD_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(EMPLOYEE_ID AS INT) as EMPLOYEE_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(var_HOURS_WORKED_CALC AS DECIMAL(9,2)) as EMPL_13WK_AVG_HRS_WORKED",
	"CAST(FLAG AS TINYINT) as FLAG",
	"CAST(null AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_EMPL_PT_36HR_FLAG.write.mode("append").saveAsTable(f'{legacy}.EMPL_PT_36HR_FLAG')

# COMMAND ----------


