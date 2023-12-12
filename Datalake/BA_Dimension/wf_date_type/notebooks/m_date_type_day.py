# Databricks notebook source
# Code converted on 2023-10-24 09:48:26
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
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# Processing node SQ_Shortcut_To_DAYS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_DAYS = spark.sql(f"""/* DAY */
SELECT 1 date_type_id,

       CURRENT_DATE - 1 day_dt

UNION

/* LW */
SELECT  3 date_type_id,

        d2.day_dt DAY_DT

FROM    {enterprise}.days d1

        INNER JOIN (

                   SELECT       Fiscal_Yr, max(fiscal_wk) as LastYearMaxFiscal_wk

                   FROM         {enterprise}.days

                   GROUP BY     Fiscal_Yr

                   order by fiscal_yr

                   ) d3 ON (d1.fiscal_yr - 1) = d3.fiscal_yr

        INNER JOIN {enterprise}.days d2 ON d1.day_dt = CURRENT_DATE

           AND CAST(CASE d1.FISCAL_WK_NBR WHEN 1 THEN d3.LastYearMaxFiscal_wk  ELSE (d1.fiscal_wk - 1) END AS INT) = d2.fiscal_wk

UNION

/* 4Wk */
SELECT 9 DATE_TYPE_ID,

       D.DAY_DT

  FROM {enterprise}.days D

  CROSS JOIN (SELECT DAY_OF_WK_NBR 

                FROM {enterprise}.days 

                        WHERE DAY_DT = CURRENT_DATE

                     ) P

  WHERE D.DAY_DT BETWEEN CURRENT_DATE -27 - P.DAY_OF_WK_NBR AND CURRENT_DATE - P.DAY_OF_WK_NBR

UNION

/* WTD */
SELECT 2 date_type_id,

       d2.day_dt

  FROM {enterprise}.days d1

  JOIN {enterprise}.days d2

    ON d1.day_dt = CURRENT_DATE - 1

   AND d1.fiscal_wk = d2.fiscal_wk

   AND d1.day_dt >= d2.day_dt

UNION

/* PTD */
SELECT 4 date_type_id,

       d2.day_dt

  FROM {enterprise}.days d1

  JOIN {enterprise}.days d2

    ON d1.day_dt = CURRENT_DATE - 1

   AND d1.fiscal_mo = d2.fiscal_mo

   AND d1.day_dt >= d2.day_dt

UNION

/* QTD */
SELECT 6 date_type_id,

       d2.day_dt

  FROM {enterprise}.days d1

  JOIN {enterprise}.days d2

    ON d1.day_dt = CURRENT_DATE - 1

   AND d1.fiscal_qtr = d2.fiscal_qtr

   AND d1.day_dt >= d2.day_dt

UNION

/* YTD */
SELECT 8 date_type_id,

       d2.day_dt

  FROM {enterprise}.days d1

  JOIN {enterprise}.days d2

    ON d1.day_dt = CURRENT_DATE - 1

   AND d1.fiscal_yr = d2.fiscal_yr

   AND d1.day_dt >= d2.day_dt

UNION

/* Q3Q4TD */
SELECT 22 date_type_id,

       d2.day_dt

  FROM {enterprise}.days d1

  JOIN {enterprise}.days d2

    ON d1.day_dt = CURRENT_DATE - 1

   AND d1.fiscal_yr = d2.fiscal_yr

  where substr(d2.FISCAL_QTR,5,2) in ('03','04')

  and d2.day_dt < CURRENT_DATE

   AND d1.day_dt >= d2.day_dt

UNION

/* STD */
SELECT  23 DATE_TYPE_ID,

		d1.DAY_DT

		FROM {enterprise}.days d1

		WHERE DAY_DT < CURRENT_DATE

		AND FISCAL_HALF=(SELECT FISCAL_HALF FROM {enterprise}.days d2

                 		WHERE d2.DAY_DT=(SELECT MAX(d3.DAY_DT) FROM {enterprise}.days d3 WHERE d3.DAY_DT < CURRENT_DATE))

UNION

/* DAYS OF FISCAL MONTH */
SELECT (23 + FISCAL_DAY_OF_MO_NBR) DAY_TYPE_ID, DAY_DT

FROM {enterprise}.days 

WHERE 

FISCAL_MO = (SELECT MAX(FISCAL_MO) FROM {enterprise}.days WHERE DAY_DT < CURRENT_DATE)

UNION

/* WEEKS OF FISCAL MONTH */
SELECT

CASE WHEN W.TYPE_ID=1 THEN 59

WHEN W.TYPE_ID=2 THEN 60

WHEN W.TYPE_ID=3 THEN 61

WHEN W.TYPE_ID=4 THEN 62

WHEN W.TYPE_ID=5 THEN 63

END DATE_TYPE_ID,

D.DAY_DT

FROM {enterprise}.days D

JOIN

( 

SELECT ROW_NUMBER() OVER (ORDER BY WEEK_DT) AS TYPE_ID, WEEK_DT 

FROM {enterprise}.WEEKS WHERE FISCAL_MO = (SELECT MAX(FISCAL_MO) FROM {enterprise}.days WHERE DAY_DT < CURRENT_DATE)

) W

ON D.WEEK_DT=W.WEEK_DT

UNION

/*  CALENDAR YEAR,QUARTER,MONTH */
SELECT A.DATE_TYPE_ID,

       D.DAY_DT

  FROM (

       SELECT 80 DATE_TYPE_ID, CAL_YR, NULL CAL_QTR, NULL CAL_MO FROM {enterprise}.days WHERE DAY_DT = CURRENT_DATE - 1 UNION ALL

       SELECT 81 DATE_TYPE_ID, NULL CAL_YR, CAL_QTR, NULL CAL_MO FROM {enterprise}.days WHERE DAY_DT = CURRENT_DATE - 1 UNION ALL

       SELECT 82 DATE_TYPE_ID, NULL CAL_YR, NULL CAL_QTR, CAL_MO FROM {enterprise}.days WHERE DAY_DT = CURRENT_DATE - 1

       ) A

  JOIN {enterprise}.days D

    ON NVL(A.CAL_YR, D.CAL_YR)  = D.CAL_YR

   AND NVL(A.CAL_QTR,D.CAL_QTR) = D.CAL_QTR

   AND NVL(A.CAL_MO, D.CAL_MO)  = D.CAL_MO

   AND D.DAY_DT <= CURRENT_DATE - 1""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_DAYS = SQ_Shortcut_To_DAYS \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[0],'DATE_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_To_DAYS.columns[1],'DAY_DT')

# COMMAND ----------

# Processing node Shortcut_to_DATE_TYPE_DAY, type TARGET 
# COLUMN COUNT: 2


Shortcut_to_DATE_TYPE_DAY = SQ_Shortcut_To_DAYS.selectExpr(
	"CAST(DATE_TYPE_ID AS BIGINT) as DATE_TYPE_ID",
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT"
)
try:
	# chk=DuplicateChecker()
	# chk.check_for_duplicate_primary_keys(spark,f'{legacy}.DATE_TYPE_DAY',Shortcut_to_DATE_TYPE_DAY,["KEY1","KEY1"])
	Shortcut_to_DATE_TYPE_DAY.write.mode("overwrite").saveAsTable(f'{legacy}.DATE_TYPE_DAY')
except Exception as e:
	raise e

# COMMAND ----------


