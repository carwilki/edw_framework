# Databricks notebook source
# Code converted on 2023-08-22 11:01:59
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
from datetime import timedelta

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


# COMMAND ----------

# Processing node SQ_Shortcut_To_PHYS_INV_HDR_PRE, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_To_PHYS_INV_HDR_PRE = spark.sql(f"""SELECT  NEW_NZ.day_dt,

   NEW_NZ.location_id,

   NEW_NZ.PHYS_INV_TYPE_ID,

   NEW_NZ.PLANNED_COUNT_DT,

   OLD_NZ.curr_planned_dt,

   OLD_NZ.prev_planned_dt,

   NEW_NZ.LAST_COUNT_DT,

   OLD_NZ.curr_actual_dt,

   OLD_NZ.prev_actual_dt,

   NEW_NZ.INV_DOC_NBR,

   NEW_NZ.LOAD_DT

FROM (

SELECT DISTINCT

CASE WHEN t1x.doc_posting_dt = to_date('00010101', 'yyyyMMdd')  THEN CURRENT_DATE - 1

ELSE t1x.doc_posting_dt

END AS day_dt ,

  site.location_id as location_id,

   site.store_nbr as store_nbr,

  typ.PHYS_INV_TYPE_ID as PHYS_INV_TYPE_ID,

   t1x.PLANNED_COUNT_DT as PLANNED_COUNT_DT,

   t1x.LAST_COUNT_DT as LAST_COUNT_DT,

   MAX(t1x.phys_INV_DOC_NBR) over

  (PARTITION BY t1x.store_nbr,t1x.doc_posting_dt,t1x.LAST_COUNT_DT,typ.PHYS_INV_TYPE_ID) as INV_DOC_NBR,

   CURRENT_DATE AS load_Dt

FROM {raw}.PHYS_INV_HDR_PRE t1x,

   {legacy}.PHYS_INV_TYPE typ,

   {legacy}.SITE_PROFILE site

WHERE t1x.store_nbr = site.store_nbr

 AND t1x.PHYS_INV_DESC = typ.PHYS_INV_TYPE_DESC

ORDER BY location_id,

  LAST_COUNT_DT ) NEW_NZ LEFT OUTER JOIN (

SELECT DISTINCT  location_id,

   PHYS_INV_TYPE_ID,

   MAX(CURR_PLANNED_CNT_DT) AS curr_planned_dt ,

  MAX(CURR_ACTUAL_CNT_DT) AS curr_actual_dt ,

  MAX(PREV_PLANNED_CNT_DT) AS prev_planned_dt ,

  MAX(PREV_ACTUAL_CNT_DT) AS prev_actual_dt

FROM {legacy}.PHYS_INV_CURRENT

WHERE location_id > 0

GROUP BY location_id,

  PHYS_INV_TYPE_ID) OLD_NZ ON NEW_NZ.location_id = OLD_NZ.location_id

 AND NEW_NZ.PHYS_INV_TYPE_ID = OLD_NZ.PHYS_INV_TYPE_ID

ORDER BY NEW_NZ.INV_DOC_NBR""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_PHYS_INV_HDR_PRE = SQ_Shortcut_To_PHYS_INV_HDR_PRE \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[2],'PHYS_INV_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[3],'PLANNED_CNT_DT') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[4],'CURR_PLANNED_DT') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[5],'PREV_PLANNED_DT') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[6],'ACTUAL_CNT_DT') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[7],'CURR_ACTUAL_DT') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[8],'PREV_ACTUAL_DT') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[9],'PHYS_INV_DOC_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns[10],'LOAD_DT')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_PHYS_INV_HDR_PRE_temp = SQ_Shortcut_To_PHYS_INV_HDR_PRE.toDF(*["SQ_Shortcut_To_PHYS_INV_HDR_PRE___" + col for col in SQ_Shortcut_To_PHYS_INV_HDR_PRE.columns])

is_leap_year_lambda = lambda year: 366 if ((year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)) else 365

is_leap_year_udf = udf(is_leap_year_lambda, IntegerType())

EXPTRANS = SQ_Shortcut_To_PHYS_INV_HDR_PRE_temp\
  .withColumn("NO_DATE", to_date ( lit('00010101') , 'yyyyMMdd' )) \
  .withColumn("LAST_MONTH", date_add(current_date(), -dayofmonth(trunc(current_date(), "MM")-timedelta(days=1))))\
	.withColumn("LAST_YEAR", date_add(current_date(), -is_leap_year_udf((year(trunc(current_date(), "YYYY")-timedelta(days=1)))) )).\
   selectExpr(
	"SQ_Shortcut_To_PHYS_INV_HDR_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_To_PHYS_INV_HDR_PRE___DAY_DT as DAY_DT",
	"SQ_Shortcut_To_PHYS_INV_HDR_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_To_PHYS_INV_HDR_PRE___PHYS_INV_TYPE_ID as PHYS_INV_TYPE_ID",
	"SQ_Shortcut_To_PHYS_INV_HDR_PRE___PHYS_INV_DOC_NBR as PHYS_INV_DOC_NBR",
	"SQ_Shortcut_To_PHYS_INV_HDR_PRE___LOAD_DT as LOAD_DT",
	"SQ_Shortcut_To_PHYS_INV_HDR_PRE___PLANNED_CNT_DT as PLANNED_CNT_DT",
	"SQ_Shortcut_To_PHYS_INV_HDR_PRE___ACTUAL_CNT_DT as ACTUAL_CNT_DT",
	"DECODE ( TRUE , SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_ACTUAL_DT IS NULL , IF (SQ_Shortcut_To_PHYS_INV_HDR_PRE___PHYS_INV_TYPE_ID = 1, LAST_MONTH, LAST_YEAR) , SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_ACTUAL_DT = NO_DATE , SQ_Shortcut_To_PHYS_INV_HDR_PRE___PREV_PLANNED_DT , SQ_Shortcut_To_PHYS_INV_HDR_PRE___PLANNED_CNT_DT = SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_PLANNED_DT AND SQ_Shortcut_To_PHYS_INV_HDR_PRE___ACTUAL_CNT_DT = SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_ACTUAL_DT , SQ_Shortcut_To_PHYS_INV_HDR_PRE___PREV_PLANNED_DT , SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_PLANNED_DT ) as PREV_PLANNED_CNT_DT",
	"DECODE ( TRUE , SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_ACTUAL_DT IS NULL , IF (SQ_Shortcut_To_PHYS_INV_HDR_PRE___PHYS_INV_TYPE_ID = 1, LAST_MONTH, LAST_YEAR) , SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_ACTUAL_DT = NO_DATE , SQ_Shortcut_To_PHYS_INV_HDR_PRE___PREV_ACTUAL_DT , SQ_Shortcut_To_PHYS_INV_HDR_PRE___PLANNED_CNT_DT = SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_PLANNED_DT AND SQ_Shortcut_To_PHYS_INV_HDR_PRE___ACTUAL_CNT_DT = SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_ACTUAL_DT , SQ_Shortcut_To_PHYS_INV_HDR_PRE___PREV_ACTUAL_DT , SQ_Shortcut_To_PHYS_INV_HDR_PRE___CURR_ACTUAL_DT ) as PREV_ACTUAL_CNT_DT"
)

# COMMAND ----------

# Processing node Shortcut_To_PHYS_INV_HISTORY, type TARGET 
# COLUMN COUNT: 9


Shortcut_To_PHYS_INV_HISTORY = EXPTRANS.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PHYS_INV_TYPE_ID AS TINYINT) as PHYS_INV_TYPE_ID",
	"CAST(PLANNED_CNT_DT AS TIMESTAMP) as CURR_PLANNED_CNT_DT",
	"CAST(PREV_PLANNED_CNT_DT AS TIMESTAMP) as PREV_PLANNED_CNT_DT",
	"CAST(ACTUAL_CNT_DT AS TIMESTAMP) as CURR_ACTUAL_CNT_DT",
	"CAST(PREV_ACTUAL_CNT_DT AS TIMESTAMP) as PREV_ACTUAL_CNT_DT",
	"CAST(PHYS_INV_DOC_NBR AS BIGINT) as PHYS_INV_DOC_NBR",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
#overwriteDeltaPartition(Shortcut_To_PHYS_INV_HISTORY,'DC_NBR',dcnbr,f'{raw}.PHYS_INV_HISTORY')
# Shortcut_To_PHYS_INV_HISTORY.write.mode("append").saveAsTable(f'{legacy}.PHYS_INV_HISTORY')

# COMMAND ----------

try:
  refined_perf_table = f"{legacy}.PHYS_INV_HISTORY"
  Shortcut_To_PHYS_INV_HISTORY.createOrReplaceTempView('temp_PHYS_INV_HISTORY')
  merge_sql = f"""MERGE INTO {refined_perf_table} as target
                  USING temp_PHYS_INV_HISTORY as source
                  ON source.DAY_DT = target.DAY_DT AND source.LOCATION_ID = target.LOCATION_ID AND source.PHYS_INV_TYPE_ID = target.PHYS_INV_TYPE_ID
                  WHEN MATCHED THEN
                    UPDATE SET *
                  WHEN NOT MATCHED THEN
                    INSERT *
                  """
  spark.sql(merge_sql)
except Exception as e:
  raise e

# COMMAND ----------


