# Databricks notebook source
#Code converted on 2023-09-26 09:20:18
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

# Processing node SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC = spark.sql(f"""
SELECT
    POSTING_DT,
    LOCATION_ID,
    PRODUCT_ID,
    OVERSTOCK_LOC_NBR,
    LOAD_TSTMP
FROM {legacy}.INV_PHYS_OVERSTOCK_LOC""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE = spark.sql(f"""
SELECT
    POSTING_DT,
    r.LOCATION_ID,
    r.PRODUCT_ID,
    OVERSTOCK_LOC_NBR,
    OVERSTOCK_LOC_TYPE_ID,
    PLANNED_DT,
    COUNT_QTY,
    POG_LISTED_IND
FROM {raw}.INV_PHYS_OVERSTOCK_LOC_PRE r, {legacy}.INV_INSTOCK_PRICE_DAY l
WHERE l.DAY_DT = r.POSTING_DT
  and l.PRODUCT_ID = r.PRODUCT_ID
  and l.LOCATION_ID = r.LOCATION_ID""").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node FIL_TARGET_TABLE, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_temp = SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC.toDF(*["SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC___" + col for col in SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC.columns])

FIL_TARGET_TABLE = SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_temp.selectExpr(
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC___POSTING_DT as POSTING_DT",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC___OVERSTOCK_LOC_NBR as OVERSTOCK_LOC_NBR",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC___LOAD_TSTMP as LOAD_TSTMP").filter("POSTING_DT > DATE_ADD(CURRENT_DATE, -365)").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_INV_PHYS_OVERSTOCK_LOC, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
FIL_TARGET_TABLE_temp = FIL_TARGET_TABLE.toDF(*["FIL_TARGET_TABLE___" + col for col in FIL_TARGET_TABLE.columns])
SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE_temp = SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE.toDF(*["SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___" + col for col in SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE.columns])

JNR_INV_PHYS_OVERSTOCK_LOC = FIL_TARGET_TABLE_temp.join(SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE_temp,[FIL_TARGET_TABLE_temp.FIL_TARGET_TABLE___POSTING_DT == SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE_temp.SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___POSTING_DT, FIL_TARGET_TABLE_temp.FIL_TARGET_TABLE___LOCATION_ID == SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE_temp.SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___LOCATION_ID, FIL_TARGET_TABLE_temp.FIL_TARGET_TABLE___PRODUCT_ID == SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE_temp.SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___PRODUCT_ID, FIL_TARGET_TABLE_temp.FIL_TARGET_TABLE___OVERSTOCK_LOC_NBR == SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE_temp.SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___OVERSTOCK_LOC_NBR],'right_outer').selectExpr(
  "SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___sys_row_id AS sys_row_id",
	"FIL_TARGET_TABLE___POSTING_DT as target_POSTING_DT",
	"FIL_TARGET_TABLE___LOCATION_ID as target_LOCATION_ID",
	"FIL_TARGET_TABLE___PRODUCT_ID as target_PRODUCT_ID",
	"FIL_TARGET_TABLE___OVERSTOCK_LOC_NBR as target_OVERSTOCK_LOC_NBR",
	"FIL_TARGET_TABLE___LOAD_TSTMP as target_LOAD_TSTMP",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___POSTING_DT as POSTING_DT",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___OVERSTOCK_LOC_NBR as OVERSTOCK_LOC_NBR",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___OVERSTOCK_LOC_TYPE_ID as OVERSTOCK_LOC_TYPE_ID",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___PLANNED_DT as PLANNED_DT",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___COUNT_QTY as COUNT_QTY",
	"SQ_Shortcut_to_INV_PHYS_OVERSTOCK_LOC_PRE___POG_LISTED_IND as POG_LISTED_IND")

# COMMAND ----------

# Processing node EXP_INV_PHYS_OVERSTOCK_LOC, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_INV_PHYS_OVERSTOCK_LOC_temp = JNR_INV_PHYS_OVERSTOCK_LOC.toDF(*["JNR_INV_PHYS_OVERSTOCK_LOC___" + col for col in JNR_INV_PHYS_OVERSTOCK_LOC.columns])

EXP_INV_PHYS_OVERSTOCK_LOC = JNR_INV_PHYS_OVERSTOCK_LOC_temp.selectExpr(
	"JNR_INV_PHYS_OVERSTOCK_LOC___sys_row_id as sys_row_id",
	"JNR_INV_PHYS_OVERSTOCK_LOC___POSTING_DT as POSTING_DT",
	"JNR_INV_PHYS_OVERSTOCK_LOC___LOCATION_ID as LOCATION_ID",
	"JNR_INV_PHYS_OVERSTOCK_LOC___PRODUCT_ID as PRODUCT_ID",
	"JNR_INV_PHYS_OVERSTOCK_LOC___OVERSTOCK_LOC_NBR as OVERSTOCK_LOC_NBR",
	"JNR_INV_PHYS_OVERSTOCK_LOC___OVERSTOCK_LOC_TYPE_ID as OVERSTOCK_LOC_TYPE_ID",
	"JNR_INV_PHYS_OVERSTOCK_LOC___PLANNED_DT as PLANNED_DT",
	"JNR_INV_PHYS_OVERSTOCK_LOC___COUNT_QTY as COUNT_QTY",
	"JNR_INV_PHYS_OVERSTOCK_LOC___target_LOAD_TSTMP as target_LOAD_TSTMP",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF(JNR_INV_PHYS_OVERSTOCK_LOC___target_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNR_INV_PHYS_OVERSTOCK_LOC___target_LOAD_TSTMP) as LOAD_TSTMP",
	"JNR_INV_PHYS_OVERSTOCK_LOC___POG_LISTED_IND as POG_LISTED_IND"
)

# COMMAND ----------

# Processing node UPD_INV_PHYS_OVERSTOCK_LOC, type UPDATE_STRATEGY 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
EXP_INV_PHYS_OVERSTOCK_LOC_temp = EXP_INV_PHYS_OVERSTOCK_LOC.toDF(*["EXP_INV_PHYS_OVERSTOCK_LOC___" + col for col in EXP_INV_PHYS_OVERSTOCK_LOC.columns])

UPD_INV_PHYS_OVERSTOCK_LOC = EXP_INV_PHYS_OVERSTOCK_LOC_temp.selectExpr(
	"EXP_INV_PHYS_OVERSTOCK_LOC___POSTING_DT as POSTING_DT",
	"EXP_INV_PHYS_OVERSTOCK_LOC___LOCATION_ID as LOCATION_ID",
	"EXP_INV_PHYS_OVERSTOCK_LOC___PRODUCT_ID as PRODUCT_ID",
	"EXP_INV_PHYS_OVERSTOCK_LOC___OVERSTOCK_LOC_NBR as OVERSTOCK_LOC_NBR",
	"EXP_INV_PHYS_OVERSTOCK_LOC___OVERSTOCK_LOC_TYPE_ID as OVERSTOCK_LOC_TYPE_ID",
	"EXP_INV_PHYS_OVERSTOCK_LOC___PLANNED_DT as PLANNED_DT",
	"EXP_INV_PHYS_OVERSTOCK_LOC___COUNT_QTY as COUNT_QTY",
	"EXP_INV_PHYS_OVERSTOCK_LOC___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_INV_PHYS_OVERSTOCK_LOC___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_INV_PHYS_OVERSTOCK_LOC___target_LOAD_TSTMP as target_LOAD_TSTMP",
	"EXP_INV_PHYS_OVERSTOCK_LOC___POG_LISTED_IND as POG_LISTED_IND").withColumn('pyspark_data_action', when((col("target_LOAD_TSTMP").isNull()), (lit(0))).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_INV_PHYS_OVERSTOCK_LOC1, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_INV_PHYS_OVERSTOCK_LOC1 = UPD_INV_PHYS_OVERSTOCK_LOC.selectExpr(
	"CAST(POSTING_DT AS DATE) as POSTING_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(OVERSTOCK_LOC_NBR AS INT) as OVERSTOCK_LOC_NBR",
	"CAST(OVERSTOCK_LOC_TYPE_ID AS SMALLINT) as OVERSTOCK_LOC_TYPE_ID",
	"CAST(PLANNED_DT AS DATE) as PLANNED_DT",
	"CAST(COUNT_QTY AS INT) as COUNT_QTY",
	"CAST(POG_LISTED_IND AS INT) as POG_LISTED_IND",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.POSTING_DT = target.POSTING_DT AND source.LOCATION_ID = target.LOCATION_ID AND source.PRODUCT_ID = target.PRODUCT_ID AND source.OVERSTOCK_LOC_NBR = target.OVERSTOCK_LOC_NBR"""
	refined_perf_table = f"{legacy}.INV_PHYS_OVERSTOCK_LOC"
	executeMerge(Shortcut_to_INV_PHYS_OVERSTOCK_LOC1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("INV_PHYS_OVERSTOCK_LOC", "INV_PHYS_OVERSTOCK_LOC", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("INV_PHYS_OVERSTOCK_LOC", "INV_PHYS_OVERSTOCK_LOC","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		
