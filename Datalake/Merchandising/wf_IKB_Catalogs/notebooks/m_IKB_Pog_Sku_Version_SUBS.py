# Databricks notebook source
# Code converted on 2023-08-30 11:26:02
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


# COMMAND ----------

# Processing node SQ_Shortcut_to_POG_SKU_VERSION, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_POG_SKU_VERSION = spark.sql(f"""SELECT DP.LINK_PRODUCT_ID AS PRODUCT_ID,

       PS1.POG_DBKEY,

       MAX(DP.LINK_SKU_NBR) AS SKU_NBR,

       MAX(PS1.PRODUCT_ID)  AS LINK_PRODUCT_ID,

       MAX(PS1.SKU_NBR)     AS LINK_SKU_NBR,

       NVL(MAX(I.SKU_CAPACITY_QTY),1) AS SKU_CAPACITY_QTY,

       NVL(MAX(I.SKU_FACINGS_QTY),1)  AS SKU_FACINGS_QTY,

       NVL(MAX(I.SKU_HEIGHT_IN),1)    AS SKU_HEIGHT_IN,

       NVL(MAX(I.SKU_DEPTH_IN),1)     AS SKU_DEPTH_IN,

       NVL(MAX(I.SKU_WIDTH_IN),1)     AS SKU_WIDTH_IN,

       NVL(MAX(I.UNIT_OF_MEASURE),'PC') AS UOM,

       MAX(I.LAST_CHNG_DT) AS LAST_CHNG_DT,

       CURRENT_TIMESTAMP AS UPDATE_TSTMP,

       NVL(MAX(PS2.LOAD_TSTMP), CURRENT_TIMESTAMP) AS LOAD_TSTMP,

       CASE WHEN MAX(PS2.PRODUCT_ID) IS NULL

THEN 0 /*  INSERT */
ELSE 1 /*  UPDATE */
       END AS UPDATE_FLAG

  FROM {legacy}.POG_SKU_VERSION PS1

          JOIN

       {legacy}.POG_VERSION

          ON PS1.POG_DBKEY   = POG_VERSION.POG_DBKEY

          JOIN

       (SELECT DISTINCT

               SKU_LINK_EFF_DT,

               SKU_LINK_END_DT,

               DP_SKU_LINK.PRODUCT_ID,

               LINK_PRODUCT_ID,

               SKU_PROFILE.SKU_NBR AS LINK_SKU_NBR

          FROM {legacy}.DP_SKU_LINK,

               {legacy}.SKU_PROFILE

         WHERE SKU_LINK_TYPE_CD = 'SUB'

           AND DP_SKU_LINK.LINK_PRODUCT_ID = SKU_PROFILE.PRODUCT_ID) DP

          ON PS1.PRODUCT_ID  = DP.PRODUCT_ID

             AND POG_VERSION.POG_EFFECTIVE_FROM_DT BETWEEN DP.SKU_LINK_EFF_DT AND DP.SKU_LINK_END_DT

          LEFT OUTER JOIN

       {raw}.IKB_SKU_ATTR_PRE I

          ON DP.LINK_SKU_NBR = I.SKU_NBR

          LEFT OUTER JOIN

       {legacy}.POG_SKU_VERSION PS2

          ON PS1.POG_DBKEY      = PS2.POG_DBKEY

             AND DP.LINK_PRODUCT_ID = PS2.PRODUCT_ID

 GROUP BY DP.LINK_PRODUCT_ID,

          PS1.POG_DBKEY

HAVING MAX(PS2.PRODUCT_ID) IS NULL

    OR (MAX(PS2.PRODUCT_ID) IS NOT NULL

AND MAX(PS2.PRODUCT_ID) <> MAX(PS2.LINK_SUB_PRODUCT_ID) /*  ONLY UPDATE BONUS SKU THAT DO NOT EXIST IN POG */
        AND (NVL(MAX(I.SKU_CAPACITY_QTY), 1)                          <> MAX(PS2.SKU_CAPACITY_QTY)

             OR NVL(MAX(I.SKU_FACINGS_QTY), 1)                        <> MAX(PS2.SKU_FACINGS_QTY)

             OR NVL(MAX(I.SKU_HEIGHT_IN), 1)                          <> MAX(PS2.SKU_HEIGHT_IN)

             OR NVL(MAX(I.SKU_DEPTH_IN), 1)                           <> MAX(PS2.SKU_DEPTH_IN)

             OR NVL(MAX(I.SKU_WIDTH_IN), 1)                           <> MAX(PS2.SKU_WIDTH_IN)

             OR NVL(MAX(I.UNIT_OF_MEASURE), 'PC')                     <> MAX(PS2.UNIT_OF_MEASURE)

             OR NVL(MAX(I.LAST_CHNG_DT), CAST('12/31/9999' AS DATE))  <> NVL(MAX(PS2.LAST_CHNG_DT), CAST('12/31/9999' AS DATE))))""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_POG_SKU_VERSION = SQ_Shortcut_to_POG_SKU_VERSION \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[1],'POG_DBKEY') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[2],'SKU_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[3],'LINK_PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[4],'LINK_SKU_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[5],'SKU_CAPACITY_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[6],'SKU_FACINGS_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[7],'SKU_HEIGHT_IN') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[8],'SKU_DEPTH_IN') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[9],'SKU_WIDTH_IN') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[10],'UNIT_OF_MEASURE') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[11],'LAST_CHNG_DT') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[12],'UPDATE_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[13],'LOAD_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_POG_SKU_VERSION.columns[14],'UPDATE_FLAG')

# COMMAND ----------

# Processing node UPD_Ins_Upd, type UPDATE_STRATEGY 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_POG_SKU_VERSION_temp = SQ_Shortcut_to_POG_SKU_VERSION.toDF(*["SQ_Shortcut_to_POG_SKU_VERSION___" + col for col in SQ_Shortcut_to_POG_SKU_VERSION.columns])

UPD_Ins_Upd = SQ_Shortcut_to_POG_SKU_VERSION_temp.selectExpr(
	"SQ_Shortcut_to_POG_SKU_VERSION___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_POG_SKU_VERSION___POG_DBKEY as POG_DBKEY",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_NBR as SKU_NBR",
	"SQ_Shortcut_to_POG_SKU_VERSION___LINK_PRODUCT_ID as LINK_PRODUCT_ID",
	"SQ_Shortcut_to_POG_SKU_VERSION___LINK_SKU_NBR as LINK_SKU_NBR",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_CAPACITY_QTY as SKU_CAPACITY_QTY",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_FACINGS_QTY as SKU_FACINGS_QTY",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_HEIGHT_IN as SKU_HEIGHT_IN",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_DEPTH_IN as SKU_DEPTH_IN",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_WIDTH_IN as SKU_WIDTH_IN",
	"SQ_Shortcut_to_POG_SKU_VERSION___UNIT_OF_MEASURE as UNIT_OF_MEASURE",
	"SQ_Shortcut_to_POG_SKU_VERSION___LAST_CHNG_DT as LAST_CHNG_DT",
	"SQ_Shortcut_to_POG_SKU_VERSION___UPDATE_TSTMP as UPDATE_TSTMP",
	"SQ_Shortcut_to_POG_SKU_VERSION___LOAD_TSTMP as LOAD_TSTMP",
	"SQ_Shortcut_to_POG_SKU_VERSION___UPDATE_FLAG as UPDATE_FLAG",
 	"if(SQ_Shortcut_to_POG_SKU_VERSION___UPDATE_FLAG == 0, 0, 1) as pyspark_data_action")

# COMMAND ----------

# Processing node Shortcut_to_POG_SKU_VERSION2, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_POG_SKU_VERSION2 = UPD_Ins_Upd.selectExpr(
	"CAST(PRODUCT_ID AS BIGINT) as PRODUCT_ID",
	"CAST(POG_DBKEY AS BIGINT) as POG_DBKEY",
	"CAST(SKU_NBR AS BIGINT) as SKU_NBR",
	"CAST(LINK_PRODUCT_ID AS BIGINT) as LINK_SUB_PRODUCT_ID",
	"CAST(LINK_SKU_NBR AS BIGINT) as LINK_SUB_SKU_NBR",
	"CAST(SKU_CAPACITY_QTY AS INT) as SKU_CAPACITY_QTY",
	"CAST(SKU_FACINGS_QTY AS INT) as SKU_FACINGS_QTY",
	"CAST(SKU_HEIGHT_IN AS DECIMAL(7,2)) as SKU_HEIGHT_IN",
	"CAST(SKU_DEPTH_IN AS DECIMAL(7,2)) as SKU_DEPTH_IN",
	"CAST(SKU_WIDTH_IN AS DECIMAL(7,2)) as SKU_WIDTH_IN",
	"CAST(UNIT_OF_MEASURE AS STRING) as UNIT_OF_MEASURE",
	"CAST(LAST_CHNG_DT AS DATE) as LAST_CHNG_DT",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.PRODUCT_ID = target.PRODUCT_ID AND source.POG_DBKEY = target.POG_DBKEY"""
	refined_perf_table = f"{legacy}.POG_SKU_VERSION"
	executeMerge(Shortcut_to_POG_SKU_VERSION2, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("POG_SKU_VERSION", "POG_SKU_VERSION", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("POG_SKU_VERSION", "POG_SKU_VERSION","Failed",str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


