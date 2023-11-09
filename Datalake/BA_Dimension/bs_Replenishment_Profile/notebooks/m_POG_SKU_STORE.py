# Databricks notebook source
#Code converted on 2023-09-27 16:52:01
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script

# COMMAND ----------

# Processing node SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE = spark.sql(f"""SELECT z.POG_ID

	,z.POG_DBKEY

	,z.EFFECTIVE_DT

	,z.EFFECTIVE_ENDT

	,z.NUMBER_OF_POSITIONS

	,z.NUMBER_OF_FACINGS

	,z.CAPACITY

	,z.PRESENTATION_QUANTITY

	,z.POSITION_STATUS

	,z.POG_TYPE

	,z.LAST_CHG_DATE

	,z.LAST_CHG_TIME

	,s.LOCATION_ID

	,i.PRODUCT_ID

	,t.PRODUCT_ID as PRODUCT_ID1

	,t.SAP_LAST_CHANGE_TSTMP

	,t.LOAD_TSTMP

FROM {raw}.ZTB_ART_LOC_SITE_PRE z

Join {legacy}.SITE_PROFILE s

 on z.SITE = s.STORE_NBR

Join {legacy}.SKU_PROFILE i

 on z.ARTICLE = i.SKU_NBR

Left Join {legacy}.POG_SKU_STORE t

 on t.PRODUCT_ID=i.PRODUCT_ID

 and t.LOCATION_ID=s.LOCATION_ID

 and t.POG_NBR=z.POG_ID

 and t.POG_DBKEY=z.POG_DBKEY

 and t.LISTING_START_DT=z.EFFECTIVE_DT""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE = SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[0],'POG_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[1],'POG_DBKEY') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[2],'EFFECTIVE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[3],'EFFECTIVE_ENDT') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[4],'NUMBER_OF_POSITIONS') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[5],'NUMBER_OF_FACINGS') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[6],'CAPACITY') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[7],'PRESENTATION_QUANTITY') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[8],'POSITION_STATUS') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[9],'POG_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[10],'LAST_CHG_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[11],'LAST_CHG_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[12],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[13],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[14],'PRODUCT_ID1') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[15],'SAP_LAST_CHANGE_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns[16],'LOAD_TSTMP')

# COMMAND ----------

# Processing node EXP_INS_UPD_Flag, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE_temp = SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.toDF(*["SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___" + col for col in SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE.columns])

EXP_INS_UPD_Flag = SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE_temp.selectExpr(
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___PRODUCT_ID1 as TGT_PRODUCT_ID",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___SAP_LAST_CHANGE_TSTMP as TGT_SAP_LAST_CHANGE_TSTMP",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___LOAD_TSTMP as TGT_LOAD_TSTMP",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___LAST_CHG_DATE as LAST_CHG_DATE",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___LAST_CHG_TIME as LAST_CHG_TIME",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___POG_ID as POG_ID",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___POG_DBKEY as POG_DBKEY",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___EFFECTIVE_DT as EFFECTIVE_DT",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___EFFECTIVE_ENDT as EFFECTIVE_ENDT",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___NUMBER_OF_POSITIONS as NUMBER_OF_POSITIONS",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___NUMBER_OF_FACINGS as NUMBER_OF_FACINGS",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___CAPACITY as CAPACITY",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___PRESENTATION_QUANTITY as PRESENTATION_QUANTITY",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___POSITION_STATUS as POSITION_STATUS",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___POG_TYPE as POG_TYPE",
	"SQ_Shortcut_to_ZTB_ART_LOC_SITE_PRE___sys_row_id as sys_row_id",
   ).selectExpr(
	"sys_row_id as sys_row_id",
	"PRODUCT_ID as PRODUCT_ID",
	"LOCATION_ID as LOCATION_ID",
	"POG_ID as POG_ID",
	"POG_DBKEY as POG_DBKEY",
	"EFFECTIVE_DT as EFFECTIVE_DT",
	"EFFECTIVE_ENDT as EFFECTIVE_ENDT",
	"NUMBER_OF_POSITIONS as NUMBER_OF_POSITIONS",
	"NUMBER_OF_FACINGS as NUMBER_OF_FACINGS",
	"CAPACITY as CAPACITY",
	"PRESENTATION_QUANTITY as PRESENTATION_QUANTITY",
	"POSITION_STATUS as POSITION_STATUS",
	"POG_TYPE as POG_TYPE",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (IF (TGT_PRODUCT_ID IS NULL, 'I', IF (TO_TIMESTAMP (concat( date_format(LAST_CHG_DATE, 'yyyy-MM-dd') , ' ' , LAST_CHG_TIME ), 'yyyy-MM-dd Hmmss' ) != TGT_SAP_LAST_CHANGE_TSTMP, 'U', 'R')) = 'I', CURRENT_TIMESTAMP, TGT_LOAD_TSTMP) as LOAD_TSTMP",
	"0 as DELETE_FLAG",
  	"TO_TIMESTAMP (concat( date_format(LAST_CHG_DATE, 'yyyy-MM-dd') , ' ' , LAST_CHG_TIME ) , 'yyyy-MM-dd Hmmss' ) as o_SAP_LAST_CHANGE_TSTMP",
    "IF (TGT_PRODUCT_ID IS NULL, 'I', IF (TO_TIMESTAMP (concat( date_format(LAST_CHG_DATE, 'yyyy-MM-dd') , ' ' , LAST_CHG_TIME ) , 'yyyy-MM-dd Hmmss' ) != TGT_SAP_LAST_CHANGE_TSTMP, 'U', 'R')) as o_INS_UPD_FLAG"
)

# .withColumn("v_SAP_LAST_CHANGE_TSTMP", TO_DATE (concat( date_format(LAST_CHG_DATE, 'MM/DD/YYYY') , ' ' , LAST_CHG_TIME ), 'MM/DD/YYYY HH24MISS' )) \
# .withColumn("v_INS_UPD_FLAG", expr("""IF (TGT_PRODUCT_ID IS NULL, 'I', IF (v_SAP_LAST_CHANGE_TSTMP != TGT_SAP_LAST_CHANGE_TSTMP, 'U', 'R'))"""))

# COMMAND ----------

# Processing node FIL_Rejects, type FILTER 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
EXP_INS_UPD_Flag_temp = EXP_INS_UPD_Flag.toDF(*["EXP_INS_UPD_Flag___" + col for col in EXP_INS_UPD_Flag.columns])

FIL_Rejects = EXP_INS_UPD_Flag_temp.selectExpr(
	"EXP_INS_UPD_Flag___PRODUCT_ID as PRODUCT_ID",
	"EXP_INS_UPD_Flag___LOCATION_ID as LOCATION_ID",
	"EXP_INS_UPD_Flag___POG_ID as POG_ID",
	"EXP_INS_UPD_Flag___POG_DBKEY as POG_DBKEY",
	"EXP_INS_UPD_Flag___EFFECTIVE_DT as EFFECTIVE_DT",
	"EXP_INS_UPD_Flag___EFFECTIVE_ENDT as EFFECTIVE_ENDT",
	"EXP_INS_UPD_Flag___NUMBER_OF_POSITIONS as NUMBER_OF_POSITIONS",
	"EXP_INS_UPD_Flag___NUMBER_OF_FACINGS as NUMBER_OF_FACINGS",
	"EXP_INS_UPD_Flag___CAPACITY as CAPACITY",
	"EXP_INS_UPD_Flag___PRESENTATION_QUANTITY as PRESENTATION_QUANTITY",
	"EXP_INS_UPD_Flag___POSITION_STATUS as POSITION_STATUS",
	"EXP_INS_UPD_Flag___POG_TYPE as POG_TYPE",
	"EXP_INS_UPD_Flag___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_INS_UPD_Flag___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_INS_UPD_Flag___DELETE_FLAG as DELETE_FLAG",
	"EXP_INS_UPD_Flag___o_SAP_LAST_CHANGE_TSTMP as o_SAP_LAST_CHANGE_TSTMP",
	"EXP_INS_UPD_Flag___o_INS_UPD_FLAG as o_INS_UPD_FLAG").filter("o_INS_UPD_FLAG != 'R'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
FIL_Rejects_temp = FIL_Rejects.toDF(*["FIL_Rejects___" + col for col in FIL_Rejects.columns])

UPD_INS_UPD = FIL_Rejects_temp.selectExpr(
	"FIL_Rejects___PRODUCT_ID as PRODUCT_ID",
	"FIL_Rejects___LOCATION_ID as LOCATION_ID",
	"FIL_Rejects___POG_ID as POG_ID",
	"FIL_Rejects___POG_DBKEY as POG_DBKEY",
	"FIL_Rejects___EFFECTIVE_DT as EFFECTIVE_DT",
	"FIL_Rejects___EFFECTIVE_ENDT as EFFECTIVE_ENDT",
	"FIL_Rejects___NUMBER_OF_POSITIONS as NUMBER_OF_POSITIONS",
	"FIL_Rejects___NUMBER_OF_FACINGS as NUMBER_OF_FACINGS",
	"FIL_Rejects___CAPACITY as CAPACITY",
	"FIL_Rejects___PRESENTATION_QUANTITY as PRESENTATION_QUANTITY",
	"FIL_Rejects___POSITION_STATUS as POSITION_STATUS",
	"FIL_Rejects___POG_TYPE as POG_TYPE",
	"FIL_Rejects___UPDATE_TSTMP as UPDATE_TSTMP",
	"FIL_Rejects___LOAD_TSTMP as LOAD_TSTMP",
	"FIL_Rejects___DELETE_FLAG as DELETE_FLAG",
	"FIL_Rejects___o_SAP_LAST_CHANGE_TSTMP as o_SAP_LAST_CHANGE_TSTMP",
	"FIL_Rejects___o_INS_UPD_FLAG as o_INS_UPD_FLAG") \
	.withColumn('pyspark_data_action', when(col('o_INS_UPD_FLAG') ==(lit('I')) , lit(0)) .when(col('o_INS_UPD_FLAG') ==(lit('U')) , lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_POG_SKU_STORE, type TARGET 
# COLUMN COUNT: 16


Shortcut_to_POG_SKU_STORE = UPD_INS_UPD.selectExpr(
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(POG_ID AS STRING) as POG_NBR",
	"CAST(POG_DBKEY AS INT) as POG_DBKEY",
	"CAST(EFFECTIVE_DT AS DATE) as LISTING_START_DT",
	"CAST(EFFECTIVE_ENDT AS DATE) as LISTING_END_DT",
	"CAST(NUMBER_OF_POSITIONS AS INT) as POSITIONS_CNT",
	"CAST(NUMBER_OF_FACINGS AS INT) as FACINGS_CNT",
	"CAST(CAPACITY AS INT) as CAPACITY_CNT",
	"CAST(PRESENTATION_QUANTITY AS INT) as PRESENTATION_QTY",
	"CAST(POG_TYPE AS STRING) as POG_TYPE_CD",
	"CAST(POSITION_STATUS AS TINYINT) as POG_SKU_POSITION_STATUS_ID",
	"CAST(DELETE_FLAG AS TINYINT) as DELETE_FLAG",
	"CAST(o_SAP_LAST_CHANGE_TSTMP AS TIMESTAMP) as SAP_LAST_CHANGE_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)
# Shortcut_to_POG_SKU_STORE.write.saveAsTable(f'{refine}.POG_SKU_STORE', mode = 'overwrite')
try:
  primary_key = """source.PRODUCT_ID = target.PRODUCT_ID AND source.LOCATION_ID = target.LOCATION_ID AND source.POG_NBR = target.POG_NBR AND source.POG_DBKEY = target.POG_DBKEY AND source.LISTING_START_DT = target.LISTING_START_DT"""
  refined_perf_table = f"{legacy}.POG_SKU_STORE"
  executeMerge(Shortcut_to_POG_SKU_STORE, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("POG_SKU_STORE", "POG_SKU_STORE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("POG_SKU_STORE", "POG_SKU_STORE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------


