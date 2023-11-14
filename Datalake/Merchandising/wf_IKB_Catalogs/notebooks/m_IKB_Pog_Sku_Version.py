# Databricks notebook source
# Code converted on 2023-08-30 11:25:55
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

(username,password,connection_string) = ckb_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE = spark.sql(f"""SELECT
SKU_PROFILE.SKU_NBR,
SKU_PROFILE.PRODUCT_ID
FROM {legacy}.SKU_PROFILE
ORDER BY 1""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_POG_SKU_VERSION, type SOURCE 
# COLUMN COUNT: 10

SQ_Shortcut_to_POG_SKU_VERSION = spark.sql(f"""SELECT
POG_SKU_VERSION.POG_DBKEY as POG_DB_KEY ,
POG_SKU_VERSION.PRODUCT_ID,
POG_SKU_VERSION.SKU_CAPACITY_QTY,
POG_SKU_VERSION.SKU_FACINGS_QTY,
POG_SKU_VERSION.SKU_HEIGHT_IN,
POG_SKU_VERSION.SKU_DEPTH_IN,
POG_SKU_VERSION.SKU_WIDTH_IN,
POG_SKU_VERSION.UNIT_OF_MEASURE,
POG_SKU_VERSION.LAST_CHNG_DT,
POG_SKU_VERSION.LOAD_TSTMP
FROM {legacy}.POG_SKU_VERSION
ORDER BY 1,2""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_POG_VERSION, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_POG_VERSION = spark.sql(f"""SELECT
POG_VERSION.POG_DBKEY as POG_DB_KEY 
FROM {legacy}.POG_VERSION
ORDER BY 1""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_ix_spc_product, type SOURCE 
# COLUMN COUNT: 9

SQ_ix_spc_product = jdbcSqlServerConnection(f"""(SELECT
ix_spc_product.ID,
ix_spc_performance.Capacity,
ix_spc_performance.Facings,
ix_spc_product.Height,
ix_spc_product.Depth,
ix_spc_product.Width,
ix_spc_product.UOM,
ix_spc_product.DateModified,
ix_spc_performance.DBParentPlanogramKey
FROM CKB_PRD.dbo.ix_spc_performance, CKB_PRD.dbo.ix_spc_product
WHERE ix_spc_performance.DBParentProductKey = ix_spc_product.DBKey
AND ix_spc_performance.Facings > 0
AND
ix_spc_product.ID < 'A') as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Datatypes, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_ix_spc_product_temp = SQ_ix_spc_product.toDF(*["SQ_ix_spc_product___" + col for col in SQ_ix_spc_product.columns])

SRT_ID = SQ_ix_spc_product_temp.selectExpr(
	# "SQ_ix_spc_product___ID as i_ID",
	# "SQ_ix_spc_product___Capacity as i_Capacity",
	# "SQ_ix_spc_product___Facings as i_Facings",
	# "SQ_ix_spc_product___Height as i_Height",
	# "SQ_ix_spc_product___Depth as i_Depth",
	# "SQ_ix_spc_product___Width as i_Width",
	# "SQ_ix_spc_product___UOM as UNIT_OF_MEASURE",
	# "SQ_ix_spc_product___DateModified as LAST_CHNG_DT",
	# "SQ_ix_spc_product___DBParentPlanogramKey as POG_DB_KEY").selectExpr(
	# "SQ_ix_spc_product___sys_row_id as sys_row_id",
	"cast(SQ_ix_spc_product___ID as int) as ID",
	"ROUND(SQ_ix_spc_product___Capacity, 0) as SKU_CAPACITY_QTY",
	"ROUND(SQ_ix_spc_product___Facings, 0) as SKU_FACINGS_QTY",
	"CAST(SQ_ix_spc_product___Height AS DECIMAL(7,2)) as SKU_HEIGHT_IN",
	"CAST(SQ_ix_spc_product___Depth AS DECIMAL(7,2)) as SKU_DEPTH_IN",
	"CAST(SQ_ix_spc_product___Width AS DECIMAL(7,2)) as SKU_WIDTH_IN",
	"SQ_ix_spc_product___UOM as UNIT_OF_MEASURE",
	"SQ_ix_spc_product___DateModified as LAST_CHNG_DT",
	"SQ_ix_spc_product___DBParentPlanogramKey as POG_DB_KEY"
).sort(col('ID').asc())

# COMMAND ----------

# Processing node SRT_ID, type SORTER 
# COLUMN COUNT: 9

# SRT_ID = EXP_Datatypes.select("EXP_Datatypes.ID as ID"
# ).sort(col('ID').asc())

# COMMAND ----------

# Processing node JNR_SKU_PROFILE, type JOINER 
# COLUMN COUNT: 11

SRT_POG_DB_KEY = SQ_Shortcut_to_SKU_PROFILE.join(SRT_ID,[SQ_Shortcut_to_SKU_PROFILE.SKU_NBR == SRT_ID.ID],'inner').sort(col('POG_DB_KEY').asc())

# COMMAND ----------

# Processing node SRT_POG_DB_KEY, type SORTER 
# COLUMN COUNT: 10

# SRT_POG_DB_KEY = JNR_SKU_PROFILE.select(
# ).sort(col('POG_DB_KEY').asc())

# COMMAND ----------

# Processing node JNR_POG_VERSION, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_POG_VERSION_temp = SQ_Shortcut_to_POG_VERSION.toDF(*["SQ_Shortcut_to_POG_VERSION___" + col for col in SQ_Shortcut_to_POG_VERSION.columns])
SRT_POG_DB_KEY_temp = SRT_POG_DB_KEY.toDF(*["SRT_POG_DB_KEY___" + col for col in SRT_POG_DB_KEY.columns])

SRT_POG_DB_KEY_PRODUCT_ID = SQ_Shortcut_to_POG_VERSION_temp.join(SRT_POG_DB_KEY_temp,[SQ_Shortcut_to_POG_VERSION_temp.SQ_Shortcut_to_POG_VERSION___POG_DB_KEY == SRT_POG_DB_KEY_temp.SRT_POG_DB_KEY___POG_DB_KEY],'inner').selectExpr(
	"SRT_POG_DB_KEY___PRODUCT_ID as PRODUCT_ID",
	"SRT_POG_DB_KEY___SKU_CAPACITY_QTY as SKU_CAPACITY_QTY",
	"SRT_POG_DB_KEY___SKU_FACINGS_QTY as SKU_FACINGS_QTY",
	"SRT_POG_DB_KEY___SKU_HEIGHT_IN as SKU_HEIGHT_IN",
	"SRT_POG_DB_KEY___SKU_DEPTH_IN as SKU_DEPTH_IN",
	"SRT_POG_DB_KEY___SKU_WIDTH_IN as SKU_WIDTH_IN",
	"SRT_POG_DB_KEY___UNIT_OF_MEASURE as UNIT_OF_MEASURE",
	"SRT_POG_DB_KEY___LAST_CHNG_DT as LAST_CHNG_DT",
	"SRT_POG_DB_KEY___POG_DB_KEY as POG_DB_KEY",
	"SQ_Shortcut_to_POG_VERSION___POG_DB_KEY as POG_VERSION_POG_DB_KEY",
	"SRT_POG_DB_KEY___SKU_NBR as SKU_NBR").sort(col('POG_DB_KEY').asc(), col('PRODUCT_ID').asc())

# COMMAND ----------

# Processing node SRT_POG_DB_KEY_PRODUCT_ID, type SORTER 
# COLUMN COUNT: 10

# SRT_POG_DB_KEY_PRODUCT_ID = JNR_POG_VERSION.select(
# ).sort(col('POG_DB_KEY').asc(), col('PRODUCT_ID').asc())

# COMMAND ----------

# Processing node JNR_POG_SKU_VERSION, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
SRT_POG_DB_KEY_PRODUCT_ID_temp = SRT_POG_DB_KEY_PRODUCT_ID.toDF(*["SRT_POG_DB_KEY_PRODUCT_ID___" + col for col in SRT_POG_DB_KEY_PRODUCT_ID.columns])
SQ_Shortcut_to_POG_SKU_VERSION_temp = SQ_Shortcut_to_POG_SKU_VERSION.toDF(*["SQ_Shortcut_to_POG_SKU_VERSION___" + col for col in SQ_Shortcut_to_POG_SKU_VERSION.columns])

JNR_POG_SKU_VERSION = SRT_POG_DB_KEY_PRODUCT_ID_temp.join(SQ_Shortcut_to_POG_SKU_VERSION_temp,[SRT_POG_DB_KEY_PRODUCT_ID_temp.SRT_POG_DB_KEY_PRODUCT_ID___POG_DB_KEY == SQ_Shortcut_to_POG_SKU_VERSION_temp.SQ_Shortcut_to_POG_SKU_VERSION___POG_DB_KEY, SRT_POG_DB_KEY_PRODUCT_ID_temp.SRT_POG_DB_KEY_PRODUCT_ID___PRODUCT_ID == SQ_Shortcut_to_POG_SKU_VERSION_temp.SQ_Shortcut_to_POG_SKU_VERSION___PRODUCT_ID],'left_outer').selectExpr(
	"SRT_POG_DB_KEY_PRODUCT_ID___POG_DB_KEY as POG_DB_KEY",
	"SRT_POG_DB_KEY_PRODUCT_ID___PRODUCT_ID as PRODUCT_ID",
	"SRT_POG_DB_KEY_PRODUCT_ID___SKU_CAPACITY_QTY as SKU_CAPACITY_QTY",
	"SRT_POG_DB_KEY_PRODUCT_ID___SKU_FACINGS_QTY as SKU_FACINGS_QTY",
	"SRT_POG_DB_KEY_PRODUCT_ID___SKU_HEIGHT_IN as SKU_HEIGHT_IN",
	"SRT_POG_DB_KEY_PRODUCT_ID___SKU_DEPTH_IN as SKU_DEPTH_IN",
	"SRT_POG_DB_KEY_PRODUCT_ID___SKU_WIDTH_IN as SKU_WIDTH_IN",
	"SRT_POG_DB_KEY_PRODUCT_ID___UNIT_OF_MEASURE as UNIT_OF_MEASURE",
	"SRT_POG_DB_KEY_PRODUCT_ID___LAST_CHNG_DT as LAST_CHNG_DT",
	"SQ_Shortcut_to_POG_SKU_VERSION___POG_DB_KEY as lkp_POG_DB_KEY",
	"SQ_Shortcut_to_POG_SKU_VERSION___PRODUCT_ID as lkp_PRODUCT_ID",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_CAPACITY_QTY as lkp_SKU_CAPACITY_QTY",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_FACINGS_QTY as lkp_SKU_FACINGS_QTY",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_HEIGHT_IN as lkp_SKU_HEIGHT_IN",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_DEPTH_IN as lkp_SKU_DEPTH_IN",
	"SQ_Shortcut_to_POG_SKU_VERSION___SKU_WIDTH_IN as lkp_SKU_WIDTH_IN",
	"SQ_Shortcut_to_POG_SKU_VERSION___UNIT_OF_MEASURE as lkp_UNIT_OF_MEASURE",
	"SQ_Shortcut_to_POG_SKU_VERSION___LAST_CHNG_DT as lkp_LAST_CHNG_DT",
	"SQ_Shortcut_to_POG_SKU_VERSION___LOAD_TSTMP as lkp_LOAD_TSTMP",
	"SRT_POG_DB_KEY_PRODUCT_ID___SKU_NBR as SKU_NBR")

# COMMAND ----------

# Processing node EXP_change_flag, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
JNR_POG_SKU_VERSION_temp = JNR_POG_SKU_VERSION.toDF(*["JNR_POG_SKU_VERSION___" + col for col in JNR_POG_SKU_VERSION.columns])

EXP_change_flag = JNR_POG_SKU_VERSION_temp.selectExpr(
	# "JNR_POG_SKU_VERSION___lkp_PRODUCT_ID as lkp_PRODUCT_ID",
	# "JNR_POG_SKU_VERSION___PRODUCT_ID as PRODUCT_ID",
	# "JNR_POG_SKU_VERSION___SKU_CAPACITY_QTY as SKU_CAPACITY_QTY1",
	# "JNR_POG_SKU_VERSION___SKU_FACINGS_QTY as SKU_FACINGS_QTY1",
	# "JNR_POG_SKU_VERSION___SKU_HEIGHT_IN as SKU_HEIGHT_IN1",
	# "JNR_POG_SKU_VERSION___SKU_DEPTH_IN as SKU_DEPTH_IN1",
	# "JNR_POG_SKU_VERSION___SKU_WIDTH_IN as SKU_WIDTH_IN1",
	# "JNR_POG_SKU_VERSION___UNIT_OF_MEASURE as UNIT_OF_MEASURE1",
	# "JNR_POG_SKU_VERSION___LAST_CHNG_DT as LAST_CHNG_DT1",
	# "JNR_POG_SKU_VERSION___lkp_SKU_CAPACITY_QTY as SKU_CAPACITY_QTY",
	# "JNR_POG_SKU_VERSION___lkp_SKU_FACINGS_QTY as SKU_FACINGS_QTY",
	# "JNR_POG_SKU_VERSION___lkp_SKU_HEIGHT_IN as SKU_HEIGHT_IN",
	# "JNR_POG_SKU_VERSION___lkp_SKU_DEPTH_IN as SKU_DEPTH_IN",
	# "JNR_POG_SKU_VERSION___lkp_SKU_WIDTH_IN as SKU_WIDTH_IN",
	# "JNR_POG_SKU_VERSION___lkp_UNIT_OF_MEASURE as UNIT_OF_MEASURE",
	# "JNR_POG_SKU_VERSION___lkp_LAST_CHNG_DT as LAST_CHNG_DT",
	# "JNR_POG_SKU_VERSION___lkp_LOAD_TSTMP as LOAD_DT1",
	# "JNR_POG_SKU_VERSION___POG_DB_KEY as POG_DB_KEY",
	# "JNR_POG_SKU_VERSION___SKU_NBR as SKU_NBR").selectExpr(
	# "JNR_POG_SKU_VERSION___sys_row_id as sys_row_id",
	"JNR_POG_SKU_VERSION___lkp_PRODUCT_ID as lkp_PRODUCT_ID",
	"JNR_POG_SKU_VERSION___PRODUCT_ID as PRODUCT_ID",
	"JNR_POG_SKU_VERSION___SKU_CAPACITY_QTY as SKU_CAPACITY_QTY1",
	"JNR_POG_SKU_VERSION___SKU_FACINGS_QTY as SKU_FACINGS_QTY1",
	"JNR_POG_SKU_VERSION___SKU_HEIGHT_IN as SKU_HEIGHT_IN1",
	"JNR_POG_SKU_VERSION___SKU_DEPTH_IN as SKU_DEPTH_IN1",
	"JNR_POG_SKU_VERSION___SKU_WIDTH_IN as SKU_WIDTH_IN1",
	"JNR_POG_SKU_VERSION___UNIT_OF_MEASURE as UNIT_OF_MEASURE1",
	"JNR_POG_SKU_VERSION___LAST_CHNG_DT as LAST_CHNG_DT1",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"IF (JNR_POG_SKU_VERSION___lkp_PRODUCT_ID IS NULL, CURRENT_TIMESTAMP, JNR_POG_SKU_VERSION___lkp_LOAD_TSTMP) as LOAD_DT",
    "MD5 ( concat ( NVL(JNR_POG_SKU_VERSION___lkp_SKU_CAPACITY_QTY,'') , NVL(JNR_POG_SKU_VERSION___lkp_SKU_FACINGS_QTY,'') , NVL(JNR_POG_SKU_VERSION___lkp_SKU_HEIGHT_IN,'')  , NVL(JNR_POG_SKU_VERSION___lkp_SKU_DEPTH_IN ,'') , NVL(JNR_POG_SKU_VERSION___lkp_SKU_WIDTH_IN ,'') , NVL(JNR_POG_SKU_VERSION___lkp_UNIT_OF_MEASURE ,'') , NVL(JNR_POG_SKU_VERSION___lkp_LAST_CHNG_DT ,'') ) ) as md5_lkp",
    "MD5 (concat( NVL(JNR_POG_SKU_VERSION___SKU_CAPACITY_QTY,'')  , NVL(JNR_POG_SKU_VERSION___SKU_FACINGS_QTY,'')  , NVL(JNR_POG_SKU_VERSION___SKU_HEIGHT_IN ,'') , NVL(JNR_POG_SKU_VERSION___SKU_DEPTH_IN ,'') , NVL(JNR_POG_SKU_VERSION___SKU_WIDTH_IN,'')  , NVL(JNR_POG_SKU_VERSION___UNIT_OF_MEASURE,'')  , NVL(JNR_POG_SKU_VERSION___LAST_CHNG_DT,'')  ) )as md5_base",
    "IF(JNR_POG_SKU_VERSION___lkp_PRODUCT_ID IS NULL,2,IF(md5_lkp!=md5_base, 1, 0)) as Change_Flag",
    #"IF (JNR_POG_SKU_VERSION___lkp_PRODUCT_ID IS NULL, 2, IF (MD5 ( concat ( NVL(JNR_POG_SKU_VERSION___SKU_CAPACITY_QTY,'') , NVL(JNR_POG_SKU_VERSION___SKU_FACINGS_QTY,'') , NVL(JNR_POG_SKU_VERSION___SKU_HEIGHT_IN,'')  , NVL(JNR_POG_SKU_VERSION___SKU_DEPTH_IN ,'') , NVL(JNR_POG_SKU_VERSION___SKU_WIDTH_IN ,'') , NVL(JNR_POG_SKU_VERSION___UNIT_OF_MEASURE ,'') , NVL(JNR_POG_SKU_VERSION___LAST_CHNG_DT ,'') ) ) != MD5 (concat( NVL(JNR_POG_SKU_VERSION___SKU_CAPACITY_QTY,'')  , NVL(JNR_POG_SKU_VERSION___SKU_FACINGS_QTY,'')  , NVL(JNR_POG_SKU_VERSION___SKU_HEIGHT_IN ,'') , NVL(JNR_POG_SKU_VERSION___SKU_DEPTH_IN ,'') , NVL(JNR_POG_SKU_VERSION___SKU_WIDTH_IN,'')  , NVL(JNR_POG_SKU_VERSION___UNIT_OF_MEASURE,'')  , NVL(JNR_POG_SKU_VERSION___LAST_CHNG_DT,'')  )), 1, 0)) as Change_Flag",	
    "JNR_POG_SKU_VERSION___POG_DB_KEY as POG_DB_KEY",
	"JNR_POG_SKU_VERSION___SKU_NBR as SKU_NBR"
)

# COMMAND ----------

# Processing node FIL_change_flag, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
EXP_change_flag_temp = EXP_change_flag.toDF(*["EXP_change_flag___" + col for col in EXP_change_flag.columns])

FIL_change_flag = EXP_change_flag_temp.selectExpr(
	"EXP_change_flag___PRODUCT_ID as PRODUCT_ID",
	"EXP_change_flag___SKU_CAPACITY_QTY1 as SKU_CAPACITY_QTY",
	"EXP_change_flag___SKU_FACINGS_QTY1 as SKU_FACINGS_QTY",
	"EXP_change_flag___SKU_HEIGHT_IN1 as SKU_HEIGHT_IN",
	"EXP_change_flag___SKU_DEPTH_IN1 as SKU_DEPTH_IN",
	"EXP_change_flag___SKU_WIDTH_IN1 as SKU_WIDTH_IN",
	"EXP_change_flag___UNIT_OF_MEASURE1 as UNIT_OF_MEASURE",
	"EXP_change_flag___LAST_CHNG_DT1 as LAST_CHNG_DT",
	"EXP_change_flag___UPDATE_DT as UPDATE_DT",
	"EXP_change_flag___LOAD_DT as LOAD_DT",
	"EXP_change_flag___Change_Flag as Change_Flag",
	"EXP_change_flag___POG_DB_KEY as POG_DB_KEY",
	"EXP_change_flag___SKU_NBR as SKU_NBR").filter("Change_Flag != 0").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node UPD_Row_Action, type UPDATE_STRATEGY 
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
FIL_change_flag_temp = FIL_change_flag.toDF(*["FIL_change_flag___" + col for col in FIL_change_flag.columns])

UPD_Row_Action = FIL_change_flag_temp.selectExpr(
	"FIL_change_flag___PRODUCT_ID as PRODUCT_ID",
	"FIL_change_flag___POG_DB_KEY as POG_DB_KEY",
	"FIL_change_flag___SKU_CAPACITY_QTY as SKU_CAPACITY_QTY",
	"FIL_change_flag___SKU_FACINGS_QTY as SKU_FACINGS_QTY",
	"FIL_change_flag___SKU_HEIGHT_IN as SKU_HEIGHT_IN",
	"FIL_change_flag___SKU_DEPTH_IN as SKU_DEPTH_IN",
	"FIL_change_flag___SKU_WIDTH_IN as SKU_WIDTH_IN",
	"FIL_change_flag___UNIT_OF_MEASURE as UNIT_OF_MEASURE",
	"FIL_change_flag___LAST_CHNG_DT as LAST_CHNG_DT",
	"FIL_change_flag___UPDATE_DT as UPDATE_DT",
	"FIL_change_flag___LOAD_DT as LOAD_DT",
	"FIL_change_flag___Change_Flag as Change_Flag",
	"FIL_change_flag___SKU_NBR as SKU_NBR",
 	"if(FIL_change_flag___Change_Flag == 2, 0, 1) as pyspark_data_action") 

# COMMAND ----------

# Processing node Shortcut_to_POG_SKU_VERSION, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_POG_SKU_VERSION = UPD_Row_Action.selectExpr(
	"CAST(PRODUCT_ID AS BIGINT) as PRODUCT_ID",
	"CAST(POG_DB_KEY AS BIGINT) as POG_DBKEY",
	"CAST(SKU_NBR AS BIGINT) as SKU_NBR",
	"CAST(PRODUCT_ID AS BIGINT) as LINK_SUB_PRODUCT_ID",
	"CAST(SKU_NBR AS BIGINT) as LINK_SUB_SKU_NBR",
	"CAST(SKU_CAPACITY_QTY AS INT) as SKU_CAPACITY_QTY",
	"CAST(SKU_FACINGS_QTY AS INT) as SKU_FACINGS_QTY",
	"CAST(SKU_HEIGHT_IN AS DECIMAL(7,2)) as SKU_HEIGHT_IN",
	"CAST(SKU_DEPTH_IN AS DECIMAL(7,2)) as SKU_DEPTH_IN",
	"CAST(SKU_WIDTH_IN AS DECIMAL(7,2)) as SKU_WIDTH_IN",
	"CAST(UNIT_OF_MEASURE AS STRING) as UNIT_OF_MEASURE",
	"CAST(LAST_CHNG_DT AS DATE) as LAST_CHNG_DT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.PRODUCT_ID = target.PRODUCT_ID AND source.POG_DBKEY = target.POG_DBKEY"""
	refined_perf_table = f"{legacy}.POG_SKU_VERSION"
	executeMerge(Shortcut_to_POG_SKU_VERSION, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("POG_SKU_VERSION", "POG_SKU_VERSION", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("POG_SKU_VERSION", "POG_SKU_VERSION","Failed",str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


