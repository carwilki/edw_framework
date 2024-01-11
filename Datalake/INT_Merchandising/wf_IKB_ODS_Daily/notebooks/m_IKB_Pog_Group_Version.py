# Databricks notebook source
#Code converted on 2023-09-12 13:31:08
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

# Processing node LKP_POG_GROUP_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_POG_GROUP_SRC = spark.sql(f"""SELECT
POG_GROUP_ID,
POG_GROUP_DESC
FROM {legacy}.POG_GROUP"""
)


# COMMAND ----------

# Processing node SQ_Shortcut_to_POG_GROUP_VERSION, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_POG_GROUP_VERSION = spark.sql(f"""SELECT
POG_DBKEY as POG_DBKEY_trg ,
POG_GROUP_ID as POG_GROUP_ID_trg ,
POG_GROUP_DESC as POG_GROUP_DESC_trg ,
DELETE_TSTMP as DELETE_TSTMP_trg ,
UPDATE_TSTMP as UPDATE_TSTMP_trg ,
LOAD_TSTMP as LOAD_TSTMP_trg 
FROM {legacy}.POG_GROUP_VERSION""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_POG_VERSION, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_POG_VERSION = spark.sql(f"""SELECT
POG_DBKEY,
POG_GROUP_ID,
COMBO1,
COMBO2,
COMBO3
FROM {legacy}.POG_VERSION""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node NRM_Pog_Version, type NORMALIZER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

unpivotExpr = '''stack(4,
POG_GROUP_ID_COMBO_in1, POG_GROUP_ID_COMBO_in2, POG_GROUP_ID_COMBO_in3,
POG_GROUP_ID_COMBO_in4 ) as (POG_GROUP_ID_COMBO)'''

NRM_Pog_Version = SQ_Shortcut_to_POG_VERSION.selectExpr(
	"POG_DBKEY as POG_DBKEY_in",
	"POG_GROUP_ID as POG_GROUP_ID_COMBO_in1",
	"COMBO1 as POG_GROUP_ID_COMBO_in2",
	"COMBO2 as POG_GROUP_ID_COMBO_in3",
	"COMBO3 as POG_GROUP_ID_COMBO_in4").select(
	col('POG_DBKEY_in').alias('POG_DBKEY'),
	expr(unpivotExpr)) \
	.withColumn('GK_POG_GROUP_ID_COMBO', monotonically_increasing_id()) \
	.withColumn('GCID_POG_GROUP_ID_COMBO', monotonically_increasing_id()) \
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_Group_id_not_zero, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
NRM_Pog_Version_temp = NRM_Pog_Version.toDF(*["NRM_Pog_Version___" + col for col in NRM_Pog_Version.columns])

FIL_Group_id_not_zero = NRM_Pog_Version_temp.selectExpr(
	"NRM_Pog_Version___POG_DBKEY as POG_DBKEY",
	"NRM_Pog_Version___POG_GROUP_ID_COMBO as POG_GROUP_ID_COMBO") \
	.withColumn('POG_DBKEY_in', lit(None)) \
	.filter("POG_GROUP_ID_COMBO != 0").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_Conv_to_int, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
FIL_Group_id_not_zero_temp = FIL_Group_id_not_zero.toDF(*["FIL_Group_id_not_zero___" + col for col in FIL_Group_id_not_zero.columns])

EXP_Conv_to_int = FIL_Group_id_not_zero_temp.selectExpr(
	"FIL_Group_id_not_zero___sys_row_id as sys_row_id",
	"cast(FIL_Group_id_not_zero___POG_GROUP_ID_COMBO as int) as POG_GROUP_ID",
	"FIL_Group_id_not_zero___POG_DBKEY as i_POG_DBKEY",
	"cast(FIL_Group_id_not_zero___POG_DBKEY as int) as POG_DBKEY"
)

# COMMAND ----------

# Processing node LKP_POG_GROUP, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3


LKP_POG_GROUP_lookup_result = EXP_Conv_to_int.selectExpr(
	"POG_GROUP_ID as POG_GROUP_ID_source",
  "sys_row_id").join(LKP_POG_GROUP_SRC, (col('POG_GROUP_ID') == col('POG_GROUP_ID_source')), 'left') \
.withColumn('row_num_POG_GROUP_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("POG_GROUP_ID")))

LKP_POG_GROUP = LKP_POG_GROUP_lookup_result.filter(col("row_num_POG_GROUP_ID") == 1).select(
	col('sys_row_id'),
	col('POG_GROUP_DESC'),
	col('POG_GROUP_ID_source')
)

# COMMAND ----------

# Processing node AGG_Unique, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# Joining dataframes EXP_Conv_to_int, LKP_POG_GROUP to form AGG_Unique
AGG_Unique_joined = EXP_Conv_to_int.join(LKP_POG_GROUP, EXP_Conv_to_int.sys_row_id == LKP_POG_GROUP.sys_row_id, 'inner')
AGG_Unique = AGG_Unique_joined.selectExpr(
	"POG_DBKEY as POG_DBKEY",
	"POG_GROUP_ID_source as POG_GROUP_ID",
	"POG_GROUP_DESC as POG_GROUP_DESC") \
	.groupBy("POG_DBKEY","POG_GROUP_ID") \
	.agg(min(col('POG_GROUP_DESC')).alias('POG_GROUP_DESC'))  #.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_POG_GROUP_VERSION_POG_VERSION, type JOINER 
# COLUMN COUNT: 9

JNR_POG_GROUP_VERSION_POG_VERSION = SQ_Shortcut_to_POG_GROUP_VERSION.join(AGG_Unique,[SQ_Shortcut_to_POG_GROUP_VERSION.POG_DBKEY_trg == AGG_Unique.POG_DBKEY, SQ_Shortcut_to_POG_GROUP_VERSION.POG_GROUP_ID_trg == AGG_Unique.POG_GROUP_ID],'fullouter')

# COMMAND ----------

# Processing node EXP_dates, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
JNR_POG_GROUP_VERSION_POG_VERSION_temp = JNR_POG_GROUP_VERSION_POG_VERSION.toDF(*["JNR_POG_GROUP_VERSION_POG_VERSION___" + col for col in JNR_POG_GROUP_VERSION_POG_VERSION.columns])

EXP_dates = JNR_POG_GROUP_VERSION_POG_VERSION_temp.selectExpr(
	"JNR_POG_GROUP_VERSION_POG_VERSION___sys_row_id as sys_row_id",
	"JNR_POG_GROUP_VERSION_POG_VERSION___POG_DBKEY_trg as POG_DBKEY_trg",
	"JNR_POG_GROUP_VERSION_POG_VERSION___POG_GROUP_ID_trg as POG_GROUP_ID_trg",
	"JNR_POG_GROUP_VERSION_POG_VERSION___POG_GROUP_DESC_trg as POG_GROUP_DESC_trg",
	"JNR_POG_GROUP_VERSION_POG_VERSION___DELETE_TSTMP_trg as DELETE_TSTMP_trg",
	"JNR_POG_GROUP_VERSION_POG_VERSION___UPDATE_TSTMP_trg as UPDATE_TSTMP_trg",
	"JNR_POG_GROUP_VERSION_POG_VERSION___LOAD_TSTMP_trg as LOAD_TSTMP_trg",
	"JNR_POG_GROUP_VERSION_POG_VERSION___POG_DBKEY as POG_DBKEY",
	"JNR_POG_GROUP_VERSION_POG_VERSION___POG_GROUP_ID as POG_GROUP_ID",
	"JNR_POG_GROUP_VERSION_POG_VERSION___POG_GROUP_DESC as POG_GROUP_DESC",
	"NULL as NULL_TSTMP",
	"CURRENT_TIMESTAMP as TODAY_TSTMP"
)

# COMMAND ----------

# Processing node RTRTRANS, type ROUTER 
# COLUMN COUNT: 55


# Creating output dataframe for RTRTRANS, output group INSERT
RTRTRANS_INSERT = EXP_dates.select(EXP_dates.sys_row_id.alias('sys_row_id'),
	EXP_dates.POG_DBKEY_trg.alias('POG_DBKEY_trg1'),
	EXP_dates.POG_GROUP_ID_trg.alias('POG_GROUP_ID_trg1'),
	EXP_dates.POG_GROUP_DESC_trg.alias('POG_GROUP_DESC_trg1'),
	EXP_dates.DELETE_TSTMP_trg.alias('DELETE_TSTMP_trg1'),
	EXP_dates.UPDATE_TSTMP_trg.alias('UPDATE_TSTMP_trg1'),
	EXP_dates.LOAD_TSTMP_trg.alias('LOAD_TSTMP_trg1'),
	EXP_dates.POG_DBKEY.alias('POG_DBKEY1'),
	EXP_dates.POG_GROUP_ID.alias('POG_GROUP_ID1'),
	EXP_dates.POG_GROUP_DESC.alias('POG_GROUP_DESC1'),
	EXP_dates.NULL_TSTMP.alias('NULL_TSTMP1'),
	EXP_dates.TODAY_TSTMP.alias('TODAY_TSTMP1')).filter("POG_DBKEY_trg is Null")

# COMMAND ----------

# Creating output dataframe for RTRTRANS, output group UPD_DEL_TSTMP
RTRTRANS_UPD_DEL_TSTMP = EXP_dates.select(EXP_dates.sys_row_id.alias('sys_row_id'),
	EXP_dates.POG_DBKEY_trg.alias('POG_DBKEY_trg4'),
	EXP_dates.POG_GROUP_ID_trg.alias('POG_GROUP_ID_trg4'),
	EXP_dates.POG_GROUP_DESC_trg.alias('POG_GROUP_DESC_trg4'),
	EXP_dates.DELETE_TSTMP_trg.alias('DELETE_TSTMP_trg4'),
	EXP_dates.UPDATE_TSTMP_trg.alias('UPDATE_TSTMP_trg4'),
	EXP_dates.LOAD_TSTMP_trg.alias('LOAD_TSTMP_trg4'),
	EXP_dates.POG_DBKEY.alias('POG_DBKEY4'),
	EXP_dates.POG_GROUP_ID.alias('POG_GROUP_ID4'),
	EXP_dates.POG_GROUP_DESC.alias('POG_GROUP_DESC4'),
	EXP_dates.NULL_TSTMP.alias('NULL_TSTMP4'),
	EXP_dates.TODAY_TSTMP.alias('TODAY_TSTMP4')).filter("POG_DBKEY is Null and POG_DBKEY_trg is not Null and DELETE_TSTMP_trg is Null")

# COMMAND ----------

# Creating output dataframe for RTRTRANS, output group UPD_DESC
RTRTRANS_UPD_DESC = EXP_dates.select(EXP_dates.sys_row_id.alias('sys_row_id'),
	EXP_dates.POG_DBKEY_trg.alias('POG_DBKEY_trg3'),
	EXP_dates.POG_GROUP_ID_trg.alias('POG_GROUP_ID_trg3'),
	EXP_dates.POG_GROUP_DESC_trg.alias('POG_GROUP_DESC_trg3'),
	EXP_dates.DELETE_TSTMP_trg.alias('DELETE_TSTMP_trg3'),
	EXP_dates.UPDATE_TSTMP_trg.alias('UPDATE_TSTMP_trg3'),
	EXP_dates.LOAD_TSTMP_trg.alias('LOAD_TSTMP_trg3'),
	EXP_dates.POG_DBKEY.alias('POG_DBKEY3'),
	EXP_dates.POG_GROUP_ID.alias('POG_GROUP_ID3'),
	EXP_dates.POG_GROUP_DESC.alias('POG_GROUP_DESC3'),
	EXP_dates.NULL_TSTMP.alias('NULL_TSTMP3'),
	EXP_dates.TODAY_TSTMP.alias('TODAY_TSTMP3')).filter("POG_DBKEY_trg=POG_DBKEY and POG_GROUP_DESC_trg!=POG_GROUP_DESC and DELETE_TSTMP_trg is Null")

# COMMAND ----------


# Creating output dataframe for RTRTRANS, output group UPD_RMV_DEL_TSTMP
RTRTRANS_UPD_RMV_DEL_TSTMP = EXP_dates.select(EXP_dates.sys_row_id.alias('sys_row_id'),
	EXP_dates.POG_DBKEY_trg.alias('POG_DBKEY_trg5'),
	EXP_dates.POG_GROUP_ID_trg.alias('POG_GROUP_ID_trg5'),
	EXP_dates.POG_GROUP_DESC_trg.alias('POG_GROUP_DESC_trg5'),
	EXP_dates.DELETE_TSTMP_trg.alias('DELETE_TSTMP_trg5'),
	EXP_dates.UPDATE_TSTMP_trg.alias('UPDATE_TSTMP_trg5'),
	EXP_dates.LOAD_TSTMP_trg.alias('LOAD_TSTMP_trg5'),
	EXP_dates.POG_DBKEY.alias('POG_DBKEY5'),
	EXP_dates.POG_GROUP_ID.alias('POG_GROUP_ID5'),
	EXP_dates.POG_GROUP_DESC.alias('POG_GROUP_DESC5'),
	EXP_dates.NULL_TSTMP.alias('NULL_TSTMP5'),
	EXP_dates.TODAY_TSTMP.alias('TODAY_TSTMP5')).filter("not POG_DBKEY is Null and POG_DBKEY_trg is not Null and DELETE_TSTMP_trg is not Null")


# COMMAND ----------

def execute_update(dataframe, target_table, join_condition):
  # Create a temporary view from the DataFrame
  sourceTempView = "temp_source_" + target_table.split(".")[1]

  dataframe.createOrReplaceTempView(sourceTempView)

  merge_sql =  f"""MERGE INTO {target_table} target
                   USING {sourceTempView} source
                   ON {join_condition}
                   WHEN MATCHED THEN UPDATE
                      SET {", ".join([f"target.{col} = source.{col}" for col in dataframe.columns if col!='pyspark_data_action'])}          
                """

  spark.sql(merge_sql)

# COMMAND ----------

# Processing node Shortcut_to_POG_GROUP_VERSION_INSERT, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_POG_GROUP_VERSION_INSERT = RTRTRANS_INSERT.selectExpr(
	"CAST(POG_DBKEY1 AS INT) as POG_DBKEY",
	"CAST(POG_GROUP_ID1 AS INT) as POG_GROUP_ID",
	"CAST(POG_GROUP_DESC1 AS STRING) as POG_GROUP_DESC",
	"CAST(NULL_TSTMP1 AS TIMESTAMP) as DELETE_TSTMP",
	"CAST(TODAY_TSTMP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(TODAY_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP"
)

# try:
# 	primary_key = """source.POG_DBKEY = target.POG_DBKEY AND source.POG_GROUP_ID = target.POG_GROUP_ID"""
# 	refined_perf_table = f"{legacy}.POG_GROUP_VERSION"
# 	executeMerge(Shortcut_to_POG_GROUP_VERSION_INSERT, refined_perf_table, primary_key)
# 	logger.info(f"Merge with {refined_perf_table} completed]")
# 	logPrevRunDt("POG_GROUP_VERSION", "POG_GROUP_VERSION", "Completed", "N/A", f"{raw}.log_run_details")
# except Exception as e:
# 	print(e)
# 	logPrevRunDt("POG_GROUP_VERSION", "POG_GROUP_VERSION","Failed",str(e), f"{raw}.log_run_details", )
# 	raise e

Shortcut_to_POG_GROUP_VERSION_INSERT.write.mode("append").saveAsTable(f'{legacy}.POG_GROUP_VERSION')

# COMMAND ----------

# Processing node Shortcut_to_POG_GROUP_VERSION_UPD_DESC, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_POG_GROUP_VERSION_UPD_DESC = RTRTRANS_UPD_DESC.selectExpr(
	"CAST(POG_DBKEY_trg3 AS INT) as POG_DBKEY",
	"CAST(POG_GROUP_ID_trg3 AS INT) as POG_GROUP_ID",
	"CAST(POG_GROUP_DESC3 AS STRING) as POG_GROUP_DESC",
	# "CAST(NULL AS TIMESTAMP) as DELETE_TSTMP",
	"CAST(TODAY_TSTMP3 AS TIMESTAMP) as UPDATE_TSTMP"  #,
	# "CAST(NULL AS TIMESTAMP) as LOAD_TSTMP"
)

try:
	primary_key = """source.POG_DBKEY = target.POG_DBKEY AND source.POG_GROUP_ID = target.POG_GROUP_ID"""
	refined_perf_table = f"{legacy}.POG_GROUP_VERSION"
	execute_update(Shortcut_to_POG_GROUP_VERSION_INSERT, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("POG_GROUP_VERSION", "POG_GROUP_VERSION", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("POG_GROUP_VERSION", "POG_GROUP_VERSION","Failed",str(e), f"{raw}.log_run_details", )
	raise e

# Change to Update; Key: POG_DBKEY, POG_GROUP_ID
# Shortcut_to_POG_GROUP_VERSION_UPD_DESC.write.saveAsTable(f'{legacy}.POG_GROUP_VERSION')

# COMMAND ----------

# Processing node Shortcut_to_POG_GROUP_VERSION_UPD_DEL_TSTMP, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_POG_GROUP_VERSION_UPD_DEL_TSTMP = RTRTRANS_UPD_DEL_TSTMP.selectExpr(
	"CAST(POG_DBKEY_trg4 AS INT) as POG_DBKEY",
	"CAST(POG_GROUP_ID_trg4 AS INT) as POG_GROUP_ID",
	# "CAST(NULL AS STRING) as POG_GROUP_DESC",
	"CAST(TODAY_TSTMP4 AS TIMESTAMP) as DELETE_TSTMP",
	"CAST(TODAY_TSTMP4 AS TIMESTAMP) as UPDATE_TSTMP"  #,
	# "CAST(NULL AS TIMESTAMP) as LOAD_TSTMP"
)

try:
	primary_key = """source.POG_DBKEY = target.POG_DBKEY AND source.POG_GROUP_ID = target.POG_GROUP_ID"""
	refined_perf_table = f"{legacy}.POG_GROUP_VERSION"
	execute_update(Shortcut_to_POG_GROUP_VERSION_INSERT, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("POG_GROUP_VERSION", "POG_GROUP_VERSION", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("POG_GROUP_VERSION", "POG_GROUP_VERSION","Failed",str(e), f"{raw}.log_run_details", )
	raise e

# Change to Update; Key: POG_DBKEY, POG_GROUP_ID
# Shortcut_to_POG_GROUP_VERSION_UPD_DEL_TSTMP.write.saveAsTable(f'{legacy}.POG_GROUP_VERSION')

# COMMAND ----------

# Processing node Shortcut_to_POG_GROUP_VERSION_UPD_RMV_DEL_TSTMP, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_POG_GROUP_VERSION_UPD_RMV_DEL_TSTMP = RTRTRANS_UPD_RMV_DEL_TSTMP.selectExpr(
	"CAST(POG_DBKEY_trg5 AS INT) as POG_DBKEY",
	"CAST(POG_GROUP_ID_trg5 AS INT) as POG_GROUP_ID",
	"CAST(POG_GROUP_DESC5 AS STRING) as POG_GROUP_DESC",
	# "CAST(NULL_TSTMP5 AS TIMESTAMP) as DELETE_TSTMP",
	"CAST(TODAY_TSTMP5 AS TIMESTAMP) as UPDATE_TSTMP"  #,
	# "CAST(NULL AS TIMESTAMP) as LOAD_TSTMP"
)

try:
	primary_key = """source.POG_DBKEY = target.POG_DBKEY AND source.POG_GROUP_ID = target.POG_GROUP_ID"""
	refined_perf_table = f"{legacy}.POG_GROUP_VERSION"
	execute_update(Shortcut_to_POG_GROUP_VERSION_INSERT, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("POG_GROUP_VERSION", "POG_GROUP_VERSION", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("POG_GROUP_VERSION", "POG_GROUP_VERSION","Failed",str(e), f"{raw}.log_run_details", )
	raise e

# Change to Update; Key: POG_DBKEY, POG_GROUP_ID
# Shortcut_to_POG_GROUP_VERSION_UPD_RMV_DEL_TSTMP.write.saveAsTable(f'{legacy}.POG_GROUP_VERSION')

# COMMAND ----------


