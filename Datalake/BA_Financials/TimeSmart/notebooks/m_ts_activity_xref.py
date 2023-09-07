#Code converted on 2023-08-07 16:26:08
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

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')

args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------
# Processing node LKP_CHECKRFCDESC_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 4

# LKP_CHECKRFCDESC_SRC = spark.sql(f"""SELECT
# Ts_Work_Assign_Cd,
# Ts_Activity_Xref_Id,
# load_tstmp
# FROM {legacy}.UDH_TS_WORK_ASSIGNMENT""")

query ="""(SELECT UDH_TS_WORK_ASSIGNMENT.Ts_Work_Assign_Cd, UDH_TS_WORK_ASSIGNMENT.Ts_Activity_Xref_Id from UserDataFeed.dbo.UDH_TS_WORK_ASSIGNMENT WHERE 1=1) AS SRC"""
username,password,connection_string =mtx_prd_sqlServer(env)
UDH_TS_WORK_ASSIGNMENT= jdbcSqlServerConnection(query,username,password,connection_string)

LKP_CHECKRFCDESC_SRC = UDH_TS_WORK_ASSIGNMENT.select(
    UDH_TS_WORK_ASSIGNMENT.Ts_Work_Assign_Cd,
    UDH_TS_WORK_ASSIGNMENT.Ts_Activity_Xref_Id,
    current_timestamp().cast(TimestampType()).alias("load_tstmp")
)
# Conforming fields names to the component layout
LKP_CHECKRFCDESC_SRC = LKP_CHECKRFCDESC_SRC \
	.withColumnRenamed(LKP_CHECKRFCDESC_SRC.columns[0],'IN_ACTXREFID') \
	.withColumnRenamed(LKP_CHECKRFCDESC_SRC.columns[0],'Ts_Activity_Xref_Id') \
	.withColumnRenamed(LKP_CHECKRFCDESC_SRC.columns[1],'Ts_Work_Assign_Cd') \
	.withColumnRenamed(LKP_CHECKRFCDESC_SRC.columns[2],'load_tstmp')

# COMMAND ----------
# Processing node SQ_Shortcut_to_TS_ACTIVITY_XREF, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_TS_ACTIVITY_XREF = spark.sql(f"""SELECT
TS_ACTIVITY_XREF.TS_ACTIVITY_XREF_ID,
TS_ACTIVITY_XREF.TS_ACTIVITY_ID,
TS_ACTIVITY_XREF.TS_ACTIVITY_TYPE_ID,
TS_ACTIVITY_XREF.TS_ACTIVITY_CAT_ID,
TS_ACTIVITY_XREF.TS_ACTIVITY_STATUS_ID,
TS_ACTIVITY_XREF.TS_RFC_DESC,
TS_ACTIVITY_XREF.TS_UDH_WORK_ASSIGN_CD,
TS_ACTIVITY_XREF.LOAD_TSTMP
FROM {legacy}.TS_ACTIVITY_XREF""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TS_ACTIVITY_XREF_PRE, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_TS_ACTIVITY_XREF_PRE = spark.sql(f"""SELECT
TS_ACTIVITY_XREF_PRE.ACTXREFID,
TS_ACTIVITY_XREF_PRE.ACTIVITYID,
TS_ACTIVITY_XREF_PRE.ACTTYPEID,
TS_ACTIVITY_XREF_PRE.ACTCATEGORYID,
TS_ACTIVITY_XREF_PRE.ACTSTATUSID,
TS_ACTIVITY_XREF_PRE.RFCNBR
FROM {raw}.TS_ACTIVITY_XREF_PRE""").withColumn("sys_row_id2", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_LeftJoin, type JOINER 
# COLUMN COUNT: 14

JNR_LeftJoin = SQ_Shortcut_to_TS_ACTIVITY_XREF.join(SQ_Shortcut_to_TS_ACTIVITY_XREF_PRE,[SQ_Shortcut_to_TS_ACTIVITY_XREF.TS_ACTIVITY_XREF_ID == SQ_Shortcut_to_TS_ACTIVITY_XREF_PRE.ACTXREFID],'right_outer')

# COMMAND ----------
# Processing node LKP_CHECKRFCDESC, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

LKP_CHECKRFCDESC_lookup_result = JNR_LeftJoin.selectExpr(
	"ACTXREFID", "Ts_Activity_Xref_Id", "sys_row_id", "TS_UDH_WORK_ASSIGN_CD").join(LKP_CHECKRFCDESC_SRC, (col('Ts_Activity_Xref_Id') == col('IN_ACTXREFID')), 'left') \
.withColumn('row_num_Ts_Work_Assign_Cd', row_number().over(Window.partitionBy("sys_row_id").orderBy("Ts_Work_Assign_Cd")))
LKP_CHECKRFCDESC = LKP_CHECKRFCDESC_lookup_result.filter(col("row_num_Ts_Work_Assign_Cd") == 1).select(
	LKP_CHECKRFCDESC_lookup_result.sys_row_id,
	col('Ts_Work_Assign_Cd')
)

# COMMAND ----------
# Processing node EXP_CheckChanges, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
LKP_CHECKRFCDESC_temp = LKP_CHECKRFCDESC.toDF(*["LKP_CHECKRFCDESC___" + col for col in LKP_CHECKRFCDESC.columns])
JNR_LeftJoin_temp = JNR_LeftJoin.toDF(*["JNR_LeftJoin___" + col for col in JNR_LeftJoin.columns])

# Joining dataframes JNR_LeftJoin, LKP_CHECKRFCDESC to form EXP_CheckChanges
EXP_CheckChanges_joined = JNR_LeftJoin_temp.join(LKP_CHECKRFCDESC_temp, JNR_LeftJoin_temp.JNR_LeftJoin___sys_row_id == LKP_CHECKRFCDESC_temp.LKP_CHECKRFCDESC___sys_row_id, 'inner')

EXP_CheckChanges = EXP_CheckChanges_joined \
.withColumn("v_WORKASSIGNCD", expr("""IF (LKP_CHECKRFCDESC___Ts_Work_Assign_Cd IS NULL, JNR_LeftJoin___TS_UDH_WORK_ASSIGN_CD, LKP_CHECKRFCDESC___Ts_Work_Assign_Cd)""")).selectExpr(
	"LKP_CHECKRFCDESC___Ts_Work_Assign_Cd as WORKASSIGNCD",
	"JNR_LeftJoin___sys_row_id as sys_row_id",
	"v_WORKASSIGNCD as o_WORKASSIGNCD",
	"JNR_LeftJoin___ACTXREFID as ACTXREFID",
	"IF (JNR_LeftJoin___ACTXREFID IS NULL, JNR_LeftJoin___TS_ACTIVITY_XREF_ID, JNR_LeftJoin___ACTXREFID) as ACTXREFID_NN",
	"JNR_LeftJoin___ACTIVITYID as ACTIVITYID",
	"JNR_LeftJoin___ACTTYPEID as ACTTYPEID",
	"JNR_LeftJoin___ACTCATEGORYID as ACTCATEGORYID",
	"JNR_LeftJoin___ACTSTATUSID as ACTSTATUSID",
	"JNR_LeftJoin___RFCNBR as RFCNBR",
	"IF (JNR_LeftJoin___TS_ACTIVITY_XREF_ID IS NULL, 0, IF (JNR_LeftJoin___ACTIVITYID <> JNR_LeftJoin___TS_ACTIVITY_ID OR JNR_LeftJoin___ACTTYPEID <> JNR_LeftJoin___TS_ACTIVITY_TYPE_ID OR JNR_LeftJoin___ACTCATEGORYID <> JNR_LeftJoin___TS_ACTIVITY_CAT_ID OR JNR_LeftJoin___ACTSTATUSID <> JNR_LeftJoin___TS_ACTIVITY_STATUS_ID OR IF (JNR_LeftJoin___RFCNBR IS NULL, 'XNULL', JNR_LeftJoin___RFCNBR) <> IF (JNR_LeftJoin___TS_RFC_DESC IS NULL, 'XNULL', JNR_LeftJoin___TS_RFC_DESC) OR IF (v_WORKASSIGNCD IS NULL, 'XNULL', v_WORKASSIGNCD) <> IF (JNR_LeftJoin___TS_UDH_WORK_ASSIGN_CD IS NULL, 'XNULL', JNR_LeftJoin___TS_UDH_WORK_ASSIGN_CD), 1, 3)) as UpdateStrategy",
	"CURRENT_TIMESTAMP as UpdateDt",
	"JNR_LeftJoin___LOAD_TSTMP as LOAD_TSTMP",
	"IF (JNR_LeftJoin___LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNR_LeftJoin___LOAD_TSTMP) as LOAD_TSTMP_NN")

# COMMAND ----------
# Processing node FIL_RemoveRejected, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
EXP_CheckChanges_temp = EXP_CheckChanges.toDF(*["EXP_CheckChanges___" + col for col in EXP_CheckChanges.columns])

FIL_RemoveRejected = EXP_CheckChanges_temp.selectExpr(
	"EXP_CheckChanges___o_WORKASSIGNCD as WORKASSIGNCD",
	"EXP_CheckChanges___ACTXREFID_NN as ACTXREFID_NN",
	"EXP_CheckChanges___ACTIVITYID as ACTIVITYID",
	"EXP_CheckChanges___ACTTYPEID as ACTTYPEID",
	"EXP_CheckChanges___ACTCATEGORYID as ACTCATEGORYID",
	"EXP_CheckChanges___ACTSTATUSID as ACTSTATUSID",
	"EXP_CheckChanges___RFCNBR as RFCNBR",
	"EXP_CheckChanges___UpdateStrategy as UpdateStrategy",
	"EXP_CheckChanges___UpdateDt as UpdateDt",
	"EXP_CheckChanges___LOAD_TSTMP_NN as LOAD_TSTMP_NN").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_SetStrategy, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
FIL_RemoveRejected_temp = FIL_RemoveRejected.toDF(*["FIL_RemoveRejected___" + col for col in FIL_RemoveRejected.columns])

UPD_SetStrategy = FIL_RemoveRejected_temp.selectExpr(
	"FIL_RemoveRejected___WORKASSIGNCD as WORKASSIGNCD",
	"FIL_RemoveRejected___ACTXREFID_NN as ACTXREFID",
	"FIL_RemoveRejected___ACTIVITYID as ACTIVITYID",
	"FIL_RemoveRejected___ACTTYPEID as ACTTYPEID",
	"FIL_RemoveRejected___ACTCATEGORYID as ACTCATEGORYID",
	"FIL_RemoveRejected___ACTSTATUSID as ACTSTATUSID",
	"FIL_RemoveRejected___RFCNBR as RFCNBR",
	"FIL_RemoveRejected___UpdateStrategy as UpdateStrategy",
	"FIL_RemoveRejected___UpdateDt as UpdateDt",
	"FIL_RemoveRejected___LOAD_TSTMP_NN as LOAD_TSTMP_NN",
	"FIL_RemoveRejected___UpdateStrategy as pyspark_data_action")

# COMMAND ----------
# Processing node Shortcut_to_TS_ACTIVITY_XREF_1, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_TS_ACTIVITY_XREF_1 = UPD_SetStrategy.selectExpr(
	"CAST(ACTXREFID AS BIGINT) as TS_ACTIVITY_XREF_ID",
	"CAST(ACTIVITYID AS BIGINT) as TS_ACTIVITY_ID",
	"CAST(ACTTYPEID AS BIGINT) as TS_ACTIVITY_TYPE_ID",
	"CAST(ACTCATEGORYID AS BIGINT) as TS_ACTIVITY_CAT_ID",
	"CAST(ACTSTATUSID AS BIGINT) as TS_ACTIVITY_STATUS_ID",
	"CAST(RFCNBR AS STRING) as TS_RFC_DESC",
	"CAST(WORKASSIGNCD AS STRING) as TS_UDH_WORK_ASSIGN_CD",
	"CAST(UpdateDt AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP_NN AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.TS_ACTIVITY_XREF_ID = target.TS_ACTIVITY_XREF_ID"""
	refined_perf_table = f"{legacy}.TS_ACTIVITY_XREF"
	executeMerge(Shortcut_to_TS_ACTIVITY_XREF_1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("TS_ACTIVITY_XREF", "TS_ACTIVITY_XREF", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TS_ACTIVITY_XREF", "TS_ACTIVITY_XREF","Failed",str(e), f"{raw}.log_run_details")
	raise e
		