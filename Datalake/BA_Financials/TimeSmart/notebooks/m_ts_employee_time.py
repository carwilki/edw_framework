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
# Processing node SQ_Shortcut_to_TS_EMPLOYEE_TIME, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_TS_EMPLOYEE_TIME = spark.sql(f"""SELECT
TS_EMPLOYEE_TIME.TS_DAY_DT,
TS_EMPLOYEE_TIME.EMPLOYEE_ID,
TS_EMPLOYEE_TIME.TS_ACTIVITY_XREF_ID,
TS_EMPLOYEE_TIME.TS_ACTIVITY_ID,
TS_EMPLOYEE_TIME.TS_ACTIVITY_CAT_ID,
TS_EMPLOYEE_TIME.TS_ACTIVITY_TYPE_ID,
TS_EMPLOYEE_TIME.TS_RFC_DESC,
TS_EMPLOYEE_TIME.TS_WORK_ASSIGN_CD,
TS_EMPLOYEE_TIME.TS_WORK_HOURS,
TS_EMPLOYEE_TIME.TS_COMMENT,
TS_EMPLOYEE_TIME.LOAD_TSTMP
FROM {legacy}.TS_EMPLOYEE_TIME
WHERE TS_EMPLOYEE_TIME.TS_DAY_DT >= (current_date - 45)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE = spark.sql(f"""SELECT
TS_EMPLOYEE_TIME_PRE.DAYDT,
TS_EMPLOYEE_TIME_PRE.EMPID,
TS_EMPLOYEE_TIME_PRE.ACTXREFID,
TS_EMPLOYEE_TIME_PRE.HOURS,
TS_EMPLOYEE_TIME_PRE.RFCNBR,
TS_EMPLOYEE_TIME_PRE.CREATEDATE,
TS_EMPLOYEE_TIME_PRE.COMMENT,
TS_ACTIVITY_XREF.TS_ACTIVITY_ID,
TS_ACTIVITY_XREF.TS_ACTIVITY_TYPE_ID,
TS_ACTIVITY_XREF.TS_ACTIVITY_CAT_ID,
TS_ACTIVITY_XREF.TS_UDH_WORK_ASSIGN_CD,
TS_ACTIVITY.TS_ACTIVITY_NAME,
TS_ACTIVITY.TS_ACTIVITY_DESC,
TS_ACTIVITY_CATEGORY.TS_ACTIVITY_CAT_DESC,
TS_ACTIVITY_TYPE.TS_ACTIVITY_TYPE_DESC
FROM {legacy}.TS_ACTIVITY, {legacy}.TS_ACTIVITY_CATEGORY, {raw}.TS_EMPLOYEE_TIME_PRE, {legacy}.TS_ACTIVITY_XREF, {legacy}.TS_ACTIVITY_TYPE
WHERE TS_EMPLOYEE_TIME_PRE.ACTXREFID = TS_ACTIVITY_XREF.TS_ACTIVITY_XREF_ID

AND TS_ACTIVITY_XREF.TS_ACTIVITY_ID = TS_ACTIVITY.TS_ACTIVITY_ID

AND TS_ACTIVITY_XREF.TS_ACTIVITY_TYPE_ID = TS_ACTIVITY_TYPE.TS_ACTIVITY_TYPE_ID

AND TS_ACTIVITY_XREF.TS_ACTIVITY_CAT_ID = TS_ACTIVITY_CATEGORY.TS_ACTIVITY_CAT_ID""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_FullOuterJoin, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TS_EMPLOYEE_TIME_temp = SQ_Shortcut_to_TS_EMPLOYEE_TIME.toDF(*["SQ_Shortcut_to_TS_EMPLOYEE_TIME___" + col for col in SQ_Shortcut_to_TS_EMPLOYEE_TIME.columns])
SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE_temp = SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE.toDF(*["SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___" + col for col in SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE.columns])

JNR_FullOuterJoin = SQ_Shortcut_to_TS_EMPLOYEE_TIME_temp.join(SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE_temp,[SQ_Shortcut_to_TS_EMPLOYEE_TIME_temp.SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_DAY_DT == SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE_temp.SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___DAYDT, SQ_Shortcut_to_TS_EMPLOYEE_TIME_temp.SQ_Shortcut_to_TS_EMPLOYEE_TIME___EMPLOYEE_ID == SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE_temp.SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___EMPID, SQ_Shortcut_to_TS_EMPLOYEE_TIME_temp.SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_ACTIVITY_XREF_ID == SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE_temp.SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___ACTXREFID],'fullouter').selectExpr(
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___DAYDT as DAYDT",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___EMPID as EMPID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___ACTXREFID as ACTXREFID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___HOURS as HOURS",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___RFCNBR as RFCNBR",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___CREATEDATE as CREATEDATE",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___COMMENT as COMMENT",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___TS_ACTIVITY_ID as TS_ACTIVITY_ID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___TS_ACTIVITY_TYPE_ID as TS_ACTIVITY_TYPE_ID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___TS_ACTIVITY_CAT_ID as TS_ACTIVITY_CAT_ID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___TS_UDH_WORK_ASSIGN_CD as TS_UDH_WORK_ASSIGN_CD",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___TS_ACTIVITY_NAME as TS_ACTIVITY_NAME",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___TS_ACTIVITY_DESC as TS_ACTIVITY_DESC",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___TS_ACTIVITY_CAT_DESC as TS_ACTIVITY_CAT_DESC",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME_PRE___TS_ACTIVITY_TYPE_DESC as TS_ACTIVITY_TYPE_DESC",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_DAY_DT as M_TS_DAY_DT",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___EMPLOYEE_ID as M_EMPLOYEE_ID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_ACTIVITY_XREF_ID as M_TS_ACTIVITY_XREF_ID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_ACTIVITY_ID as M_TS_ACTIVITY_ID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_ACTIVITY_CAT_ID as M_TS_ACTIVITY_CAT_ID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_ACTIVITY_TYPE_ID as M_TS_ACTIVITY_TYPE_ID",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_RFC_DESC as M_TS_RFC_DESC",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_WORK_ASSIGN_CD as M_TS_WORK_ASSIGN_CD",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_WORK_HOURS as M_TS_WORK_HOURS",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___TS_COMMENT as M_TS_COMMENT",
	"SQ_Shortcut_to_TS_EMPLOYEE_TIME___LOAD_TSTMP as LOAD_TSTMP")

# COMMAND ----------
# Processing node EXP_DeriveRFC, type EXPRESSION 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column

EXP_DeriveRFC = JNR_FullOuterJoin.selectExpr(
	"sys_row_id as sys_row_id",
	"IF (TS_UDH_WORK_ASSIGN_CD IS NULL, IF (TS_ACTIVITY_CAT_ID = 67 OR TS_ACTIVITY_CAT_ID = 68, IF (cast(SUBSTR ( RFCNBR , 1 , 4 ) as double) IS NOT NULL, SUBSTR ( RFCNBR , 1 , 4 ), 'NA'), IF (TS_ACTIVITY_ID = 3, IF (cast(SUBSTR ( TS_ACTIVITY_CAT_DESC , 3 , 4 ) as double) IS NOT NULL, SUBSTR ( TS_ACTIVITY_CAT_DESC , 1 , INSTR ( TS_ACTIVITY_CAT_DESC , ' ' ) ), 'NA'), 'NA')), TS_UDH_WORK_ASSIGN_CD) as Work_Assign_CD"
)

# COMMAND ----------
# Processing node EXP_CheckChanges, type EXPRESSION 
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
JNR_FullOuterJoin_temp = JNR_FullOuterJoin.toDF(*["JNR_FullOuterJoin___" + col for col in JNR_FullOuterJoin.columns])
EXP_DeriveRFC_temp = EXP_DeriveRFC.toDF(*["EXP_DeriveRFC___" + col for col in EXP_DeriveRFC.columns])

# Joining dataframes JNR_FullOuterJoin, EXP_DeriveRFC to form EXP_CheckChanges
EXP_CheckChanges_joined = JNR_FullOuterJoin_temp.join(EXP_DeriveRFC_temp, JNR_FullOuterJoin_temp.JNR_FullOuterJoin___sys_row_id == EXP_DeriveRFC_temp.EXP_DeriveRFC___sys_row_id, 'inner')
EXP_CheckChanges = EXP_CheckChanges_joined.selectExpr(
	"JNR_FullOuterJoin___sys_row_id as sys_row_id",
	"EXP_DeriveRFC___Work_Assign_CD as Work_Assign_CD",
	"JNR_FullOuterJoin___DAYDT as DAYDT",
	"IF (JNR_FullOuterJoin___DAYDT IS NULL, JNR_FullOuterJoin___M_TS_DAY_DT, JNR_FullOuterJoin___DAYDT) as DAYDT_NN",
	"JNR_FullOuterJoin___EMPID as EMPID",
	"IF (JNR_FullOuterJoin___EMPID IS NULL, JNR_FullOuterJoin___M_EMPLOYEE_ID, JNR_FullOuterJoin___EMPID) as EMPID_NN",
	"JNR_FullOuterJoin___ACTXREFID as ACTXREFID",
	"IF (JNR_FullOuterJoin___ACTXREFID IS NULL, JNR_FullOuterJoin___M_TS_ACTIVITY_XREF_ID, JNR_FullOuterJoin___ACTXREFID) as ACTXREFID_NN",
	"JNR_FullOuterJoin___HOURS as HOURS",
	"JNR_FullOuterJoin___RFCNBR as RFCNBR",
	"JNR_FullOuterJoin___CREATEDATE as CREATEDATE",
	"JNR_FullOuterJoin___COMMENT as COMMENT",
	"JNR_FullOuterJoin___TS_ACTIVITY_ID as TS_ACTIVITY_ID",
	"JNR_FullOuterJoin___TS_ACTIVITY_TYPE_ID as TS_ACTIVITY_TYPE_ID",
	"JNR_FullOuterJoin___TS_ACTIVITY_CAT_ID as TS_ACTIVITY_CAT_ID",
	"JNR_FullOuterJoin___TS_UDH_WORK_ASSIGN_CD as TS_UDH_WORK_ASSIGN_CD",
	"JNR_FullOuterJoin___TS_ACTIVITY_NAME as TS_ACTIVITY_NAME",
	"JNR_FullOuterJoin___TS_ACTIVITY_DESC as TS_ACTIVITY_DESC",
	"JNR_FullOuterJoin___TS_ACTIVITY_CAT_DESC as TS_ACTIVITY_CAT_DESC",
	"JNR_FullOuterJoin___TS_ACTIVITY_TYPE_DESC as TS_ACTIVITY_TYPE_DESC",
	"IF (JNR_FullOuterJoin___M_EMPLOYEE_ID IS NULL, 0, IF (JNR_FullOuterJoin___EMPID IS NULL, 2, IF (JNR_FullOuterJoin___TS_ACTIVITY_ID <> JNR_FullOuterJoin___M_TS_ACTIVITY_ID OR JNR_FullOuterJoin___TS_ACTIVITY_CAT_ID <> JNR_FullOuterJoin___M_TS_ACTIVITY_CAT_ID OR JNR_FullOuterJoin___TS_ACTIVITY_TYPE_ID <> JNR_FullOuterJoin___M_TS_ACTIVITY_TYPE_ID OR IF (JNR_FullOuterJoin___RFCNBR IS NULL, 'XNULL', JNR_FullOuterJoin___RFCNBR) <> IF (JNR_FullOuterJoin___M_TS_RFC_DESC IS NULL, 'XNULL', JNR_FullOuterJoin___M_TS_RFC_DESC) OR IF (JNR_FullOuterJoin___COMMENT IS NULL, 'XNULL', JNR_FullOuterJoin___COMMENT) <> IF (JNR_FullOuterJoin___M_TS_COMMENT IS NULL, 'XNULL', JNR_FullOuterJoin___M_TS_COMMENT) OR IF (EXP_DeriveRFC___Work_Assign_CD IS NULL, 'XNULL', EXP_DeriveRFC___Work_Assign_CD) <> IF (JNR_FullOuterJoin___M_TS_WORK_ASSIGN_CD IS NULL, 'XNULL', JNR_FullOuterJoin___M_TS_WORK_ASSIGN_CD) OR JNR_FullOuterJoin___HOURS <> JNR_FullOuterJoin___M_TS_WORK_HOURS, 1, 3))) as UpdateStrategy",
	"CURRENT_TIMESTAMP as UpdateDt",
	"JNR_FullOuterJoin___LOAD_TSTMP as LOAD_TSTMP",
	"IF (JNR_FullOuterJoin___LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNR_FullOuterJoin___LOAD_TSTMP) as LOAD_TSTMP_NN"
)

# COMMAND ----------
# Processing node FIL_RemoveRejected, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
EXP_CheckChanges_temp = EXP_CheckChanges.toDF(*["EXP_CheckChanges___" + col for col in EXP_CheckChanges.columns])

FIL_RemoveRejected = EXP_CheckChanges_temp.selectExpr(
	"EXP_CheckChanges___Work_Assign_CD as Work_Assign_Cd",
	"EXP_CheckChanges___DAYDT_NN as DAYDT",
	"EXP_CheckChanges___EMPID_NN as EMPID",
	"EXP_CheckChanges___ACTXREFID_NN as ACTXREFID",
	"EXP_CheckChanges___HOURS as HOURS",
	"EXP_CheckChanges___RFCNBR as RFCNBR",
	"EXP_CheckChanges___CREATEDATE as CREATEDATE",
	"EXP_CheckChanges___COMMENT as COMMENT",
	"EXP_CheckChanges___TS_ACTIVITY_ID as TS_ACTIVITY_ID",
	"EXP_CheckChanges___TS_ACTIVITY_TYPE_ID as TS_ACTIVITY_TYPE_ID",
	"EXP_CheckChanges___TS_ACTIVITY_CAT_ID as TS_ACTIVITY_CAT_ID",
	"EXP_CheckChanges___TS_ACTIVITY_NAME as TS_ACTIVITY_NAME",
	"EXP_CheckChanges___TS_ACTIVITY_DESC as TS_ACTIVITY_DESC",
	"EXP_CheckChanges___TS_ACTIVITY_CAT_DESC as TS_ACTIVITY_CAT_DESC",
	"EXP_CheckChanges___TS_ACTIVITY_TYPE_DESC as TS_ACTIVITY_TYPE_DESC",
	"EXP_CheckChanges___UpdateStrategy as UpdateStrategy",
	"EXP_CheckChanges___UpdateDt as UpdateDt",
	"EXP_CheckChanges___LOAD_TSTMP_NN as LOAD_TSTMP_NN").filter("UpdateStrategy != 3").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node UPD_SetStrategy, type UPDATE_STRATEGY 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
FIL_RemoveRejected_temp = FIL_RemoveRejected.toDF(*["FIL_RemoveRejected___" + col for col in FIL_RemoveRejected.columns])

UPD_SetStrategy = FIL_RemoveRejected_temp.selectExpr(
	"FIL_RemoveRejected___Work_Assign_Cd as Work_Assign_Cd",
	"FIL_RemoveRejected___DAYDT as DAYDT",
	"FIL_RemoveRejected___EMPID as EMPID",
	"FIL_RemoveRejected___ACTXREFID as ACTXREFID",
	"FIL_RemoveRejected___HOURS as HOURS",
	"FIL_RemoveRejected___RFCNBR as RFCNBR",
	"FIL_RemoveRejected___CREATEDATE as CREATEDATE",
	"FIL_RemoveRejected___COMMENT as COMMENT",
	"FIL_RemoveRejected___TS_ACTIVITY_ID as TS_ACTIVITY_ID",
	"FIL_RemoveRejected___TS_ACTIVITY_TYPE_ID as TS_ACTIVITY_TYPE_ID",
	"FIL_RemoveRejected___TS_ACTIVITY_CAT_ID as TS_ACTIVITY_CAT_ID",
	"FIL_RemoveRejected___TS_ACTIVITY_NAME as TS_ACTIVITY_NAME",
	"FIL_RemoveRejected___TS_ACTIVITY_DESC as TS_ACTIVITY_DESC",
	"FIL_RemoveRejected___TS_ACTIVITY_CAT_DESC as TS_ACTIVITY_CAT_DESC",
	"FIL_RemoveRejected___TS_ACTIVITY_TYPE_DESC as TS_ACTIVITY_TYPE_DESC",
	"FIL_RemoveRejected___UpdateStrategy as UpdateStrategy",
	"FIL_RemoveRejected___UpdateDt as UpdateDt",
	"FIL_RemoveRejected___LOAD_TSTMP_NN as LOAD_TSTMP_NN",
	"FIL_RemoveRejected___UpdateStrategy as pyspark_data_action")

# COMMAND ----------
# Processing node Shortcut_to_TS_EMPLOYEE_TIME_tgt, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_TS_EMPLOYEE_TIME_tgt = UPD_SetStrategy.selectExpr(
	"CAST(DAYDT AS DATE) as TS_DAY_DT",
	"CAST(EMPID AS BIGINT) as EMPLOYEE_ID",
	"CAST(ACTXREFID AS BIGINT) as TS_ACTIVITY_XREF_ID",
	"CAST(TS_ACTIVITY_ID AS BIGINT) as TS_ACTIVITY_ID",
	"CAST(TS_ACTIVITY_NAME AS STRING) as TS_ACTIVITY_NAME",
	"CAST(TS_ACTIVITY_DESC AS STRING) as TS_ACTIVITY_DESC",
	"CAST(TS_ACTIVITY_CAT_ID AS BIGINT) as TS_ACTIVITY_CAT_ID",
	"CAST(TS_ACTIVITY_CAT_DESC AS STRING) as TS_ACTIVITY_CAT_DESC",
	"CAST(TS_ACTIVITY_TYPE_ID AS BIGINT) as TS_ACTIVITY_TYPE_ID",
	"CAST(TS_ACTIVITY_TYPE_DESC AS STRING) as TS_ACTIVITY_TYPE_DESC",
	"CAST(RFCNBR AS STRING) as TS_RFC_DESC",
	"CAST(Work_Assign_Cd AS STRING) as TS_WORK_ASSIGN_CD",
	"CAST(HOURS AS DECIMAL(4,2)) as TS_WORK_HOURS",
	"CAST(COMMENT AS STRING) as TS_COMMENT",
	"CAST(CREATEDATE AS DATE) as TS_RECORD_CREATE_DT",
	"CAST(UpdateDt AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP_NN AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.TS_DAY_DT = target.TS_DAY_DT AND source.EMPLOYEE_ID = target.EMPLOYEE_ID AND source.TS_ACTIVITY_XREF_ID = target.TS_ACTIVITY_XREF_ID"""
	refined_perf_table = f"{legacy}.TS_EMPLOYEE_TIME"
	executeMerge(Shortcut_to_TS_EMPLOYEE_TIME_tgt, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("TS_EMPLOYEE_TIME", "TS_EMPLOYEE_TIME", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("TS_EMPLOYEE_TIME", "TS_EMPLOYEE_TIME","Failed",str(e), f"{raw}.log_run_details")
	raise e
		