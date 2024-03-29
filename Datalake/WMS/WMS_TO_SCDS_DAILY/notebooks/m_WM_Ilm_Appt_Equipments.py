#Code converted on 2023-06-21 18:23:05
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
raw_perf_table = f"{raw}.WM_ILM_APPT_EQUIPMENTS_PRE"
refined_perf_table = f"{refine}.WM_ILM_APPT_EQUIPMENTS"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table.split('.')[1], refine,raw)
Del_Logic =  ' -- ' #args.Del_logic
# soft_delete_logic_WM_Ilm_Appt_Equipments= '  ' #args.soft_delete_logic_WM_Ilm_Appt_Equipments

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS, type SOURCE 
# COLUMN COUNT: 10

SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS = spark.sql(f"""SELECT
LOCATION_ID,
WM_APPOINTMENT_ID,
WM_COMPANY_ID,
WM_EQUIPMENT_INSTANCE_ID,
WM_EQUIPMENT_NBR,
WM_EQUIPMENT_LICENSE_NBR,
WM_EQUIPMENT_LICENSE_STATE,
WM_EQUIPMENT_TYPE,
DELETE_FLAG,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 
DELETE_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id()).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_APPOINTMENTS, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_WM_ILM_APPOINTMENTS = spark.sql(f"""SELECT
WM_ILM_APPOINTMENTS.WM_APPOINTMENT_ID,
WM_ILM_APPOINTMENTS.WM_CREATED_TSTMP,
WM_ILM_APPOINTMENTS.WM_LAST_UPDATED_TSTMP
FROM {refine}.WM_ILM_APPOINTMENTS""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE = spark.sql(f"""SELECT
DC_NBR,
APPOINTMENT_ID,
COMPANY_ID,
EQUIPMENT_INSTANCE_ID,
EQUIPMENT_NUMBER,
EQUIPMENT_LICENSE_NUMBER,
EQUIPMENT_LICENSE_STATE,
EQUIPMENT_TYPE,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE_temp = SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE.toDF(*["SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___" + col for col in SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___APPOINTMENT_ID as APPOINTMENT_ID", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___COMPANY_ID as COMPANY_ID", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___EQUIPMENT_INSTANCE_ID as EQUIPMENT_INSTANCE_ID", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___EQUIPMENT_NUMBER as EQUIPMENT_NUMBER", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___EQUIPMENT_LICENSE_NUMBER as EQUIPMENT_LICENSE_NUMBER", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___EQUIPMENT_LICENSE_STATE as EQUIPMENT_LICENSE_STATE", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___EQUIPMENT_TYPE as EQUIPMENT_TYPE", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 11

JNR_SITE_PROFILE = EXPTRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXPTRANS.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_APPT_EQUIP, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_temp = SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS.toDF(*["SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___" + col for col in SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS.columns])
SQ_Shortcut_to_WM_ILM_APPOINTMENTS_temp = SQ_Shortcut_to_WM_ILM_APPOINTMENTS.toDF(*["SQ_Shortcut_to_WM_ILM_APPOINTMENTS___" + col for col in SQ_Shortcut_to_WM_ILM_APPOINTMENTS.columns])

JNR_APPT_EQUIP = SQ_Shortcut_to_WM_ILM_APPOINTMENTS_temp.join(SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_temp,[SQ_Shortcut_to_WM_ILM_APPOINTMENTS_temp.SQ_Shortcut_to_WM_ILM_APPOINTMENTS___WM_APPOINTMENT_ID == SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS_temp.SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___WM_APPOINTMENT_ID],'inner').selectExpr( \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___WM_APPOINTMENT_ID as WM_APPOINTMENT_ID", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___WM_COMPANY_ID as WM_COMPANY_ID", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_INSTANCE_ID as WM_EQUIPMENT_INSTANCE_ID", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_NBR as WM_EQUIPMENT_NBR", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_LICENSE_NBR as WM_EQUIPMENT_LICENSE_NBR", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_LICENSE_STATE as WM_EQUIPMENT_LICENSE_STATE", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_TYPE as WM_EQUIPMENT_TYPE", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___DELETE_FLAG as DELETE_FLAG", \
	"SQ_Shortcut_to_WM_ILM_APPT_EQUIPMENTS___LOAD_TSTMP as LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENTS___WM_APPOINTMENT_ID as WM_APPOINTMENT_ID1", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENTS___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENTS___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP")

# COMMAND ----------
# Processing node JNR_WM_ILM_APPT_EQUIPMENTS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
JNR_APPT_EQUIP_temp = JNR_APPT_EQUIP.toDF(*["JNR_APPT_EQUIP___" + col for col in JNR_APPT_EQUIP.columns])

JNR_WM_ILM_APPT_EQUIPMENTS = JNR_APPT_EQUIP_temp.join(JNR_SITE_PROFILE_temp,[JNR_APPT_EQUIP_temp.JNR_APPT_EQUIP___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, JNR_APPT_EQUIP_temp.JNR_APPT_EQUIP___WM_APPOINTMENT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___APPOINTMENT_ID, JNR_APPT_EQUIP_temp.JNR_APPT_EQUIP___WM_COMPANY_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___COMPANY_ID, JNR_APPT_EQUIP_temp.JNR_APPT_EQUIP___WM_EQUIPMENT_INSTANCE_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___EQUIPMENT_INSTANCE_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___APPOINTMENT_ID as APPOINTMENT_ID", \
	"JNR_SITE_PROFILE___COMPANY_ID as COMPANY_ID", \
	"JNR_SITE_PROFILE___EQUIPMENT_INSTANCE_ID as EQUIPMENT_INSTANCE_ID", \
	"JNR_SITE_PROFILE___EQUIPMENT_NUMBER as EQUIPMENT_NUMBER", \
	"JNR_SITE_PROFILE___EQUIPMENT_LICENSE_NUMBER as EQUIPMENT_LICENSE_NUMBER", \
	"JNR_SITE_PROFILE___EQUIPMENT_LICENSE_STATE as EQUIPMENT_LICENSE_STATE", \
	"JNR_SITE_PROFILE___EQUIPMENT_TYPE as EQUIPMENT_TYPE", \
	"JNR_APPT_EQUIP___LOCATION_ID as in_LOCATION_ID", \
	"JNR_APPT_EQUIP___WM_APPOINTMENT_ID as WM_APPOINTMENT_ID", \
	"JNR_APPT_EQUIP___WM_COMPANY_ID as WM_COMPANY_ID", \
	"JNR_APPT_EQUIP___WM_EQUIPMENT_INSTANCE_ID as WM_EQUIPMENT_INSTANCE_ID", \
	"JNR_APPT_EQUIP___WM_EQUIPMENT_NBR as WM_EQUIPMENT_NBR", \
	"JNR_APPT_EQUIP___WM_EQUIPMENT_LICENSE_NBR as WM_EQUIPMENT_LICENSE_NBR", \
	"JNR_APPT_EQUIP___WM_EQUIPMENT_LICENSE_STATE as WM_EQUIPMENT_LICENSE_STATE", \
	"JNR_APPT_EQUIP___WM_EQUIPMENT_TYPE as WM_EQUIPMENT_TYPE", \
	"JNR_APPT_EQUIP___DELETE_FLAG as in_DELETE_FLAG", \
	"JNR_APPT_EQUIP___LOAD_TSTMP as in_LOAD_TSTMP", \
	"JNR_APPT_EQUIP___WM_CREATED_TSTMP as in_WM_CREATED_TSTMP", \
	"JNR_APPT_EQUIP___WM_LAST_UPDATED_TSTMP as in_WM_LAST_UPDATED_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_REC, type FILTER 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_ILM_APPT_EQUIPMENTS_temp = JNR_WM_ILM_APPT_EQUIPMENTS.toDF(*["JNR_WM_ILM_APPT_EQUIPMENTS___" + col for col in JNR_WM_ILM_APPT_EQUIPMENTS.columns])

FIL_UNCHANGED_REC = JNR_WM_ILM_APPT_EQUIPMENTS_temp.selectExpr( \
	"JNR_WM_ILM_APPT_EQUIPMENTS___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___APPOINTMENT_ID as APPOINTMENT_ID", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___COMPANY_ID as COMPANY_ID", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___EQUIPMENT_INSTANCE_ID as EQUIPMENT_INSTANCE_ID", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___EQUIPMENT_NUMBER as EQUIPMENT_NUMBER", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___EQUIPMENT_LICENSE_NUMBER as EQUIPMENT_LICENSE_NUMBER", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___EQUIPMENT_LICENSE_STATE as EQUIPMENT_LICENSE_STATE", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___EQUIPMENT_TYPE as EQUIPMENT_TYPE", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___WM_APPOINTMENT_ID as WM_APPOINTMENT_ID", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___WM_COMPANY_ID as WM_COMPANY_ID", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_INSTANCE_ID as WM_EQUIPMENT_INSTANCE_ID", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_NBR as WM_EQUIPMENT_NBR", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_LICENSE_NBR as WM_EQUIPMENT_LICENSE_NBR", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_LICENSE_STATE as WM_EQUIPMENT_LICENSE_STATE", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___WM_EQUIPMENT_TYPE as WM_EQUIPMENT_TYPE", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___in_DELETE_FLAG as in_DELETE_FLAG", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___in_WM_CREATED_TSTMP as in_WM_CREATED_TSTMP", \
	"JNR_WM_ILM_APPT_EQUIPMENTS___in_WM_LAST_UPDATED_TSTMP as in_WM_LAST_UPDATED_TSTMP") \
    .filter("WM_APPOINTMENT_ID is Null OR APPOINTMENT_ID is Null OR (WM_APPOINTMENT_ID is not Null AND \
     ( COALESCE(EQUIPMENT_NUMBER, '') != COALESCE(WM_EQUIPMENT_NBR, '') \
     OR COALESCE(EQUIPMENT_LICENSE_NUMBER, '') != COALESCE(WM_EQUIPMENT_LICENSE_NBR, '') \
     OR COALESCE(EQUIPMENT_LICENSE_STATE, '') != COALESCE(WM_EQUIPMENT_LICENSE_STATE, '') \
     OR COALESCE(EQUIPMENT_TYPE, '') != COALESCE(WM_EQUIPMENT_TYPE, '')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_REC_temp = FIL_UNCHANGED_REC.toDF(*["FIL_UNCHANGED_REC___" + col for col in FIL_UNCHANGED_REC.columns]) \
.withColumn("FIL_UNCHANGED_REC___v_EQUIPMENT_NUMBER", expr("""IF(FIL_UNCHANGED_REC___EQUIPMENT_NUMBER IS NULL, '', FIL_UNCHANGED_REC___EQUIPMENT_NUMBER)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_EQUIPMENT_LICENSE_NUMBER", expr("""IF(FIL_UNCHANGED_REC___EQUIPMENT_LICENSE_NUMBER IS NULL, '', FIL_UNCHANGED_REC___EQUIPMENT_LICENSE_NUMBER)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_EQUIPMENT_LICENSE_STATE", expr("""IF(FIL_UNCHANGED_REC___EQUIPMENT_LICENSE_STATE IS NULL, '', FIL_UNCHANGED_REC___EQUIPMENT_LICENSE_STATE)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_EQUIPMENT_TYPE", expr("""IF(FIL_UNCHANGED_REC___EQUIPMENT_TYPE IS NULL, 0, FIL_UNCHANGED_REC___EQUIPMENT_TYPE)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_WM_EQUIPMENT_NBR", expr("""IF(FIL_UNCHANGED_REC___WM_EQUIPMENT_NBR IS NULL, '', FIL_UNCHANGED_REC___WM_EQUIPMENT_NBR)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_WM_EQUIPMENT_LICENSE_NBR", expr("""IF(FIL_UNCHANGED_REC___WM_EQUIPMENT_LICENSE_NBR IS NULL, '', FIL_UNCHANGED_REC___WM_EQUIPMENT_LICENSE_NBR)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_WM_EQUIPMENT_LICENSE_STATE", expr("""IF(FIL_UNCHANGED_REC___WM_EQUIPMENT_LICENSE_STATE IS NULL, '', FIL_UNCHANGED_REC___WM_EQUIPMENT_LICENSE_STATE)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_WM_EQUIPMENT_TYPE", expr("""IF(FIL_UNCHANGED_REC___WM_EQUIPMENT_TYPE IS NULL, 0, FIL_UNCHANGED_REC___WM_EQUIPMENT_TYPE)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_in_WM_CREATED_TSTMP", expr("""IF(FIL_UNCHANGED_REC___in_WM_CREATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_REC___in_WM_CREATED_TSTMP)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_in_WM_LAST_UPDATED_TSTMP", expr("""IF(FIL_UNCHANGED_REC___in_WM_LAST_UPDATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_REC___in_WM_LAST_UPDATED_TSTMP)"""))

EXP_UPD_VALIDATOR = FIL_UNCHANGED_REC_temp.selectExpr( \
	"FIL_UNCHANGED_REC___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_REC___APPOINTMENT_ID as APPOINTMENT_ID", \
	"FIL_UNCHANGED_REC___COMPANY_ID as COMPANY_ID", \
	"FIL_UNCHANGED_REC___EQUIPMENT_INSTANCE_ID as EQUIPMENT_INSTANCE_ID", \
	"FIL_UNCHANGED_REC___EQUIPMENT_NUMBER as EQUIPMENT_NUMBER", \
	"FIL_UNCHANGED_REC___EQUIPMENT_LICENSE_NUMBER as EQUIPMENT_LICENSE_NUMBER", \
	"FIL_UNCHANGED_REC___EQUIPMENT_LICENSE_STATE as EQUIPMENT_LICENSE_STATE", \
	"FIL_UNCHANGED_REC___EQUIPMENT_TYPE as EQUIPMENT_TYPE", \
	"FIL_UNCHANGED_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_REC___WM_APPOINTMENT_ID as WM_APPOINTMENT_ID", \
	"FIL_UNCHANGED_REC___WM_COMPANY_ID as WM_COMPANY_ID", \
	"FIL_UNCHANGED_REC___WM_EQUIPMENT_INSTANCE_ID as WM_EQUIPMENT_INSTANCE_ID", \
	"FIL_UNCHANGED_REC___WM_EQUIPMENT_NBR as WM_EQUIPMENT_NBR", \
	"FIL_UNCHANGED_REC___WM_EQUIPMENT_LICENSE_NBR as WM_EQUIPMENT_LICENSE_NBR", \
	"FIL_UNCHANGED_REC___WM_EQUIPMENT_LICENSE_STATE as WM_EQUIPMENT_LICENSE_STATE", \
	"FIL_UNCHANGED_REC___WM_EQUIPMENT_TYPE as WM_EQUIPMENT_TYPE", \
	"IF(FIL_UNCHANGED_REC___APPOINTMENT_ID IS NULL AND FIL_UNCHANGED_REC___WM_APPOINTMENT_ID IS NOT NULL, 1, 0) as DELETE_FLAG", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_REC___EQUIPMENT_LICENSE_STATE IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_REC___APPOINTMENT_ID IS NOT NULL AND FIL_UNCHANGED_REC___WM_APPOINTMENT_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_REC___APPOINTMENT_ID IS NOT NULL AND FIL_UNCHANGED_REC___WM_APPOINTMENT_ID IS NOT NULL AND ( FIL_UNCHANGED_REC___v_EQUIPMENT_NUMBER <> FIL_UNCHANGED_REC___v_WM_EQUIPMENT_NBR OR FIL_UNCHANGED_REC___v_EQUIPMENT_LICENSE_NUMBER <> FIL_UNCHANGED_REC___v_WM_EQUIPMENT_LICENSE_NBR OR FIL_UNCHANGED_REC___v_EQUIPMENT_LICENSE_STATE <> FIL_UNCHANGED_REC___v_WM_EQUIPMENT_LICENSE_STATE OR FIL_UNCHANGED_REC___v_EQUIPMENT_TYPE <> FIL_UNCHANGED_REC___v_WM_EQUIPMENT_TYPE ), 'UPDATE', NULL)) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID1", \
	"EXP_UPD_VALIDATOR___APPOINTMENT_ID as APPOINTMENT_ID1", \
	"EXP_UPD_VALIDATOR___COMPANY_ID as COMPANY_ID1", \
	"EXP_UPD_VALIDATOR___EQUIPMENT_INSTANCE_ID as EQUIPMENT_INSTANCE_ID1", \
	"EXP_UPD_VALIDATOR___EQUIPMENT_NUMBER as EQUIPMENT_NUMBER1", \
	"EXP_UPD_VALIDATOR___EQUIPMENT_LICENSE_NUMBER as EQUIPMENT_LICENSE_NUMBER1", \
	"EXP_UPD_VALIDATOR___EQUIPMENT_LICENSE_STATE as EQUIPMENT_LICENSE_STATE1", \
	"EXP_UPD_VALIDATOR___EQUIPMENT_TYPE as EQUIPMENT_TYPE1", \
	"EXP_UPD_VALIDATOR___DELETE_FLAG as DELETE_FLAG1", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP1", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP1", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR1") \
	.withColumn('pyspark_data_action', when(col('o_UPD_VALIDATOR1') ==(lit('INSERT')), lit(0)).when(col('o_UPD_VALIDATOR1') ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_ILM_APPT_EQUIPMENTS, type TARGET 
# COLUMN COUNT: 11

Shortcut_to_WM_ILM_APPT_EQUIPMENTS = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(APPOINTMENT_ID1 AS DECIMAL(9,0)) as WM_APPOINTMENT_ID",
	"CAST(COMPANY_ID1 AS DECIMAL(9,0)) as WM_COMPANY_ID",
	"CAST(EQUIPMENT_INSTANCE_ID1 AS DECIMAL(9,0)) as WM_EQUIPMENT_INSTANCE_ID",
	"CAST(EQUIPMENT_NUMBER1 AS STRING) as WM_EQUIPMENT_NBR",
	"CAST(EQUIPMENT_LICENSE_NUMBER1 AS STRING) as WM_EQUIPMENT_LICENSE_NBR",
	"CAST(EQUIPMENT_LICENSE_STATE1 AS STRING) as WM_EQUIPMENT_LICENSE_STATE",
	"CAST(EQUIPMENT_TYPE1 AS DECIMAL(4,0)) as WM_EQUIPMENT_TYPE",
	"CAST(DELETE_FLAG1 AS DECIMAL(1,0)) as DELETE_FLAG",
	"CAST(UPDATE_TSTMP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_APPOINTMENT_ID = target.WM_APPOINTMENT_ID AND source.WM_COMPANY_ID = target.WM_COMPANY_ID AND source.WM_EQUIPMENT_INSTANCE_ID = target.WM_EQUIPMENT_INSTANCE_ID"""
#   refined_perf_table = "WM_ILM_APPT_EQUIPMENTS"
  executeMerge(Shortcut_to_WM_ILM_APPT_EQUIPMENTS, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_ILM_APPT_EQUIPMENTS", "WM_ILM_APPT_EQUIPMENTS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_ILM_APPT_EQUIPMENTS", "WM_ILM_APPT_EQUIPMENTS","Failed",str(e), f"{raw}.log_run_details", )
  raise e


# post sql

# soft_delete_logic_WM_Ilm_Appt_Equipments= ' $$soft_delete_logic_WM_Ilm_Appt_Equipments=
# update WM_Ilm_Appt_Equipments set delete_flag=1, update_tstmp=current_timestamp where(location_id, WM_APPOINTMENT_ID) in (select base_table.location_id, base_table.WM_APPOINTMENT_ID from  WM_Ilm_Appt_Equipments base_table, WM_Ilm_Appointments ba where ba.wm_appointment_id=base_table.wm_appointment_id                                                                                                                         and base_table.DELETE_FLAG=0 d (cast(ba.WM_CREATED_tstmp as date) >= (cast('$$Prev_Run_Dt' as date )-14) or cast(ba.WM_LAST_UPDATED_TSTMP as date) >= (cast('$$Prev_Run_Dt' as date )-14)) and (base_table.location_id, base_table.WM_APPOINTMENT_ID,base_table.WM_COMPANY_ID, base_table.WM_EQUIPMENT_INSTANCE_ID) not in ( select   SP.location_id, pre_table.APPOINTMENT_ID,pre_table.COMPANY_ID, pre_table.EQUIPMENT_INSTANCE_ID from  WM_Ilm_Appt_Equipments_PRE pre_table,site_profile SP where SP.store_nbr =pre_table.dc_nbr));


# update wm_trailer_contents set delete_flag=1, update_tstmp=current_timestamp where
soft_delete_logic_WM_Ilm_Appt_Equipments= f""" 
select  location_id, WM_APPOINTMENT_ID 
from {refined_perf_table}  
where (location_id, WM_APPOINTMENT_ID) in 
(select base_table.location_id, base_table.WM_APPOINTMENT_ID 
from  {refined_perf_table}  base_table , {refine}.WM_Ilm_Appointments ba 
where ba.wm_appointment_id=base_table.wm_appointment_id 
and base_table.DELETE_FLAG=0 
and ( cast(ba.WM_CREATED_tstmp as date) >= (cast('{Prev_Run_Dt}' as date )-14) 
		or cast(ba.WM_LAST_UPDATED_TSTMP as date) >= (cast('{Prev_Run_Dt}' as date )-14))
and (base_table.location_id, base_table.WM_APPOINTMENT_ID,base_table.WM_COMPANY_ID, base_table.WM_EQUIPMENT_INSTANCE_ID) not in ( select   SP.location_id, pre_table.APPOINTMENT_ID,pre_table.COMPANY_ID, pre_table.EQUIPMENT_INSTANCE_ID from  {raw_perf_table} pre_table, {site_profile_table} SP 
where SP.store_nbr =pre_table.dc_nbr))"""

sd_df = spark.sql(soft_delete_logic_WM_Ilm_Appt_Equipments)

sd_df.createOrReplaceTempView('WM_ILM_APPT_EQUIPMENTS_SD')

spark.sql(f"""
          MERGE INTO {refined_perf_table} tgt
          USING  WM_ILM_APPT_EQUIPMENTS_SD src
          ON src.location_id = tgt.location_id and src.WM_APPOINTMENT_ID = tgt.WM_APPOINTMENT_ID
          WHEN MATCHED THEN UPDATE
          SET tgt.delete_flag = 1,
          tgt.update_tstmp=current_timestamp
          """)

