#Code converted on 2023-06-21 15:27:59
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
raw_perf_table = f"{raw}.WM_ILM_APPOINTMENT_OBJECTS_PRE"
refined_perf_table = f"{refine}.WM_ILM_APPOINTMENT_OBJECTS"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table.split('.')[1], refine,raw)
Del_Logic= ' -- ' #args.Del_Logic
# soft_delete_logic_WM_Ilm_Appointment_Objects= '  ' #args.soft_delete_logic_WM_Ilm_Appointment_Objects

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS = spark.sql(f"""SELECT
LOCATION_ID,
WM_ILM_APPOINTMENT_OBJECTS_ID,
WM_COMPANY_ID,
WM_APPOINTMENT_ID,
WM_STOP_SEQ,
WM_APPT_OBJ_ID,
WM_APPT_OBJ_TYPE,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
DELETE_FLAG,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 
DELETE_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id()).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE = spark.sql(f"""SELECT
DC_NBR,
ID,
APPT_OBJ_TYPE,
APPT_OBJ_ID,
COMPANY_ID,
APPOINTMENT_ID,
STOP_SEQ,
CREATED_DTTM,
LAST_UPDATED_DTTM
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE_temp = SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE.toDF(*["SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___" + col for col in SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___ID as ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___APPT_OBJ_TYPE as APPT_OBJ_TYPE", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___APPT_OBJ_ID as APPT_OBJ_ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___COMPANY_ID as COMPANY_ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___APPOINTMENT_ID as APPOINTMENT_ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___STOP_SEQ as STOP_SEQ", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 11

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_ILM_APPOINTMENT_OBJECTS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_temp = SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS.toDF(*["SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___" + col for col in SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS.columns])

JNR_WM_ILM_APPOINTMENT_OBJECTS = SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_temp.SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS_temp.SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___WM_ILM_APPOINTMENT_OBJECTS_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___ID as ID", \
	"JNR_SITE_PROFILE___APPT_OBJ_TYPE as APPT_OBJ_TYPE", \
	"JNR_SITE_PROFILE___APPT_OBJ_ID as APPT_OBJ_ID", \
	"JNR_SITE_PROFILE___COMPANY_ID as COMPANY_ID", \
	"JNR_SITE_PROFILE___APPOINTMENT_ID as APPOINTMENT_ID", \
	"JNR_SITE_PROFILE___STOP_SEQ as STOP_SEQ", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___LOCATION_ID as i_LOCATION_ID1", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___WM_ILM_APPOINTMENT_OBJECTS_ID as i_WM_ILM_APPOINTMENT_OBJECTS_ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___WM_COMPANY_ID as i_WM_COMPANY_ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___WM_APPOINTMENT_ID as i_WM_APPOINTMENT_ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___WM_STOP_SEQ as i_WM_STOP_SEQ", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___WM_APPT_OBJ_ID as i_WM_APPT_OBJ_ID", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___WM_APPT_OBJ_TYPE as i_WM_APPT_OBJ_TYPE", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___DELETE_FLAG as i_DELETE_FLAG", \
	"SQ_Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_ILM_APPOINTMENT_OBJECTS_temp = JNR_WM_ILM_APPOINTMENT_OBJECTS.toDF(*["JNR_WM_ILM_APPOINTMENT_OBJECTS___" + col for col in JNR_WM_ILM_APPOINTMENT_OBJECTS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_ILM_APPOINTMENT_OBJECTS_temp.selectExpr( \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___ID as ID", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___APPT_OBJ_TYPE as APPT_OBJ_TYPE", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___APPT_OBJ_ID as APPT_OBJ_ID", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___COMPANY_ID as COMPANY_ID", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___APPOINTMENT_ID as APPOINTMENT_ID", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___STOP_SEQ as STOP_SEQ", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_LOCATION_ID1 as i_LOCATION_ID1", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_WM_ILM_APPOINTMENT_OBJECTS_ID as i_WM_ILM_APPOINTMENT_OBJECTS_ID", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_WM_COMPANY_ID as i_WM_COMPANY_ID", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_WM_APPOINTMENT_ID as i_WM_APPOINTMENT_ID", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_WM_STOP_SEQ as i_WM_STOP_SEQ", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_WM_APPT_OBJ_ID as i_WM_APPT_OBJ_ID", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_WM_APPT_OBJ_TYPE as i_WM_APPT_OBJ_TYPE", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_DELETE_FLAG as i_DELETE_FLAG", \
	"JNR_WM_ILM_APPOINTMENT_OBJECTS___i_LOAD_TSTMP as i_LOAD_TSTMP") \
    .filter("ID is Null OR i_WM_ILM_APPOINTMENT_OBJECTS_ID is Null OR ( i_WM_ILM_APPOINTMENT_OBJECTS_ID is not Null AND \
     ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01') \
     OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns]) \
.withColumn("FIL_UNCHANGED_RECORDS___v_CREATED_DTTM", expr("""IF(FIL_UNCHANGED_RECORDS___CREATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___CREATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM", expr("""IF(FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP)"""))

EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___ID as ID", \
	"FIL_UNCHANGED_RECORDS___APPT_OBJ_TYPE as APPT_OBJ_TYPE", \
	"FIL_UNCHANGED_RECORDS___APPT_OBJ_ID as APPT_OBJ_ID", \
	"FIL_UNCHANGED_RECORDS___COMPANY_ID as COMPANY_ID", \
	"FIL_UNCHANGED_RECORDS___APPOINTMENT_ID as APPOINTMENT_ID", \
	"FIL_UNCHANGED_RECORDS___STOP_SEQ as STOP_SEQ", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID1 as i_LOCATION_ID1", \
	"FIL_UNCHANGED_RECORDS___i_WM_ILM_APPOINTMENT_OBJECTS_ID as i_WM_ILM_APPOINTMENT_OBJECTS_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_COMPANY_ID as i_WM_COMPANY_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_APPOINTMENT_ID as i_WM_APPOINTMENT_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_STOP_SEQ as i_WM_STOP_SEQ", \
	"FIL_UNCHANGED_RECORDS___i_WM_APPT_OBJ_ID as i_WM_APPT_OBJ_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_APPT_OBJ_TYPE as i_WM_APPT_OBJ_TYPE", \
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_DELETE_FLAG as i_DELETE_FLAG", \
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_ILM_APPOINTMENT_OBJECTS_ID IS NOT NULL, 1, 0) as DELETE_FLAG", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_ILM_APPOINTMENT_OBJECTS_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_RECORDS___ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_ILM_APPOINTMENT_OBJECTS_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATED_DTTM OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM ), 'UPDATE', NULL)) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
EXP_OUTPUT_VALIDATOR_temp = EXP_OUTPUT_VALIDATOR.toDF(*["EXP_OUTPUT_VALIDATOR___" + col for col in EXP_OUTPUT_VALIDATOR.columns])

UPD_INS_UPD = EXP_OUTPUT_VALIDATOR_temp.selectExpr( \
	"EXP_OUTPUT_VALIDATOR___LOCATION_ID as LOCATION_ID1", \
	"EXP_OUTPUT_VALIDATOR___ID as ID1", \
	"EXP_OUTPUT_VALIDATOR___APPT_OBJ_TYPE as APPT_OBJ_TYPE1", \
	"EXP_OUTPUT_VALIDATOR___APPT_OBJ_ID as APPT_OBJ_ID1", \
	"EXP_OUTPUT_VALIDATOR___COMPANY_ID as COMPANY_ID1", \
	"EXP_OUTPUT_VALIDATOR___APPOINTMENT_ID as APPOINTMENT_ID1", \
	"EXP_OUTPUT_VALIDATOR___STOP_SEQ as STOP_SEQ1", \
	"EXP_OUTPUT_VALIDATOR___CREATED_DTTM as CREATED_DTTM1", \
	"EXP_OUTPUT_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM1", \
	"EXP_OUTPUT_VALIDATOR___DELETE_FLAG as DELETE_FLAG1", \
	"EXP_OUTPUT_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP1", \
	"EXP_OUTPUT_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP1", \
	"EXP_OUTPUT_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR1") \
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR1') ==(lit('INSERT')), lit(0)).when(col('o_UPDATE_VALIDATOR1') ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS1, type TARGET 
# COLUMN COUNT: 12

Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(ID1 AS DECIMAL(9,0)) as WM_ILM_APPOINTMENT_OBJECTS_ID",
	"CAST(COMPANY_ID1 AS DECIMAL(9,0)) as WM_COMPANY_ID",
	"CAST(APPOINTMENT_ID1 AS DECIMAL(9,0)) as WM_APPOINTMENT_ID",
	"CAST(STOP_SEQ1 AS DECIMAL(4,0)) as WM_STOP_SEQ",
	"CAST(APPT_OBJ_ID1 AS DECIMAL(9,0)) as WM_APPT_OBJ_ID",
	"CAST(APPT_OBJ_TYPE1 AS DECIMAL(3,0)) as WM_APPT_OBJ_TYPE",
	"CAST(CREATED_DTTM1 AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_DTTM1 AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(DELETE_FLAG1 AS DECIMAL(1,0)) as DELETE_FLAG",
	"CAST(UPDATE_TSTMP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_ILM_APPOINTMENT_OBJECTS_ID = target.WM_ILM_APPOINTMENT_OBJECTS_ID"""
#   refined_perf_table = "WM_ILM_APPOINTMENT_OBJECTS"
  executeMerge(Shortcut_to_WM_ILM_APPOINTMENT_OBJECTS1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_ILM_APPOINTMENT_OBJECTS", "WM_ILM_APPOINTMENT_OBJECTS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_ILM_APPOINTMENT_OBJECTS", "WM_ILM_APPOINTMENT_OBJECTS","Failed",str(e), f"{raw}.log_run_details", )
  raise e


# post sql

# soft_delete_logic_WM_Ilm_Appointment_Objects= ' $$soft_delete_logic_WM_Ilm_Appointment_Objects=
# update WM_Ilm_Appointment_Objects set delete_flag=1 , update_tstmp=current_date where (location_id, WM_Ilm_Appointment_Objects_ID) in (select location_id, WM_Ilm_Appointment_Objects_ID 
# from  WM_Ilm_Appointment_Objects  base_table 
# where base_table.DELETE_FLAG=0 
# and (cast(base_table.WM_CREATED_TSTMP as date) >= (cast('$$Prev_Run_Dt' as date )-14) 
#      or cast(base_table.WM_LAST_UPDATED_TSTMP as date) >= (cast('$$Prev_Run_Dt' as date )-14)) 
# 		 and (location_id, WM_Ilm_Appointment_Objects_ID) not in ( select   SP.location_id, pre_table.ID from  WM_Ilm_Appointment_Objects_pre pre_table, site_profile SP where SP.store_nbr =pre_table.dc_nbr));# ' # args.soft_delete_logic_WM_Ilm_Appointment_Objects

# update wm_trailer_contents set delete_flag=1, update_tstmp=current_timestamp where
soft_delete_logic_WM_Ilm_Appointment_Objects= f""" 
select  location_id, WM_Ilm_Appointment_Objects_ID 
from {refined_perf_table}  
where (location_id, WM_Ilm_Appointment_Objects_ID) in 
(select location_id, WM_Ilm_Appointment_Objects_ID 
from  {refined_perf_table}  base_table 
where base_table.DELETE_FLAG=0 
and (cast(base_table.WM_CREATED_TSTMP as date) >= (cast('{Prev_Run_Dt}' as date )-14) 
or cast(base_table.WM_LAST_UPDATED_TSTMP as date) >= (cast('{Prev_Run_Dt}' as date )-14)) 
and (location_id, WM_Ilm_Appointment_Objects_ID) not in 
( select   SP.location_id, pre_table.ID 
from  {raw_perf_table} pre_table, {site_profile_table} SP 
where SP.store_nbr =pre_table.dc_nbr))"""

sd_df = spark.sql(soft_delete_logic_WM_Ilm_Appointment_Objects)

sd_df.createOrReplaceTempView('WM_ILM_APPOINTMENT_OBJECTS_SD')

spark.sql(f"""
          MERGE INTO {refined_perf_table} tgt
          USING  WM_ILM_APPOINTMENT_OBJECTS_SD src
          ON src.location_id = tgt.location_id and src.WM_Ilm_Appointment_Objects_ID = tgt.WM_Ilm_Appointment_Objects_ID
          WHEN MATCHED THEN UPDATE
          SET tgt.delete_flag = 1,
          tgt.update_tstmp=current_timestamp
          """)

