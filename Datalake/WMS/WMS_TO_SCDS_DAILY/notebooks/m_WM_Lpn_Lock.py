#Code converted on 2023-06-27 09:40:21
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
raw_perf_table = f"{raw}.WM_LPN_LOCK_PRE"
refined_perf_table = f"{refine}.WM_LPN_LOCK"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table.split(".")[1], refine,raw)
Del_Logic=' -- ' #args.Del_Logic
# soft_delete_logic_WM_Lpn_Lock=' ' #args.soft_delete_logic_WM_Lpn_Lock

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_LOCK, type SOURCE 
# COLUMN COUNT: 16

SQ_Shortcut_to_WM_LPN_LOCK = spark.sql(f"""SELECT
LOCATION_ID,
WM_LPN_LOCK_ID,
WM_LPN_ID,
WM_TC_LPN_ID,
WM_INVENTORY_LOCK_CD,
WM_REASON_CD,
LOCK_CNT,
WM_CREATED_SOURCE_TYPE,
WM_CREATED_SOURCE,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_SOURCE_TYPE,
WM_LAST_UPDATED_SOURCE,
WM_LAST_UPDATED_TSTMP,
DELETE_FLAG,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 

DELETE_FLAG =0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_LOCK_PRE, type SOURCE 
# COLUMN COUNT: 14

SQ_Shortcut_to_WM_LPN_LOCK_PRE = spark.sql(f"""SELECT
DC_NBR,
LPN_LOCK_ID,
LPN_ID,
INVENTORY_LOCK_CODE,
REASON_CODE,
LOCK_COUNT,
TC_LPN_ID,
CREATED_SOURCE_TYPE,
CREATED_SOURCE,
CREATED_DTTM,
LAST_UPDATED_SOURCE_TYPE,
LAST_UPDATED_SOURCE,
LAST_UPDATED_DTTM,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LPN_LOCK_PRE_temp = SQ_Shortcut_to_WM_LPN_LOCK_PRE.toDF(*["SQ_Shortcut_to_WM_LPN_LOCK_PRE___" + col for col in SQ_Shortcut_to_WM_LPN_LOCK_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_LPN_LOCK_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_LPN_LOCK_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___LPN_LOCK_ID as LPN_LOCK_ID", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___LPN_ID as LPN_ID", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___INVENTORY_LOCK_CODE as INVENTORY_LOCK_CODE", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___REASON_CODE as REASON_CODE", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___LOCK_COUNT as LOCK_COUNT", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___TC_LPN_ID as TC_LPN_ID", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___CREATED_SOURCE as CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_LPN_LOCK_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
EXP_INT_CONVERSION_temp = EXP_INT_CONVERSION.toDF(*["EXP_INT_CONVERSION___" + col for col in EXP_INT_CONVERSION.columns])
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE_temp.join(EXP_INT_CONVERSION_temp,[SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR == EXP_INT_CONVERSION_temp.EXP_INT_CONVERSION___o_DC_NBR],'inner').selectExpr( \
	"EXP_INT_CONVERSION___o_DC_NBR as DC_NBR", \
	"EXP_INT_CONVERSION___LPN_LOCK_ID as LPN_LOCK_ID", \
	"EXP_INT_CONVERSION___LPN_ID as LPN_ID", \
	"EXP_INT_CONVERSION___INVENTORY_LOCK_CODE as INVENTORY_LOCK_CODE", \
	"EXP_INT_CONVERSION___REASON_CODE as REASON_CODE", \
	"EXP_INT_CONVERSION___LOCK_COUNT as LOCK_COUNT", \
	"EXP_INT_CONVERSION___TC_LPN_ID as TC_LPN_ID", \
	"EXP_INT_CONVERSION___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"EXP_INT_CONVERSION___CREATED_SOURCE as CREATED_SOURCE", \
	"EXP_INT_CONVERSION___CREATED_DTTM as CREATED_DTTM", \
	"EXP_INT_CONVERSION___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"EXP_INT_CONVERSION___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"EXP_INT_CONVERSION___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_INT_CONVERSION___LOAD_TSTMP as LOAD_TSTMP", \
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR")

# COMMAND ----------
# Processing node JNR_WM_LPN_LOCK, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 30

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_LPN_LOCK_temp = SQ_Shortcut_to_WM_LPN_LOCK.toDF(*["SQ_Shortcut_to_WM_LPN_LOCK___" + col for col in SQ_Shortcut_to_WM_LPN_LOCK.columns])

JNR_WM_LPN_LOCK = SQ_Shortcut_to_WM_LPN_LOCK_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_LPN_LOCK_temp.SQ_Shortcut_to_WM_LPN_LOCK___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_LPN_LOCK_temp.SQ_Shortcut_to_WM_LPN_LOCK___WM_LPN_LOCK_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LPN_LOCK_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LPN_LOCK_ID as LPN_LOCK_ID", \
	"JNR_SITE_PROFILE___LPN_ID as LPN_ID", \
	"JNR_SITE_PROFILE___INVENTORY_LOCK_CODE as INVENTORY_LOCK_CODE", \
	"JNR_SITE_PROFILE___REASON_CODE as REASON_CODE", \
	"JNR_SITE_PROFILE___LOCK_COUNT as LOCK_COUNT", \
	"JNR_SITE_PROFILE___TC_LPN_ID as TC_LPN_ID", \
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___LOAD_TSTMP as LOAD_TSTMP", \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_WM_LPN_LOCK___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_LPN_LOCK_ID as i_WM_LPN_LOCK_ID", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_LPN_ID as WM_LPN_ID", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_TC_LPN_ID as WM_TC_LPN_ID", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_INVENTORY_LOCK_CD as WM_INVENTORY_LOCK_CD", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_REASON_CD as WM_REASON_CD", \
	"SQ_Shortcut_to_WM_LPN_LOCK___LOCK_CNT as LOCK_CNT", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_LPN_LOCK___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_LOCK___UPDATE_TSTMP as UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_LOCK___LOAD_TSTMP as i_LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_LOCK___DELETE_FLAG as WM_DELETE_FLAG")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 30

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_LPN_LOCK_temp = JNR_WM_LPN_LOCK.toDF(*["JNR_WM_LPN_LOCK___" + col for col in JNR_WM_LPN_LOCK.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_LPN_LOCK_temp.selectExpr( \
	"JNR_WM_LPN_LOCK___LPN_LOCK_ID as LPN_LOCK_ID", \
	"JNR_WM_LPN_LOCK___LPN_ID as LPN_ID", \
	"JNR_WM_LPN_LOCK___INVENTORY_LOCK_CODE as INVENTORY_LOCK_CODE", \
	"JNR_WM_LPN_LOCK___REASON_CODE as REASON_CODE", \
	"JNR_WM_LPN_LOCK___LOCK_COUNT as LOCK_COUNT", \
	"JNR_WM_LPN_LOCK___TC_LPN_ID as TC_LPN_ID", \
	"JNR_WM_LPN_LOCK___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_WM_LPN_LOCK___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_WM_LPN_LOCK___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_LPN_LOCK___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_LPN_LOCK___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_WM_LPN_LOCK___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_LPN_LOCK___LOAD_TSTMP as LOAD_TSTMP", \
	"JNR_WM_LPN_LOCK___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_LPN_LOCK___i_LOCATION_ID as i_LOCATION_ID", \
	"JNR_WM_LPN_LOCK___i_WM_LPN_LOCK_ID as i_WM_LPN_LOCK_ID", \
	"JNR_WM_LPN_LOCK___WM_LPN_ID as WM_LPN_ID", \
	"JNR_WM_LPN_LOCK___WM_TC_LPN_ID as WM_TC_LPN_ID", \
	"JNR_WM_LPN_LOCK___WM_INVENTORY_LOCK_CD as WM_INVENTORY_LOCK_CD", \
	"JNR_WM_LPN_LOCK___WM_REASON_CD as WM_REASON_CD", \
	"JNR_WM_LPN_LOCK___LOCK_CNT as LOCK_CNT", \
	"JNR_WM_LPN_LOCK___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"JNR_WM_LPN_LOCK___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"JNR_WM_LPN_LOCK___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"JNR_WM_LPN_LOCK___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_LPN_LOCK___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"JNR_WM_LPN_LOCK___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_LPN_LOCK___UPDATE_TSTMP as UPDATE_TSTMP", \
	"JNR_WM_LPN_LOCK___i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"JNR_WM_LPN_LOCK___WM_DELETE_FLAG as WM_DELETE_FLAG") \
    .filter("LPN_LOCK_ID is Null OR i_WM_LPN_LOCK_ID is Null OR (  i_WM_LPN_LOCK_ID is NOT Null AND ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01') \
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns]) \
.withColumn("FIL_UNCHANGED_RECORDS___v_CREATED_DTTM", expr("""IF(FIL_UNCHANGED_RECORDS___CREATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___CREATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM", expr("""IF(FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP)""")) \
	.withColumn("FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP", expr("""IF(FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP)"""))
             
EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___LPN_LOCK_ID as LPN_LOCK_ID", \
	"FIL_UNCHANGED_RECORDS___LPN_ID as LPN_ID", \
	"FIL_UNCHANGED_RECORDS___INVENTORY_LOCK_CODE as INVENTORY_LOCK_CODE", \
	"FIL_UNCHANGED_RECORDS___REASON_CODE as REASON_CODE", \
	"FIL_UNCHANGED_RECORDS___LOCK_COUNT as LOCK_COUNT", \
	"FIL_UNCHANGED_RECORDS___TC_LPN_ID as TC_LPN_ID", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID as i_LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___i_WM_LPN_LOCK_ID as i_WM_LPN_LOCK_ID", \
	"FIL_UNCHANGED_RECORDS___WM_LPN_ID as WM_LPN_ID", \
	"FIL_UNCHANGED_RECORDS___WM_TC_LPN_ID as WM_TC_LPN_ID", \
	"FIL_UNCHANGED_RECORDS___WM_INVENTORY_LOCK_CD as WM_INVENTORY_LOCK_CD", \
	"FIL_UNCHANGED_RECORDS___WM_REASON_CD as WM_REASON_CD", \
	"FIL_UNCHANGED_RECORDS___LOCK_CNT as LOCK_CNT", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___UPDATE_TSTMP as UPDATE_TSTMP", \
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"FIL_UNCHANGED_RECORDS___WM_DELETE_FLAG as WM_DELETE_FLAG", \
	"IF(FIL_UNCHANGED_RECORDS___LPN_LOCK_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_LPN_LOCK_ID IS NOT NULL, 1, 0) as DEL_FLAG", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP_EXP", \
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___LPN_LOCK_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_LPN_LOCK_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_RECORDS___LPN_LOCK_ID IS NOT NULL AND FIL_UNCHANGED_RECORDS___i_WM_LPN_LOCK_ID IS NOT NULL AND ( FIL_UNCHANGED_RECORDS___v_i_WM_CREATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_CREATED_DTTM OR FIL_UNCHANGED_RECORDS___v_i_WM_LAST_UPDATED_TSTMP <> FIL_UNCHANGED_RECORDS___v_LAST_UPDATED_DTTM ), 'UPDATE', NULL)) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID1", \
	"EXP_UPD_VALIDATOR___LPN_LOCK_ID as LPN_LOCK_ID1", \
	"EXP_UPD_VALIDATOR___LPN_ID as LPN_ID1", \
	"EXP_UPD_VALIDATOR___TC_LPN_ID as TC_LPN_ID1", \
	"EXP_UPD_VALIDATOR___INVENTORY_LOCK_CODE as INVENTORY_LOCK_CODE1", \
	"EXP_UPD_VALIDATOR___REASON_CODE as REASON_CODE1", \
	"EXP_UPD_VALIDATOR___LOCK_COUNT as LOCK_COUNT1", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE1", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE1", \
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM1", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE1", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE1", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM1", \
	"EXP_UPD_VALIDATOR___DEL_FLAG as DEL_FLAG", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP_EXP as UPDATE_TSTMP_EXP1", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP1", \
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR1") \
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR1') ==(lit('INSERT')), lit(0)).when(col('o_UPDATE_VALIDATOR1') ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_LPN_LOCK3, type TARGET 
# COLUMN COUNT: 16

Shortcut_to_WM_LPN_LOCK3 = UPD_INSERT_UPDATE.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(LPN_LOCK_ID1 AS BIGINT) as WM_LPN_LOCK_ID",
	"CAST(LPN_ID1 AS BIGINT) as WM_LPN_ID",
	"CAST(TC_LPN_ID1 AS STRING) as WM_TC_LPN_ID",
	"CAST(INVENTORY_LOCK_CODE1 AS STRING) as WM_INVENTORY_LOCK_CD",
	"CAST(REASON_CODE1 AS STRING) as WM_REASON_CD",
	"CAST(LOCK_COUNT1 AS TINYINT) as LOCK_CNT",
	"CAST(CREATED_SOURCE_TYPE1 AS SMALLINT) as WM_CREATED_SOURCE_TYPE",
	"CAST(CREATED_SOURCE1 AS STRING) as WM_CREATED_SOURCE",
	"CAST(CREATED_DTTM1 AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_SOURCE_TYPE1 AS SMALLINT) as WM_LAST_UPDATED_SOURCE_TYPE",
	"CAST(LAST_UPDATED_SOURCE1 AS STRING) as WM_LAST_UPDATED_SOURCE",
	"CAST(LAST_UPDATED_DTTM1 AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(DEL_FLAG AS TINYINT) as DELETE_FLAG",
	"CAST(UPDATE_TSTMP_EXP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_LPN_LOCK_ID = target.WM_LPN_LOCK_ID"""
#   refined_perf_table = "WM_LPN_LOCK"
  executeMerge(Shortcut_to_WM_LPN_LOCK3, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_LPN_LOCK", "WM_LPN_LOCK", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_LPN_LOCK", "WM_LPN_LOCK","Failed",str(e), f"{raw}.log_run_details", )
  raise e


# post sql

# soft_delete_logic_WM_Lpn_Lock= ' $$soft_delete_logic_WM_Lpn_Lock=update WM_Lpn_Lock set delete_flag=1 , update_tstmp=current_date where (location_id, WM_LPN_LOCK_ID) in (select location_id, WM_LPN_LOCK_ID from  WM_Lpn_Lock  base_table where base_table.DELETE_FLAG=0 and (cast(base_table.WM_CREATED_tstmp as date) >= (cast('$$Prev_Run_Dt' as date )-14) or cast(base_table.WM_LAST_UPDATED_TSTMP as date) >= (cast('$$Prev_Run_Dt' as date )-14)) and (location_id, WM_LPN_LOCK_ID) not in ( select   SP.location_id, pre_table.LPN_LOCK_ID from  WM_Lpn_Lock_pre pre_table, site_profile SP where SP.store_nbr =pre_table.dc_nbr));
# ' # args.soft_delete_logic_WM_Lpn_Lock

# update wm_trailer_contents set delete_flag=1, update_tstmp=current_timestamp where
soft_delete_logic_WM_Lpn_Lock= f""" 
select  location_id, WM_LPN_LOCK_ID 
from {refined_perf_table}  
where (location_id, WM_LPN_LOCK_ID) in 
(select location_id, WM_LPN_LOCK_ID 
from  {refined_perf_table}  base_table 
where base_table.DELETE_FLAG=0 
and (cast(base_table.WM_CREATED_tstmp as date) >= (cast('{Prev_Run_Dt}' as date )-14) 
or cast(base_table.WM_LAST_UPDATED_TSTMP as date) >= (cast('{Prev_Run_Dt}' as date )-14)) 
and (location_id, WM_LPN_LOCK_ID) not in 
( select   SP.location_id, pre_table.LPN_LOCK_ID 
from  {raw_perf_table} pre_table, {site_profile_table} SP 
where SP.store_nbr =pre_table.dc_nbr))"""

sd_df = spark.sql(soft_delete_logic_WM_Lpn_Lock)

sd_df.createOrReplaceTempView('WM_LPN_LOCK_SD')

spark.sql(f"""
          MERGE INTO {refined_perf_table} tgt
          USING  WM_LPN_LOCK_SD src
          ON src.location_id = tgt.location_id and src.WM_LPN_LOCK_ID = tgt.WM_LPN_LOCK_ID
          WHEN MATCHED THEN UPDATE
          SET tgt.delete_flag = 1,
          tgt.update_tstmp=current_timestamp
          """)
