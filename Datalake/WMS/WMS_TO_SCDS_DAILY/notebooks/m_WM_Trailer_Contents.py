#Code converted on 2023-06-22 21:02:55
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

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
refined_perf_table = f"{refine}.WM_TRAILER_CONTENTS"
raw_perf_table = f"{raw}.WM_TRAILER_CONTENTS_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


Prev_Run_Dt=genPrevRunDt(refined_perf_table, refine,raw)
Del_Logic=args.Del_Logic
Soft_Delete_Logic_trailer_contents1=args.Soft_Delete_Logic_trailer_contents1

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE, type SOURCE 
# COLUMN COUNT: 13

SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE = spark.sql(f"""SELECT
DC_NBR,
TRAILER_CONTENTS_ID,
VISIT_DETAIL_ID,
IS_PLANNED,
SHIPMENT_ID,
ASN_ID,
PO_ID,
CREATED_DTTM,
CREATED_SOURCE_TYPE,
CREATED_SOURCE,
LAST_UPDATED_DTTM,
LAST_UPDATED_SOURCE_TYPE,
LAST_UPDATED_SOURCE
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE_temp = SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE.toDF(*["SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___" + col for col in SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___TRAILER_CONTENTS_ID as TRAILER_CONTENTS_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___IS_PLANNED as IS_PLANNED", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___SHIPMENT_ID as SHIPMENT_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___ASN_ID as ASN_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___PO_ID as PO_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___CREATED_SOURCE as CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_TRAILER_CONTENTS, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_WM_TRAILER_CONTENTS = spark.sql(f"""SELECT
LOCATION_ID,
WM_TRAILER_CONTENTS_ID,
WM_VISIT_DETAIL_ID,
WM_PLANNED_FLAG,
WM_SHIPMENT_ID,
WM_ASN_ID,
WM_PO_ID,
WM_CREATED_TSTMP,
WM_CREATED_SOURCE_TYPE,
WM_CREATED_SOURCE,
WM_LAST_UPDATED_TSTMP,
WM_LAST_UPDATED_SOURCE_TYPE,
WM_LAST_UPDATED_SOURCE,
DELETE_FLAG,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and DELETE_FLAG =0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 15

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_TRAILER_CONTENTS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_TRAILER_CONTENTS_temp = SQ_Shortcut_to_WM_TRAILER_CONTENTS.toDF(*["SQ_Shortcut_to_WM_TRAILER_CONTENTS___" + col for col in SQ_Shortcut_to_WM_TRAILER_CONTENTS.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_TRAILER_CONTENTS = SQ_Shortcut_to_WM_TRAILER_CONTENTS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_TRAILER_CONTENTS_temp.SQ_Shortcut_to_WM_TRAILER_CONTENTS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_TRAILER_CONTENTS_temp.SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_TRAILER_CONTENTS_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___TRAILER_CONTENTS_ID],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___TRAILER_CONTENTS_ID as TRAILER_CONTENTS_ID", 
	"JNR_SITE_PROFILE___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"JNR_SITE_PROFILE___IS_PLANNED as IS_PLANNED", 
	"JNR_SITE_PROFILE___SHIPMENT_ID as SHIPMENT_ID", 
	"JNR_SITE_PROFILE___ASN_ID as ASN_ID", 
	"JNR_SITE_PROFILE___PO_ID as PO_ID", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___LOCATION_ID as i_LOCATION_ID1", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_TRAILER_CONTENTS_ID as i_WM_TRAILER_CONTENTS_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_VISIT_DETAIL_ID as i_WM_VISIT_DETAIL_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_PLANNED_FLAG as i_WM_PLANNED_FLAG", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_SHIPMENT_ID as i_WM_SHIPMENT_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_ASN_ID as i_WM_ASN_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_PO_ID as i_WM_PO_ID", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_CREATED_SOURCE_TYPE as i_WM_CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_CREATED_SOURCE as i_WM_CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_LAST_UPDATED_SOURCE_TYPE as i_WM_LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___WM_LAST_UPDATED_SOURCE as i_WM_LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___DELETE_FLAG as i_DELETE_FLAG", 
	"SQ_Shortcut_to_WM_TRAILER_CONTENTS___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_TRAILER_CONTENTS_temp = JNR_WM_TRAILER_CONTENTS.toDF(*["JNR_WM_TRAILER_CONTENTS___" + col for col in JNR_WM_TRAILER_CONTENTS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_TRAILER_CONTENTS_temp.selectExpr( 
	"JNR_WM_TRAILER_CONTENTS___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_TRAILER_CONTENTS___TRAILER_CONTENTS_ID as TRAILER_CONTENTS_ID", 
	"JNR_WM_TRAILER_CONTENTS___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"JNR_WM_TRAILER_CONTENTS___IS_PLANNED as IS_PLANNED", 
	"JNR_WM_TRAILER_CONTENTS___SHIPMENT_ID as SHIPMENT_ID", 
	"JNR_WM_TRAILER_CONTENTS___ASN_ID as ASN_ID", 
	"JNR_WM_TRAILER_CONTENTS___PO_ID as PO_ID", 
	"JNR_WM_TRAILER_CONTENTS___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_TRAILER_CONTENTS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_WM_TRAILER_CONTENTS___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_WM_TRAILER_CONTENTS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_WM_TRAILER_CONTENTS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_TRAILER_CONTENTS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_TRAILER_CONTENTS_ID as i_WM_TRAILER_CONTENTS_ID", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_VISIT_DETAIL_ID as i_WM_VISIT_DETAIL_ID", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_PLANNED_FLAG as i_WM_PLANNED_FLAG", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_SHIPMENT_ID as i_WM_SHIPMENT_ID", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_ASN_ID as i_WM_ASN_ID", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_PO_ID as i_WM_PO_ID", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_CREATED_SOURCE_TYPE as i_WM_CREATED_SOURCE_TYPE", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_CREATED_SOURCE as i_WM_CREATED_SOURCE", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_LAST_UPDATED_SOURCE_TYPE as i_WM_LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_TRAILER_CONTENTS___i_WM_LAST_UPDATED_SOURCE as i_WM_LAST_UPDATED_SOURCE", 
	"JNR_WM_TRAILER_CONTENTS___i_DELETE_FLAG as i_DELETE_FLAG", 
	"JNR_WM_TRAILER_CONTENTS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"JNR_WM_TRAILER_CONTENTS___i_LOCATION_ID1 as i_LOCATION_ID1").filter(expr("TRAILER_CONTENTS_ID IS NULL OR i_WM_TRAILER_CONTENTS_ID IS NULL OR (NOT i_WM_TRAILER_CONTENTS_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___TRAILER_CONTENTS_ID as TRAILER_CONTENTS_ID", 
	"FIL_UNCHANGED_RECORDS___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"FIL_UNCHANGED_RECORDS___IS_PLANNED as IS_PLANNED", 
	"FIL_UNCHANGED_RECORDS___SHIPMENT_ID as SHIPMENT_ID", 
	"FIL_UNCHANGED_RECORDS___ASN_ID as ASN_ID", 
	"FIL_UNCHANGED_RECORDS___PO_ID as PO_ID", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___i_WM_TRAILER_CONTENTS_ID as i_WM_TRAILER_CONTENTS_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_VISIT_DETAIL_ID as i_WM_VISIT_DETAIL_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_PLANNED_FLAG as i_WM_PLANNED_FLAG", 
	"FIL_UNCHANGED_RECORDS___i_WM_SHIPMENT_ID as i_WM_SHIPMENT_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_ASN_ID as i_WM_ASN_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_PO_ID as i_WM_PO_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_SOURCE_TYPE as i_WM_CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___i_WM_CREATED_SOURCE as i_WM_CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_SOURCE_TYPE as i_WM_LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___i_WM_LAST_UPDATED_SOURCE as i_WM_LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___i_DELETE_FLAG as i_DELETE_FLAG", 
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID1 as i_LOCATION_ID1", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___TRAILER_CONTENTS_ID IS NULL AND FIL_UNCHANGED_RECORDS___i_WM_TRAILER_CONTENTS_ID IS NOT NULL, 1, 0) as DELETE_FLAG_EXP", 
	"IF(FIL_UNCHANGED_RECORDS___i_WM_TRAILER_CONTENTS_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_UPDATE_INSERT, type UPDATE_STRATEGY 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_UPDATE_INSERT = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_UPD_VALIDATOR___TRAILER_CONTENTS_ID as TRAILER_CONTENTS_ID", 
	"EXP_UPD_VALIDATOR___VISIT_DETAIL_ID as VISIT_DETAIL_ID", 
	"EXP_UPD_VALIDATOR___IS_PLANNED as IS_PLANNED", 
	"EXP_UPD_VALIDATOR___SHIPMENT_ID as SHIPMENT_ID", 
	"EXP_UPD_VALIDATOR___ASN_ID as ASN_ID", 
	"EXP_UPD_VALIDATOR___PO_ID as PO_ID", 
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_UPD_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"EXP_UPD_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPD_VALIDATOR___DELETE_FLAG_EXP as DELETE_FLAG_EXP", 
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1))lit(0)).when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2))lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_TRAILER_CONTENTS_1, type TARGET 
# COLUMN COUNT: 16

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_TRAILER_CONTENTS_ID = target.WM_TRAILER_CONTENTS_ID"""
  # refined_perf_table = "WM_TRAILER_CONTENTS"
  executeMerge(UPD_UPDATE_INSERT, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_TRAILER_CONTENTS", "WM_TRAILER_CONTENTS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_TRAILER_CONTENTS", "WM_TRAILER_CONTENTS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	