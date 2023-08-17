#Code converted on 2023-06-22 21:00:04
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
refined_perf_table = f"{refine}.WM_SHIP_VIA"
raw_perf_table = f"{raw}.WM_SHIP_VIA_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SHIP_VIA_PRE, type SOURCE 
# COLUMN COUNT: 24

SQ_Shortcut_to_WM_SHIP_VIA_PRE = spark.sql(f"""SELECT
DC_NBR,
SHIP_VIA_ID,
TC_COMPANY_ID,
CARRIER_ID,
SERVICE_LEVEL_ID,
MOT_ID,
LABEL_TYPE,
SERVICE_LEVEL_ICON,
EXECUTION_LEVEL_ID,
BILL_SHIP_VIA_ID,
IS_TRACKING_NBR_REQ,
MARKED_FOR_DELETION,
DESCRIPTION,
ACCESSORIAL_SEARCH_STRING,
INS_COVER_TYPE_ID,
MIN_DECLARED_VALUE,
MAX_DECLARED_VALUE,
SERVICE_LEVEL_INDICATOR,
DECLARED_VALUE_CURRENCY,
SHIP_VIA,
CUSTOM_SHIPVIA_ATTRIB,
CREATED_DTTM,
LAST_UPDATED_DTTM,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 24

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_SHIP_VIA_PRE_temp = SQ_Shortcut_to_WM_SHIP_VIA_PRE.toDF(*["SQ_Shortcut_to_WM_SHIP_VIA_PRE___" + col for col in SQ_Shortcut_to_WM_SHIP_VIA_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_SHIP_VIA_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_SHIP_VIA_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___SHIP_VIA_ID as SHIP_VIA_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___TC_COMPANY_ID as TC_COMPANY_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___CARRIER_ID as CARRIER_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___SERVICE_LEVEL_ID as SERVICE_LEVEL_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___MOT_ID as MOT_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___LABEL_TYPE as LABEL_TYPE", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___SERVICE_LEVEL_ICON as SERVICE_LEVEL_ICON", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___EXECUTION_LEVEL_ID as EXECUTION_LEVEL_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___BILL_SHIP_VIA_ID as BILL_SHIP_VIA_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___IS_TRACKING_NBR_REQ as IS_TRACKING_NBR_REQ", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___MARKED_FOR_DELETION as MARKED_FOR_DELETION", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___DESCRIPTION as DESCRIPTION", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___ACCESSORIAL_SEARCH_STRING as ACCESSORIAL_SEARCH_STRING", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___INS_COVER_TYPE_ID as INS_COVER_TYPE_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___MIN_DECLARED_VALUE as MIN_DECLARED_VALUE", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___MAX_DECLARED_VALUE as MAX_DECLARED_VALUE", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___SERVICE_LEVEL_INDICATOR as SERVICE_LEVEL_INDICATOR", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___DECLARED_VALUE_CURRENCY as DECLARED_VALUE_CURRENCY", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___SHIP_VIA as SHIP_VIA", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___CUSTOM_SHIPVIA_ATTRIB as CUSTOM_SHIPVIA_ATTRIB", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_SHIP_VIA_PRE___LOAD_TSTMP as LOAD_TSTMP" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SHIP_VIA, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_SHIP_VIA = spark.sql(f"""SELECT
LOCATION_ID,
WM_SHIP_VIA_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_SHIP_VIA_ID IN (SELECT SHIP_VIA_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 25

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_SHIP_VIA, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_SHIP_VIA_temp = SQ_Shortcut_to_WM_SHIP_VIA.toDF(*["SQ_Shortcut_to_WM_SHIP_VIA___" + col for col in SQ_Shortcut_to_WM_SHIP_VIA.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_SHIP_VIA = SQ_Shortcut_to_WM_SHIP_VIA_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_SHIP_VIA_temp.SQ_Shortcut_to_WM_SHIP_VIA___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_SHIP_VIA_temp.SQ_Shortcut_to_WM_SHIP_VIA___WM_SHIP_VIA_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___SHIP_VIA_ID],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___SHIP_VIA_ID as SHIP_VIA_ID", 
	"JNR_SITE_PROFILE___TC_COMPANY_ID as TC_COMPANY_ID", 
	"JNR_SITE_PROFILE___CARRIER_ID as CARRIER_ID", 
	"JNR_SITE_PROFILE___SERVICE_LEVEL_ID as SERVICE_LEVEL_ID", 
	"JNR_SITE_PROFILE___MOT_ID as MOT_ID", 
	"JNR_SITE_PROFILE___LABEL_TYPE as LABEL_TYPE", 
	"JNR_SITE_PROFILE___SERVICE_LEVEL_ICON as SERVICE_LEVEL_ICON", 
	"JNR_SITE_PROFILE___EXECUTION_LEVEL_ID as EXECUTION_LEVEL_ID", 
	"JNR_SITE_PROFILE___BILL_SHIP_VIA_ID as BILL_SHIP_VIA_ID", 
	"JNR_SITE_PROFILE___IS_TRACKING_NBR_REQ as IS_TRACKING_NBR_REQ", 
	"JNR_SITE_PROFILE___MARKED_FOR_DELETION as MARKED_FOR_DELETION", 
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", 
	"JNR_SITE_PROFILE___ACCESSORIAL_SEARCH_STRING as ACCESSORIAL_SEARCH_STRING", 
	"JNR_SITE_PROFILE___INS_COVER_TYPE_ID as INS_COVER_TYPE_ID", 
	"JNR_SITE_PROFILE___MIN_DECLARED_VALUE as MIN_DECLARED_VALUE", 
	"JNR_SITE_PROFILE___MAX_DECLARED_VALUE as MAX_DECLARED_VALUE", 
	"JNR_SITE_PROFILE___SERVICE_LEVEL_INDICATOR as SERVICE_LEVEL_INDICATOR", 
	"JNR_SITE_PROFILE___DECLARED_VALUE_CURRENCY as DECLARED_VALUE_CURRENCY", 
	"JNR_SITE_PROFILE___SHIP_VIA as SHIP_VIA", 
	"JNR_SITE_PROFILE___CUSTOM_SHIPVIA_ATTRIB as CUSTOM_SHIPVIA_ATTRIB", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_SHIP_VIA___LOCATION_ID as i_LOCATION_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA___WM_SHIP_VIA_ID as i_WM_SHIP_VIA_ID", 
	"SQ_Shortcut_to_WM_SHIP_VIA___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_SHIP_VIA___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_SHIP_VIA___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 27

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_SHIP_VIA_temp = JNR_WM_SHIP_VIA.toDF(*["JNR_WM_SHIP_VIA___" + col for col in JNR_WM_SHIP_VIA.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_SHIP_VIA_temp.selectExpr( 
	"JNR_WM_SHIP_VIA___i_WM_SHIP_VIA_ID as i_WM_SHIP_VIA_ID", 
	"JNR_WM_SHIP_VIA___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"JNR_WM_SHIP_VIA___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_SHIP_VIA___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"JNR_WM_SHIP_VIA___LOCATION_ID as LOCATION_ID1", 
	"JNR_WM_SHIP_VIA___SHIP_VIA_ID as SHIP_VIA_ID", 
	"JNR_WM_SHIP_VIA___TC_COMPANY_ID as TC_COMPANY_ID", 
	"JNR_WM_SHIP_VIA___CARRIER_ID as CARRIER_ID", 
	"JNR_WM_SHIP_VIA___SERVICE_LEVEL_ID as SERVICE_LEVEL_ID", 
	"JNR_WM_SHIP_VIA___MOT_ID as MOT_ID", 
	"JNR_WM_SHIP_VIA___LABEL_TYPE as LABEL_TYPE", 
	"JNR_WM_SHIP_VIA___SERVICE_LEVEL_ICON as SERVICE_LEVEL_ICON", 
	"JNR_WM_SHIP_VIA___EXECUTION_LEVEL_ID as EXECUTION_LEVEL_ID", 
	"JNR_WM_SHIP_VIA___BILL_SHIP_VIA_ID as BILL_SHIP_VIA_ID", 
	"JNR_WM_SHIP_VIA___IS_TRACKING_NBR_REQ as IS_TRACKING_NBR_REQ", 
	"JNR_WM_SHIP_VIA___MARKED_FOR_DELETION as MARKED_FOR_DELETION1", 
	"JNR_WM_SHIP_VIA___DESCRIPTION as DESCRIPTION", 
	"JNR_WM_SHIP_VIA___ACCESSORIAL_SEARCH_STRING as ACCESSORIAL_SEARCH_STRING1", 
	"JNR_WM_SHIP_VIA___INS_COVER_TYPE_ID as INS_COVER_TYPE_ID", 
	"JNR_WM_SHIP_VIA___MIN_DECLARED_VALUE as MIN_DECLARED_VALUE1", 
	"JNR_WM_SHIP_VIA___MAX_DECLARED_VALUE as MAX_DECLARED_VALUE1", 
	"JNR_WM_SHIP_VIA___SERVICE_LEVEL_INDICATOR as SERVICE_LEVEL_INDICATOR", 
	"JNR_WM_SHIP_VIA___DECLARED_VALUE_CURRENCY as DECLARED_VALUE_CURRENCY1", 
	"JNR_WM_SHIP_VIA___SHIP_VIA as SHIP_VIA", 
	"JNR_WM_SHIP_VIA___CUSTOM_SHIPVIA_ATTRIB as CUSTOM_SHIPVIA_ATTRIB", 
	"JNR_WM_SHIP_VIA___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_SHIP_VIA___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM").filter(expr("i_WM_SHIP_VIA_ID IS NULL OR (NOT i_WM_SHIP_VIA_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID1 as LOCATION_ID1", 
	"FIL_UNCHANGED_RECORDS___SHIP_VIA_ID as SHIP_VIA_ID", 
	"FIL_UNCHANGED_RECORDS___TC_COMPANY_ID as TC_COMPANY_ID", 
	"FIL_UNCHANGED_RECORDS___CARRIER_ID as CARRIER_ID", 
	"FIL_UNCHANGED_RECORDS___SERVICE_LEVEL_ID as SERVICE_LEVEL_ID", 
	"FIL_UNCHANGED_RECORDS___MOT_ID as MOT_ID", 
	"FIL_UNCHANGED_RECORDS___LABEL_TYPE as LABEL_TYPE", 
	"FIL_UNCHANGED_RECORDS___SERVICE_LEVEL_ICON as SERVICE_LEVEL_ICON", 
	"FIL_UNCHANGED_RECORDS___EXECUTION_LEVEL_ID as EXECUTION_LEVEL_ID", 
	"FIL_UNCHANGED_RECORDS___BILL_SHIP_VIA_ID as BILL_SHIP_VIA_ID", 
	"FIL_UNCHANGED_RECORDS___IS_TRACKING_NBR_REQ as IS_TRACKING_NBR_REQ", 
	"FIL_UNCHANGED_RECORDS___MARKED_FOR_DELETION1 as MARKED_FOR_DELETION1", 
	"FIL_UNCHANGED_RECORDS___DESCRIPTION as DESCRIPTION", 
	"FIL_UNCHANGED_RECORDS___ACCESSORIAL_SEARCH_STRING1 as ACCESSORIAL_SEARCH_STRING1", 
	"FIL_UNCHANGED_RECORDS___INS_COVER_TYPE_ID as INS_COVER_TYPE_ID", 
	"FIL_UNCHANGED_RECORDS___MIN_DECLARED_VALUE1 as MIN_DECLARED_VALUE1", 
	"FIL_UNCHANGED_RECORDS___MAX_DECLARED_VALUE1 as MAX_DECLARED_VALUE1", 
	"FIL_UNCHANGED_RECORDS___SERVICE_LEVEL_INDICATOR as SERVICE_LEVEL_INDICATOR", 
	"FIL_UNCHANGED_RECORDS___DECLARED_VALUE_CURRENCY1 as DECLARED_VALUE_CURRENCY1", 
	"FIL_UNCHANGED_RECORDS___SHIP_VIA as SHIP_VIA", 
	"FIL_UNCHANGED_RECORDS___CUSTOM_SHIPVIA_ATTRIB as CUSTOM_SHIPVIA_ATTRIB", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_WM_SHIP_VIA_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID1 as LOCATION_ID1", 
	"EXP_UPD_VALIDATOR___SHIP_VIA_ID as SHIP_VIA_ID", 
	"EXP_UPD_VALIDATOR___TC_COMPANY_ID as TC_COMPANY_ID", 
	"EXP_UPD_VALIDATOR___CARRIER_ID as CARRIER_ID", 
	"EXP_UPD_VALIDATOR___SERVICE_LEVEL_ID as SERVICE_LEVEL_ID", 
	"EXP_UPD_VALIDATOR___MOT_ID as MOT_ID", 
	"EXP_UPD_VALIDATOR___LABEL_TYPE as LABEL_TYPE", 
	"EXP_UPD_VALIDATOR___SERVICE_LEVEL_ICON as SERVICE_LEVEL_ICON", 
	"EXP_UPD_VALIDATOR___EXECUTION_LEVEL_ID as EXECUTION_LEVEL_ID", 
	"EXP_UPD_VALIDATOR___BILL_SHIP_VIA_ID as BILL_SHIP_VIA_ID", 
	"EXP_UPD_VALIDATOR___IS_TRACKING_NBR_REQ as IS_TRACKING_NBR_REQ", 
	"EXP_UPD_VALIDATOR___MARKED_FOR_DELETION1 as MARKED_FOR_DELETION1", 
	"EXP_UPD_VALIDATOR___DESCRIPTION as DESCRIPTION", 
	"EXP_UPD_VALIDATOR___ACCESSORIAL_SEARCH_STRING1 as ACCESSORIAL_SEARCH_STRING1", 
	"EXP_UPD_VALIDATOR___INS_COVER_TYPE_ID as INS_COVER_TYPE_ID", 
	"EXP_UPD_VALIDATOR___MIN_DECLARED_VALUE1 as MIN_DECLARED_VALUE1", 
	"EXP_UPD_VALIDATOR___MAX_DECLARED_VALUE1 as MAX_DECLARED_VALUE1", 
	"EXP_UPD_VALIDATOR___SERVICE_LEVEL_INDICATOR as SERVICE_LEVEL_INDICATOR", 
	"EXP_UPD_VALIDATOR___DECLARED_VALUE_CURRENCY1 as DECLARED_VALUE_CURRENCY1", 
	"EXP_UPD_VALIDATOR___SHIP_VIA as SHIP_VIA", 
	"EXP_UPD_VALIDATOR___CUSTOM_SHIPVIA_ATTRIB as CUSTOM_SHIPVIA_ATTRIB", 
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)),lit(0)).when(col('o_UPDATE_VALIDATOR') ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_SHIP_VIA, type TARGET 
# COLUMN COUNT: 25


Shortcut_to_WM_SHIP_VIA = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(SHIP_VIA_ID AS INT) as WM_SHIP_VIA_ID",
	"CAST(SHIP_VIA AS STRING) as WM_SHIP_VIA",
	"CAST(DESCRIPTION AS STRING) as WM_SHIP_VIA_DESC",
	"CAST(TC_COMPANY_ID AS INT) as WM_TC_COMPANY_ID",
	"CAST(CARRIER_ID AS BIGINT) as WM_CARRIER_ID",
	"CAST(EXECUTION_LEVEL_ID AS INT) as WM_EXECUTION_LEVEL_ID",
	"CAST(MOT_ID AS BIGINT) as WM_MOT_ID",
	"CAST(IS_TRACKING_NBR_REQ AS TINYINT) as TRACKING_NBR_REQ_FLAG",
	"CAST(SERVICE_LEVEL_ID AS BIGINT) as WM_SERVICE_LEVEL_ID",
	"CAST(SERVICE_LEVEL_INDICATOR AS STRING) as WM_SERVICE_LEVEL_IND",
	"CAST(SERVICE_LEVEL_ICON AS STRING) as WM_SERVICE_LEVEL_ICON",
	"CAST(LABEL_TYPE AS STRING) as WM_LABEL_TYPE",
	"CAST(INS_COVER_TYPE_ID AS TINYINT) as WM_INS_COVER_TYPE_ID",
	"CAST(BILL_SHIP_VIA_ID AS INT) as WM_BILL_SHIP_VIA_ID",
	"CAST(CUSTOM_SHIPVIA_ATTRIB AS STRING) as WM_CUSTOM_SHIP_VIA_ATTR",
	"CAST(ACCESSORIAL_SEARCH_STRING1 AS STRING) as ACCESSORIAL_SEARCH_STRING",
	"CAST(MIN_DECLARED_VALUE1 AS DECIMAL(14,3)) as MIN_DECLARED_VALUE",
	"CAST(MAX_DECLARED_VALUE1 AS DECIMAL(14,3)) as MAX_DECLARED_VALUE",
	"CAST(DECLARED_VALUE_CURRENCY1 AS STRING) as DECLARED_VALUE_CURRENCY",
	"CAST(MARKED_FOR_DELETION1 AS TINYINT) as MARK_FOR_DELETION_FLAG",
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_SHIP_VIA_ID = target.WM_SHIP_VIA_ID"""
  # refined_perf_table = "WM_SHIP_VIA"
  executeMerge(Shortcut_to_WM_SHIP_VIA, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_SHIP_VIA", "WM_SHIP_VIA", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_SHIP_VIA", "WM_SHIP_VIA","Failed",str(e), f"{raw}.log_run_details", )
  raise e
