#Code converted on 2023-06-22 11:02:26
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
raw_perf_table = f"{raw}.WM_LABOR_CRITERIA_PRE"
refined_perf_table = f"{refine}.WM_LABOR_CRITERIA"
site_profile_table = f"{legacy}.SITE_PROFILE"

Prev_Run_Dt=genPrevRunDt(refined_perf_table.split(".")[1], refine,raw)
Del_Logic= ' -- ' #args.Del_Logic

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE, type SOURCE 
# COLUMN COUNT: 16

SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE = spark.sql(f"""SELECT
DC_NBR,
CRIT_ID,
CRIT_CODE,
DESCRIPTION,
RULE_FILTER,
DATA_TYPE,
DATA_SIZE,
HIBERNATE_VERSION,
CREATED_SOURCE_TYPE,
CREATED_SOURCE,
CREATED_DTTM,
LAST_UPDATED_SOURCE_TYPE,
LAST_UPDATED_SOURCE,
LAST_UPDATED_DTTM,
COMPANY_ID,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LABOR_CRITERIA, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_WM_LABOR_CRITERIA = spark.sql(f"""SELECT
LOCATION_ID,
WM_CRIT_ID,
WM_CRIT_CD,
WM_CRIT_DESC,
RULE_FILTER_FLAG,
DATA_TYPE,
DATA_SIZE,
WM_COMPANY_ID,
WM_HIBERNATE_VERSION,
WM_CREATED_SOURCE_TYPE,
WM_CREATED_SOURCE,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_SOURCE_TYPE,
WM_LAST_UPDATED_SOURCE,
WM_LAST_UPDATED_TSTMP,
DELETE_FLAG,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE {Del_Logic} 1=0 and 
DELETE_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id()).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE_temp = SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE.toDF(*["SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___" + col for col in SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___DC_NBR as int) as DC_NBR_EXP", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___CRIT_ID as CRIT_ID", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___CRIT_CODE as CRIT_CODE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___RULE_FILTER as RULE_FILTER", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___DATA_TYPE as DATA_TYPE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___DATA_SIZE as DATA_SIZE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___CREATED_SOURCE as CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___COMPANY_ID as COMPANY_ID", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 17

JNR_SITE_PROFILE = EXPTRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXPTRANS.DC_NBR_EXP == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_LABOR_CRITERIA, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LABOR_CRITERIA_temp = SQ_Shortcut_to_WM_LABOR_CRITERIA.toDF(*["SQ_Shortcut_to_WM_LABOR_CRITERIA___" + col for col in SQ_Shortcut_to_WM_LABOR_CRITERIA.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_LABOR_CRITERIA = SQ_Shortcut_to_WM_LABOR_CRITERIA_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_LABOR_CRITERIA_temp.SQ_Shortcut_to_WM_LABOR_CRITERIA___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_LABOR_CRITERIA_temp.SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_CRIT_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___CRIT_ID],'fullouter').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___CRIT_ID as CRIT_ID", \
	"JNR_SITE_PROFILE___CRIT_CODE as CRIT_CODE", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___RULE_FILTER as RULE_FILTER", \
	"JNR_SITE_PROFILE___DATA_TYPE as DATA_TYPE", \
	"JNR_SITE_PROFILE___DATA_SIZE as DATA_SIZE", \
	"JNR_SITE_PROFILE___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___COMPANY_ID as COMPANY_ID", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_CRIT_ID as WM_CRIT_ID", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_CRIT_CD as WM_CRIT_CD", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_CRIT_DESC as WM_CRIT_DESC", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___RULE_FILTER_FLAG as RULE_FILTER_FLAG", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___DATA_TYPE as in_DATA_TYPE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___DATA_SIZE as in_DATA_SIZE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_COMPANY_ID as WM_COMPANY_ID", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_HIBERNATE_VERSION as WM_HIBERNATE_VERSION", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___DELETE_FLAG as DELETE_FLAG", \
	"SQ_Shortcut_to_WM_LABOR_CRITERIA___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_REC, type FILTER 
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_LABOR_CRITERIA_temp = JNR_WM_LABOR_CRITERIA.toDF(*["JNR_WM_LABOR_CRITERIA___" + col for col in JNR_WM_LABOR_CRITERIA.columns])

FIL_UNCHANGED_REC = JNR_WM_LABOR_CRITERIA_temp.selectExpr( \
	"JNR_WM_LABOR_CRITERIA___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_LABOR_CRITERIA___CRIT_ID as CRIT_ID", \
	"JNR_WM_LABOR_CRITERIA___CRIT_CODE as CRIT_CODE", \
	"JNR_WM_LABOR_CRITERIA___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_LABOR_CRITERIA___RULE_FILTER as RULE_FILTER", \
	"JNR_WM_LABOR_CRITERIA___DATA_TYPE as DATA_TYPE", \
	"JNR_WM_LABOR_CRITERIA___DATA_SIZE as DATA_SIZE", \
	"JNR_WM_LABOR_CRITERIA___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"JNR_WM_LABOR_CRITERIA___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_WM_LABOR_CRITERIA___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_WM_LABOR_CRITERIA___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_LABOR_CRITERIA___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_LABOR_CRITERIA___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_WM_LABOR_CRITERIA___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_LABOR_CRITERIA___COMPANY_ID as COMPANY_ID", \
	"JNR_WM_LABOR_CRITERIA___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_LABOR_CRITERIA___WM_CRIT_ID as WM_CRIT_ID", \
	"JNR_WM_LABOR_CRITERIA___WM_CRIT_CD as WM_CRIT_CD", \
	"JNR_WM_LABOR_CRITERIA___WM_CRIT_DESC as WM_CRIT_DESC", \
	"JNR_WM_LABOR_CRITERIA___RULE_FILTER_FLAG as RULE_FILTER_FLAG", \
	"JNR_WM_LABOR_CRITERIA___in_DATA_TYPE as in_DATA_TYPE", \
	"JNR_WM_LABOR_CRITERIA___in_DATA_SIZE as in_DATA_SIZE", \
	"JNR_WM_LABOR_CRITERIA___WM_COMPANY_ID as WM_COMPANY_ID", \
	"JNR_WM_LABOR_CRITERIA___WM_HIBERNATE_VERSION as WM_HIBERNATE_VERSION", \
	"JNR_WM_LABOR_CRITERIA___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"JNR_WM_LABOR_CRITERIA___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"JNR_WM_LABOR_CRITERIA___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"JNR_WM_LABOR_CRITERIA___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_LABOR_CRITERIA___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"JNR_WM_LABOR_CRITERIA___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_LABOR_CRITERIA___DELETE_FLAG as DELETE_FLAG", \
	"JNR_WM_LABOR_CRITERIA___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("WM_CRIT_ID is Null OR CRIT_ID is Null OR ( WM_CRIT_ID is not Null AND \
     ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATED_TSTMP, date'1900-01-01') \
    OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 34

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_REC_temp = FIL_UNCHANGED_REC.toDF(*["FIL_UNCHANGED_REC___" + col for col in FIL_UNCHANGED_REC.columns]) \
.withColumn("FIL_UNCHANGED_REC___v_CREATED_DTTM", expr("""IF(FIL_UNCHANGED_REC___CREATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_REC___CREATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_LAST_UPDATED_DTTM", expr("""IF(FIL_UNCHANGED_REC___LAST_UPDATED_DTTM IS NULL, date'1900-01-01', FIL_UNCHANGED_REC___LAST_UPDATED_DTTM)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_WM_CREATED_TSTMP", expr("""IF(FIL_UNCHANGED_REC___WM_CREATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_REC___WM_CREATED_TSTMP)""")) \
	.withColumn("FIL_UNCHANGED_REC___v_WM_LAST_UPDATED_TSTMP", expr("""IF(FIL_UNCHANGED_REC___WM_LAST_UPDATED_TSTMP IS NULL, date'1900-01-01', FIL_UNCHANGED_REC___WM_LAST_UPDATED_TSTMP)"""))

EXP_UPD_VALIDATOR = FIL_UNCHANGED_REC_temp.selectExpr( \
	"FIL_UNCHANGED_REC___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_REC___CRIT_ID as CRIT_ID", \
	"FIL_UNCHANGED_REC___CRIT_CODE as CRIT_CODE", \
	"FIL_UNCHANGED_REC___DESCRIPTION as DESCRIPTION", \
    "CASE WHEN TRIM(UPPER(FIL_UNCHANGED_REC___RULE_FILTER)) IN ('Y', '1') THEN '1' ELSE '0' END as o_RULE_FILTER", \
	"FIL_UNCHANGED_REC___DATA_TYPE as DATA_TYPE", \
	"FIL_UNCHANGED_REC___DATA_SIZE as DATA_SIZE", \
	"FIL_UNCHANGED_REC___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"FIL_UNCHANGED_REC___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_REC___CREATED_SOURCE as CREATED_SOURCE", \
	"FIL_UNCHANGED_REC___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_REC___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_REC___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_REC___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_REC___COMPANY_ID as COMPANY_ID", \
	"FIL_UNCHANGED_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_REC___WM_CRIT_ID as WM_CRIT_ID", \
	"FIL_UNCHANGED_REC___WM_CRIT_CD as WM_CRIT_CD", \
	"FIL_UNCHANGED_REC___WM_CRIT_DESC as WM_CRIT_DESC", \
	"FIL_UNCHANGED_REC___RULE_FILTER_FLAG as RULE_FILTER_FLAG", \
	"FIL_UNCHANGED_REC___in_DATA_TYPE as in_DATA_TYPE", \
	"FIL_UNCHANGED_REC___in_DATA_SIZE as in_DATA_SIZE", \
	"FIL_UNCHANGED_REC___WM_COMPANY_ID as WM_COMPANY_ID", \
	"FIL_UNCHANGED_REC___WM_HIBERNATE_VERSION as WM_HIBERNATE_VERSION", \
	"FIL_UNCHANGED_REC___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_REC___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"FIL_UNCHANGED_REC___WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_REC___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_REC___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_REC___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_REC___CRIT_ID IS NULL AND FIL_UNCHANGED_REC___WM_CRIT_ID IS NOT NULL, 1, 0) as DELETE_FLAG", \
	f"IF(FIL_UNCHANGED_REC___CRIT_ID IS NOT NULL AND FIL_UNCHANGED_REC___WM_CRIT_ID IS NULL, 'INSERT', IF(FIL_UNCHANGED_REC___CRIT_ID IS NULL AND FIL_UNCHANGED_REC___WM_CRIT_ID IS NOT NULL AND ( FIL_UNCHANGED_REC___v_WM_CREATED_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) OR FIL_UNCHANGED_REC___v_WM_LAST_UPDATED_TSTMP >= DATE_ADD('{Prev_Run_Dt}',-14) ), 'DELETE', IF(FIL_UNCHANGED_REC___CRIT_ID IS NOT NULL AND FIL_UNCHANGED_REC___WM_CRIT_ID IS NOT NULL AND ( FIL_UNCHANGED_REC___v_WM_CREATED_TSTMP <> FIL_UNCHANGED_REC___v_CREATED_DTTM OR FIL_UNCHANGED_REC___v_WM_LAST_UPDATED_TSTMP <> FIL_UNCHANGED_REC___v_LAST_UPDATED_DTTM ), 'UPDATE', NULL))) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node RTR_DELETE, type ROUTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 36


# Creating output dataframe for RTR_DELETE, output group DELETE
RTR_DELETE_DELETE = EXP_UPD_VALIDATOR.selectExpr( \
	"LOCATION_ID as LOCATION_ID", \
	"CRIT_ID as CRIT_ID", \
	"CRIT_CODE as CRIT_CODE", \
	"DESCRIPTION as DESCRIPTION", \
	"o_RULE_FILTER as RULE_FILTER", \
	"DATA_TYPE as DATA_TYPE", \
	"DATA_SIZE as DATA_SIZE", \
	"HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"CREATED_SOURCE as CREATED_SOURCE", \
	"CREATED_DTTM as CREATED_DTTM", \
	"LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"COMPANY_ID as COMPANY_ID", \
	"in_LOCATION_ID as in_LOCATION_ID", \
	"WM_CRIT_ID as WM_CRIT_ID", \
	"WM_CRIT_CD as WM_CRIT_CD", \
	"WM_CRIT_DESC as WM_CRIT_DESC", \
	"RULE_FILTER_FLAG as RULE_FILTER_FLAG", \
	"in_DATA_TYPE as in_DATA_TYPE", \
	"in_DATA_SIZE as in_DATA_SIZE", \
	"WM_COMPANY_ID as WM_COMPANY_ID", \
	"WM_HIBERNATE_VERSION as WM_HIBERNATE_VERSION", \
	"WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"UPDATE_TSTMP as UPDATE_TSTMP", \
	"LOAD_TSTMP as LOAD_TSTMP", \
	"DELETE_FLAG as DELETE_FLAG", \
	"o_UPD_VALIDATOR as o_UPD_VALIDATOR" , \
      "sys_row_id as sys_row_id") \
	.withColumn('in_DELETE_FLAG', lit(None)) \
	.withColumn('in_LOAD_TSTMP', lit(None)) \
	.select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID3'), \
	col('CRIT_ID').alias('CRIT_ID3'), \
	col('CRIT_CODE').alias('CRIT_CODE3'), \
	col('DESCRIPTION').alias('DESCRIPTION3'), \
	col('RULE_FILTER').alias('RULE_FILTER3'), \
	col('DATA_TYPE').alias('DATA_TYPE3'), \
	col('DATA_SIZE').alias('DATA_SIZE3'), \
	col('HIBERNATE_VERSION').alias('HIBERNATE_VERSION3'), \
	col('CREATED_SOURCE_TYPE').alias('CREATED_SOURCE_TYPE3'), \
	col('CREATED_SOURCE').alias('CREATED_SOURCE3'), \
	col('CREATED_DTTM').alias('CREATED_DTTM3'), \
	col('LAST_UPDATED_SOURCE_TYPE').alias('LAST_UPDATED_SOURCE_TYPE3'), \
	col('LAST_UPDATED_SOURCE').alias('LAST_UPDATED_SOURCE3'), \
	col('LAST_UPDATED_DTTM').alias('LAST_UPDATED_DTTM3'), \
	col('COMPANY_ID').alias('COMPANY_ID3'), \
	col('in_LOCATION_ID').alias('in_LOCATION_ID3'), \
	col('WM_CRIT_ID').alias('WM_CRIT_ID3'), \
	col('WM_CRIT_CD').alias('WM_CRIT_CD3'), \
	col('WM_CRIT_DESC').alias('WM_CRIT_DESC3'), \
	col('RULE_FILTER_FLAG').alias('RULE_FILTER_FLAG3'), \
	col('in_DATA_TYPE').alias('in_DATA_TYPE3'), \
	col('in_DATA_SIZE').alias('in_DATA_SIZE3'), \
	col('WM_COMPANY_ID').alias('WM_COMPANY_ID3'), \
	col('WM_HIBERNATE_VERSION').alias('WM_HIBERNATE_VERSION3'), \
	col('WM_CREATED_SOURCE_TYPE').alias('WM_CREATED_SOURCE_TYPE3'), \
	col('WM_CREATED_SOURCE').alias('WM_CREATED_SOURCE3'), \
	col('WM_CREATED_TSTMP').alias('WM_CREATED_TSTMP3'), \
	col('WM_LAST_UPDATED_SOURCE_TYPE').alias('WM_LAST_UPDATED_SOURCE_TYPE3'), \
	col('WM_LAST_UPDATED_SOURCE').alias('WM_LAST_UPDATED_SOURCE3'), \
	col('WM_LAST_UPDATED_TSTMP').alias('WM_LAST_UPDATED_TSTMP3'), \
	col('in_DELETE_FLAG').alias('in_DELETE_FLAG3'), \
	col('in_LOAD_TSTMP').alias('in_LOAD_TSTMP3'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP3'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP3'), \
	col('DELETE_FLAG').alias('DELETE_FLAG3'), \
	col('o_UPD_VALIDATOR').alias('o_UPD_VALIDATOR3')).filter("o_UPD_VALIDATOR = 'DELETE'")

# Creating output dataframe for RTR_DELETE, output group INSERT_UPDATE
RTR_DELETE_INSERT_UPDATE = EXP_UPD_VALIDATOR.selectExpr( \
	"LOCATION_ID as LOCATION_ID", \
	"CRIT_ID as CRIT_ID", \
	"CRIT_CODE as CRIT_CODE", \
	"DESCRIPTION as DESCRIPTION", \
	"o_RULE_FILTER as RULE_FILTER", \
	"DATA_TYPE as DATA_TYPE", \
	"DATA_SIZE as DATA_SIZE", \
	"HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"CREATED_SOURCE as CREATED_SOURCE", \
	"CREATED_DTTM as CREATED_DTTM", \
	"LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"COMPANY_ID as COMPANY_ID", \
	"in_LOCATION_ID as in_LOCATION_ID", \
	"WM_CRIT_ID as WM_CRIT_ID", \
	"WM_CRIT_CD as WM_CRIT_CD", \
	"WM_CRIT_DESC as WM_CRIT_DESC", \
	"RULE_FILTER_FLAG as RULE_FILTER_FLAG", \
	"in_DATA_TYPE as in_DATA_TYPE", \
	"in_DATA_SIZE as in_DATA_SIZE", \
	"WM_COMPANY_ID as WM_COMPANY_ID", \
	"WM_HIBERNATE_VERSION as WM_HIBERNATE_VERSION", \
	"WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"WM_CREATED_TSTMP as WM_CREATED_TSTMP", \
	"WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", \
	"UPDATE_TSTMP as UPDATE_TSTMP", \
	"LOAD_TSTMP as LOAD_TSTMP", \
	"DELETE_FLAG as DELETE_FLAG", \
	"o_UPD_VALIDATOR as o_UPD_VALIDATOR",\
      "sys_row_id as sys_row_id") \
	.withColumn('in_DELETE_FLAG', lit(None)) \
	.withColumn('in_LOAD_TSTMP', lit(None)) \
	.select(col('sys_row_id'), \
	col('LOCATION_ID').alias('LOCATION_ID1'), \
	col('CRIT_ID').alias('CRIT_ID1'), \
	col('CRIT_CODE').alias('CRIT_CODE1'), \
	col('DESCRIPTION').alias('DESCRIPTION1'), \
	col('RULE_FILTER').alias('RULE_FILTER1'), \
	col('DATA_TYPE').alias('DATA_TYPE1'), \
	col('DATA_SIZE').alias('DATA_SIZE1'), \
	col('HIBERNATE_VERSION').alias('HIBERNATE_VERSION1'), \
	col('CREATED_SOURCE_TYPE').alias('CREATED_SOURCE_TYPE1'), \
	col('CREATED_SOURCE').alias('CREATED_SOURCE1'), \
	col('CREATED_DTTM').alias('CREATED_DTTM1'), \
	col('LAST_UPDATED_SOURCE_TYPE').alias('LAST_UPDATED_SOURCE_TYPE1'), \
	col('LAST_UPDATED_SOURCE').alias('LAST_UPDATED_SOURCE1'), \
	col('LAST_UPDATED_DTTM').alias('LAST_UPDATED_DTTM1'), \
	col('COMPANY_ID').alias('COMPANY_ID1'), \
	col('in_LOCATION_ID').alias('in_LOCATION_ID1'), \
	col('WM_CRIT_ID').alias('WM_CRIT_ID1'), \
	col('WM_CRIT_CD').alias('WM_CRIT_CD1'), \
	col('WM_CRIT_DESC').alias('WM_CRIT_DESC1'), \
	col('RULE_FILTER_FLAG').alias('RULE_FILTER_FLAG1'), \
	col('in_DATA_TYPE').alias('in_DATA_TYPE1'), \
	col('in_DATA_SIZE').alias('in_DATA_SIZE1'), \
	col('WM_COMPANY_ID').alias('WM_COMPANY_ID1'), \
	col('WM_HIBERNATE_VERSION').alias('WM_HIBERNATE_VERSION1'), \
	col('WM_CREATED_SOURCE_TYPE').alias('WM_CREATED_SOURCE_TYPE1'), \
	col('WM_CREATED_SOURCE').alias('WM_CREATED_SOURCE1'), \
	col('WM_CREATED_TSTMP').alias('WM_CREATED_TSTMP1'), \
	col('WM_LAST_UPDATED_SOURCE_TYPE').alias('WM_LAST_UPDATED_SOURCE_TYPE1'), \
	col('WM_LAST_UPDATED_SOURCE').alias('WM_LAST_UPDATED_SOURCE1'), \
	col('WM_LAST_UPDATED_TSTMP').alias('WM_LAST_UPDATED_TSTMP1'), \
	col('in_DELETE_FLAG').alias('in_DELETE_FLAG1'), \
	col('in_LOAD_TSTMP').alias('in_LOAD_TSTMP1'), \
	col('UPDATE_TSTMP').alias('UPDATE_TSTMP1'), \
	col('LOAD_TSTMP').alias('LOAD_TSTMP1'), \
	col('DELETE_FLAG').alias('DELETE_FLAG1'), \
	col('o_UPD_VALIDATOR').alias('o_UPD_VALIDATOR1')).filter("o_UPD_VALIDATOR = 'INSERT' OR o_UPD_VALIDATOR = 'UPDATE'")


# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
RTR_DELETE_INSERT_UPDATE_temp = RTR_DELETE_INSERT_UPDATE.toDF(*["RTR_DELETE_INSERT_UPDATE___" + col for col in RTR_DELETE_INSERT_UPDATE.columns])

UPD_INS_UPD = RTR_DELETE_INSERT_UPDATE_temp.selectExpr( \
	"RTR_DELETE_INSERT_UPDATE___LOCATION_ID1 as LOCATION_ID1", \
	"RTR_DELETE_INSERT_UPDATE___CRIT_ID1 as CRIT_ID1", \
	"RTR_DELETE_INSERT_UPDATE___CRIT_CODE1 as CRIT_CODE1", \
	"RTR_DELETE_INSERT_UPDATE___DESCRIPTION1 as DESCRIPTION1", \
	"RTR_DELETE_INSERT_UPDATE___RULE_FILTER1 as RULE_FILTER1", \
	"RTR_DELETE_INSERT_UPDATE___DATA_TYPE1 as DATA_TYPE1", \
	"RTR_DELETE_INSERT_UPDATE___DATA_SIZE1 as DATA_SIZE1", \
	"RTR_DELETE_INSERT_UPDATE___COMPANY_ID1 as COMPANY_ID1", \
	"RTR_DELETE_INSERT_UPDATE___HIBERNATE_VERSION1 as HIBERNATE_VERSION1", \
	"RTR_DELETE_INSERT_UPDATE___CREATED_SOURCE_TYPE1 as CREATED_SOURCE_TYPE1", \
	"RTR_DELETE_INSERT_UPDATE___CREATED_SOURCE1 as CREATED_SOURCE1", \
	"RTR_DELETE_INSERT_UPDATE___CREATED_DTTM1 as CREATED_DTTM1", \
	"RTR_DELETE_INSERT_UPDATE___LAST_UPDATED_SOURCE_TYPE1 as LAST_UPDATED_SOURCE_TYPE1", \
	"RTR_DELETE_INSERT_UPDATE___LAST_UPDATED_SOURCE1 as LAST_UPDATED_SOURCE1", \
	"RTR_DELETE_INSERT_UPDATE___LAST_UPDATED_DTTM1 as LAST_UPDATED_DTTM1", \
	"RTR_DELETE_INSERT_UPDATE___DELETE_FLAG1 as DELETE_FLAG1", \
	"RTR_DELETE_INSERT_UPDATE___UPDATE_TSTMP1 as UPDATE_TSTMP1", \
	"RTR_DELETE_INSERT_UPDATE___LOAD_TSTMP1 as LOAD_TSTMP1", \
	"RTR_DELETE_INSERT_UPDATE___o_UPD_VALIDATOR1 as o_UPD_VALIDATOR1") \
	.withColumn('pyspark_data_action', when(col('o_UPD_VALIDATOR1') ==(lit('INSERT')), lit(0)).when(col('o_UPD_VALIDATOR1') ==(lit('UPDATE')), lit(1)))

# COMMAND ----------
# Processing node UPD_DELETE, type UPDATE_STRATEGY 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
RTR_DELETE_DELETE_temp = RTR_DELETE_DELETE.toDF(*["RTR_DELETE_DELETE___" + col for col in RTR_DELETE_DELETE.columns])

UPD_DELETE = RTR_DELETE_DELETE_temp.selectExpr( \
	"RTR_DELETE_DELETE___in_LOCATION_ID3 as in_LOCATION_ID3", \
	"RTR_DELETE_DELETE___WM_CRIT_ID3 as WM_CRIT_ID3", \
	"RTR_DELETE_DELETE___WM_CRIT_CD3 as WM_CRIT_CD3", \
	"RTR_DELETE_DELETE___WM_CRIT_DESC3 as WM_CRIT_DESC3", \
	"RTR_DELETE_DELETE___RULE_FILTER_FLAG3 as RULE_FILTER_FLAG3", \
	"RTR_DELETE_DELETE___in_DATA_TYPE3 as in_DATA_TYPE3", \
	"RTR_DELETE_DELETE___in_DATA_SIZE3 as in_DATA_SIZE3", \
	"RTR_DELETE_DELETE___WM_COMPANY_ID3 as WM_COMPANY_ID3", \
	"RTR_DELETE_DELETE___WM_HIBERNATE_VERSION3 as WM_HIBERNATE_VERSION3", \
	"RTR_DELETE_DELETE___WM_CREATED_SOURCE_TYPE3 as WM_CREATED_SOURCE_TYPE3", \
	"RTR_DELETE_DELETE___WM_CREATED_SOURCE3 as WM_CREATED_SOURCE3", \
	"RTR_DELETE_DELETE___WM_CREATED_TSTMP3 as WM_CREATED_TSTMP3", \
	"RTR_DELETE_DELETE___WM_LAST_UPDATED_SOURCE_TYPE3 as WM_LAST_UPDATED_SOURCE_TYPE3", \
	"RTR_DELETE_DELETE___WM_LAST_UPDATED_SOURCE3 as WM_LAST_UPDATED_SOURCE3", \
	"RTR_DELETE_DELETE___WM_LAST_UPDATED_TSTMP3 as WM_LAST_UPDATED_TSTMP3", \
	"RTR_DELETE_DELETE___DELETE_FLAG3 as DELETE_FLAG3", \
	"RTR_DELETE_DELETE___UPDATE_TSTMP3 as UPDATE_TSTMP3", \
	"RTR_DELETE_DELETE___LOAD_TSTMP3 as LOAD_TSTMP3") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_WM_LABOR_CRITERIA2, type TARGET 
# COLUMN COUNT: 18


# Shortcut_to_WM_LABOR_CRITERIA2 = UPD_DELETE.selectExpr( \
# 	"CAST(in_LOCATION_ID3 AS BIGINT) as LOCATION_ID", \
# 	"CAST(WM_CRIT_ID3 AS BIGINT) as WM_CRIT_ID", \
# 	"CAST(lit(None) AS STRING) as WM_CRIT_CD", \
# 	"CAST(lit(None) AS STRING) as WM_CRIT_DESC", \
# 	"CAST(lit(None) AS BIGINT) as RULE_FILTER_FLAG", \
# 	"CAST(lit(None) AS STRING) as DATA_TYPE", \
# 	"CAST(lit(None) AS BIGINT) as DATA_SIZE", \
# 	"CAST(lit(None) AS BIGINT) as WM_COMPANY_ID", \
# 	"CAST(lit(None) AS BIGINT) as WM_HIBERNATE_VERSION", \
# 	"CAST(lit(None) AS BIGINT) as WM_CREATED_SOURCE_TYPE", \
# 	"CAST(lit(None) AS STRING) as WM_CREATED_SOURCE", \
# 	"CAST(lit(None) AS TIMESTAMP) as WM_CREATED_TSTMP", \
# 	"CAST(lit(None) AS BIGINT) as WM_LAST_UPDATED_SOURCE_TYPE", \
# 	"CAST(lit(None) AS STRING) as WM_LAST_UPDATED_SOURCE", \
# 	"CAST(lit(None) AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", \
# 	"CAST(DELETE_FLAG3 AS BIGINT) as DELETE_FLAG", \
# 	"CAST(UPDATE_TSTMP3 AS TIMESTAMP) as UPDATE_TSTMP", \
# 	"CAST(LOAD_TSTMP3 AS TIMESTAMP) as LOAD_TSTMP" \
# )
# Shortcut_to_WM_LABOR_CRITERIA2.write.saveAsTable(f'{raw}.WM_LABOR_CRITERIA')

# COMMAND ----------
# Processing node Shortcut_to_WM_LABOR_CRITERIA1, type TARGET 
# COLUMN COUNT: 18

Shortcut_to_WM_LABOR_CRITERIA1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(CRIT_ID1 AS INT) as WM_CRIT_ID",
	"CAST(CRIT_CODE1 AS STRING) as WM_CRIT_CD",
	"CAST(DESCRIPTION1 AS STRING) as WM_CRIT_DESC",
	"CAST(RULE_FILTER1 AS TINYINT) as RULE_FILTER_FLAG",
	"CAST(DATA_TYPE1 AS STRING) as DATA_TYPE",
	"CAST(DATA_SIZE1 AS INT) as DATA_SIZE",
	"CAST(COMPANY_ID1 AS INT) as WM_COMPANY_ID",
	"CAST(HIBERNATE_VERSION1 AS BIGINT) as WM_HIBERNATE_VERSION",
	"CAST(CREATED_SOURCE_TYPE1 AS TINYINT) as WM_CREATED_SOURCE_TYPE",
	"CAST(CREATED_SOURCE1 AS STRING) as WM_CREATED_SOURCE",
	"CAST(CREATED_DTTM1 AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_SOURCE_TYPE1 AS TINYINT) as WM_LAST_UPDATED_SOURCE_TYPE",
	"CAST(LAST_UPDATED_SOURCE1 AS STRING) as WM_LAST_UPDATED_SOURCE",
	"CAST(LAST_UPDATED_DTTM1 AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(DELETE_FLAG1 AS TINYINT) as DELETE_FLAG",
	"CAST(UPDATE_TSTMP1 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP1 AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_CRIT_ID = target.WM_CRIT_ID"""
#   refined_perf_table = "WM_LABOR_CRITERIA"
  executeMerge(Shortcut_to_WM_LABOR_CRITERIA1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_LABOR_CRITERIA", "WM_LABOR_CRITERIA", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_LABOR_CRITERIA", "WM_LABOR_CRITERIA","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	