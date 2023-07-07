#Code converted on 2023-06-20 18:04:09
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

refined_perf_table = f"{refine}.WM_LPN_TYPE"
raw_perf_table = f"{raw}.WM_LPN_TYPE_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_TYPE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_LPN_TYPE = spark.sql(f"""SELECT
WM_LPN_TYPE.LOCATION_ID,
WM_LPN_TYPE.WM_LPN_TYPE,
WM_LPN_TYPE.WM_LPN_TYPE_DESC,
WM_LPN_TYPE.WM_PHYSICAL_ENTITY_CD,
WM_LPN_TYPE.LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_LPN_TYPE IN (SELECT LPN_TYPE FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_TYPE_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_LPN_TYPE_PRE = spark.sql(f"""SELECT
WM_LPN_TYPE_PRE.DC_NBR,
WM_LPN_TYPE_PRE.LPN_TYPE,
WM_LPN_TYPE_PRE.DESCRIPTION,
WM_LPN_TYPE_PRE.PHYSICAL_ENTITY_CODE,
WM_LPN_TYPE_PRE.LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LPN_TYPE_PRE_temp = SQ_Shortcut_to_WM_LPN_TYPE_PRE.toDF(*["SQ_Shortcut_to_WM_LPN_TYPE_PRE___" + col for col in SQ_Shortcut_to_WM_LPN_TYPE_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_LPN_TYPE_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_LPN_TYPE_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_LPN_TYPE_PRE___DC_NBR as int) as DC_NBR_EXP", \
	"SQ_Shortcut_to_WM_LPN_TYPE_PRE___LPN_TYPE as LPN_TYPE", \
	"SQ_Shortcut_to_WM_LPN_TYPE_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_LPN_TYPE_PRE___PHYSICAL_ENTITY_CODE as PHYSICAL_ENTITY_CODE", \
	"SQ_Shortcut_to_WM_LPN_TYPE_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_STE_PROFILE, type JOINER 
# COLUMN COUNT: 7

JNR_STE_PROFILE = EXPTRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXPTRANS.DC_NBR_EXP == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_LPN_TYPE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LPN_TYPE_temp = SQ_Shortcut_to_WM_LPN_TYPE.toDF(*["SQ_Shortcut_to_WM_LPN_TYPE___" + col for col in SQ_Shortcut_to_WM_LPN_TYPE.columns])
JNR_STE_PROFILE_temp = JNR_STE_PROFILE.toDF(*["JNR_STE_PROFILE___" + col for col in JNR_STE_PROFILE.columns])

JNR_WM_LPN_TYPE = SQ_Shortcut_to_WM_LPN_TYPE_temp.join(JNR_STE_PROFILE_temp,[SQ_Shortcut_to_WM_LPN_TYPE_temp.SQ_Shortcut_to_WM_LPN_TYPE___LOCATION_ID == JNR_STE_PROFILE_temp.JNR_STE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_LPN_TYPE_temp.SQ_Shortcut_to_WM_LPN_TYPE___WM_LPN_TYPE == JNR_STE_PROFILE_temp.JNR_STE_PROFILE___LPN_TYPE],'right_outer').selectExpr( \
	"JNR_STE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_STE_PROFILE___LPN_TYPE as LPN_TYPE", \
	"JNR_STE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_STE_PROFILE___PHYSICAL_ENTITY_CODE as PHYSICAL_ENTITY_CODE", \
	"JNR_STE_PROFILE___LOAD_TSTMP as LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_TYPE___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_LPN_TYPE___WM_LPN_TYPE as WM_LPN_TYPE", \
	"SQ_Shortcut_to_WM_LPN_TYPE___WM_LPN_TYPE_DESC as WM_LPN_TYPE_DESC", \
	"SQ_Shortcut_to_WM_LPN_TYPE___WM_PHYSICAL_ENTITY_CD as WM_PHYSICAL_ENTITY_CD", \
	"SQ_Shortcut_to_WM_LPN_TYPE___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_REC, type FILTER 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_LPN_TYPE_temp = JNR_WM_LPN_TYPE.toDF(*["JNR_WM_LPN_TYPE___" + col for col in JNR_WM_LPN_TYPE.columns])

FIL_UNCHANGED_REC = JNR_WM_LPN_TYPE_temp.selectExpr( \
	"JNR_WM_LPN_TYPE___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_LPN_TYPE___LPN_TYPE as LPN_TYPE", \
	"JNR_WM_LPN_TYPE___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_LPN_TYPE___PHYSICAL_ENTITY_CODE as PHYSICAL_ENTITY_CODE", \
	"JNR_WM_LPN_TYPE___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_LPN_TYPE___WM_LPN_TYPE as WM_LPN_TYPE", \
	"JNR_WM_LPN_TYPE___WM_LPN_TYPE_DESC as WM_LPN_TYPE_DESC", \
	"JNR_WM_LPN_TYPE___WM_PHYSICAL_ENTITY_CD as WM_PHYSICAL_ENTITY_CD", \
	"JNR_WM_LPN_TYPE___in_LOAD_TSTMP as in_LOAD_TSTMP")\
    .filter("WM_LPN_TYPE is Null OR (WM_LPN_TYPE is not Null AND\
     ( COALESCE( DESCRIPTION , '') != COALESCE( WM_LPN_TYPE_DESC, '') \
		OR COALESCE( PHYSICAL_ENTITY_CODE , '') != COALESCE( WM_PHYSICAL_ENTITY_CD, '')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_REC_temp = FIL_UNCHANGED_REC.toDF(*["FIL_UNCHANGED_REC___" + col for col in FIL_UNCHANGED_REC.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_REC_temp.selectExpr( \
	"FIL_UNCHANGED_REC___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_REC___LPN_TYPE as LPN_TYPE", \
	"FIL_UNCHANGED_REC___DESCRIPTION as DESCRIPTION", \
	"FIL_UNCHANGED_REC___PHYSICAL_ENTITY_CODE as PHYSICAL_ENTITY_CODE", \
	"FIL_UNCHANGED_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_REC___WM_LPN_TYPE as WM_LPN_TYPE", \
	"FIL_UNCHANGED_REC___WM_LPN_TYPE_DESC as WM_LPN_TYPE_DESC", \
	"FIL_UNCHANGED_REC___WM_PHYSICAL_ENTITY_CD as WM_PHYSICAL_ENTITY_CD", \
	"FIL_UNCHANGED_REC___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_REC___WM_LPN_TYPE IS NULL, 1, 2) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___LPN_TYPE as LPN_TYPE", \
	"EXP_UPD_VALIDATOR___DESCRIPTION as DESCRIPTION", \
	"EXP_UPD_VALIDATOR___PHYSICAL_ENTITY_CODE as PHYSICAL_ENTITY_CODE", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR ==(lit(1)) , lit(0)).when(EXP_UPD_VALIDATOR.o_UPD_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_LPN_TYPE1, type TARGET 
# COLUMN COUNT: 6

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_LPN_TYPE = target.WM_LPN_TYPE"""
#   refined_perf_table = "WM_LPN_TYPE"
  executeMerge(UPD_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_LPN_TYPE", "WM_LPN_TYPE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_LPN_TYPE", "WM_LPN_TYPE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	