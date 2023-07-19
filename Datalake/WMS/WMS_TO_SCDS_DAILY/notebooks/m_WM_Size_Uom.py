#Code converted on 2023-06-22 21:00:01
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
refined_perf_table = f"{refine}.WM_SIZE_UOM"
raw_perf_table = f"{raw}.WM_SIZE_UOM_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SIZE_UOM_PRE, type SOURCE 
# COLUMN COUNT: 26

SQ_Shortcut_to_WM_SIZE_UOM_PRE = spark.sql(f"""SELECT
DC_NBR,
SIZE_UOM_ID,
TC_COMPANY_ID,
SIZE_UOM,
DESCRIPTION,
CONSOLIDATION_CODE,
DO_NOT_INHERIT_TO_ORDER,
SIZE_MAPPING,
STANDARD_UOM,
STANDARD_UNITS,
SPLITTER_CONS_CODE,
APPLY_TO_VENDOR,
MARK_FOR_DELETION,
DISCRETE,
ADJUSTMENT,
ADJUSTMENT_SIZE_UOM_ID,
HIBERNATE_VERSION,
AUDIT_CREATED_SOURCE,
AUDIT_CREATED_SOURCE_TYPE,
AUDIT_CREATED_DTTM,
AUDIT_LAST_UPDATED_SOURCE,
AUDIT_LAST_UPDATED_SOURCE_TYPE,
AUDIT_LAST_UPDATED_DTTM,
CREATED_DTTM,
LAST_UPDATED_DTTM,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_SIZE_UOM, type SOURCE 
# COLUMN COUNT: 26

SQ_Shortcut_to_WM_SIZE_UOM = spark.sql(f"""SELECT
LOCATION_ID,
WM_SIZE_UOM_ID,
WM_TC_COMPANY_ID,
WM_SIZE_UOM,
WM_SIZE_UOM_DESC,
WM_SIZE_MAPPING,
WM_STANDARD_UOM_ID,
WM_STANDARD_UNITS,
WM_CONSOLIDATION_CD,
WM_SPLITTER_CONS_CD,
DO_NOT_INHERIT_TO_ORDER_FLAG,
APPLY_TO_VENDOR_FLAG,
DISCRETE_FLAG,
WM_ADJUSTMENT_SIZE,
WM_ADJUSTMENT_SIZE_UOM_ID,
WM_AUDIT_CREATED_SOURCE_TYPE,
WM_AUDIT_CREATED_SOURCE,
WM_AUDIT_CREATED_TSTMP,
WM_AUDIT_LAST_UPDATED_SOURCE_TYPE,
WM_AUDIT_LAST_UPDATED_SOURCE,
WM_AUDIT_LAST_UPDATED_TSTMP,
WM_HIBERNATE_VERSION,
MARK_FOR_DELETION_FLAG,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_SIZE_UOM_ID IN (SELECT SIZE_UOM_ID FROM {raw_perf_table} )""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 26

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_SIZE_UOM_PRE_temp = SQ_Shortcut_to_WM_SIZE_UOM_PRE.toDF(*["SQ_Shortcut_to_WM_SIZE_UOM_PRE___" + col for col in SQ_Shortcut_to_WM_SIZE_UOM_PRE.columns])

EXPTRANS = SQ_Shortcut_to_WM_SIZE_UOM_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_SIZE_UOM_PRE___DC_NBR as int) as DC_NBR_EXP", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___SIZE_UOM_ID as SIZE_UOM_ID", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___TC_COMPANY_ID as TC_COMPANY_ID", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___SIZE_UOM as SIZE_UOM", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___DESCRIPTION as DESCRIPTION", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___CONSOLIDATION_CODE as CONSOLIDATION_CODE", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___DO_NOT_INHERIT_TO_ORDER as DO_NOT_INHERIT_TO_ORDER", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___SIZE_MAPPING as SIZE_MAPPING", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___STANDARD_UOM as STANDARD_UOM", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___STANDARD_UNITS as STANDARD_UNITS", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___SPLITTER_CONS_CODE as SPLITTER_CONS_CODE", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___APPLY_TO_VENDOR as APPLY_TO_VENDOR", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___DISCRETE as DISCRETE", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___ADJUSTMENT as ADJUSTMENT", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___ADJUSTMENT_SIZE_UOM_ID as ADJUSTMENT_SIZE_UOM_ID", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___HIBERNATE_VERSION as HIBERNATE_VERSION", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_SIZE_UOM_PRE___LOAD_TSTMP as LOAD_TSTMP" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 28

JNR_SITE_PROFILE = EXPTRANS.join(SQ_Shortcut_to_SITE_PROFILE,[EXPTRANS.DC_NBR_EXP == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_SIZE_UOM, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 51

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_SIZE_UOM_temp = SQ_Shortcut_to_WM_SIZE_UOM.toDF(*["SQ_Shortcut_to_WM_SIZE_UOM___" + col for col in SQ_Shortcut_to_WM_SIZE_UOM.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_SIZE_UOM = SQ_Shortcut_to_WM_SIZE_UOM_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_SIZE_UOM_temp.SQ_Shortcut_to_WM_SIZE_UOM___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_SIZE_UOM_temp.SQ_Shortcut_to_WM_SIZE_UOM___WM_SIZE_UOM_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___SIZE_UOM_ID],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___SIZE_UOM_ID as SIZE_UOM_ID", 
	"JNR_SITE_PROFILE___TC_COMPANY_ID as TC_COMPANY_ID", 
	"JNR_SITE_PROFILE___SIZE_UOM as SIZE_UOM", 
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", 
	"JNR_SITE_PROFILE___CONSOLIDATION_CODE as CONSOLIDATION_CODE", 
	"JNR_SITE_PROFILE___DO_NOT_INHERIT_TO_ORDER as DO_NOT_INHERIT_TO_ORDER", 
	"JNR_SITE_PROFILE___SIZE_MAPPING as SIZE_MAPPING", 
	"JNR_SITE_PROFILE___STANDARD_UOM as STANDARD_UOM", 
	"JNR_SITE_PROFILE___STANDARD_UNITS as STANDARD_UNITS", 
	"JNR_SITE_PROFILE___SPLITTER_CONS_CODE as SPLITTER_CONS_CODE", 
	"JNR_SITE_PROFILE___APPLY_TO_VENDOR as APPLY_TO_VENDOR", 
	"JNR_SITE_PROFILE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"JNR_SITE_PROFILE___DISCRETE as DISCRETE", 
	"JNR_SITE_PROFILE___ADJUSTMENT as ADJUSTMENT", 
	"JNR_SITE_PROFILE___ADJUSTMENT_SIZE_UOM_ID as ADJUSTMENT_SIZE_UOM_ID", 
	"JNR_SITE_PROFILE___HIBERNATE_VERSION as HIBERNATE_VERSION", 
	"JNR_SITE_PROFILE___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", 
	"JNR_SITE_PROFILE___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", 
	"JNR_SITE_PROFILE___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", 
	"JNR_SITE_PROFILE___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_SIZE_UOM___LOCATION_ID as in_LOCATION_ID", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_SIZE_UOM_ID as WM_SIZE_UOM_ID", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_TC_COMPANY_ID as WM_TC_COMPANY_ID", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_SIZE_UOM as WM_SIZE_UOM", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_SIZE_UOM_DESC as WM_SIZE_UOM_DESC", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_SIZE_MAPPING as WM_SIZE_MAPPING", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_STANDARD_UOM_ID as WM_STANDARD_UOM_ID", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_STANDARD_UNITS as WM_STANDARD_UNITS", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_CONSOLIDATION_CD as WM_CONSOLIDATION_CD", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_SPLITTER_CONS_CD as WM_SPLITTER_CONS_CD", 
	"SQ_Shortcut_to_WM_SIZE_UOM___DO_NOT_INHERIT_TO_ORDER_FLAG as DO_NOT_INHERIT_TO_ORDER_FLAG", 
	"SQ_Shortcut_to_WM_SIZE_UOM___APPLY_TO_VENDOR_FLAG as APPLY_TO_VENDOR_FLAG", 
	"SQ_Shortcut_to_WM_SIZE_UOM___DISCRETE_FLAG as DISCRETE_FLAG", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_ADJUSTMENT_SIZE as WM_ADJUSTMENT_SIZE", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_ADJUSTMENT_SIZE_UOM_ID as WM_ADJUSTMENT_SIZE_UOM_ID", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_AUDIT_CREATED_SOURCE_TYPE as WM_AUDIT_CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_AUDIT_CREATED_SOURCE as WM_AUDIT_CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_AUDIT_CREATED_TSTMP as WM_AUDIT_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_AUDIT_LAST_UPDATED_SOURCE_TYPE as WM_AUDIT_LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_AUDIT_LAST_UPDATED_SOURCE as WM_AUDIT_LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_AUDIT_LAST_UPDATED_TSTMP as WM_AUDIT_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_HIBERNATE_VERSION as WM_HIBERNATE_VERSION", 
	"SQ_Shortcut_to_WM_SIZE_UOM___MARK_FOR_DELETION_FLAG as MARK_FOR_DELETION_FLAG", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_CREATED_TSTMP as WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_SIZE_UOM___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_SIZE_UOM___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_REC, type FILTER 
# COLUMN COUNT: 51

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_SIZE_UOM_temp = JNR_WM_SIZE_UOM.toDF(*["JNR_WM_SIZE_UOM___" + col for col in JNR_WM_SIZE_UOM.columns])

FIL_UNCHANGED_REC = JNR_WM_SIZE_UOM_temp.selectExpr( 
	"JNR_WM_SIZE_UOM___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_SIZE_UOM___SIZE_UOM_ID as SIZE_UOM_ID", 
	"JNR_WM_SIZE_UOM___TC_COMPANY_ID as TC_COMPANY_ID", 
	"JNR_WM_SIZE_UOM___SIZE_UOM as SIZE_UOM", 
	"JNR_WM_SIZE_UOM___DESCRIPTION as DESCRIPTION", 
	"JNR_WM_SIZE_UOM___CONSOLIDATION_CODE as CONSOLIDATION_CODE", 
	"JNR_WM_SIZE_UOM___DO_NOT_INHERIT_TO_ORDER as DO_NOT_INHERIT_TO_ORDER", 
	"JNR_WM_SIZE_UOM___SIZE_MAPPING as SIZE_MAPPING", 
	"JNR_WM_SIZE_UOM___STANDARD_UOM as STANDARD_UOM", 
	"JNR_WM_SIZE_UOM___STANDARD_UNITS as STANDARD_UNITS", 
	"JNR_WM_SIZE_UOM___SPLITTER_CONS_CODE as SPLITTER_CONS_CODE", 
	"JNR_WM_SIZE_UOM___APPLY_TO_VENDOR as APPLY_TO_VENDOR", 
	"JNR_WM_SIZE_UOM___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"JNR_WM_SIZE_UOM___DISCRETE as DISCRETE", 
	"JNR_WM_SIZE_UOM___ADJUSTMENT as ADJUSTMENT", 
	"JNR_WM_SIZE_UOM___ADJUSTMENT_SIZE_UOM_ID as ADJUSTMENT_SIZE_UOM_ID", 
	"JNR_WM_SIZE_UOM___HIBERNATE_VERSION as HIBERNATE_VERSION", 
	"JNR_WM_SIZE_UOM___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", 
	"JNR_WM_SIZE_UOM___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", 
	"JNR_WM_SIZE_UOM___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", 
	"JNR_WM_SIZE_UOM___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", 
	"JNR_WM_SIZE_UOM___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_SIZE_UOM___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", 
	"JNR_WM_SIZE_UOM___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_SIZE_UOM___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_WM_SIZE_UOM___in_LOCATION_ID as in_LOCATION_ID", 
	"JNR_WM_SIZE_UOM___WM_SIZE_UOM_ID as WM_SIZE_UOM_ID", 
	"JNR_WM_SIZE_UOM___WM_TC_COMPANY_ID as WM_TC_COMPANY_ID", 
	"JNR_WM_SIZE_UOM___WM_SIZE_UOM as WM_SIZE_UOM", 
	"JNR_WM_SIZE_UOM___WM_SIZE_UOM_DESC as WM_SIZE_UOM_DESC", 
	"JNR_WM_SIZE_UOM___WM_SIZE_MAPPING as WM_SIZE_MAPPING", 
	"JNR_WM_SIZE_UOM___WM_STANDARD_UOM_ID as WM_STANDARD_UOM_ID", 
	"JNR_WM_SIZE_UOM___WM_STANDARD_UNITS as WM_STANDARD_UNITS", 
	"JNR_WM_SIZE_UOM___WM_CONSOLIDATION_CD as WM_CONSOLIDATION_CD", 
	"JNR_WM_SIZE_UOM___WM_SPLITTER_CONS_CD as WM_SPLITTER_CONS_CD", 
	"JNR_WM_SIZE_UOM___DO_NOT_INHERIT_TO_ORDER_FLAG as DO_NOT_INHERIT_TO_ORDER_FLAG", 
	"JNR_WM_SIZE_UOM___APPLY_TO_VENDOR_FLAG as APPLY_TO_VENDOR_FLAG", 
	"JNR_WM_SIZE_UOM___DISCRETE_FLAG as DISCRETE_FLAG", 
	"JNR_WM_SIZE_UOM___WM_ADJUSTMENT_SIZE as WM_ADJUSTMENT_SIZE", 
	"JNR_WM_SIZE_UOM___WM_ADJUSTMENT_SIZE_UOM_ID as WM_ADJUSTMENT_SIZE_UOM_ID", 
	"JNR_WM_SIZE_UOM___WM_AUDIT_CREATED_SOURCE_TYPE as WM_AUDIT_CREATED_SOURCE_TYPE", 
	"JNR_WM_SIZE_UOM___WM_AUDIT_CREATED_SOURCE as WM_AUDIT_CREATED_SOURCE", 
	"JNR_WM_SIZE_UOM___WM_AUDIT_CREATED_TSTMP as WM_AUDIT_CREATED_TSTMP", 
	"JNR_WM_SIZE_UOM___WM_AUDIT_LAST_UPDATED_SOURCE_TYPE as WM_AUDIT_LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_SIZE_UOM___WM_AUDIT_LAST_UPDATED_SOURCE as WM_AUDIT_LAST_UPDATED_SOURCE", 
	"JNR_WM_SIZE_UOM___WM_AUDIT_LAST_UPDATED_TSTMP as WM_AUDIT_LAST_UPDATED_TSTMP", 
	"JNR_WM_SIZE_UOM___WM_HIBERNATE_VERSION as WM_HIBERNATE_VERSION", 
	"JNR_WM_SIZE_UOM___MARK_FOR_DELETION_FLAG as MARK_FOR_DELETION_FLAG", 
	"JNR_WM_SIZE_UOM___WM_CREATED_TSTMP as WM_CREATED_TSTMP", 
	"JNR_WM_SIZE_UOM___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_SIZE_UOM___in_LOAD_TSTMP as in_LOAD_TSTMP").filter(expr("WM_SIZE_UOM_ID IS NULL OR (NOT WM_SIZE_UOM_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 54

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_REC_temp = FIL_UNCHANGED_REC.toDF(*["FIL_UNCHANGED_REC___" + col for col in FIL_UNCHANGED_REC.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_REC_temp.selectExpr( 
	"FIL_UNCHANGED_REC___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_REC___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_REC___SIZE_UOM_ID as SIZE_UOM_ID", 
	"FIL_UNCHANGED_REC___TC_COMPANY_ID as TC_COMPANY_ID", 
	"FIL_UNCHANGED_REC___SIZE_UOM as SIZE_UOM", 
	"FIL_UNCHANGED_REC___DESCRIPTION as DESCRIPTION", 
	"FIL_UNCHANGED_REC___CONSOLIDATION_CODE as CONSOLIDATION_CODE", 
	"FIL_UNCHANGED_REC___DO_NOT_INHERIT_TO_ORDER as DO_NOT_INHERIT_TO_ORDER", 
	"FIL_UNCHANGED_REC___SIZE_MAPPING as SIZE_MAPPING", 
	"FIL_UNCHANGED_REC___STANDARD_UOM as STANDARD_UOM", 
	"FIL_UNCHANGED_REC___STANDARD_UNITS as STANDARD_UNITS", 
	"FIL_UNCHANGED_REC___SPLITTER_CONS_CODE as SPLITTER_CONS_CODE", 
	"FIL_UNCHANGED_REC___APPLY_TO_VENDOR as APPLY_TO_VENDOR", 
	"FIL_UNCHANGED_REC___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"FIL_UNCHANGED_REC___DISCRETE as DISCRETE", 
	"FIL_UNCHANGED_REC___ADJUSTMENT as ADJUSTMENT", 
	"FIL_UNCHANGED_REC___ADJUSTMENT_SIZE_UOM_ID as ADJUSTMENT_SIZE_UOM_ID", 
	"FIL_UNCHANGED_REC___HIBERNATE_VERSION as HIBERNATE_VERSION", 
	"FIL_UNCHANGED_REC___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", 
	"FIL_UNCHANGED_REC___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_REC___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", 
	"FIL_UNCHANGED_REC___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_REC___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_REC___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_REC___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_REC___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_REC___in_LOCATION_ID as in_LOCATION_ID", 
	"FIL_UNCHANGED_REC___WM_SIZE_UOM_ID as WM_SIZE_UOM_ID", 
	"FIL_UNCHANGED_REC___WM_TC_COMPANY_ID as WM_TC_COMPANY_ID", 
	"FIL_UNCHANGED_REC___WM_SIZE_UOM as WM_SIZE_UOM", 
	"FIL_UNCHANGED_REC___WM_SIZE_UOM_DESC as WM_SIZE_UOM_DESC", 
	"FIL_UNCHANGED_REC___WM_SIZE_MAPPING as WM_SIZE_MAPPING", 
	"FIL_UNCHANGED_REC___WM_STANDARD_UOM_ID as WM_STANDARD_UOM_ID", 
	"FIL_UNCHANGED_REC___WM_STANDARD_UNITS as WM_STANDARD_UNITS", 
	"FIL_UNCHANGED_REC___WM_CONSOLIDATION_CD as WM_CONSOLIDATION_CD", 
	"FIL_UNCHANGED_REC___WM_SPLITTER_CONS_CD as WM_SPLITTER_CONS_CD", 
	"FIL_UNCHANGED_REC___DO_NOT_INHERIT_TO_ORDER_FLAG as DO_NOT_INHERIT_TO_ORDER_FLAG", 
	"FIL_UNCHANGED_REC___APPLY_TO_VENDOR_FLAG as APPLY_TO_VENDOR_FLAG", 
	"FIL_UNCHANGED_REC___DISCRETE_FLAG as DISCRETE_FLAG", 
	"FIL_UNCHANGED_REC___WM_ADJUSTMENT_SIZE as WM_ADJUSTMENT_SIZE", 
	"FIL_UNCHANGED_REC___WM_ADJUSTMENT_SIZE_UOM_ID as WM_ADJUSTMENT_SIZE_UOM_ID", 
	"FIL_UNCHANGED_REC___WM_AUDIT_CREATED_SOURCE_TYPE as WM_AUDIT_CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_REC___WM_AUDIT_CREATED_SOURCE as WM_AUDIT_CREATED_SOURCE", 
	"FIL_UNCHANGED_REC___WM_AUDIT_CREATED_TSTMP as WM_AUDIT_CREATED_TSTMP", 
	"FIL_UNCHANGED_REC___WM_AUDIT_LAST_UPDATED_SOURCE_TYPE as WM_AUDIT_LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_REC___WM_AUDIT_LAST_UPDATED_SOURCE as WM_AUDIT_LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_REC___WM_AUDIT_LAST_UPDATED_TSTMP as WM_AUDIT_LAST_UPDATED_TSTMP", 
	"FIL_UNCHANGED_REC___WM_HIBERNATE_VERSION as WM_HIBERNATE_VERSION", 
	"FIL_UNCHANGED_REC___MARK_FOR_DELETION_FLAG as MARK_FOR_DELETION_FLAG", 
	"FIL_UNCHANGED_REC___WM_CREATED_TSTMP as WM_CREATED_TSTMP", 
	"FIL_UNCHANGED_REC___WM_LAST_UPDATED_TSTMP as WM_LAST_UPDATED_TSTMP", 
	"FIL_UNCHANGED_REC___in_LOAD_TSTMP as in_LOAD_TSTMP", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_REC___WM_SIZE_UOM_ID IS NULL, 1, 2) as o_UPD_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_UPD_VALIDATOR___SIZE_UOM_ID as SIZE_UOM_ID", 
	"EXP_UPD_VALIDATOR___TC_COMPANY_ID as TC_COMPANY_ID", 
	"EXP_UPD_VALIDATOR___SIZE_UOM as SIZE_UOM", 
	"EXP_UPD_VALIDATOR___DESCRIPTION as DESCRIPTION", 
	"EXP_UPD_VALIDATOR___SIZE_MAPPING as SIZE_MAPPING", 
	"EXP_UPD_VALIDATOR___STANDARD_UOM as STANDARD_UOM", 
	"EXP_UPD_VALIDATOR___STANDARD_UNITS as STANDARD_UNITS", 
	"EXP_UPD_VALIDATOR___CONSOLIDATION_CODE as CONSOLIDATION_CODE", 
	"EXP_UPD_VALIDATOR___SPLITTER_CONS_CODE as SPLITTER_CONS_CODE", 
	"EXP_UPD_VALIDATOR___DO_NOT_INHERIT_TO_ORDER as DO_NOT_INHERIT_TO_ORDER", 
	"EXP_UPD_VALIDATOR___APPLY_TO_VENDOR as APPLY_TO_VENDOR", 
	"EXP_UPD_VALIDATOR___DISCRETE as DISCRETE", 
	"EXP_UPD_VALIDATOR___ADJUSTMENT as ADJUSTMENT", 
	"EXP_UPD_VALIDATOR___ADJUSTMENT_SIZE_UOM_ID as ADJUSTMENT_SIZE_UOM_ID", 
	"EXP_UPD_VALIDATOR___AUDIT_CREATED_SOURCE_TYPE as AUDIT_CREATED_SOURCE_TYPE", 
	"EXP_UPD_VALIDATOR___AUDIT_CREATED_SOURCE as AUDIT_CREATED_SOURCE", 
	"EXP_UPD_VALIDATOR___AUDIT_CREATED_DTTM as AUDIT_CREATED_DTTM", 
	"EXP_UPD_VALIDATOR___AUDIT_LAST_UPDATED_SOURCE_TYPE as AUDIT_LAST_UPDATED_SOURCE_TYPE", 
	"EXP_UPD_VALIDATOR___AUDIT_LAST_UPDATED_SOURCE as AUDIT_LAST_UPDATED_SOURCE", 
	"EXP_UPD_VALIDATOR___AUDIT_LAST_UPDATED_DTTM as AUDIT_LAST_UPDATED_DTTM", 
	"EXP_UPD_VALIDATOR___HIBERNATE_VERSION as HIBERNATE_VERSION", 
	"EXP_UPD_VALIDATOR___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPD_VALIDATOR') ==(lit(1)),lit(0)).when(col('o_UPD_VALIDATOR') ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_SIZE_UOM1, type TARGET 
# COLUMN COUNT: 27


Shortcut_to_WM_SIZE_UOM1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(SIZE_UOM_ID AS INT) as WM_SIZE_UOM_ID",
	"CAST(TC_COMPANY_ID AS INT) as WM_TC_COMPANY_ID",
	"CAST(SIZE_UOM AS STRING) as WM_SIZE_UOM",
	"CAST(DESCRIPTION AS STRING) as WM_SIZE_UOM_DESC",
	"CAST(SIZE_MAPPING AS STRING) as WM_SIZE_MAPPING",
	"CAST(STANDARD_UOM AS SMALLINT) as WM_STANDARD_UOM_ID",
	"CAST(STANDARD_UNITS AS DECIMAL(16,8)) as WM_STANDARD_UNITS",
	"CAST(CONSOLIDATION_CODE AS STRING) as WM_CONSOLIDATION_CD",
	"CAST(SPLITTER_CONS_CODE AS STRING) as WM_SPLITTER_CONS_CD",
	"CAST(DO_NOT_INHERIT_TO_ORDER AS TINYINT) as DO_NOT_INHERIT_TO_ORDER_FLAG",
	"CAST(APPLY_TO_VENDOR AS TINYINT) as APPLY_TO_VENDOR_FLAG",
	"CAST(DISCRETE AS TINYINT) as DISCRETE_FLAG",
	"CAST(ADJUSTMENT AS DECIMAL(8,4)) as WM_ADJUSTMENT_SIZE",
	"CAST(ADJUSTMENT_SIZE_UOM_ID AS INT) as WM_ADJUSTMENT_SIZE_UOM_ID",
	"CAST(AUDIT_CREATED_SOURCE_TYPE AS SMALLINT) as WM_AUDIT_CREATED_SOURCE_TYPE",
	"CAST(AUDIT_CREATED_SOURCE AS STRING) as WM_AUDIT_CREATED_SOURCE",
	"CAST(AUDIT_CREATED_DTTM AS TIMESTAMP) as WM_AUDIT_CREATED_TSTMP",
	"CAST(AUDIT_LAST_UPDATED_SOURCE_TYPE AS SMALLINT) as WM_AUDIT_LAST_UPDATED_SOURCE_TYPE",
	"CAST(AUDIT_LAST_UPDATED_SOURCE AS STRING) as WM_AUDIT_LAST_UPDATED_SOURCE",
	"CAST(AUDIT_LAST_UPDATED_DTTM AS TIMESTAMP) as WM_AUDIT_LAST_UPDATED_TSTMP",
	"CAST(HIBERNATE_VERSION AS BIGINT) as WM_HIBERNATE_VERSION",
	"CAST(MARK_FOR_DELETION AS TINYINT) as MARK_FOR_DELETION_FLAG",
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_SIZE_UOM_ID = target.WM_SIZE_UOM_ID"""
  # refined_perf_table = "WM_SIZE_UOM"
  executeMerge(Shortcut_to_WM_SIZE_UOM1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_SIZE_UOM", "WM_SIZE_UOM", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_SIZE_UOM", "WM_SIZE_UOM","Failed",str(e), f"{raw}.log_run_details", )
  raise e
