#Code converted on 2023-06-21 18:23:00
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
refined_perf_table = f"{refine}.WM_ILM_YARD_ACTIVITY"
raw_perf_table = f"{raw}.WM_ILM_YARD_ACTIVITY_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY, type SOURCE 
# COLUMN COUNT: 18

SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY = spark.sql(f"""SELECT
LOCATION_ID,
WM_ACTIVITY_ID,
WM_COMPANY_ID,
WM_FACILITY_ID,
WM_LOCN_ID,
WM_LOCATION_ID,
WM_ACTIVITY_TYPE_ID,
WM_ACTIVITY_SOURCE,
WM_ACTIVITY_TSTMP,
WM_APPOINTMENT_ID,
WM_TASK_ID,
WM_VISIT_DETAIL_ID,
WM_EQUIPMENT_ID_1,
WM_EQUIPMENT_ID_2,
WM_EQUIP_INS_STATUS_ID,
WM_DRIVER_ID,
NO_OF_PALLETS,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_ACTIVITY_ID IN ( SELECT ACTIVITY_ID FROM {raw_perf_table} )""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE, type SOURCE 
# COLUMN COUNT: 18

SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE = spark.sql(f"""SELECT
DC_NBR,
ACTIVITY_ID,
COMPANY_ID,
APPOINTMENT_ID,
EQUIPMENT_ID1,
ACTIVITY_TYPE,
ACTIVITY_SOURCE,
ACTIVITY_DTTM,
DRIVER_ID,
EQUIPMENT_ID2,
TASK_ID,
LOCATION_ID,
NO_OF_PALLETS,
EQUIP_INS_STATUS,
FACILITY_ID,
VISIT_DETAIL_ID,
LOCN_ID,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE_temp = SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE.toDF(*["SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___" + col for col in SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___ACTIVITY_ID as ACTIVITY_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___COMPANY_ID as COMPANY_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___APPOINTMENT_ID as APPOINTMENT_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___EQUIPMENT_ID1 as EQUIPMENT_ID1", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___ACTIVITY_TYPE as ACTIVITY_TYPE", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___ACTIVITY_SOURCE as ACTIVITY_SOURCE", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___ACTIVITY_DTTM as ACTIVITY_DTTM", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___DRIVER_ID as DRIVER_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___EQUIPMENT_ID2 as EQUIPMENT_ID2", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___TASK_ID as TASK_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___NO_OF_PALLETS as NO_OF_PALLETS", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___EQUIP_INS_STATUS as EQUIP_INS_STATUS", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___FACILITY_ID as FACILITY_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___VISIT_DETAIL_ID as VISIT_DETAIL_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___LOCN_ID as LOCN_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
EXP_INT_CONVERSION_temp = EXP_INT_CONVERSION.toDF(*["EXP_INT_CONVERSION___" + col for col in EXP_INT_CONVERSION.columns])
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE_temp.join(EXP_INT_CONVERSION_temp,[SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR == EXP_INT_CONVERSION_temp.EXP_INT_CONVERSION___o_DC_NBR],'inner').selectExpr( \
	"EXP_INT_CONVERSION___o_DC_NBR as o_DC_NBR", \
	"EXP_INT_CONVERSION___ACTIVITY_ID as ACTIVITY_ID", \
	"EXP_INT_CONVERSION___COMPANY_ID as COMPANY_ID", \
	"EXP_INT_CONVERSION___APPOINTMENT_ID as APPOINTMENT_ID", \
	"EXP_INT_CONVERSION___EQUIPMENT_ID1 as EQUIPMENT_ID1", \
	"EXP_INT_CONVERSION___ACTIVITY_TYPE as ACTIVITY_TYPE", \
	"EXP_INT_CONVERSION___ACTIVITY_SOURCE as ACTIVITY_SOURCE", \
	"EXP_INT_CONVERSION___ACTIVITY_DTTM as ACTIVITY_DTTM", \
	"EXP_INT_CONVERSION___DRIVER_ID as DRIVER_ID", \
	"EXP_INT_CONVERSION___EQUIPMENT_ID2 as EQUIPMENT_ID2", \
	"EXP_INT_CONVERSION___TASK_ID as TASK_ID", \
	"EXP_INT_CONVERSION___LOCATION_ID as LOCATION_ID", \
	"EXP_INT_CONVERSION___NO_OF_PALLETS as NO_OF_PALLETS", \
	"EXP_INT_CONVERSION___EQUIP_INS_STATUS as EQUIP_INS_STATUS", \
	"EXP_INT_CONVERSION___FACILITY_ID as FACILITY_ID", \
	"EXP_INT_CONVERSION___VISIT_DETAIL_ID as VISIT_DETAIL_ID", \
	"EXP_INT_CONVERSION___LOCN_ID as LOCN_ID", \
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as SP_LOCATION_ID", \
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR")

# COMMAND ----------
# Processing node JNR_WM_ILM_YARD_ACTIVITY, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 35

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_temp = SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY.toDF(*["SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___" + col for col in SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY.columns])

JNR_WM_ILM_YARD_ACTIVITY = SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_temp.SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___SP_LOCATION_ID, SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY_temp.SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_ACTIVITY_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___ACTIVITY_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___ACTIVITY_ID as ACTIVITY_ID", \
	"JNR_SITE_PROFILE___COMPANY_ID as COMPANY_ID", \
	"JNR_SITE_PROFILE___APPOINTMENT_ID as APPOINTMENT_ID", \
	"JNR_SITE_PROFILE___EQUIPMENT_ID1 as EQUIPMENT_ID1", \
	"JNR_SITE_PROFILE___ACTIVITY_TYPE as ACTIVITY_TYPE", \
	"JNR_SITE_PROFILE___ACTIVITY_SOURCE as ACTIVITY_SOURCE", \
	"JNR_SITE_PROFILE___ACTIVITY_DTTM as ACTIVITY_DTTM", \
	"JNR_SITE_PROFILE___DRIVER_ID as DRIVER_ID", \
	"JNR_SITE_PROFILE___EQUIPMENT_ID2 as EQUIPMENT_ID2", \
	"JNR_SITE_PROFILE___TASK_ID as TASK_ID", \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___NO_OF_PALLETS as NO_OF_PALLETS", \
	"JNR_SITE_PROFILE___EQUIP_INS_STATUS as EQUIP_INS_STATUS", \
	"JNR_SITE_PROFILE___FACILITY_ID as FACILITY_ID", \
	"JNR_SITE_PROFILE___VISIT_DETAIL_ID as VISIT_DETAIL_ID", \
	"JNR_SITE_PROFILE___LOCN_ID as LOCN_ID", \
	"JNR_SITE_PROFILE___SP_LOCATION_ID as SP_LOCATION_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_ACTIVITY_ID as WM_ACTIVITY_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_COMPANY_ID as WM_COMPANY_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_FACILITY_ID as WM_FACILITY_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_LOCN_ID as WM_LOCN_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_LOCATION_ID as WM_LOCATION_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_ACTIVITY_TYPE_ID as WM_ACTIVITY_TYPE_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_ACTIVITY_SOURCE as WM_ACTIVITY_SOURCE", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_ACTIVITY_TSTMP as WM_ACTIVITY_TSTMP", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_APPOINTMENT_ID as WM_APPOINTMENT_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_TASK_ID as WM_TASK_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_VISIT_DETAIL_ID as WM_VISIT_DETAIL_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_EQUIPMENT_ID_1 as WM_EQUIPMENT_ID_1", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_EQUIPMENT_ID_2 as WM_EQUIPMENT_ID_2", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_EQUIP_INS_STATUS_ID as WM_EQUIP_INS_STATUS_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___WM_DRIVER_ID as WM_DRIVER_ID", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___NO_OF_PALLETS as NO_OF_PALLETS1", \
	"SQ_Shortcut_to_WM_ILM_YARD_ACTIVITY___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_REC, type FILTER 
# COLUMN COUNT: 35

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_ILM_YARD_ACTIVITY_temp = JNR_WM_ILM_YARD_ACTIVITY.toDF(*["JNR_WM_ILM_YARD_ACTIVITY___" + col for col in JNR_WM_ILM_YARD_ACTIVITY.columns])

FIL_UNCHANGED_REC = JNR_WM_ILM_YARD_ACTIVITY_temp.selectExpr( \
	"JNR_WM_ILM_YARD_ACTIVITY___ACTIVITY_ID as ACTIVITY_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___COMPANY_ID as COMPANY_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___APPOINTMENT_ID as APPOINTMENT_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___EQUIPMENT_ID1 as EQUIPMENT_ID1", \
	"JNR_WM_ILM_YARD_ACTIVITY___ACTIVITY_TYPE as ACTIVITY_TYPE", \
	"JNR_WM_ILM_YARD_ACTIVITY___ACTIVITY_SOURCE as ACTIVITY_SOURCE", \
	"JNR_WM_ILM_YARD_ACTIVITY___ACTIVITY_DTTM as ACTIVITY_DTTM", \
	"JNR_WM_ILM_YARD_ACTIVITY___DRIVER_ID as DRIVER_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___EQUIPMENT_ID2 as EQUIPMENT_ID2", \
	"JNR_WM_ILM_YARD_ACTIVITY___TASK_ID as TASK_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___NO_OF_PALLETS as NO_OF_PALLETS", \
	"JNR_WM_ILM_YARD_ACTIVITY___EQUIP_INS_STATUS as EQUIP_INS_STATUS", \
	"JNR_WM_ILM_YARD_ACTIVITY___FACILITY_ID as FACILITY_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___VISIT_DETAIL_ID as VISIT_DETAIL_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___LOCN_ID as LOCN_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___SP_LOCATION_ID as SP_LOCATION_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_ACTIVITY_ID as WM_ACTIVITY_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_COMPANY_ID as WM_COMPANY_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_FACILITY_ID as WM_FACILITY_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_LOCN_ID as WM_LOCN_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_LOCATION_ID as WM_LOCATION_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_ACTIVITY_TYPE_ID as WM_ACTIVITY_TYPE_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_ACTIVITY_SOURCE as WM_ACTIVITY_SOURCE", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_ACTIVITY_TSTMP as WM_ACTIVITY_TSTMP", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_APPOINTMENT_ID as WM_APPOINTMENT_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_TASK_ID as WM_TASK_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_VISIT_DETAIL_ID as WM_VISIT_DETAIL_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_EQUIPMENT_ID_1 as WM_EQUIPMENT_ID_1", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_EQUIPMENT_ID_2 as WM_EQUIPMENT_ID_2", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_EQUIP_INS_STATUS_ID as WM_EQUIP_INS_STATUS_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___WM_DRIVER_ID as WM_DRIVER_ID", \
	"JNR_WM_ILM_YARD_ACTIVITY___NO_OF_PALLETS1 as NO_OF_PALLETS1", \
	"JNR_WM_ILM_YARD_ACTIVITY___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("WM_ACTIVITY_ID is Null OR ( WM_ACTIVITY_ID is not Null AND \
     ( COALESCE(COMPANY_ID, 0) != COALESCE(WM_COMPANY_ID, 0) \
        OR COALESCE(APPOINTMENT_ID, 0) != COALESCE(WM_APPOINTMENT_ID, 0) \
        OR COALESCE(EQUIPMENT_ID1, 0) != COALESCE(WM_EQUIPMENT_ID_1, 0) \
        OR COALESCE(ACTIVITY_TYPE, 0) != COALESCE(WM_ACTIVITY_TYPE_ID, 0) \
        OR COALESCE(ltrim (rtrim(upper( ACTIVITY_SOURCE ))), '') != COALESCE(ltrim(rtrim(upper( WM_ACTIVITY_SOURCE))), '') \
 		OR COALESCE(ACTIVITY_DTTM, date'1900-01-01') != COALESCE(WM_ACTIVITY_TSTMP, date'1900-01-01') \
        OR COALESCE(DRIVER_ID, 0) != COALESCE(WM_DRIVER_ID, 0) \
        OR COALESCE(EQUIPMENT_ID2, 0) != COALESCE(WM_EQUIPMENT_ID_2, 0) \
        OR COALESCE(TASK_ID, 0) != COALESCE(WM_TASK_ID, 0) \
        OR COALESCE(LOCATION_ID, 0) != COALESCE(WM_LOCATION_ID, 0) \
        OR COALESCE(NO_OF_PALLETS, 0) != COALESCE(NO_OF_PALLETS1, 0) \
        OR COALESCE(EQUIP_INS_STATUS, 0) != COALESCE(WM_EQUIP_INS_STATUS_ID, 0) \
        OR COALESCE(FACILITY_ID, 0) != COALESCE(WM_FACILITY_ID, 0) \
        OR COALESCE(VISIT_DETAIL_ID, 0) != COALESCE(WM_VISIT_DETAIL_ID, 0) \
        OR COALESCE(ltrim (rtrim(upper( LOCN_ID ))), '') != COALESCE(ltrim(rtrim(upper( WM_LOCN_ID))), '')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 38

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_REC_temp = FIL_UNCHANGED_REC.toDF(*["FIL_UNCHANGED_REC___" + col for col in FIL_UNCHANGED_REC.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_REC_temp.selectExpr( \
	"FIL_UNCHANGED_REC___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_REC___ACTIVITY_ID as ACTIVITY_ID", \
	"FIL_UNCHANGED_REC___COMPANY_ID as COMPANY_ID", \
	"FIL_UNCHANGED_REC___APPOINTMENT_ID as APPOINTMENT_ID", \
	"FIL_UNCHANGED_REC___EQUIPMENT_ID1 as EQUIPMENT_ID1", \
	"FIL_UNCHANGED_REC___ACTIVITY_TYPE as ACTIVITY_TYPE", \
	"FIL_UNCHANGED_REC___ACTIVITY_SOURCE as ACTIVITY_SOURCE", \
	"FIL_UNCHANGED_REC___ACTIVITY_DTTM as ACTIVITY_DTTM", \
	"FIL_UNCHANGED_REC___DRIVER_ID as DRIVER_ID", \
	"FIL_UNCHANGED_REC___EQUIPMENT_ID2 as EQUIPMENT_ID2", \
	"FIL_UNCHANGED_REC___TASK_ID as TASK_ID", \
	"FIL_UNCHANGED_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_REC___NO_OF_PALLETS as NO_OF_PALLETS", \
	"FIL_UNCHANGED_REC___EQUIP_INS_STATUS as EQUIP_INS_STATUS", \
	"FIL_UNCHANGED_REC___FACILITY_ID as FACILITY_ID", \
	"FIL_UNCHANGED_REC___VISIT_DETAIL_ID as VISIT_DETAIL_ID", \
	"FIL_UNCHANGED_REC___LOCN_ID as LOCN_ID", \
	"FIL_UNCHANGED_REC___SP_LOCATION_ID as SP_LOCATION_ID", \
	"FIL_UNCHANGED_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_REC___WM_ACTIVITY_ID as WM_ACTIVITY_ID", \
	"FIL_UNCHANGED_REC___WM_COMPANY_ID as WM_COMPANY_ID", \
	"FIL_UNCHANGED_REC___WM_FACILITY_ID as WM_FACILITY_ID", \
	"FIL_UNCHANGED_REC___WM_LOCN_ID as WM_LOCN_ID", \
	"FIL_UNCHANGED_REC___WM_LOCATION_ID as WM_LOCATION_ID", \
	"FIL_UNCHANGED_REC___WM_ACTIVITY_TYPE_ID as WM_ACTIVITY_TYPE_ID", \
	"FIL_UNCHANGED_REC___WM_ACTIVITY_SOURCE as WM_ACTIVITY_SOURCE", \
	"FIL_UNCHANGED_REC___WM_ACTIVITY_TSTMP as WM_ACTIVITY_TSTMP", \
	"FIL_UNCHANGED_REC___WM_APPOINTMENT_ID as WM_APPOINTMENT_ID", \
	"FIL_UNCHANGED_REC___WM_TASK_ID as WM_TASK_ID", \
	"FIL_UNCHANGED_REC___WM_VISIT_DETAIL_ID as WM_VISIT_DETAIL_ID", \
	"FIL_UNCHANGED_REC___WM_EQUIPMENT_ID_1 as WM_EQUIPMENT_ID_1", \
	"FIL_UNCHANGED_REC___WM_EQUIPMENT_ID_2 as WM_EQUIPMENT_ID_2", \
	"FIL_UNCHANGED_REC___WM_EQUIP_INS_STATUS_ID as WM_EQUIP_INS_STATUS_ID", \
	"FIL_UNCHANGED_REC___WM_DRIVER_ID as WM_DRIVER_ID", \
	"FIL_UNCHANGED_REC___NO_OF_PALLETS1 as NO_OF_PALLETS1", \
	"FIL_UNCHANGED_REC___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"CURRENT_TIMESTAMP() as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP(), FIL_UNCHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_REC___WM_ACTIVITY_ID IS NULL, 1, 2) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___SP_LOCATION_ID as SP_LOCATION_ID", \
	"EXP_UPD_VALIDATOR___ACTIVITY_ID as ACTIVITY_ID", \
	"EXP_UPD_VALIDATOR___COMPANY_ID as COMPANY_ID", \
	"EXP_UPD_VALIDATOR___FACILITY_ID as FACILITY_ID", \
	"EXP_UPD_VALIDATOR___LOCN_ID as LOCN_ID", \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___ACTIVITY_TYPE as ACTIVITY_TYPE", \
	"EXP_UPD_VALIDATOR___ACTIVITY_SOURCE as ACTIVITY_SOURCE", \
	"EXP_UPD_VALIDATOR___ACTIVITY_DTTM as ACTIVITY_DTTM", \
	"EXP_UPD_VALIDATOR___APPOINTMENT_ID as APPOINTMENT_ID", \
	"EXP_UPD_VALIDATOR___TASK_ID as TASK_ID", \
	"EXP_UPD_VALIDATOR___VISIT_DETAIL_ID as VISIT_DETAIL_ID", \
	"EXP_UPD_VALIDATOR___EQUIPMENT_ID1 as EQUIPMENT_ID1", \
	"EXP_UPD_VALIDATOR___EQUIPMENT_ID2 as EQUIPMENT_ID2", \
	"EXP_UPD_VALIDATOR___EQUIP_INS_STATUS as EQUIP_INS_STATUS", \
	"EXP_UPD_VALIDATOR___DRIVER_ID as DRIVER_ID", \
	"EXP_UPD_VALIDATOR___NO_OF_PALLETS as NO_OF_PALLETS", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPD_VALIDATOR') ==(lit(1)), lit(0)).when(col('o_UPD_VALIDATOR') ==(lit(2)), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_ILM_YARD_ACTIVITY, type TARGET 
# COLUMN COUNT: 19

Shortcut_to_WM_ILM_YARD_ACTIVITY = UPD_INS_UPD.selectExpr(
	"CAST(SP_LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(ACTIVITY_ID AS DECIMAL(9,0)) as WM_ACTIVITY_ID",
	"CAST(COMPANY_ID AS DECIMAL(9,0)) as WM_COMPANY_ID",
	"CAST(FACILITY_ID AS DECIMAL(9,0)) as WM_FACILITY_ID",
	"CAST(LOCN_ID AS STRING) as WM_LOCN_ID",
	"CAST(LOCATION_ID AS DECIMAL(9,0)) as WM_LOCATION_ID",
	"CAST(ACTIVITY_TYPE AS DECIMAL(4,0)) as WM_ACTIVITY_TYPE_ID",
	"CAST(ACTIVITY_SOURCE AS STRING) as WM_ACTIVITY_SOURCE",
	"CAST(ACTIVITY_DTTM AS TIMESTAMP) as WM_ACTIVITY_TSTMP",
	"CAST(APPOINTMENT_ID AS DECIMAL(9,0)) as WM_APPOINTMENT_ID",
	"CAST(TASK_ID AS DECIMAL(9,0)) as WM_TASK_ID",
	"CAST(VISIT_DETAIL_ID AS DECIMAL(9,0)) as WM_VISIT_DETAIL_ID",
	"CAST(EQUIPMENT_ID1 AS DECIMAL(9,0)) as WM_EQUIPMENT_ID_1",
	"CAST(EQUIPMENT_ID2 AS DECIMAL(9,0)) as WM_EQUIPMENT_ID_2",
	"CAST(EQUIP_INS_STATUS AS DECIMAL(4,0)) as WM_EQUIP_INS_STATUS_ID",
	"CAST(DRIVER_ID AS DECIMAL(9,0)) as WM_DRIVER_ID",
	"CAST(NO_OF_PALLETS AS DECIMAL(9,0)) as NO_OF_PALLETS",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_ACTIVITY_ID = target.WM_ACTIVITY_ID"""
#   refined_perf_table = "WM_ILM_YARD_ACTIVITY"
  executeMerge(Shortcut_to_WM_ILM_YARD_ACTIVITY, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_ILM_YARD_ACTIVITY", "WM_ILM_YARD_ACTIVITY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_ILM_YARD_ACTIVITY", "WM_ILM_YARD_ACTIVITY","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	