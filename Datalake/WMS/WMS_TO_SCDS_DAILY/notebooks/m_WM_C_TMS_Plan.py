#Code converted on 2023-06-20 18:38:19
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
# args = parser.parse_args()
# env = args.env
env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
refined_perf_table = f"{refine}.WM_C_TMS_PLAN"
raw_perf_table = f"{raw}.WM_C_TMS_PLAN_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_C_TMS_PLAN_PRE, type SOURCE 
# COLUMN COUNT: 44

SQ_Shortcut_to_WM_C_TMS_PLAN_PRE = spark.sql(f"""SELECT
DC_NBR,
C_TMS_PLAN_ID,
PLAN_ID,
STO_NBR,
ROUTE_ID,
STOP_ID,
STOP_LOC,
ORIGIN,
DESTINATION,
CONSOLIDATOR,
CONS_ADDR_1,
CONS_ADDR_2,
CONS_ADDR_3,
CITY,
STATE,
ZIP,
CNTRY,
CONTACT_PERSON,
PHONE,
COMMODITY,
NUM_PALLETS,
WEIGHT,
VOLUME,
DROP_DEAD_DT,
ETA,
SPLIT_ROUTE,
TMS_CARRIER_ID,
TMS_CARRIER_NAME,
LOAD_STAT,
SHIP_VIA,
MILES,
DRIVE_TIME,
EQUIP_TYPE,
DEL_TYPE,
TMS_TRAILER_ID,
CARRIER_REF_NUM,
TMS_PLAN_TIME,
PLT_WEIGHT,
ERROR_SEQ_NBR,
STAT_CODE,
CREATE_DATE_TIME,
MOD_DATE_TIME,
USER_ID,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_C_TMS_PLAN, type SOURCE 
# COLUMN COUNT: 44

SQ_Shortcut_to_WM_C_TMS_PLAN = spark.sql(f"""SELECT
LOCATION_ID,
WM_C_TMS_PLAN_ID,
WM_PLAN_ID,
WM_STO_NBR,
WM_ROUTE_ID,
WM_STOP_ID,
WM_STOP_LOC,
SPLIT_ROUTE,
WM_SHIP_VIA,
WM_EQUIP_TYPE,
WM_DEL_TYPE,
WM_TMS_CARRIER_ID,
WM_TMS_CARRIER_NAME,
WM_CARRIER_REF_NBR,
WM_TMS_TRAILER_ID,
WM_LOAD_STAT_CD,
WM_STAT_CD,
MILES,
DRIVE_TIME,
ERROR_SEQ_NBR,
WM_TMS_PLAN_TSTMP,
DROP_DEAD_TSTMP,
ETA_TSTMP,
ORIGIN,
DESTINATION,
CONSOLIDATOR,
CONS_ADDR_1,
CONS_ADDR_2,
CONS_ADDR_3,
CITY,
STATE,
ZIP,
COUNTRY_CD,
CONTACT_PERSON,
PHONE,
COMMODITY,
NUM_PALLETS,
PLT_WEIGHT,
WEIGHT,
VOLUME,
WM_USER_ID,
WM_CREATE_TSTMP,
WM_MOD_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_C_TMS_PLAN_ID IN ( SELECT C_TMS_PLAN_ID FROM {raw_perf_table} )""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 44

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_C_TMS_PLAN_PRE_temp = SQ_Shortcut_to_WM_C_TMS_PLAN_PRE.toDF(*["SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___" + col for col in SQ_Shortcut_to_WM_C_TMS_PLAN_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_C_TMS_PLAN_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___C_TMS_PLAN_ID as C_TMS_PLAN_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___PLAN_ID as PLAN_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___STO_NBR as STO_NBR", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___ROUTE_ID as ROUTE_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___STOP_ID as STOP_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___STOP_LOC as STOP_LOC", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___ORIGIN as ORIGIN", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___DESTINATION as DESTINATION", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___CONSOLIDATOR as CONSOLIDATOR", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___CONS_ADDR_1 as CONS_ADDR_1", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___CONS_ADDR_2 as CONS_ADDR_2", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___CONS_ADDR_3 as CONS_ADDR_3", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___CITY as CITY", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___STATE as STATE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___ZIP as ZIP", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___CNTRY as CNTRY", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___CONTACT_PERSON as CONTACT_PERSON", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___PHONE as PHONE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___COMMODITY as COMMODITY", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___NUM_PALLETS as NUM_PALLETS", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___WEIGHT as WEIGHT", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___VOLUME as VOLUME", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___DROP_DEAD_DT as DROP_DEAD_DT", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___ETA as ETA", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___SPLIT_ROUTE as SPLIT_ROUTE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___TMS_CARRIER_ID as TMS_CARRIER_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___TMS_CARRIER_NAME as TMS_CARRIER_NAME", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___LOAD_STAT as LOAD_STAT", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___SHIP_VIA as SHIP_VIA", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___MILES as MILES", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___DRIVE_TIME as DRIVE_TIME", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___EQUIP_TYPE as EQUIP_TYPE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___DEL_TYPE as DEL_TYPE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___TMS_TRAILER_ID as TMS_TRAILER_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___CARRIER_REF_NUM as CARRIER_REF_NUM", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___TMS_PLAN_TIME as TMS_PLAN_TIME", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___PLT_WEIGHT as PLT_WEIGHT", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___ERROR_SEQ_NBR as ERROR_SEQ_NBR", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___STAT_CODE as STAT_CODE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___USER_ID as USER_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 45

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_C_TMS_PLAN, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 87

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_C_TMS_PLAN_temp = SQ_Shortcut_to_WM_C_TMS_PLAN.toDF(*["SQ_Shortcut_to_WM_C_TMS_PLAN___" + col for col in SQ_Shortcut_to_WM_C_TMS_PLAN.columns])

JNR_WM_C_TMS_PLAN = SQ_Shortcut_to_WM_C_TMS_PLAN_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_C_TMS_PLAN_temp.SQ_Shortcut_to_WM_C_TMS_PLAN___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_C_TMS_PLAN_temp.SQ_Shortcut_to_WM_C_TMS_PLAN___WM_C_TMS_PLAN_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___C_TMS_PLAN_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___C_TMS_PLAN_ID as C_TMS_PLAN_ID", \
	"JNR_SITE_PROFILE___PLAN_ID as PLAN_ID", \
	"JNR_SITE_PROFILE___STO_NBR as STO_NBR", \
	"JNR_SITE_PROFILE___ROUTE_ID as ROUTE_ID", \
	"JNR_SITE_PROFILE___STOP_ID as STOP_ID", \
	"JNR_SITE_PROFILE___STOP_LOC as STOP_LOC", \
	"JNR_SITE_PROFILE___ORIGIN as ORIGIN", \
	"JNR_SITE_PROFILE___DESTINATION as DESTINATION", \
	"JNR_SITE_PROFILE___CONSOLIDATOR as CONSOLIDATOR", \
	"JNR_SITE_PROFILE___CONS_ADDR_1 as CONS_ADDR_1", \
	"JNR_SITE_PROFILE___CONS_ADDR_2 as CONS_ADDR_2", \
	"JNR_SITE_PROFILE___CONS_ADDR_3 as CONS_ADDR_3", \
	"JNR_SITE_PROFILE___CITY as CITY", \
	"JNR_SITE_PROFILE___STATE as STATE", \
	"JNR_SITE_PROFILE___ZIP as ZIP", \
	"JNR_SITE_PROFILE___CNTRY as CNTRY", \
	"JNR_SITE_PROFILE___CONTACT_PERSON as CONTACT_PERSON", \
	"JNR_SITE_PROFILE___PHONE as PHONE", \
	"JNR_SITE_PROFILE___COMMODITY as COMMODITY", \
	"JNR_SITE_PROFILE___NUM_PALLETS as NUM_PALLETS", \
	"JNR_SITE_PROFILE___WEIGHT as WEIGHT", \
	"JNR_SITE_PROFILE___VOLUME as VOLUME", \
	"JNR_SITE_PROFILE___DROP_DEAD_DT as DROP_DEAD_DT", \
	"JNR_SITE_PROFILE___ETA as ETA", \
	"JNR_SITE_PROFILE___SPLIT_ROUTE as SPLIT_ROUTE", \
	"JNR_SITE_PROFILE___TMS_CARRIER_ID as TMS_CARRIER_ID", \
	"JNR_SITE_PROFILE___TMS_CARRIER_NAME as TMS_CARRIER_NAME", \
	"JNR_SITE_PROFILE___LOAD_STAT as LOAD_STAT", \
	"JNR_SITE_PROFILE___SHIP_VIA as SHIP_VIA", \
	"JNR_SITE_PROFILE___MILES as MILES", \
	"JNR_SITE_PROFILE___DRIVE_TIME as DRIVE_TIME", \
	"JNR_SITE_PROFILE___EQUIP_TYPE as EQUIP_TYPE", \
	"JNR_SITE_PROFILE___DEL_TYPE as DEL_TYPE", \
	"JNR_SITE_PROFILE___TMS_TRAILER_ID as TMS_TRAILER_ID", \
	"JNR_SITE_PROFILE___CARRIER_REF_NUM as CARRIER_REF_NUM", \
	"JNR_SITE_PROFILE___TMS_PLAN_TIME as TMS_PLAN_TIME", \
	"JNR_SITE_PROFILE___PLT_WEIGHT as PLT_WEIGHT", \
	"JNR_SITE_PROFILE___ERROR_SEQ_NBR as ERROR_SEQ_NBR", \
	"JNR_SITE_PROFILE___STAT_CODE as STAT_CODE", \
	"JNR_SITE_PROFILE___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_SITE_PROFILE___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_SITE_PROFILE___USER_ID as USER_ID", \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_C_TMS_PLAN_ID as WM_C_TMS_PLAN_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_PLAN_ID as WM_PLAN_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_STO_NBR as WM_STO_NBR", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_ROUTE_ID as WM_ROUTE_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_STOP_ID as WM_STOP_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_STOP_LOC as WM_STOP_LOC", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___SPLIT_ROUTE as in_SPLIT_ROUTE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_SHIP_VIA as WM_SHIP_VIA", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_EQUIP_TYPE as WM_EQUIP_TYPE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_DEL_TYPE as WM_DEL_TYPE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_TMS_CARRIER_ID as WM_TMS_CARRIER_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_TMS_CARRIER_NAME as WM_TMS_CARRIER_NAME", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_CARRIER_REF_NBR as WM_CARRIER_REF_NBR", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_TMS_TRAILER_ID as WM_TMS_TRAILER_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_LOAD_STAT_CD as WM_LOAD_STAT_CD", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_STAT_CD as WM_STAT_CD", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___MILES as in_MILES", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___DRIVE_TIME as in_DRIVE_TIME", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___ERROR_SEQ_NBR as in_ERROR_SEQ_NBR", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_TMS_PLAN_TSTMP as WM_TMS_PLAN_TSTMP", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___DROP_DEAD_TSTMP as DROP_DEAD_TSTMP", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___ETA_TSTMP as ETA_TSTMP", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___ORIGIN as in_ORIGIN", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___DESTINATION as DESTINATION1", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___CONSOLIDATOR as in_CONSOLIDATOR", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___CONS_ADDR_1 as in_CONS_ADDR_1", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___CONS_ADDR_2 as in_CONS_ADDR_2", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___CONS_ADDR_3 as in_CONS_ADDR_3", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___CITY as in_CITY", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___STATE as in_STATE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___ZIP as in_ZIP", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___COUNTRY_CD as COUNTRY_CD", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___CONTACT_PERSON as in_CONTACT_PERSON", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___PHONE as in_PHONE", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___COMMODITY as in_COMMODITY", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___NUM_PALLETS as in_NUM_PALLETS", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___PLT_WEIGHT as in_PLT_WEIGHT", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WEIGHT as in_WEIGHT", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___VOLUME as in_VOLUME", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_USER_ID as WM_USER_ID", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"SQ_Shortcut_to_WM_C_TMS_PLAN___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_REC, type FILTER 
# COLUMN COUNT: 87

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_C_TMS_PLAN_temp = JNR_WM_C_TMS_PLAN.toDF(*["JNR_WM_C_TMS_PLAN___" + col for col in JNR_WM_C_TMS_PLAN.columns])

FIL_UNCHANGED_REC = JNR_WM_C_TMS_PLAN_temp.selectExpr( \
	"JNR_WM_C_TMS_PLAN___C_TMS_PLAN_ID as C_TMS_PLAN_ID", \
	"JNR_WM_C_TMS_PLAN___PLAN_ID as PLAN_ID", \
	"JNR_WM_C_TMS_PLAN___STO_NBR as STO_NBR", \
	"JNR_WM_C_TMS_PLAN___ROUTE_ID as ROUTE_ID", \
	"JNR_WM_C_TMS_PLAN___STOP_ID as STOP_ID", \
	"JNR_WM_C_TMS_PLAN___STOP_LOC as STOP_LOC", \
	"JNR_WM_C_TMS_PLAN___ORIGIN as ORIGIN", \
	"JNR_WM_C_TMS_PLAN___DESTINATION as DESTINATION", \
	"JNR_WM_C_TMS_PLAN___CONSOLIDATOR as CONSOLIDATOR", \
	"JNR_WM_C_TMS_PLAN___CONS_ADDR_1 as CONS_ADDR_1", \
	"JNR_WM_C_TMS_PLAN___CONS_ADDR_2 as CONS_ADDR_2", \
	"JNR_WM_C_TMS_PLAN___CONS_ADDR_3 as CONS_ADDR_3", \
	"JNR_WM_C_TMS_PLAN___CITY as CITY", \
	"JNR_WM_C_TMS_PLAN___STATE as STATE", \
	"JNR_WM_C_TMS_PLAN___ZIP as ZIP", \
	"JNR_WM_C_TMS_PLAN___CNTRY as CNTRY", \
	"JNR_WM_C_TMS_PLAN___CONTACT_PERSON as CONTACT_PERSON", \
	"JNR_WM_C_TMS_PLAN___PHONE as PHONE", \
	"JNR_WM_C_TMS_PLAN___COMMODITY as COMMODITY", \
	"JNR_WM_C_TMS_PLAN___NUM_PALLETS as NUM_PALLETS", \
	"JNR_WM_C_TMS_PLAN___WEIGHT as WEIGHT", \
	"JNR_WM_C_TMS_PLAN___VOLUME as VOLUME", \
	"JNR_WM_C_TMS_PLAN___DROP_DEAD_DT as DROP_DEAD_DT", \
	"JNR_WM_C_TMS_PLAN___ETA as ETA", \
	"JNR_WM_C_TMS_PLAN___SPLIT_ROUTE as SPLIT_ROUTE", \
	"JNR_WM_C_TMS_PLAN___TMS_CARRIER_ID as TMS_CARRIER_ID", \
	"JNR_WM_C_TMS_PLAN___TMS_CARRIER_NAME as TMS_CARRIER_NAME", \
	"JNR_WM_C_TMS_PLAN___LOAD_STAT as LOAD_STAT", \
	"JNR_WM_C_TMS_PLAN___SHIP_VIA as SHIP_VIA", \
	"JNR_WM_C_TMS_PLAN___MILES as MILES", \
	"JNR_WM_C_TMS_PLAN___DRIVE_TIME as DRIVE_TIME", \
	"JNR_WM_C_TMS_PLAN___EQUIP_TYPE as EQUIP_TYPE", \
	"JNR_WM_C_TMS_PLAN___DEL_TYPE as DEL_TYPE", \
	"JNR_WM_C_TMS_PLAN___TMS_TRAILER_ID as TMS_TRAILER_ID", \
	"JNR_WM_C_TMS_PLAN___CARRIER_REF_NUM as CARRIER_REF_NUM", \
	"JNR_WM_C_TMS_PLAN___TMS_PLAN_TIME as TMS_PLAN_TIME", \
	"JNR_WM_C_TMS_PLAN___PLT_WEIGHT as PLT_WEIGHT", \
	"JNR_WM_C_TMS_PLAN___ERROR_SEQ_NBR as ERROR_SEQ_NBR", \
	"JNR_WM_C_TMS_PLAN___STAT_CODE as STAT_CODE", \
	"JNR_WM_C_TMS_PLAN___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"JNR_WM_C_TMS_PLAN___MOD_DATE_TIME as MOD_DATE_TIME", \
	"JNR_WM_C_TMS_PLAN___USER_ID as USER_ID", \
	"JNR_WM_C_TMS_PLAN___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_C_TMS_PLAN___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_C_TMS_PLAN___WM_C_TMS_PLAN_ID as WM_C_TMS_PLAN_ID", \
	"JNR_WM_C_TMS_PLAN___WM_PLAN_ID as WM_PLAN_ID", \
	"JNR_WM_C_TMS_PLAN___WM_STO_NBR as WM_STO_NBR", \
	"JNR_WM_C_TMS_PLAN___WM_ROUTE_ID as WM_ROUTE_ID", \
	"JNR_WM_C_TMS_PLAN___WM_STOP_ID as WM_STOP_ID", \
	"JNR_WM_C_TMS_PLAN___WM_STOP_LOC as WM_STOP_LOC", \
	"JNR_WM_C_TMS_PLAN___in_SPLIT_ROUTE as in_SPLIT_ROUTE", \
	"JNR_WM_C_TMS_PLAN___WM_SHIP_VIA as WM_SHIP_VIA", \
	"JNR_WM_C_TMS_PLAN___WM_EQUIP_TYPE as WM_EQUIP_TYPE", \
	"JNR_WM_C_TMS_PLAN___WM_DEL_TYPE as WM_DEL_TYPE", \
	"JNR_WM_C_TMS_PLAN___WM_TMS_CARRIER_ID as WM_TMS_CARRIER_ID", \
	"JNR_WM_C_TMS_PLAN___WM_TMS_CARRIER_NAME as WM_TMS_CARRIER_NAME", \
	"JNR_WM_C_TMS_PLAN___WM_CARRIER_REF_NBR as WM_CARRIER_REF_NBR", \
	"JNR_WM_C_TMS_PLAN___WM_TMS_TRAILER_ID as WM_TMS_TRAILER_ID", \
	"JNR_WM_C_TMS_PLAN___WM_LOAD_STAT_CD as WM_LOAD_STAT_CD", \
	"JNR_WM_C_TMS_PLAN___WM_STAT_CD as WM_STAT_CD", \
	"JNR_WM_C_TMS_PLAN___in_MILES as in_MILES", \
	"JNR_WM_C_TMS_PLAN___in_DRIVE_TIME as in_DRIVE_TIME", \
	"JNR_WM_C_TMS_PLAN___in_ERROR_SEQ_NBR as in_ERROR_SEQ_NBR", \
	"JNR_WM_C_TMS_PLAN___WM_TMS_PLAN_TSTMP as WM_TMS_PLAN_TSTMP", \
	"JNR_WM_C_TMS_PLAN___DROP_DEAD_TSTMP as DROP_DEAD_TSTMP", \
	"JNR_WM_C_TMS_PLAN___ETA_TSTMP as ETA_TSTMP", \
	"JNR_WM_C_TMS_PLAN___in_ORIGIN as in_ORIGIN", \
	"JNR_WM_C_TMS_PLAN___DESTINATION1 as DESTINATION1", \
	"JNR_WM_C_TMS_PLAN___in_CONSOLIDATOR as in_CONSOLIDATOR", \
	"JNR_WM_C_TMS_PLAN___in_CONS_ADDR_1 as in_CONS_ADDR_1", \
	"JNR_WM_C_TMS_PLAN___in_CONS_ADDR_2 as in_CONS_ADDR_2", \
	"JNR_WM_C_TMS_PLAN___in_CONS_ADDR_3 as in_CONS_ADDR_3", \
	"JNR_WM_C_TMS_PLAN___in_CITY as in_CITY", \
	"JNR_WM_C_TMS_PLAN___in_STATE as in_STATE", \
	"JNR_WM_C_TMS_PLAN___in_ZIP as in_ZIP", \
	"JNR_WM_C_TMS_PLAN___COUNTRY_CD as COUNTRY_CD", \
	"JNR_WM_C_TMS_PLAN___in_CONTACT_PERSON as in_CONTACT_PERSON", \
	"JNR_WM_C_TMS_PLAN___in_PHONE as in_PHONE", \
	"JNR_WM_C_TMS_PLAN___in_COMMODITY as in_COMMODITY", \
	"JNR_WM_C_TMS_PLAN___in_NUM_PALLETS as in_NUM_PALLETS", \
	"JNR_WM_C_TMS_PLAN___in_PLT_WEIGHT as in_PLT_WEIGHT", \
	"JNR_WM_C_TMS_PLAN___in_WEIGHT as in_WEIGHT", \
	"JNR_WM_C_TMS_PLAN___in_VOLUME as in_VOLUME", \
	"JNR_WM_C_TMS_PLAN___WM_USER_ID as WM_USER_ID", \
	"JNR_WM_C_TMS_PLAN___WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"JNR_WM_C_TMS_PLAN___WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"JNR_WM_C_TMS_PLAN___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("WM_C_TMS_PLAN_ID is Null OR (  WM_C_TMS_PLAN_ID is NOT Null AND ( COALESCE(CREATE_DATE_TIME, date'1900-01-01') != COALESCE(WM_CREATE_TSTMP, date'1900-01-01') \
             OR COALESCE(MOD_DATE_TIME, date'1900-01-01') != COALESCE(WM_MOD_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 90

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_REC_temp = FIL_UNCHANGED_REC.toDF(*["FIL_UNCHANGED_REC___" + col for col in FIL_UNCHANGED_REC.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_REC_temp.selectExpr( \
	"FIL_UNCHANGED_REC___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_REC___C_TMS_PLAN_ID as C_TMS_PLAN_ID", \
	"FIL_UNCHANGED_REC___PLAN_ID as PLAN_ID", \
	"FIL_UNCHANGED_REC___STO_NBR as STO_NBR", \
	"FIL_UNCHANGED_REC___ROUTE_ID as ROUTE_ID", \
	"FIL_UNCHANGED_REC___STOP_ID as STOP_ID", \
	"FIL_UNCHANGED_REC___STOP_LOC as STOP_LOC", \
	"FIL_UNCHANGED_REC___ORIGIN as ORIGIN", \
	"FIL_UNCHANGED_REC___DESTINATION as DESTINATION", \
	"FIL_UNCHANGED_REC___CONSOLIDATOR as CONSOLIDATOR", \
	"FIL_UNCHANGED_REC___CONS_ADDR_1 as CONS_ADDR_1", \
	"FIL_UNCHANGED_REC___CONS_ADDR_2 as CONS_ADDR_2", \
	"FIL_UNCHANGED_REC___CONS_ADDR_3 as CONS_ADDR_3", \
	"FIL_UNCHANGED_REC___CITY as CITY", \
	"FIL_UNCHANGED_REC___STATE as STATE", \
	"FIL_UNCHANGED_REC___ZIP as ZIP", \
	"FIL_UNCHANGED_REC___CNTRY as CNTRY", \
	"FIL_UNCHANGED_REC___CONTACT_PERSON as CONTACT_PERSON", \
	"FIL_UNCHANGED_REC___PHONE as PHONE", \
	"FIL_UNCHANGED_REC___COMMODITY as COMMODITY", \
	"FIL_UNCHANGED_REC___NUM_PALLETS as NUM_PALLETS", \
	"FIL_UNCHANGED_REC___WEIGHT as WEIGHT", \
	"FIL_UNCHANGED_REC___VOLUME as VOLUME", \
	"FIL_UNCHANGED_REC___DROP_DEAD_DT as DROP_DEAD_DT", \
	"FIL_UNCHANGED_REC___ETA as ETA", \
	"FIL_UNCHANGED_REC___SPLIT_ROUTE as SPLIT_ROUTE", \
	"FIL_UNCHANGED_REC___TMS_CARRIER_ID as TMS_CARRIER_ID", \
	"FIL_UNCHANGED_REC___TMS_CARRIER_NAME as TMS_CARRIER_NAME", \
	"FIL_UNCHANGED_REC___LOAD_STAT as LOAD_STAT", \
	"FIL_UNCHANGED_REC___SHIP_VIA as SHIP_VIA", \
	"FIL_UNCHANGED_REC___MILES as MILES", \
	"FIL_UNCHANGED_REC___DRIVE_TIME as DRIVE_TIME", \
	"FIL_UNCHANGED_REC___EQUIP_TYPE as EQUIP_TYPE", \
	"FIL_UNCHANGED_REC___DEL_TYPE as DEL_TYPE", \
	"FIL_UNCHANGED_REC___TMS_TRAILER_ID as TMS_TRAILER_ID", \
	"FIL_UNCHANGED_REC___CARRIER_REF_NUM as CARRIER_REF_NUM", \
	"FIL_UNCHANGED_REC___TMS_PLAN_TIME as TMS_PLAN_TIME", \
	"FIL_UNCHANGED_REC___PLT_WEIGHT as PLT_WEIGHT", \
	"FIL_UNCHANGED_REC___ERROR_SEQ_NBR as ERROR_SEQ_NBR", \
	"FIL_UNCHANGED_REC___STAT_CODE as STAT_CODE", \
	"FIL_UNCHANGED_REC___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"FIL_UNCHANGED_REC___MOD_DATE_TIME as MOD_DATE_TIME", \
	"FIL_UNCHANGED_REC___USER_ID as USER_ID", \
	"FIL_UNCHANGED_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_REC___WM_C_TMS_PLAN_ID as WM_C_TMS_PLAN_ID", \
	"FIL_UNCHANGED_REC___WM_PLAN_ID as WM_PLAN_ID", \
	"FIL_UNCHANGED_REC___WM_STO_NBR as WM_STO_NBR", \
	"FIL_UNCHANGED_REC___WM_ROUTE_ID as WM_ROUTE_ID", \
	"FIL_UNCHANGED_REC___WM_STOP_ID as WM_STOP_ID", \
	"FIL_UNCHANGED_REC___WM_STOP_LOC as WM_STOP_LOC", \
	"FIL_UNCHANGED_REC___in_SPLIT_ROUTE as in_SPLIT_ROUTE", \
	"FIL_UNCHANGED_REC___WM_SHIP_VIA as WM_SHIP_VIA", \
	"FIL_UNCHANGED_REC___WM_EQUIP_TYPE as WM_EQUIP_TYPE", \
	"FIL_UNCHANGED_REC___WM_DEL_TYPE as WM_DEL_TYPE", \
	"FIL_UNCHANGED_REC___WM_TMS_CARRIER_ID as WM_TMS_CARRIER_ID", \
	"FIL_UNCHANGED_REC___WM_TMS_CARRIER_NAME as WM_TMS_CARRIER_NAME", \
	"FIL_UNCHANGED_REC___WM_CARRIER_REF_NBR as WM_CARRIER_REF_NBR", \
	"FIL_UNCHANGED_REC___WM_TMS_TRAILER_ID as WM_TMS_TRAILER_ID", \
	"FIL_UNCHANGED_REC___WM_LOAD_STAT_CD as WM_LOAD_STAT_CD", \
	"FIL_UNCHANGED_REC___WM_STAT_CD as WM_STAT_CD", \
	"FIL_UNCHANGED_REC___in_MILES as in_MILES", \
	"FIL_UNCHANGED_REC___in_DRIVE_TIME as in_DRIVE_TIME", \
	"FIL_UNCHANGED_REC___in_ERROR_SEQ_NBR as in_ERROR_SEQ_NBR", \
	"FIL_UNCHANGED_REC___WM_TMS_PLAN_TSTMP as WM_TMS_PLAN_TSTMP", \
	"FIL_UNCHANGED_REC___DROP_DEAD_TSTMP as DROP_DEAD_TSTMP", \
	"FIL_UNCHANGED_REC___ETA_TSTMP as ETA_TSTMP", \
	"FIL_UNCHANGED_REC___in_ORIGIN as in_ORIGIN", \
	"FIL_UNCHANGED_REC___DESTINATION1 as DESTINATION1", \
	"FIL_UNCHANGED_REC___in_CONSOLIDATOR as in_CONSOLIDATOR", \
	"FIL_UNCHANGED_REC___in_CONS_ADDR_1 as in_CONS_ADDR_1", \
	"FIL_UNCHANGED_REC___in_CONS_ADDR_2 as in_CONS_ADDR_2", \
	"FIL_UNCHANGED_REC___in_CONS_ADDR_3 as in_CONS_ADDR_3", \
	"FIL_UNCHANGED_REC___in_CITY as in_CITY", \
	"FIL_UNCHANGED_REC___in_STATE as in_STATE", \
	"FIL_UNCHANGED_REC___in_ZIP as in_ZIP", \
	"FIL_UNCHANGED_REC___COUNTRY_CD as COUNTRY_CD", \
	"FIL_UNCHANGED_REC___in_CONTACT_PERSON as in_CONTACT_PERSON", \
	"FIL_UNCHANGED_REC___in_PHONE as in_PHONE", \
	"FIL_UNCHANGED_REC___in_COMMODITY as in_COMMODITY", \
	"FIL_UNCHANGED_REC___in_NUM_PALLETS as in_NUM_PALLETS", \
	"FIL_UNCHANGED_REC___in_PLT_WEIGHT as in_PLT_WEIGHT", \
	"FIL_UNCHANGED_REC___in_WEIGHT as in_WEIGHT", \
	"FIL_UNCHANGED_REC___in_VOLUME as in_VOLUME", \
	"FIL_UNCHANGED_REC___WM_USER_ID as WM_USER_ID", \
	"FIL_UNCHANGED_REC___WM_CREATE_TSTMP as WM_CREATE_TSTMP", \
	"FIL_UNCHANGED_REC___WM_MOD_TSTMP as WM_MOD_TSTMP", \
	"FIL_UNCHANGED_REC___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"CURRENT_TIMESTAMP() as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP(), FIL_UNCHANGED_REC___in_LOAD_TSTMP) as LOAD_TSTMP_exp", \
	"IF(FIL_UNCHANGED_REC___WM_C_TMS_PLAN_ID IS NULL, 1, 2) as o_UPD_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_UPD_INS, type UPDATE_STRATEGY 
# COLUMN COUNT: 46

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_UPD_INS = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___C_TMS_PLAN_ID as C_TMS_PLAN_ID", \
	"EXP_UPD_VALIDATOR___PLAN_ID as PLAN_ID", \
	"EXP_UPD_VALIDATOR___STO_NBR as STO_NBR", \
	"EXP_UPD_VALIDATOR___ROUTE_ID as ROUTE_ID", \
	"EXP_UPD_VALIDATOR___STOP_ID as STOP_ID", \
	"EXP_UPD_VALIDATOR___STOP_LOC as STOP_LOC", \
	"EXP_UPD_VALIDATOR___SPLIT_ROUTE as SPLIT_ROUTE", \
	"EXP_UPD_VALIDATOR___SHIP_VIA as SHIP_VIA", \
	"EXP_UPD_VALIDATOR___EQUIP_TYPE as EQUIP_TYPE", \
	"EXP_UPD_VALIDATOR___DEL_TYPE as DEL_TYPE", \
	"EXP_UPD_VALIDATOR___TMS_CARRIER_ID as TMS_CARRIER_ID", \
	"EXP_UPD_VALIDATOR___TMS_CARRIER_NAME as TMS_CARRIER_NAME", \
	"EXP_UPD_VALIDATOR___CARRIER_REF_NUM as CARRIER_REF_NUM", \
	"EXP_UPD_VALIDATOR___TMS_TRAILER_ID as TMS_TRAILER_ID", \
	"EXP_UPD_VALIDATOR___STAT_CODE as STAT_CODE", \
	"EXP_UPD_VALIDATOR___LOAD_STAT as LOAD_STAT", \
	"EXP_UPD_VALIDATOR___MILES as MILES", \
	"EXP_UPD_VALIDATOR___DRIVE_TIME as DRIVE_TIME", \
	"EXP_UPD_VALIDATOR___ERROR_SEQ_NBR as ERROR_SEQ_NBR", \
	"EXP_UPD_VALIDATOR___TMS_PLAN_TIME as TMS_PLAN_TIME", \
	"EXP_UPD_VALIDATOR___DROP_DEAD_DT as DROP_DEAD_DT", \
	"EXP_UPD_VALIDATOR___ETA as ETA", \
	"EXP_UPD_VALIDATOR___ORIGIN as ORIGIN", \
	"EXP_UPD_VALIDATOR___DESTINATION as DESTINATION", \
	"EXP_UPD_VALIDATOR___CONSOLIDATOR as CONSOLIDATOR", \
	"EXP_UPD_VALIDATOR___CONS_ADDR_1 as CONS_ADDR_1", \
	"EXP_UPD_VALIDATOR___CONS_ADDR_2 as CONS_ADDR_2", \
	"EXP_UPD_VALIDATOR___CONS_ADDR_3 as CONS_ADDR_3", \
	"EXP_UPD_VALIDATOR___CITY as CITY", \
	"EXP_UPD_VALIDATOR___STATE as STATE", \
	"EXP_UPD_VALIDATOR___ZIP as ZIP", \
	"EXP_UPD_VALIDATOR___CNTRY as CNTRY", \
	"EXP_UPD_VALIDATOR___CONTACT_PERSON as CONTACT_PERSON", \
	"EXP_UPD_VALIDATOR___PHONE as PHONE", \
	"EXP_UPD_VALIDATOR___COMMODITY as COMMODITY", \
	"EXP_UPD_VALIDATOR___NUM_PALLETS as NUM_PALLETS", \
	"EXP_UPD_VALIDATOR___PLT_WEIGHT as PLT_WEIGHT", \
	"EXP_UPD_VALIDATOR___WEIGHT as WEIGHT", \
	"EXP_UPD_VALIDATOR___VOLUME as VOLUME", \
	"EXP_UPD_VALIDATOR___USER_ID as USER_ID", \
	"EXP_UPD_VALIDATOR___CREATE_DATE_TIME as CREATE_DATE_TIME", \
	"EXP_UPD_VALIDATOR___MOD_DATE_TIME as MOD_DATE_TIME", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP_exp as LOAD_TSTMP_exp", \
	"EXP_UPD_VALIDATOR___o_UPD_VALIDATOR as o_UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPD_VALIDATOR') ==(lit(1)), lit(0)).when(col('o_UPD_VALIDATOR') ==(lit(2)), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_C_TMS_PLAN1, type TARGET 
# COLUMN COUNT: 45


Shortcut_to_WM_C_TMS_PLAN1 = UPD_UPD_INS.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(C_TMS_PLAN_ID AS DECIMAL(9,0)) as WM_C_TMS_PLAN_ID",
	"CAST(PLAN_ID AS STRING) as WM_PLAN_ID",
	"CAST(STO_NBR AS STRING) as WM_STO_NBR",
	"CAST(ROUTE_ID AS STRING) as WM_ROUTE_ID",
	"CAST(STOP_ID AS DECIMAL(5,0)) as WM_STOP_ID",
	"CAST(STOP_LOC AS STRING) as WM_STOP_LOC",
	"CAST(SPLIT_ROUTE AS STRING) as SPLIT_ROUTE",
	"CAST(SHIP_VIA AS STRING) as WM_SHIP_VIA",
	"CAST(EQUIP_TYPE AS STRING) as WM_EQUIP_TYPE",
	"CAST(DEL_TYPE AS STRING) as WM_DEL_TYPE",
	"CAST(TMS_CARRIER_ID AS STRING) as WM_TMS_CARRIER_ID",
	"CAST(TMS_CARRIER_NAME AS STRING) as WM_TMS_CARRIER_NAME",
	"CAST(CARRIER_REF_NUM AS STRING) as WM_CARRIER_REF_NBR",
	"CAST(TMS_TRAILER_ID AS STRING) as WM_TMS_TRAILER_ID",
	"CAST(LOAD_STAT AS STRING) as WM_LOAD_STAT_CD",
	"CAST(STAT_CODE AS DECIMAL(2,0)) as WM_STAT_CD",
	"CAST(MILES AS DECIMAL(5,0)) as MILES",
	"CAST(DRIVE_TIME AS STRING) as DRIVE_TIME",
	"CAST(ERROR_SEQ_NBR AS DECIMAL(9,0)) as ERROR_SEQ_NBR",
	"CAST(TMS_PLAN_TIME AS TIMESTAMP) as WM_TMS_PLAN_TSTMP",
	"CAST(DROP_DEAD_DT AS TIMESTAMP) as DROP_DEAD_TSTMP",
	"CAST(ETA AS TIMESTAMP) as ETA_TSTMP",
	"CAST(ORIGIN AS STRING) as ORIGIN",
	"CAST(DESTINATION AS STRING) as DESTINATION",
	"CAST(CONSOLIDATOR AS STRING) as CONSOLIDATOR",
	"CAST(CONS_ADDR_1 AS STRING) as CONS_ADDR_1",
	"CAST(CONS_ADDR_2 AS STRING) as CONS_ADDR_2",
	"CAST(CONS_ADDR_3 AS STRING) as CONS_ADDR_3",
	"CAST(CITY AS STRING) as CITY",
	"CAST(STATE AS STRING) as STATE",
	"CAST(ZIP AS STRING) as ZIP",
	"CAST(CNTRY AS STRING) as COUNTRY_CD",
	"CAST(CONTACT_PERSON AS STRING) as CONTACT_PERSON",
	"CAST(PHONE AS STRING) as PHONE",
	"CAST(COMMODITY AS STRING) as COMMODITY",
	"CAST(NUM_PALLETS AS DECIMAL(5,0)) as NUM_PALLETS",
	"CAST(PLT_WEIGHT AS DECIMAL(13,4)) as PLT_WEIGHT",
	"CAST(WEIGHT AS DECIMAL(13,4)) as WEIGHT",
	"CAST(VOLUME AS DECIMAL(13,4)) as VOLUME",
	"CAST(USER_ID AS STRING) as WM_USER_ID",
	"CAST(CREATE_DATE_TIME AS TIMESTAMP) as WM_CREATE_TSTMP",
	"CAST(MOD_DATE_TIME AS TIMESTAMP) as WM_MOD_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP_exp AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_C_TMS_PLAN_ID = target.WM_C_TMS_PLAN_ID"""
#   refined_perf_table = "WM_C_TMS_PLAN"
  executeMerge(Shortcut_to_WM_C_TMS_PLAN1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_C_TMS_PLAN", "WM_C_TMS_PLAN", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_C_TMS_PLAN", "WM_C_TMS_PLAN","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	