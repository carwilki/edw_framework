#Code converted on 2023-06-26 17:03:53
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from pyspark.dbutils import DBUtils
from utils.genericUtilities import *
from utils.configs import *
from utils.mergeUtils import *
from utils.logger import *
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

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE, type SOURCE 
# COLUMN COUNT: 28

SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE = spark.sql(f"""SELECT
WM_PICKING_SHORT_ITEM_PRE.DC_NBR,
WM_PICKING_SHORT_ITEM_PRE.PICKING_SHORT_ITEM_ID,
WM_PICKING_SHORT_ITEM_PRE.ITEM_ID,
WM_PICKING_SHORT_ITEM_PRE.LOCN_ID,
WM_PICKING_SHORT_ITEM_PRE.LINE_ITEM_ID,
WM_PICKING_SHORT_ITEM_PRE.TC_ORDER_ID,
WM_PICKING_SHORT_ITEM_PRE.WAVE_NBR,
WM_PICKING_SHORT_ITEM_PRE.SHORT_QTY,
WM_PICKING_SHORT_ITEM_PRE.STAT_CODE,
WM_PICKING_SHORT_ITEM_PRE.TC_COMPANY_ID,
WM_PICKING_SHORT_ITEM_PRE.CREATED_DTTM,
WM_PICKING_SHORT_ITEM_PRE.CREATED_SOURCE,
WM_PICKING_SHORT_ITEM_PRE.LAST_UPDATED_DTTM,
WM_PICKING_SHORT_ITEM_PRE.LAST_UPDATED_SOURCE,
WM_PICKING_SHORT_ITEM_PRE.SHORT_TYPE,
WM_PICKING_SHORT_ITEM_PRE.TC_LPN_ID,
WM_PICKING_SHORT_ITEM_PRE.LPN_DETAIL_ID,
WM_PICKING_SHORT_ITEM_PRE.REQD_INVN_TYPE,
WM_PICKING_SHORT_ITEM_PRE.REQD_PROD_STAT,
WM_PICKING_SHORT_ITEM_PRE.REQD_BATCH_NBR,
WM_PICKING_SHORT_ITEM_PRE.REQD_SKU_ATTR_1,
WM_PICKING_SHORT_ITEM_PRE.REQD_SKU_ATTR_2,
WM_PICKING_SHORT_ITEM_PRE.REQD_SKU_ATTR_3,
WM_PICKING_SHORT_ITEM_PRE.REQD_SKU_ATTR_4,
WM_PICKING_SHORT_ITEM_PRE.REQD_SKU_ATTR_5,
WM_PICKING_SHORT_ITEM_PRE.REQD_CNTRY_OF_ORGN,
WM_PICKING_SHORT_ITEM_PRE.SHIPMENT_ID,
WM_PICKING_SHORT_ITEM_PRE.TC_SHIPMENT_ID
FROM WM_PICKING_SHORT_ITEM_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_DATA_TYPE_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE_temp = SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE.toDF(*["SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___" + col for col in SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE.columns])

EXP_DATA_TYPE_CONVERSION = SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___PICKING_SHORT_ITEM_ID as PICKING_SHORT_ITEM_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___ITEM_ID as ITEM_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___LOCN_ID as LOCN_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___LINE_ITEM_ID as LINE_ITEM_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___TC_ORDER_ID as TC_ORDER_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___WAVE_NBR as WAVE_NBR", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___SHORT_QTY as SHORT_QTY", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___STAT_CODE as STAT_CODE", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___CREATED_SOURCE as CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___SHORT_TYPE as SHORT_TYPE", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___TC_LPN_ID as TC_LPN_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___REQD_INVN_TYPE as REQD_INVN_TYPE", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___REQD_PROD_STAT as REQD_PROD_STAT", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___REQD_BATCH_NBR as REQD_BATCH_NBR", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___REQD_SKU_ATTR_1 as REQD_SKU_ATTR_1", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___REQD_SKU_ATTR_2 as REQD_SKU_ATTR_2", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___REQD_SKU_ATTR_3 as REQD_SKU_ATTR_3", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___REQD_SKU_ATTR_4 as REQD_SKU_ATTR_4", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___REQD_SKU_ATTR_5 as REQD_SKU_ATTR_5", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___REQD_CNTRY_OF_ORGN as REQD_CNTRY_OF_ORGN", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___SHIPMENT_ID as SHIPMENT_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_PRE___TC_SHIPMENT_ID as TC_SHIPMENT_ID" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_PICKING_SHORT_ITEM, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_PICKING_SHORT_ITEM = spark.sql(f"""SELECT
WM_PICKING_SHORT_ITEM.LOCATION_ID,
WM_PICKING_SHORT_ITEM.WM_PICKING_SHORT_ITEM_ID,
WM_PICKING_SHORT_ITEM.WM_CREATED_TSTMP,
WM_PICKING_SHORT_ITEM.WM_LAST_UPDATED_TSTMP,
WM_PICKING_SHORT_ITEM.LOAD_TSTMP
FROM WM_PICKING_SHORT_ITEM
WHERE WM_PICKING_SHORT_ITEM_ID in (SELECT PICKING_SHORT_ITEM_ID FROM WM_PICKING_SHORT_ITEM_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])
EXP_DATA_TYPE_CONVERSION_temp = EXP_DATA_TYPE_CONVERSION.toDF(*["EXP_DATA_TYPE_CONVERSION___" + col for col in EXP_DATA_TYPE_CONVERSION.columns])

JNR_SITE = SQ_Shortcut_to_SITE_PROFILE_temp.join(EXP_DATA_TYPE_CONVERSION_temp,[SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR == EXP_DATA_TYPE_CONVERSION_temp.EXP_DATA_TYPE_CONVERSION___o_DC_NBR],'inner').selectExpr( \
	"EXP_DATA_TYPE_CONVERSION___o_DC_NBR as o_DC_NBR", \
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR", \
	"EXP_DATA_TYPE_CONVERSION___o_DC_NBR as o_DC_NBR1", \
	"EXP_DATA_TYPE_CONVERSION___PICKING_SHORT_ITEM_ID as PICKING_SHORT_ITEM_ID", \
	"EXP_DATA_TYPE_CONVERSION___ITEM_ID as ITEM_ID", \
	"EXP_DATA_TYPE_CONVERSION___LOCN_ID as LOCN_ID", \
	"EXP_DATA_TYPE_CONVERSION___LINE_ITEM_ID as LINE_ITEM_ID", \
	"EXP_DATA_TYPE_CONVERSION___TC_ORDER_ID as TC_ORDER_ID", \
	"EXP_DATA_TYPE_CONVERSION___WAVE_NBR as WAVE_NBR", \
	"EXP_DATA_TYPE_CONVERSION___SHORT_QTY as SHORT_QTY", \
	"EXP_DATA_TYPE_CONVERSION___STAT_CODE as STAT_CODE", \
	"EXP_DATA_TYPE_CONVERSION___TC_COMPANY_ID as TC_COMPANY_ID", \
	"EXP_DATA_TYPE_CONVERSION___CREATED_DTTM as CREATED_DTTM", \
	"EXP_DATA_TYPE_CONVERSION___CREATED_SOURCE as CREATED_SOURCE", \
	"EXP_DATA_TYPE_CONVERSION___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_DATA_TYPE_CONVERSION___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"EXP_DATA_TYPE_CONVERSION___SHORT_TYPE as SHORT_TYPE", \
	"EXP_DATA_TYPE_CONVERSION___TC_LPN_ID as TC_LPN_ID", \
	"EXP_DATA_TYPE_CONVERSION___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"EXP_DATA_TYPE_CONVERSION___REQD_INVN_TYPE as REQD_INVN_TYPE", \
	"EXP_DATA_TYPE_CONVERSION___REQD_PROD_STAT as REQD_PROD_STAT", \
	"EXP_DATA_TYPE_CONVERSION___REQD_BATCH_NBR as REQD_BATCH_NBR", \
	"EXP_DATA_TYPE_CONVERSION___REQD_SKU_ATTR_1 as REQD_SKU_ATTR_1", \
	"EXP_DATA_TYPE_CONVERSION___REQD_SKU_ATTR_2 as REQD_SKU_ATTR_2", \
	"EXP_DATA_TYPE_CONVERSION___REQD_SKU_ATTR_3 as REQD_SKU_ATTR_3", \
	"EXP_DATA_TYPE_CONVERSION___REQD_SKU_ATTR_4 as REQD_SKU_ATTR_4", \
	"EXP_DATA_TYPE_CONVERSION___REQD_SKU_ATTR_5 as REQD_SKU_ATTR_5", \
	"EXP_DATA_TYPE_CONVERSION___REQD_CNTRY_OF_ORGN as REQD_CNTRY_OF_ORGN", \
	"EXP_DATA_TYPE_CONVERSION___SHIPMENT_ID as SHIPMENT_ID", \
	"EXP_DATA_TYPE_CONVERSION___TC_SHIPMENT_ID as TC_SHIPMENT_ID")

# COMMAND ----------
# Processing node JNR_WM_PICKING_SHORT_ITEM, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_temp = JNR_SITE.toDF(*["JNR_SITE___" + col for col in JNR_SITE.columns])
SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_temp = SQ_Shortcut_to_WM_PICKING_SHORT_ITEM.toDF(*["SQ_Shortcut_to_WM_PICKING_SHORT_ITEM___" + col for col in SQ_Shortcut_to_WM_PICKING_SHORT_ITEM.columns])

JNR_WM_PICKING_SHORT_ITEM = SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_temp.join(JNR_SITE_temp,[SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_temp.SQ_Shortcut_to_WM_PICKING_SHORT_ITEM___LOCATION_ID == JNR_SITE_temp.JNR_SITE___LOCATION_ID, SQ_Shortcut_to_WM_PICKING_SHORT_ITEM_temp.SQ_Shortcut_to_WM_PICKING_SHORT_ITEM___WM_PICKING_SHORT_ITEM_ID == JNR_SITE_temp.JNR_SITE___PICKING_SHORT_ITEM_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM___WM_PICKING_SHORT_ITEM_ID as i_WM_PICKING_SHORT_ITEM_ID", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_PICKING_SHORT_ITEM___LOAD_TSTMP as i_LOAD_TSTMP", \
	"JNR_SITE___LOCATION_ID as LOCATION_ID1", \
	"JNR_SITE___PICKING_SHORT_ITEM_ID as PICKING_SHORT_ITEM_ID", \
	"JNR_SITE___ITEM_ID as ITEM_ID", \
	"JNR_SITE___LOCN_ID as LOCN_ID", \
	"JNR_SITE___LINE_ITEM_ID as LINE_ITEM_ID", \
	"JNR_SITE___TC_ORDER_ID as TC_ORDER_ID", \
	"JNR_SITE___WAVE_NBR as WAVE_NBR", \
	"JNR_SITE___SHORT_QTY as SHORT_QTY", \
	"JNR_SITE___STAT_CODE as STAT_CODE", \
	"JNR_SITE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_SITE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_SITE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_SITE___SHORT_TYPE as SHORT_TYPE", \
	"JNR_SITE___TC_LPN_ID as TC_LPN_ID", \
	"JNR_SITE___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"JNR_SITE___REQD_INVN_TYPE as REQD_INVN_TYPE", \
	"JNR_SITE___REQD_PROD_STAT as REQD_PROD_STAT", \
	"JNR_SITE___REQD_BATCH_NBR as REQD_BATCH_NBR", \
	"JNR_SITE___REQD_SKU_ATTR_1 as REQD_SKU_ATTR_1", \
	"JNR_SITE___REQD_SKU_ATTR_2 as REQD_SKU_ATTR_2", \
	"JNR_SITE___REQD_SKU_ATTR_3 as REQD_SKU_ATTR_3", \
	"JNR_SITE___REQD_SKU_ATTR_4 as REQD_SKU_ATTR_4", \
	"JNR_SITE___REQD_SKU_ATTR_5 as REQD_SKU_ATTR_5", \
	"JNR_SITE___REQD_CNTRY_OF_ORGN as REQD_CNTRY_OF_ORGN", \
	"JNR_SITE___SHIPMENT_ID as SHIPMENT_ID", \
	"JNR_SITE___TC_SHIPMENT_ID as TC_SHIPMENT_ID")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_PICKING_SHORT_ITEM_temp = JNR_WM_PICKING_SHORT_ITEM.toDF(*["JNR_WM_PICKING_SHORT_ITEM___" + col for col in JNR_WM_PICKING_SHORT_ITEM.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_PICKING_SHORT_ITEM_temp.selectExpr( \
	"JNR_WM_PICKING_SHORT_ITEM___i_WM_PICKING_SHORT_ITEM_ID as i_WM_PICKING_SHORT_ITEM_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"JNR_WM_PICKING_SHORT_ITEM___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_PICKING_SHORT_ITEM___i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"JNR_WM_PICKING_SHORT_ITEM___LOCATION_ID1 as LOCATION_ID1", \
	"JNR_WM_PICKING_SHORT_ITEM___PICKING_SHORT_ITEM_ID as PICKING_SHORT_ITEM_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___ITEM_ID as ITEM_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___LOCN_ID as LOCN_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___LINE_ITEM_ID as LINE_ITEM_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___TC_ORDER_ID as TC_ORDER_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___WAVE_NBR as WAVE_NBR", \
	"JNR_WM_PICKING_SHORT_ITEM___SHORT_QTY as SHORT_QTY", \
	"JNR_WM_PICKING_SHORT_ITEM___STAT_CODE as STAT_CODE", \
	"JNR_WM_PICKING_SHORT_ITEM___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_PICKING_SHORT_ITEM___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_WM_PICKING_SHORT_ITEM___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_PICKING_SHORT_ITEM___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_WM_PICKING_SHORT_ITEM___SHORT_TYPE as SHORT_TYPE", \
	"JNR_WM_PICKING_SHORT_ITEM___TC_LPN_ID as TC_LPN_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___REQD_INVN_TYPE as REQD_INVN_TYPE", \
	"JNR_WM_PICKING_SHORT_ITEM___REQD_PROD_STAT as REQD_PROD_STAT", \
	"JNR_WM_PICKING_SHORT_ITEM___REQD_BATCH_NBR as REQD_BATCH_NBR", \
	"JNR_WM_PICKING_SHORT_ITEM___REQD_SKU_ATTR_1 as REQD_SKU_ATTR_1", \
	"JNR_WM_PICKING_SHORT_ITEM___REQD_SKU_ATTR_2 as REQD_SKU_ATTR_2", \
	"JNR_WM_PICKING_SHORT_ITEM___REQD_SKU_ATTR_3 as REQD_SKU_ATTR_3", \
	"JNR_WM_PICKING_SHORT_ITEM___REQD_SKU_ATTR_4 as REQD_SKU_ATTR_4", \
	"JNR_WM_PICKING_SHORT_ITEM___REQD_SKU_ATTR_5 as REQD_SKU_ATTR_5", \
	"JNR_WM_PICKING_SHORT_ITEM___REQD_CNTRY_OF_ORGN as REQD_CNTRY_OF_ORGN", \
	"JNR_WM_PICKING_SHORT_ITEM___SHIPMENT_ID as SHIPMENT_ID", \
	"JNR_WM_PICKING_SHORT_ITEM___TC_SHIPMENT_ID as TC_SHIPMENT_ID")\
    .filter("i_WM_PICKING_SHORT_ITEM_ID is Null OR (  i_WM_PICKING_SHORT_ITEM_ID is NOT Null AND ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01') \
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___i_WM_PICKING_SHORT_ITEM_ID as i_WM_PICKING_SHORT_ITEM_ID", \
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID1 as LOCATION_ID1", \
	"FIL_UNCHANGED_RECORDS___PICKING_SHORT_ITEM_ID as PICKING_SHORT_ITEM_ID", \
	"FIL_UNCHANGED_RECORDS___ITEM_ID as ITEM_ID", \
	"FIL_UNCHANGED_RECORDS___LOCN_ID as LOCN_ID", \
	"FIL_UNCHANGED_RECORDS___LINE_ITEM_ID as LINE_ITEM_ID", \
	"FIL_UNCHANGED_RECORDS___TC_ORDER_ID as TC_ORDER_ID", \
	"FIL_UNCHANGED_RECORDS___WAVE_NBR as WAVE_NBR", \
	"FIL_UNCHANGED_RECORDS___SHORT_QTY as SHORT_QTY", \
	"FIL_UNCHANGED_RECORDS___STAT_CODE as STAT_CODE", \
	"FIL_UNCHANGED_RECORDS___TC_COMPANY_ID as TC_COMPANY_ID", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___SHORT_TYPE as SHORT_TYPE", \
	"FIL_UNCHANGED_RECORDS___TC_LPN_ID as TC_LPN_ID", \
	"FIL_UNCHANGED_RECORDS___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"FIL_UNCHANGED_RECORDS___REQD_INVN_TYPE as REQD_INVN_TYPE", \
	"FIL_UNCHANGED_RECORDS___REQD_PROD_STAT as REQD_PROD_STAT", \
	"FIL_UNCHANGED_RECORDS___REQD_BATCH_NBR as REQD_BATCH_NBR", \
	"FIL_UNCHANGED_RECORDS___REQD_SKU_ATTR_1 as REQD_SKU_ATTR_1", \
	"FIL_UNCHANGED_RECORDS___REQD_SKU_ATTR_2 as REQD_SKU_ATTR_2", \
	"FIL_UNCHANGED_RECORDS___REQD_SKU_ATTR_3 as REQD_SKU_ATTR_3", \
	"FIL_UNCHANGED_RECORDS___REQD_SKU_ATTR_4 as REQD_SKU_ATTR_4", \
	"FIL_UNCHANGED_RECORDS___REQD_SKU_ATTR_5 as REQD_SKU_ATTR_5", \
	"FIL_UNCHANGED_RECORDS___REQD_CNTRY_OF_ORGN as REQD_CNTRY_OF_ORGN", \
	"FIL_UNCHANGED_RECORDS___SHIPMENT_ID as SHIPMENT_ID", \
	"FIL_UNCHANGED_RECORDS___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (FIL_UNCHANGED_RECORDS___i_WM_PICKING_SHORT_ITEM_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INSERT_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
EXP_VALIDATOR_temp = EXP_VALIDATOR.toDF(*["EXP_VALIDATOR___" + col for col in EXP_VALIDATOR.columns])

UPD_INSERT_UPDATE = EXP_VALIDATOR_temp.selectExpr( \
	"EXP_VALIDATOR___LOCATION_ID1 as LOCATION_ID1", \
	"EXP_VALIDATOR___PICKING_SHORT_ITEM_ID as PICKING_SHORT_ITEM_ID", \
	"EXP_VALIDATOR___ITEM_ID as ITEM_ID", \
	"EXP_VALIDATOR___LOCN_ID as LOCN_ID", \
	"EXP_VALIDATOR___LINE_ITEM_ID as LINE_ITEM_ID", \
	"EXP_VALIDATOR___TC_ORDER_ID as TC_ORDER_ID", \
	"EXP_VALIDATOR___WAVE_NBR as WAVE_NBR", \
	"EXP_VALIDATOR___SHORT_QTY as SHORT_QTY", \
	"EXP_VALIDATOR___STAT_CODE as STAT_CODE", \
	"EXP_VALIDATOR___TC_COMPANY_ID as TC_COMPANY_ID", \
	"EXP_VALIDATOR___SHORT_TYPE as SHORT_TYPE", \
	"EXP_VALIDATOR___TC_LPN_ID as TC_LPN_ID", \
	"EXP_VALIDATOR___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"EXP_VALIDATOR___REQD_INVN_TYPE as REQD_INVN_TYPE", \
	"EXP_VALIDATOR___REQD_PROD_STAT as REQD_PROD_STAT", \
	"EXP_VALIDATOR___REQD_BATCH_NBR as REQD_BATCH_NBR", \
	"EXP_VALIDATOR___REQD_SKU_ATTR_1 as REQD_SKU_ATTR_1", \
	"EXP_VALIDATOR___REQD_SKU_ATTR_2 as REQD_SKU_ATTR_2", \
	"EXP_VALIDATOR___REQD_SKU_ATTR_3 as REQD_SKU_ATTR_3", \
	"EXP_VALIDATOR___REQD_SKU_ATTR_4 as REQD_SKU_ATTR_4", \
	"EXP_VALIDATOR___REQD_SKU_ATTR_5 as REQD_SKU_ATTR_5", \
	"EXP_VALIDATOR___REQD_CNTRY_OF_ORGN as REQD_CNTRY_OF_ORGN", \
	"EXP_VALIDATOR___SHIPMENT_ID as SHIPMENT_ID", \
	"EXP_VALIDATOR___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
	"EXP_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR", \
	"EXP_VALIDATOR___CREATED_DTTM as CREATED_DTTM", \
	"EXP_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", \
	"EXP_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE") \
	.withColumn('pyspark_data_action', when(EXP_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)) , lit(0)) .when(EXP_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_PICKING_SHORT_ITEM1, type TARGET 
# COLUMN COUNT: 30

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_PICKING_SHORT_ITEM_ID = target.WM_PICKING_SHORT_ITEM_ID"""
  refined_perf_table = "WM_PICKING_SHORT_ITEM"
  executeMerge(UPD_INSERT_UPDATE, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_PICKING_SHORT_ITEM", "WM_PICKING_SHORT_ITEM", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_PICKING_SHORT_ITEM", "WM_PICKING_SHORT_ITEM","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	