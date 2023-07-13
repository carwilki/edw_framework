#Code converted on 2023-06-26 17:05:34
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
refined_perf_table = f"{refine}.WM_OUTPT_LPN_DETAIL"
raw_perf_table = f"{raw}.WM_OUTPT_LPN_DETAIL_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE, type SOURCE 
# COLUMN COUNT: 52

SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE = spark.sql(f"""SELECT
DC_NBR,
OUTPT_LPN_DETAIL_ID,
ASSORT_NBR,
BUSINESS_PARTNER,
CONSUMPTION_PRIORITY_DTTM,
CREATED_DTTM,
CREATED_SOURCE,
CREATED_SOURCE_TYPE,
GTIN,
INVC_BATCH_NBR,
INVENTORY_TYPE,
ITEM_ATTR_1,
ITEM_ATTR_2,
ITEM_ATTR_3,
ITEM_ATTR_4,
ITEM_ATTR_5,
ITEM_ID,
LAST_UPDATED_DTTM,
LAST_UPDATED_SOURCE,
LAST_UPDATED_SOURCE_TYPE,
LPN_DETAIL_ID,
MANUFACTURED_DTTM,
BATCH_NBR,
MANUFACTURED_PLANT,
CNTRY_OF_ORGN,
PRODUCT_STATUS,
PROC_DTTM,
PROC_STAT_CODE,
QTY_UOM,
REC_PROC_INDIC,
SIZE_VALUE,
TC_COMPANY_ID,
TC_LPN_ID,
VERSION_NBR,
DISTRIBUTION_ORDER_DTL_ID,
ITEM_COLOR,
ITEM_COLOR_SFX,
ITEM_SEASON,
ITEM_SEASON_YEAR,
ITEM_SECOND_DIM,
ITEM_SIZE_DESC,
ITEM_QUALITY,
ITEM_STYLE,
ITEM_STYLE_SFX,
SIZE_RANGE_CODE,
SIZE_REL_POSN_IN_TABLE,
VENDOR_ITEM_NBR,
MINOR_ORDER_NBR,
MINOR_PO_NBR,
ITEM_NAME,
TC_ORDER_LINE_ID,
DISTRO_NUMBER
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL = spark.sql(f"""SELECT
LOCATION_ID,
WM_OUTPT_LPN_DETAIL_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_OUTPT_LPN_DETAIL_ID IN (SELECT OUTPT_LPN_DETAIL_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 53

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE_temp = SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE.toDF(*["SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___" + col for col in SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___sys_row_id as sys_row_id", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___DC_NBR as DC_NBR", \
	"cast(SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___OUTPT_LPN_DETAIL_ID as OUTPT_LPN_DETAIL_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ASSORT_NBR as ASSORT_NBR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___BUSINESS_PARTNER as BUSINESS_PARTNER", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___CONSUMPTION_PRIORITY_DTTM as CONSUMPTION_PRIORITY_DTTM", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___CREATED_SOURCE as CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___GTIN as GTIN", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___INVC_BATCH_NBR as INVC_BATCH_NBR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___INVENTORY_TYPE as INVENTORY_TYPE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_ID as ITEM_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___MANUFACTURED_DTTM as MANUFACTURED_DTTM", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___BATCH_NBR as BATCH_NBR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___MANUFACTURED_PLANT as MANUFACTURED_PLANT", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___PRODUCT_STATUS as PRODUCT_STATUS", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___PROC_DTTM as PROC_DTTM", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___PROC_STAT_CODE as PROC_STAT_CODE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___QTY_UOM as QTY_UOM", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___REC_PROC_INDIC as REC_PROC_INDIC", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___SIZE_VALUE as SIZE_VALUE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___TC_LPN_ID as TC_LPN_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___VERSION_NBR as VERSION_NBR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___DISTRIBUTION_ORDER_DTL_ID as DISTRIBUTION_ORDER_DTL_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_COLOR as ITEM_COLOR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_COLOR_SFX as ITEM_COLOR_SFX", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_SEASON as ITEM_SEASON", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_SEASON_YEAR as ITEM_SEASON_YEAR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_SECOND_DIM as ITEM_SECOND_DIM", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_SIZE_DESC as ITEM_SIZE_DESC", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_QUALITY as ITEM_QUALITY", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_STYLE as ITEM_STYLE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_STYLE_SFX as ITEM_STYLE_SFX", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___VENDOR_ITEM_NBR as VENDOR_ITEM_NBR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___MINOR_ORDER_NBR as MINOR_ORDER_NBR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___MINOR_PO_NBR as MINOR_PO_NBR", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___ITEM_NAME as ITEM_NAME", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___TC_ORDER_LINE_ID as TC_ORDER_LINE_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_PRE___DISTRO_NUMBER as DISTRO_NUMBER" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 54

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_OUTPT_LPN_DETAIL, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 57

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_temp = SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL.toDF(*["SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL___" + col for col in SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL.columns])

JNR_WM_OUTPT_LPN_DETAIL = SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_temp.SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL_temp.SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL___WM_OUTPT_LPN_DETAIL_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___OUTPT_LPN_DETAIL_ID],'fullouter').selectExpr( \
	"JNR_SITE_PROFILE___OUTPT_LPN_DETAIL_ID as OUTPT_LPN_DETAIL_ID", \
	"JNR_SITE_PROFILE___ASSORT_NBR as ASSORT_NBR", \
	"JNR_SITE_PROFILE___BUSINESS_PARTNER as BUSINESS_PARTNER", \
	"JNR_SITE_PROFILE___CONSUMPTION_PRIORITY_DTTM as CONSUMPTION_PRIORITY_DTTM", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___GTIN as GTIN", \
	"JNR_SITE_PROFILE___INVC_BATCH_NBR as INVC_BATCH_NBR", \
	"JNR_SITE_PROFILE___INVENTORY_TYPE as INVENTORY_TYPE", \
	"JNR_SITE_PROFILE___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"JNR_SITE_PROFILE___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"JNR_SITE_PROFILE___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"JNR_SITE_PROFILE___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"JNR_SITE_PROFILE___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"JNR_SITE_PROFILE___ITEM_ID as ITEM_ID", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"JNR_SITE_PROFILE___MANUFACTURED_DTTM as MANUFACTURED_DTTM", \
	"JNR_SITE_PROFILE___BATCH_NBR as BATCH_NBR", \
	"JNR_SITE_PROFILE___MANUFACTURED_PLANT as MANUFACTURED_PLANT", \
	"JNR_SITE_PROFILE___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"JNR_SITE_PROFILE___PRODUCT_STATUS as PRODUCT_STATUS", \
	"JNR_SITE_PROFILE___PROC_DTTM as PROC_DTTM", \
	"JNR_SITE_PROFILE___PROC_STAT_CODE as PROC_STAT_CODE", \
	"JNR_SITE_PROFILE___QTY_UOM as QTY_UOM", \
	"JNR_SITE_PROFILE___REC_PROC_INDIC as REC_PROC_INDIC", \
	"JNR_SITE_PROFILE___SIZE_VALUE as SIZE_VALUE", \
	"JNR_SITE_PROFILE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_SITE_PROFILE___TC_LPN_ID as TC_LPN_ID", \
	"JNR_SITE_PROFILE___VERSION_NBR as VERSION_NBR", \
	"JNR_SITE_PROFILE___DISTRIBUTION_ORDER_DTL_ID as DISTRIBUTION_ORDER_DTL_ID", \
	"JNR_SITE_PROFILE___ITEM_COLOR as ITEM_COLOR", \
	"JNR_SITE_PROFILE___ITEM_COLOR_SFX as ITEM_COLOR_SFX", \
	"JNR_SITE_PROFILE___ITEM_SEASON as ITEM_SEASON", \
	"JNR_SITE_PROFILE___ITEM_SEASON_YEAR as ITEM_SEASON_YEAR", \
	"JNR_SITE_PROFILE___ITEM_SECOND_DIM as ITEM_SECOND_DIM", \
	"JNR_SITE_PROFILE___ITEM_SIZE_DESC as ITEM_SIZE_DESC", \
	"JNR_SITE_PROFILE___ITEM_QUALITY as ITEM_QUALITY", \
	"JNR_SITE_PROFILE___ITEM_STYLE as ITEM_STYLE", \
	"JNR_SITE_PROFILE___ITEM_STYLE_SFX as ITEM_STYLE_SFX", \
	"JNR_SITE_PROFILE___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"JNR_SITE_PROFILE___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"JNR_SITE_PROFILE___VENDOR_ITEM_NBR as VENDOR_ITEM_NBR", \
	"JNR_SITE_PROFILE___MINOR_ORDER_NBR as MINOR_ORDER_NBR", \
	"JNR_SITE_PROFILE___MINOR_PO_NBR as MINOR_PO_NBR", \
	"JNR_SITE_PROFILE___ITEM_NAME as ITEM_NAME", \
	"JNR_SITE_PROFILE___TC_ORDER_LINE_ID as TC_ORDER_LINE_ID", \
	"JNR_SITE_PROFILE___DISTRO_NUMBER as DISTRO_NUMBER", \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL___LOCATION_ID as i_LOCATION_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL___WM_OUTPT_LPN_DETAIL_ID as i_WM_OUTPT_LPN_DETAIL_ID", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_OUTPT_LPN_DETAIL___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 56

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_OUTPT_LPN_DETAIL_temp = JNR_WM_OUTPT_LPN_DETAIL.toDF(*["JNR_WM_OUTPT_LPN_DETAIL___" + col for col in JNR_WM_OUTPT_LPN_DETAIL.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_OUTPT_LPN_DETAIL_temp.selectExpr( \
	"JNR_WM_OUTPT_LPN_DETAIL___OUTPT_LPN_DETAIL_ID as OUTPT_LPN_DETAIL_ID", \
	"JNR_WM_OUTPT_LPN_DETAIL___ASSORT_NBR as ASSORT_NBR", \
	"JNR_WM_OUTPT_LPN_DETAIL___BUSINESS_PARTNER as BUSINESS_PARTNER", \
	"JNR_WM_OUTPT_LPN_DETAIL___CONSUMPTION_PRIORITY_DTTM as CONSUMPTION_PRIORITY_DTTM", \
	"JNR_WM_OUTPT_LPN_DETAIL___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_OUTPT_LPN_DETAIL___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_WM_OUTPT_LPN_DETAIL___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_WM_OUTPT_LPN_DETAIL___GTIN as GTIN", \
	"JNR_WM_OUTPT_LPN_DETAIL___INVC_BATCH_NBR as INVC_BATCH_NBR", \
	"JNR_WM_OUTPT_LPN_DETAIL___INVENTORY_TYPE as INVENTORY_TYPE", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_ID as ITEM_ID", \
	"JNR_WM_OUTPT_LPN_DETAIL___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_OUTPT_LPN_DETAIL___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_WM_OUTPT_LPN_DETAIL___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_OUTPT_LPN_DETAIL___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"JNR_WM_OUTPT_LPN_DETAIL___MANUFACTURED_DTTM as MANUFACTURED_DTTM", \
	"JNR_WM_OUTPT_LPN_DETAIL___BATCH_NBR as BATCH_NBR", \
	"JNR_WM_OUTPT_LPN_DETAIL___MANUFACTURED_PLANT as MANUFACTURED_PLANT", \
	"JNR_WM_OUTPT_LPN_DETAIL___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"JNR_WM_OUTPT_LPN_DETAIL___PRODUCT_STATUS as PRODUCT_STATUS", \
	"JNR_WM_OUTPT_LPN_DETAIL___PROC_DTTM as PROC_DTTM", \
	"JNR_WM_OUTPT_LPN_DETAIL___PROC_STAT_CODE as PROC_STAT_CODE", \
	"JNR_WM_OUTPT_LPN_DETAIL___QTY_UOM as QTY_UOM", \
	"JNR_WM_OUTPT_LPN_DETAIL___REC_PROC_INDIC as REC_PROC_INDIC", \
	"JNR_WM_OUTPT_LPN_DETAIL___SIZE_VALUE as SIZE_VALUE", \
	"JNR_WM_OUTPT_LPN_DETAIL___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_WM_OUTPT_LPN_DETAIL___TC_LPN_ID as TC_LPN_ID", \
	"JNR_WM_OUTPT_LPN_DETAIL___VERSION_NBR as VERSION_NBR", \
	"JNR_WM_OUTPT_LPN_DETAIL___DISTRIBUTION_ORDER_DTL_ID as DISTRIBUTION_ORDER_DTL_ID", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_COLOR as ITEM_COLOR", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_COLOR_SFX as ITEM_COLOR_SFX", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_SEASON as ITEM_SEASON", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_SEASON_YEAR as ITEM_SEASON_YEAR", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_SECOND_DIM as ITEM_SECOND_DIM", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_SIZE_DESC as ITEM_SIZE_DESC", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_QUALITY as ITEM_QUALITY", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_STYLE as ITEM_STYLE", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_STYLE_SFX as ITEM_STYLE_SFX", \
	"JNR_WM_OUTPT_LPN_DETAIL___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"JNR_WM_OUTPT_LPN_DETAIL___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"JNR_WM_OUTPT_LPN_DETAIL___VENDOR_ITEM_NBR as VENDOR_ITEM_NBR", \
	"JNR_WM_OUTPT_LPN_DETAIL___MINOR_ORDER_NBR as MINOR_ORDER_NBR", \
	"JNR_WM_OUTPT_LPN_DETAIL___MINOR_PO_NBR as MINOR_PO_NBR", \
	"JNR_WM_OUTPT_LPN_DETAIL___ITEM_NAME as ITEM_NAME", \
	"JNR_WM_OUTPT_LPN_DETAIL___TC_ORDER_LINE_ID as TC_ORDER_LINE_ID", \
	"JNR_WM_OUTPT_LPN_DETAIL___DISTRO_NUMBER as DISTRO_NUMBER", \
	"JNR_WM_OUTPT_LPN_DETAIL___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_OUTPT_LPN_DETAIL___i_WM_OUTPT_LPN_DETAIL_ID as i_WM_OUTPT_LPN_DETAIL_ID", \
	"JNR_WM_OUTPT_LPN_DETAIL___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", \
	"JNR_WM_OUTPT_LPN_DETAIL___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_OUTPT_LPN_DETAIL___i_LOAD_TSTMP as i_LOAD_TSTMP") \
    .filter("i_WM_OUTPT_LPN_DETAIL_ID is Null OR (i_WM_OUTPT_LPN_DETAIL_ID is not Null AND ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01') \
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01') ) )").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 55

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___OUTPT_LPN_DETAIL_ID as OUTPT_LPN_DETAIL_ID", \
	"FIL_UNCHANGED_RECORDS___ASSORT_NBR as ASSORT_NBR", \
	"FIL_UNCHANGED_RECORDS___BUSINESS_PARTNER as BUSINESS_PARTNER", \
	"FIL_UNCHANGED_RECORDS___CONSUMPTION_PRIORITY_DTTM as CONSUMPTION_PRIORITY_DTTM", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___GTIN as GTIN", \
	"FIL_UNCHANGED_RECORDS___INVC_BATCH_NBR as INVC_BATCH_NBR", \
	"FIL_UNCHANGED_RECORDS___INVENTORY_TYPE as INVENTORY_TYPE", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"FIL_UNCHANGED_RECORDS___ITEM_ID as ITEM_ID", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"FIL_UNCHANGED_RECORDS___MANUFACTURED_DTTM as MANUFACTURED_DTTM", \
	"FIL_UNCHANGED_RECORDS___BATCH_NBR as BATCH_NBR", \
	"FIL_UNCHANGED_RECORDS___MANUFACTURED_PLANT as MANUFACTURED_PLANT", \
	"FIL_UNCHANGED_RECORDS___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"FIL_UNCHANGED_RECORDS___PRODUCT_STATUS as PRODUCT_STATUS", \
	"FIL_UNCHANGED_RECORDS___PROC_DTTM as PROC_DTTM", \
	"FIL_UNCHANGED_RECORDS___PROC_STAT_CODE as PROC_STAT_CODE", \
	"FIL_UNCHANGED_RECORDS___QTY_UOM as QTY_UOM", \
	"decode ( ltrim ( rtrim ( upper ( FIL_UNCHANGED_RECORDS___REC_PROC_INDIC ) ) )'1','1''Y','1','0' ) as REC_PROC_INDIC_EXP", \
	"FIL_UNCHANGED_RECORDS___SIZE_VALUE as SIZE_VALUE", \
	"FIL_UNCHANGED_RECORDS___TC_COMPANY_ID as TC_COMPANY_ID", \
	"FIL_UNCHANGED_RECORDS___TC_LPN_ID as TC_LPN_ID", \
	"FIL_UNCHANGED_RECORDS___VERSION_NBR as VERSION_NBR", \
	"FIL_UNCHANGED_RECORDS___DISTRIBUTION_ORDER_DTL_ID as DISTRIBUTION_ORDER_DTL_ID", \
	"FIL_UNCHANGED_RECORDS___ITEM_COLOR as ITEM_COLOR", \
	"FIL_UNCHANGED_RECORDS___ITEM_COLOR_SFX as ITEM_COLOR_SFX", \
	"FIL_UNCHANGED_RECORDS___ITEM_SEASON as ITEM_SEASON", \
	"FIL_UNCHANGED_RECORDS___ITEM_SEASON_YEAR as ITEM_SEASON_YEAR", \
	"FIL_UNCHANGED_RECORDS___ITEM_SECOND_DIM as ITEM_SECOND_DIM", \
	"FIL_UNCHANGED_RECORDS___ITEM_SIZE_DESC as ITEM_SIZE_DESC", \
	"FIL_UNCHANGED_RECORDS___ITEM_QUALITY as ITEM_QUALITY", \
	"FIL_UNCHANGED_RECORDS___ITEM_STYLE as ITEM_STYLE", \
	"FIL_UNCHANGED_RECORDS___ITEM_STYLE_SFX as ITEM_STYLE_SFX", \
	"FIL_UNCHANGED_RECORDS___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"FIL_UNCHANGED_RECORDS___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"FIL_UNCHANGED_RECORDS___VENDOR_ITEM_NBR as VENDOR_ITEM_NBR", \
	"FIL_UNCHANGED_RECORDS___MINOR_ORDER_NBR as MINOR_ORDER_NBR", \
	"FIL_UNCHANGED_RECORDS___MINOR_PO_NBR as MINOR_PO_NBR", \
	"FIL_UNCHANGED_RECORDS___ITEM_NAME as ITEM_NAME", \
	"FIL_UNCHANGED_RECORDS___TC_ORDER_LINE_ID as TC_ORDER_LINE_ID", \
	"FIL_UNCHANGED_RECORDS___DISTRO_NUMBER as DISTRO_NUMBER", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF(FIL_UNCHANGED_RECORDS___i_WM_OUTPT_LPN_DETAIL_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 55

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___OUTPT_LPN_DETAIL_ID as OUTPT_LPN_DETAIL_ID", \
	"EXP_UPD_VALIDATOR___ASSORT_NBR as ASSORT_NBR", \
	"EXP_UPD_VALIDATOR___BUSINESS_PARTNER as BUSINESS_PARTNER", \
	"EXP_UPD_VALIDATOR___CONSUMPTION_PRIORITY_DTTM as CONSUMPTION_PRIORITY_DTTM", \
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___GTIN as GTIN", \
	"EXP_UPD_VALIDATOR___INVC_BATCH_NBR as INVC_BATCH_NBR", \
	"EXP_UPD_VALIDATOR___INVENTORY_TYPE as INVENTORY_TYPE", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"EXP_UPD_VALIDATOR___ITEM_ID as ITEM_ID", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___LPN_DETAIL_ID as LPN_DETAIL_ID", \
	"EXP_UPD_VALIDATOR___MANUFACTURED_DTTM as MANUFACTURED_DTTM", \
	"EXP_UPD_VALIDATOR___BATCH_NBR as BATCH_NBR", \
	"EXP_UPD_VALIDATOR___MANUFACTURED_PLANT as MANUFACTURED_PLANT", \
	"EXP_UPD_VALIDATOR___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"EXP_UPD_VALIDATOR___PRODUCT_STATUS as PRODUCT_STATUS", \
	"EXP_UPD_VALIDATOR___PROC_DTTM as PROC_DTTM", \
	"EXP_UPD_VALIDATOR___PROC_STAT_CODE as PROC_STAT_CODE", \
	"EXP_UPD_VALIDATOR___QTY_UOM as QTY_UOM", \
	"EXP_UPD_VALIDATOR___REC_PROC_INDIC_EXP as REC_PROC_INDIC", \
	"EXP_UPD_VALIDATOR___SIZE_VALUE as SIZE_VALUE", \
	"EXP_UPD_VALIDATOR___TC_COMPANY_ID as TC_COMPANY_ID", \
	"EXP_UPD_VALIDATOR___TC_LPN_ID as TC_LPN_ID", \
	"EXP_UPD_VALIDATOR___VERSION_NBR as VERSION_NBR", \
	"EXP_UPD_VALIDATOR___DISTRIBUTION_ORDER_DTL_ID as DISTRIBUTION_ORDER_DTL_ID", \
	"EXP_UPD_VALIDATOR___ITEM_COLOR as ITEM_COLOR", \
	"EXP_UPD_VALIDATOR___ITEM_COLOR_SFX as ITEM_COLOR_SFX", \
	"EXP_UPD_VALIDATOR___ITEM_SEASON as ITEM_SEASON", \
	"EXP_UPD_VALIDATOR___ITEM_SEASON_YEAR as ITEM_SEASON_YEAR", \
	"EXP_UPD_VALIDATOR___ITEM_SECOND_DIM as ITEM_SECOND_DIM", \
	"EXP_UPD_VALIDATOR___ITEM_SIZE_DESC as ITEM_SIZE_DESC", \
	"EXP_UPD_VALIDATOR___ITEM_QUALITY as ITEM_QUALITY", \
	"EXP_UPD_VALIDATOR___ITEM_STYLE as ITEM_STYLE", \
	"EXP_UPD_VALIDATOR___ITEM_STYLE_SFX as ITEM_STYLE_SFX", \
	"EXP_UPD_VALIDATOR___SIZE_RANGE_CODE as SIZE_RANGE_CODE", \
	"EXP_UPD_VALIDATOR___SIZE_REL_POSN_IN_TABLE as SIZE_REL_POSN_IN_TABLE", \
	"EXP_UPD_VALIDATOR___VENDOR_ITEM_NBR as VENDOR_ITEM_NBR", \
	"EXP_UPD_VALIDATOR___MINOR_ORDER_NBR as MINOR_ORDER_NBR", \
	"EXP_UPD_VALIDATOR___MINOR_PO_NBR as MINOR_PO_NBR", \
	"EXP_UPD_VALIDATOR___ITEM_NAME as ITEM_NAME", \
	"EXP_UPD_VALIDATOR___TC_ORDER_LINE_ID as TC_ORDER_LINE_ID", \
	"EXP_UPD_VALIDATOR___DISTRO_NUMBER as DISTRO_NUMBER", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1))lit(0)).when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2))lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_OUTPT_LPN_DETAIL1, type TARGET 
# COLUMN COUNT: 54


Shortcut_to_WM_OUTPT_LPN_DETAIL1 = UPD_INS_UPD.selectExpr( \
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", \
	"CAST(OUTPT_LPN_DETAIL_ID AS BIGINT) as WM_OUTPT_LPN_DETAIL_ID", \
	"CAST(LPN_DETAIL_ID AS BIGINT) as WM_LPN_DETAIL_ID", \
	"CAST(TC_LPN_ID AS STRING) as WM_TC_LPN_ID", \
	"CAST(TC_COMPANY_ID AS BIGINT) as WM_TC_COMPANY_ID", \
	"CAST(INVC_BATCH_NBR AS BIGINT) as WM_INVC_BATCH_NBR", \
	"CAST(ASSORT_NBR AS STRING) as WM_ASSORT_NBR", \
	"CAST(BUSINESS_PARTNER AS STRING) as WM_BUSINESS_PARTNER", \
	"CAST(MINOR_ORDER_NBR AS STRING) as WM_MINOR_ORDER_NBR", \
	"CAST(MINOR_PO_NBR AS STRING) as WM_MINOR_PO_NBR", \
	"CAST(TC_ORDER_LINE_ID AS STRING) as WM_TC_ORDER_LINE_ID", \
	"CAST(DISTRIBUTION_ORDER_DTL_ID AS BIGINT) as WM_DISTRIBUTION_ORDER_DTL_ID", \
	"CAST(INVENTORY_TYPE AS STRING) as WM_INVENTORY_TYPE", \
	"CAST(CONSUMPTION_PRIORITY_DTTM AS TIMESTAMP) as WM_CONSUMPTION_PRIORITY_TSTMP", \
	"CAST(GTIN AS STRING) as WM_GTIN", \
	"CAST(ITEM_ID AS BIGINT) as WM_ITEM_ID", \
	"CAST(ITEM_NAME AS STRING) as WM_ITEM_NAME", \
	"CAST(VENDOR_ITEM_NBR AS STRING) as WM_VENDOR_ITEM_NBR", \
	"CAST(BATCH_NBR AS STRING) as WM_BATCH_NBR", \
	"CAST(MANUFACTURED_DTTM AS TIMESTAMP) as WM_MANUFACTURED_TSTMP", \
	"CAST(MANUFACTURED_PLANT AS STRING) as WM_MANUFACTURED_PLANT", \
	"CAST(DISTRO_NUMBER AS STRING) as WM_DISTRO_NBR", \
	"CAST(CNTRY_OF_ORGN AS STRING) as COUNTRY_OF_ORIGIN", \
	"CAST(PRODUCT_STATUS AS STRING) as WM_PRODUCT_STATUS", \
	"CAST(REC_PROC_INDIC AS STRING) as REC_PROC_INDICATOR", \
	"CAST(PROC_DTTM AS TIMESTAMP) as WM_PROC_TSTMP", \
	"CAST(PROC_STAT_CODE AS BIGINT) as WM_PROC_STAT_CD", \
	"CAST(SIZE_RANGE_CODE AS STRING) as WM_SIZE_RANGE_CD", \
	"CAST(SIZE_REL_POSN_IN_TABLE AS STRING) as WM_SIZE_REL_POSN_IN_TABLE", \
	"CAST(SIZE_VALUE AS BIGINT) as SIZE_VALUE", \
	"CAST(ITEM_COLOR AS STRING) as ITEM_COLOR", \
	"CAST(ITEM_COLOR_SFX AS STRING) as ITEM_COLOR_SFX", \
	"CAST(ITEM_SEASON AS STRING) as ITEM_SEASON", \
	"CAST(ITEM_SEASON_YEAR AS STRING) as ITEM_SEASON_YEAR", \
	"CAST(ITEM_SECOND_DIM AS STRING) as ITEM_SECOND_DIM", \
	"CAST(ITEM_SIZE_DESC AS STRING) as ITEM_SIZE_DESC", \
	"CAST(ITEM_QUALITY AS STRING) as ITEM_QUALITY", \
	"CAST(ITEM_STYLE AS STRING) as ITEM_STYLE", \
	"CAST(ITEM_STYLE_SFX AS STRING) as ITEM_STYLE_SFX", \
	"CAST(QTY_UOM AS STRING) as WM_QTY_UOM", \
	"CAST(ITEM_ATTR_1 AS STRING) as ITEM_ATTR_1", \
	"CAST(ITEM_ATTR_2 AS STRING) as ITEM_ATTR_2", \
	"CAST(ITEM_ATTR_3 AS STRING) as ITEM_ATTR_3", \
	"CAST(ITEM_ATTR_4 AS STRING) as ITEM_ATTR_4", \
	"CAST(ITEM_ATTR_5 AS STRING) as ITEM_ATTR_5", \
	"CAST(VERSION_NBR AS BIGINT) as WM_VERSION_NBR", \
	"CAST(CREATED_SOURCE_TYPE AS BIGINT) as WM_CREATED_SOURCE_TYPE", \
	"CAST(CREATED_SOURCE AS STRING) as WM_CREATED_SOURCE", \
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP", \
	"CAST(LAST_UPDATED_SOURCE_TYPE AS BIGINT) as WM_LAST_UPDATED_SOURCE_TYPE", \
	"CAST(LAST_UPDATED_SOURCE AS STRING) as WM_LAST_UPDATED_SOURCE", \
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" , \
    "pyspark_data_action"\
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_OUTPT_LPN_DETAIL_ID = target.WM_OUTPT_LPN_DETAIL_ID"""
  # refined_perf_table = "WM_OUTPT_LPN_DETAIL"
  executeMerge(Shortcut_to_WM_OUTPT_LPN_DETAIL1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_OUTPT_LPN_DETAIL", "WM_OUTPT_LPN_DETAIL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_OUTPT_LPN_DETAIL", "WM_OUTPT_LPN_DETAIL","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	