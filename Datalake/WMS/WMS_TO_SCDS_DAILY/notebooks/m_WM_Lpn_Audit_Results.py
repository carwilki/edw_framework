#Code converted on 2023-06-27 09:40:55
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
#env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script
refined_perf_table = f"{refine}.WM_LPN_AUDIT_RESULTS"
raw_perf_table = f"{raw}.WM_LPN_AUDIT_RESULTS_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS, type SOURCE 
# COLUMN COUNT: 43

SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS = spark.sql(f"""SELECT
LOCATION_ID,
WM_LPN_AUDIT_RESULTS_ID,
WM_TC_COMPANY_ID,
WM_FACILITY_ID,
WM_FACILITY_ALIAS_ID,
WM_DEST_FACILITY_ALIAS_ID,
WM_AUDIT_TRANSACTION_ID,
AUDIT_CNT,
WM_QUAL_AUD_STAT_CD,
VALIDATION_LEVEL,
WM_TRAN_NAME,
INBOUND_OUTBOUND_IND,
WM_TC_ORDER_ID,
WM_TC_PARENT_LPN_ID,
WM_TC_LPN_ID,
WM_TC_SHIPMENT_ID,
WM_STOP_SEQ,
WM_STATIC_ROUTE_ID,
WM_INVENTORY_TYPE,
WM_ITEM_ID,
WM_GTIN,
COUNTRY_OF_ORIGIN,
WM_PRODUCT_STATUS,
QA_FLAG,
CNT_QTY,
EXPECTED_QTY,
WM_BATCH_NBR,
WM_AUDITOR_USER_ID,
WM_PICKER_USER_ID,
WM_PACKER_USER_ID,
ITEM_ATTR_1,
ITEM_ATTR_2,
ITEM_ATTR_3,
ITEM_ATTR_4,
ITEM_ATTR_5,
WM_CREATED_SOURCE_TYPE,
WM_CREATED_SOURCE,
WM_WM_CREATED_TSTMP,
WM_LAST_UPDATED_SOURCE_TYPE,
WM_LAST_UPDATED_SOURCE,
WM_WM_LAST_UPDATED_TSTMP,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_LPN_AUDIT_RESULTS_ID
IN (SELECT LPN_AUDIT_RESULTS_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE, type SOURCE 
# COLUMN COUNT: 42

SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE = spark.sql(f"""SELECT
DC_NBR,
LPN_AUDIT_RESULTS_ID,
AUDIT_TRANSACTION_ID,
AUDIT_COUNT,
TC_COMPANY_ID,
FACILITY_ALIAS_ID,
TC_SHIPMENT_ID,
STOP_SEQ,
TC_ORDER_ID,
TC_PARENT_LPN_ID,
TC_LPN_ID,
INBOUND_OUTBOUND_INDICATOR,
DEST_FACILITY_ALIAS_ID,
STATIC_ROUTE_ID,
ITEM_ID,
GTIN,
CNTRY_OF_ORGN,
INVENTORY_TYPE,
PRODUCT_STATUS,
BATCH_NBR,
ITEM_ATTR_1,
ITEM_ATTR_2,
ITEM_ATTR_3,
ITEM_ATTR_4,
ITEM_ATTR_5,
CREATED_SOURCE_TYPE,
CREATED_SOURCE,
CREATED_DTTM,
LAST_UPDATED_SOURCE_TYPE,
LAST_UPDATED_SOURCE,
LAST_UPDATED_DTTM,
AUDITOR_USERID,
PICKER_USERID,
PACKER_USERID,
QUAL_AUD_STAT_CODE,
QA_FLAG,
COUNT_QUANTITY,
EXPECTED_QUANTITY,
VALIDATION_LEVEL,
TRAN_NAME,
FACILITY_ID,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_TRANS, type EXPRESSION 
# COLUMN COUNT: 42

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE_temp = SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE.toDF(*["SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___" + col for col in SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE.columns])

EXP_TRANS = SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___DC_NBR as int) as DC_NBR_EXP", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___LPN_AUDIT_RESULTS_ID as LPN_AUDIT_RESULTS_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___AUDIT_TRANSACTION_ID as AUDIT_TRANSACTION_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___AUDIT_COUNT as AUDIT_COUNT", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___FACILITY_ALIAS_ID as FACILITY_ALIAS_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___STOP_SEQ as STOP_SEQ", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___TC_ORDER_ID as TC_ORDER_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___TC_PARENT_LPN_ID as TC_PARENT_LPN_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___TC_LPN_ID as TC_LPN_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___DEST_FACILITY_ALIAS_ID as DEST_FACILITY_ALIAS_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___STATIC_ROUTE_ID as STATIC_ROUTE_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___ITEM_ID as ITEM_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___GTIN as GTIN", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___INVENTORY_TYPE as INVENTORY_TYPE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___PRODUCT_STATUS as PRODUCT_STATUS", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___BATCH_NBR as BATCH_NBR", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___CREATED_SOURCE as CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___AUDITOR_USERID as AUDITOR_USERID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___PICKER_USERID as PICKER_USERID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___PACKER_USERID as PACKER_USERID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___QUAL_AUD_STAT_CODE as QUAL_AUD_STAT_CODE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___QA_FLAG as QA_FLAG", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___COUNT_QUANTITY as COUNT_QUANTITY", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___EXPECTED_QUANTITY as EXPECTED_QUANTITY", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___VALIDATION_LEVEL as VALIDATION_LEVEL", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___TRAN_NAME as TRAN_NAME", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___FACILITY_ID as FACILITY_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 44

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_TRANS,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_TRANS.DC_NBR_EXP],'inner')

# COMMAND ----------
# Processing node JNR_WM_LPN_AUDIT_RESULTS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 87

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_temp = SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS.toDF(*["SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___" + col for col in SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS.columns])

JNR_WM_LPN_AUDIT_RESULTS = SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_temp.SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS_temp.SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_LPN_AUDIT_RESULTS_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LPN_AUDIT_RESULTS_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___DC_NBR_EXP as DC_NBR_EXP", \
	"JNR_SITE_PROFILE___LPN_AUDIT_RESULTS_ID as LPN_AUDIT_RESULTS_ID", \
	"JNR_SITE_PROFILE___AUDIT_TRANSACTION_ID as AUDIT_TRANSACTION_ID", \
	"JNR_SITE_PROFILE___AUDIT_COUNT as AUDIT_COUNT", \
	"JNR_SITE_PROFILE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_SITE_PROFILE___FACILITY_ALIAS_ID as FACILITY_ALIAS_ID", \
	"JNR_SITE_PROFILE___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
	"JNR_SITE_PROFILE___STOP_SEQ as STOP_SEQ", \
	"JNR_SITE_PROFILE___TC_ORDER_ID as TC_ORDER_ID", \
	"JNR_SITE_PROFILE___TC_PARENT_LPN_ID as TC_PARENT_LPN_ID", \
	"JNR_SITE_PROFILE___TC_LPN_ID as TC_LPN_ID", \
	"JNR_SITE_PROFILE___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"JNR_SITE_PROFILE___DEST_FACILITY_ALIAS_ID as DEST_FACILITY_ALIAS_ID", \
	"JNR_SITE_PROFILE___STATIC_ROUTE_ID as STATIC_ROUTE_ID", \
	"JNR_SITE_PROFILE___ITEM_ID as ITEM_ID", \
	"JNR_SITE_PROFILE___GTIN as GTIN", \
	"JNR_SITE_PROFILE___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"JNR_SITE_PROFILE___INVENTORY_TYPE as INVENTORY_TYPE", \
	"JNR_SITE_PROFILE___PRODUCT_STATUS as PRODUCT_STATUS", \
	"JNR_SITE_PROFILE___BATCH_NBR as BATCH_NBR", \
	"JNR_SITE_PROFILE___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"JNR_SITE_PROFILE___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"JNR_SITE_PROFILE___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"JNR_SITE_PROFILE___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"JNR_SITE_PROFILE___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___AUDITOR_USERID as AUDITOR_USERID", \
	"JNR_SITE_PROFILE___PICKER_USERID as PICKER_USERID", \
	"JNR_SITE_PROFILE___PACKER_USERID as PACKER_USERID", \
	"JNR_SITE_PROFILE___QUAL_AUD_STAT_CODE as QUAL_AUD_STAT_CODE", \
	"JNR_SITE_PROFILE___QA_FLAG as QA_FLAG", \
	"JNR_SITE_PROFILE___COUNT_QUANTITY as COUNT_QUANTITY", \
	"JNR_SITE_PROFILE___EXPECTED_QUANTITY as EXPECTED_QUANTITY", \
	"JNR_SITE_PROFILE___VALIDATION_LEVEL as VALIDATION_LEVEL", \
	"JNR_SITE_PROFILE___TRAN_NAME as TRAN_NAME", \
	"JNR_SITE_PROFILE___FACILITY_ID as FACILITY_ID", \
	"JNR_SITE_PROFILE___LOAD_TSTMP as LOAD_TSTMP", \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___STORE_NBR as STORE_NBR", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_LPN_AUDIT_RESULTS_ID as WM_LPN_AUDIT_RESULTS_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_TC_COMPANY_ID as WM_TC_COMPANY_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_FACILITY_ID as WM_FACILITY_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_FACILITY_ALIAS_ID as WM_FACILITY_ALIAS_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_DEST_FACILITY_ALIAS_ID as WM_DEST_FACILITY_ALIAS_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_AUDIT_TRANSACTION_ID as WM_AUDIT_TRANSACTION_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___AUDIT_CNT as AUDIT_CNT", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_QUAL_AUD_STAT_CD as WM_QUAL_AUD_STAT_CD", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___VALIDATION_LEVEL as in_VALIDATION_LEVEL", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_TRAN_NAME as WM_TRAN_NAME", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___INBOUND_OUTBOUND_IND as INBOUND_OUTBOUND_IND", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_TC_ORDER_ID as WM_TC_ORDER_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_TC_PARENT_LPN_ID as WM_TC_PARENT_LPN_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_TC_LPN_ID as WM_TC_LPN_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_TC_SHIPMENT_ID as WM_TC_SHIPMENT_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_STOP_SEQ as WM_STOP_SEQ", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_STATIC_ROUTE_ID as WM_STATIC_ROUTE_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_INVENTORY_TYPE as WM_INVENTORY_TYPE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_ITEM_ID as WM_ITEM_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_GTIN as WM_GTIN", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___COUNTRY_OF_ORIGIN as COUNTRY_OF_ORIGIN", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_PRODUCT_STATUS as WM_PRODUCT_STATUS", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___QA_FLAG as in_QA_FLAG", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___CNT_QTY as CNT_QTY", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___EXPECTED_QTY as EXPECTED_QTY", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_BATCH_NBR as WM_BATCH_NBR", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_AUDITOR_USER_ID as WM_AUDITOR_USER_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_PICKER_USER_ID as WM_PICKER_USER_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_PACKER_USER_ID as WM_PACKER_USER_ID", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_1 as in_ITEM_ATTR_1", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_2 as in_ITEM_ATTR_2", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_3 as in_ITEM_ATTR_3", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_4 as in_ITEM_ATTR_4", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_5 as in_ITEM_ATTR_5", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_WM_CREATED_TSTMP as WM_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___WM_WM_LAST_UPDATED_TSTMP as WM_WM_LAST_UPDATED_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___UPDATE_TSTMP as UPDATE_TSTMP", \
	"SQ_Shortcut_to_WM_LPN_AUDIT_RESULTS___LOAD_TSTMP as in_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 86

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_LPN_AUDIT_RESULTS_temp = JNR_WM_LPN_AUDIT_RESULTS.toDF(*["JNR_WM_LPN_AUDIT_RESULTS___" + col for col in JNR_WM_LPN_AUDIT_RESULTS.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_LPN_AUDIT_RESULTS_temp.selectExpr( \
	"JNR_WM_LPN_AUDIT_RESULTS___LPN_AUDIT_RESULTS_ID as LPN_AUDIT_RESULTS_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___AUDIT_TRANSACTION_ID as AUDIT_TRANSACTION_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___AUDIT_COUNT as AUDIT_COUNT", \
	"JNR_WM_LPN_AUDIT_RESULTS___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___FACILITY_ALIAS_ID as FACILITY_ALIAS_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___STOP_SEQ as STOP_SEQ", \
	"JNR_WM_LPN_AUDIT_RESULTS___TC_ORDER_ID as TC_ORDER_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___TC_PARENT_LPN_ID as TC_PARENT_LPN_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___TC_LPN_ID as TC_LPN_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"JNR_WM_LPN_AUDIT_RESULTS___DEST_FACILITY_ALIAS_ID as DEST_FACILITY_ALIAS_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___STATIC_ROUTE_ID as STATIC_ROUTE_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___ITEM_ID as ITEM_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___GTIN as GTIN", \
	"JNR_WM_LPN_AUDIT_RESULTS___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"JNR_WM_LPN_AUDIT_RESULTS___INVENTORY_TYPE as INVENTORY_TYPE", \
	"JNR_WM_LPN_AUDIT_RESULTS___PRODUCT_STATUS as PRODUCT_STATUS", \
	"JNR_WM_LPN_AUDIT_RESULTS___BATCH_NBR as BATCH_NBR", \
	"JNR_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"JNR_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"JNR_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"JNR_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"JNR_WM_LPN_AUDIT_RESULTS___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"JNR_WM_LPN_AUDIT_RESULTS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"JNR_WM_LPN_AUDIT_RESULTS___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_WM_LPN_AUDIT_RESULTS___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_LPN_AUDIT_RESULTS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_LPN_AUDIT_RESULTS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_WM_LPN_AUDIT_RESULTS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_LPN_AUDIT_RESULTS___AUDITOR_USERID as AUDITOR_USERID", \
	"JNR_WM_LPN_AUDIT_RESULTS___PICKER_USERID as PICKER_USERID", \
	"JNR_WM_LPN_AUDIT_RESULTS___PACKER_USERID as PACKER_USERID", \
	"JNR_WM_LPN_AUDIT_RESULTS___QUAL_AUD_STAT_CODE as QUAL_AUD_STAT_CODE", \
	"JNR_WM_LPN_AUDIT_RESULTS___QA_FLAG as QA_FLAG", \
	"JNR_WM_LPN_AUDIT_RESULTS___COUNT_QUANTITY as COUNT_QUANTITY", \
	"JNR_WM_LPN_AUDIT_RESULTS___EXPECTED_QUANTITY as EXPECTED_QUANTITY", \
	"JNR_WM_LPN_AUDIT_RESULTS___VALIDATION_LEVEL as VALIDATION_LEVEL", \
	"JNR_WM_LPN_AUDIT_RESULTS___TRAN_NAME as TRAN_NAME", \
	"JNR_WM_LPN_AUDIT_RESULTS___FACILITY_ID as FACILITY_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___LOAD_TSTMP as LOAD_TSTMP", \
	"JNR_WM_LPN_AUDIT_RESULTS___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___STORE_NBR as STORE_NBR", \
	"JNR_WM_LPN_AUDIT_RESULTS___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_LPN_AUDIT_RESULTS_ID as WM_LPN_AUDIT_RESULTS_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_TC_COMPANY_ID as WM_TC_COMPANY_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_FACILITY_ID as WM_FACILITY_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_FACILITY_ALIAS_ID as WM_FACILITY_ALIAS_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_DEST_FACILITY_ALIAS_ID as WM_DEST_FACILITY_ALIAS_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_AUDIT_TRANSACTION_ID as WM_AUDIT_TRANSACTION_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___AUDIT_CNT as AUDIT_CNT", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_QUAL_AUD_STAT_CD as WM_QUAL_AUD_STAT_CD", \
	"JNR_WM_LPN_AUDIT_RESULTS___in_VALIDATION_LEVEL as in_VALIDATION_LEVEL", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_TRAN_NAME as WM_TRAN_NAME", \
	"JNR_WM_LPN_AUDIT_RESULTS___INBOUND_OUTBOUND_IND as INBOUND_OUTBOUND_IND", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_TC_ORDER_ID as WM_TC_ORDER_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_TC_PARENT_LPN_ID as WM_TC_PARENT_LPN_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_TC_LPN_ID as WM_TC_LPN_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_TC_SHIPMENT_ID as WM_TC_SHIPMENT_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_STOP_SEQ as WM_STOP_SEQ", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_STATIC_ROUTE_ID as WM_STATIC_ROUTE_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_INVENTORY_TYPE as WM_INVENTORY_TYPE", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_ITEM_ID as WM_ITEM_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_GTIN as WM_GTIN", \
	"JNR_WM_LPN_AUDIT_RESULTS___COUNTRY_OF_ORIGIN as COUNTRY_OF_ORIGIN", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_PRODUCT_STATUS as WM_PRODUCT_STATUS", \
	"JNR_WM_LPN_AUDIT_RESULTS___in_QA_FLAG as in_QA_FLAG", \
	"JNR_WM_LPN_AUDIT_RESULTS___CNT_QTY as CNT_QTY", \
	"JNR_WM_LPN_AUDIT_RESULTS___EXPECTED_QTY as EXPECTED_QTY", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_BATCH_NBR as WM_BATCH_NBR", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_AUDITOR_USER_ID as WM_AUDITOR_USER_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_PICKER_USER_ID as WM_PICKER_USER_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_PACKER_USER_ID as WM_PACKER_USER_ID", \
	"JNR_WM_LPN_AUDIT_RESULTS___in_ITEM_ATTR_1 as in_ITEM_ATTR_1", \
	"JNR_WM_LPN_AUDIT_RESULTS___in_ITEM_ATTR_2 as in_ITEM_ATTR_2", \
	"JNR_WM_LPN_AUDIT_RESULTS___in_ITEM_ATTR_3 as in_ITEM_ATTR_3", \
	"JNR_WM_LPN_AUDIT_RESULTS___in_ITEM_ATTR_4 as in_ITEM_ATTR_4", \
	"JNR_WM_LPN_AUDIT_RESULTS___in_ITEM_ATTR_5 as in_ITEM_ATTR_5", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_WM_CREATED_TSTMP as WM_WM_CREATED_TSTMP", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"JNR_WM_LPN_AUDIT_RESULTS___WM_WM_LAST_UPDATED_TSTMP as WM_WM_LAST_UPDATED_TSTMP", \
	"JNR_WM_LPN_AUDIT_RESULTS___UPDATE_TSTMP as UPDATE_TSTMP", \
	"JNR_WM_LPN_AUDIT_RESULTS___in_LOAD_TSTMP as in_LOAD_TSTMP") \
    .filter("WM_LPN_AUDIT_RESULTS_ID is Null OR ( WM_LPN_AUDIT_RESULTS_ID is not Null AND ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(WM_WM_CREATED_TSTMP, date'1900-01-01') \
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(WM_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 89

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___LPN_AUDIT_RESULTS_ID as LPN_AUDIT_RESULTS_ID", \
	"FIL_UNCHANGED_RECORDS___AUDIT_TRANSACTION_ID as AUDIT_TRANSACTION_ID", \
	"FIL_UNCHANGED_RECORDS___AUDIT_COUNT as AUDIT_COUNT", \
	"FIL_UNCHANGED_RECORDS___TC_COMPANY_ID as TC_COMPANY_ID", \
	"FIL_UNCHANGED_RECORDS___FACILITY_ALIAS_ID as FACILITY_ALIAS_ID", \
	"FIL_UNCHANGED_RECORDS___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
	"FIL_UNCHANGED_RECORDS___STOP_SEQ as STOP_SEQ", \
	"FIL_UNCHANGED_RECORDS___TC_ORDER_ID as TC_ORDER_ID", \
	"FIL_UNCHANGED_RECORDS___TC_PARENT_LPN_ID as TC_PARENT_LPN_ID", \
	"FIL_UNCHANGED_RECORDS___TC_LPN_ID as TC_LPN_ID", \
	"FIL_UNCHANGED_RECORDS___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"FIL_UNCHANGED_RECORDS___DEST_FACILITY_ALIAS_ID as DEST_FACILITY_ALIAS_ID", \
	"FIL_UNCHANGED_RECORDS___STATIC_ROUTE_ID as STATIC_ROUTE_ID", \
	"FIL_UNCHANGED_RECORDS___ITEM_ID as ITEM_ID", \
	"FIL_UNCHANGED_RECORDS___GTIN as GTIN", \
	"FIL_UNCHANGED_RECORDS___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"FIL_UNCHANGED_RECORDS___INVENTORY_TYPE as INVENTORY_TYPE", \
	"FIL_UNCHANGED_RECORDS___PRODUCT_STATUS as PRODUCT_STATUS", \
	"FIL_UNCHANGED_RECORDS___BATCH_NBR as BATCH_NBR", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"FIL_UNCHANGED_RECORDS___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_UNCHANGED_RECORDS___AUDITOR_USERID as AUDITOR_USERID", \
	"FIL_UNCHANGED_RECORDS___PICKER_USERID as PICKER_USERID", \
	"FIL_UNCHANGED_RECORDS___PACKER_USERID as PACKER_USERID", \
	"FIL_UNCHANGED_RECORDS___QUAL_AUD_STAT_CODE as QUAL_AUD_STAT_CODE", \
	"FIL_UNCHANGED_RECORDS___QA_FLAG as QA_FLAG", \
	"FIL_UNCHANGED_RECORDS___COUNT_QUANTITY as COUNT_QUANTITY", \
	"FIL_UNCHANGED_RECORDS___EXPECTED_QUANTITY as EXPECTED_QUANTITY", \
	"FIL_UNCHANGED_RECORDS___VALIDATION_LEVEL as VALIDATION_LEVEL", \
	"FIL_UNCHANGED_RECORDS___TRAN_NAME as TRAN_NAME", \
	"FIL_UNCHANGED_RECORDS___FACILITY_ID as FACILITY_ID", \
	"FIL_UNCHANGED_RECORDS___LOAD_TSTMP as LOAD_TSTMP", \
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___STORE_NBR as STORE_NBR", \
	"FIL_UNCHANGED_RECORDS___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_UNCHANGED_RECORDS___WM_LPN_AUDIT_RESULTS_ID as WM_LPN_AUDIT_RESULTS_ID", \
	"FIL_UNCHANGED_RECORDS___WM_TC_COMPANY_ID as WM_TC_COMPANY_ID", \
	"FIL_UNCHANGED_RECORDS___WM_FACILITY_ID as WM_FACILITY_ID", \
	"FIL_UNCHANGED_RECORDS___WM_FACILITY_ALIAS_ID as WM_FACILITY_ALIAS_ID", \
	"FIL_UNCHANGED_RECORDS___WM_DEST_FACILITY_ALIAS_ID as WM_DEST_FACILITY_ALIAS_ID", \
	"FIL_UNCHANGED_RECORDS___WM_AUDIT_TRANSACTION_ID as WM_AUDIT_TRANSACTION_ID", \
	"FIL_UNCHANGED_RECORDS___AUDIT_CNT as AUDIT_CNT", \
	"FIL_UNCHANGED_RECORDS___WM_QUAL_AUD_STAT_CD as WM_QUAL_AUD_STAT_CD", \
	"FIL_UNCHANGED_RECORDS___in_VALIDATION_LEVEL as in_VALIDATION_LEVEL", \
	"FIL_UNCHANGED_RECORDS___WM_TRAN_NAME as WM_TRAN_NAME", \
	"FIL_UNCHANGED_RECORDS___INBOUND_OUTBOUND_IND as INBOUND_OUTBOUND_IND", \
	"FIL_UNCHANGED_RECORDS___WM_TC_ORDER_ID as WM_TC_ORDER_ID", \
	"FIL_UNCHANGED_RECORDS___WM_TC_PARENT_LPN_ID as WM_TC_PARENT_LPN_ID", \
	"FIL_UNCHANGED_RECORDS___WM_TC_LPN_ID as WM_TC_LPN_ID", \
	"FIL_UNCHANGED_RECORDS___WM_TC_SHIPMENT_ID as WM_TC_SHIPMENT_ID", \
	"FIL_UNCHANGED_RECORDS___WM_STOP_SEQ as WM_STOP_SEQ", \
	"FIL_UNCHANGED_RECORDS___WM_STATIC_ROUTE_ID as WM_STATIC_ROUTE_ID", \
	"FIL_UNCHANGED_RECORDS___WM_INVENTORY_TYPE as WM_INVENTORY_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_ITEM_ID as WM_ITEM_ID", \
	"FIL_UNCHANGED_RECORDS___WM_GTIN as WM_GTIN", \
	"FIL_UNCHANGED_RECORDS___COUNTRY_OF_ORIGIN as COUNTRY_OF_ORIGIN", \
	"FIL_UNCHANGED_RECORDS___WM_PRODUCT_STATUS as WM_PRODUCT_STATUS", \
	"FIL_UNCHANGED_RECORDS___in_QA_FLAG as in_QA_FLAG", \
	"FIL_UNCHANGED_RECORDS___CNT_QTY as CNT_QTY", \
	"FIL_UNCHANGED_RECORDS___EXPECTED_QTY as EXPECTED_QTY", \
	"FIL_UNCHANGED_RECORDS___WM_BATCH_NBR as WM_BATCH_NBR", \
	"FIL_UNCHANGED_RECORDS___WM_AUDITOR_USER_ID as WM_AUDITOR_USER_ID", \
	"FIL_UNCHANGED_RECORDS___WM_PICKER_USER_ID as WM_PICKER_USER_ID", \
	"FIL_UNCHANGED_RECORDS___WM_PACKER_USER_ID as WM_PACKER_USER_ID", \
	"FIL_UNCHANGED_RECORDS___in_ITEM_ATTR_1 as in_ITEM_ATTR_1", \
	"FIL_UNCHANGED_RECORDS___in_ITEM_ATTR_2 as in_ITEM_ATTR_2", \
	"FIL_UNCHANGED_RECORDS___in_ITEM_ATTR_3 as in_ITEM_ATTR_3", \
	"FIL_UNCHANGED_RECORDS___in_ITEM_ATTR_4 as in_ITEM_ATTR_4", \
	"FIL_UNCHANGED_RECORDS___in_ITEM_ATTR_5 as in_ITEM_ATTR_5", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_SOURCE_TYPE as WM_CREATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_CREATED_SOURCE as WM_CREATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___WM_WM_CREATED_TSTMP as WM_WM_CREATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_SOURCE_TYPE as WM_LAST_UPDATED_SOURCE_TYPE", \
	"FIL_UNCHANGED_RECORDS___WM_LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"FIL_UNCHANGED_RECORDS___WM_WM_LAST_UPDATED_TSTMP as WM_WM_LAST_UPDATED_TSTMP", \
	"FIL_UNCHANGED_RECORDS___UPDATE_TSTMP as UPDATE_TSTMP", \
	"FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP_EXP", \
	"IF(FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___in_LOAD_TSTMP) as LOAD_TSTMP_EXP", \
	"IF(FIL_UNCHANGED_RECORDS___WM_LPN_AUDIT_RESULTS_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" \
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 44

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( \
	"EXP_UPD_VALIDATOR___LOCATION_ID as LOCATION_ID", \
	"EXP_UPD_VALIDATOR___LPN_AUDIT_RESULTS_ID as LPN_AUDIT_RESULTS_ID", \
	"EXP_UPD_VALIDATOR___TC_COMPANY_ID as TC_COMPANY_ID", \
	"EXP_UPD_VALIDATOR___FACILITY_ID as FACILITY_ID", \
	"EXP_UPD_VALIDATOR___FACILITY_ALIAS_ID as FACILITY_ALIAS_ID", \
	"EXP_UPD_VALIDATOR___DEST_FACILITY_ALIAS_ID as DEST_FACILITY_ALIAS_ID", \
	"EXP_UPD_VALIDATOR___AUDIT_TRANSACTION_ID as AUDIT_TRANSACTION_ID", \
	"EXP_UPD_VALIDATOR___AUDIT_COUNT as AUDIT_COUNT", \
	"EXP_UPD_VALIDATOR___QUAL_AUD_STAT_CODE as QUAL_AUD_STAT_CODE", \
	"EXP_UPD_VALIDATOR___VALIDATION_LEVEL as VALIDATION_LEVEL", \
	"EXP_UPD_VALIDATOR___TRAN_NAME as TRAN_NAME", \
	"EXP_UPD_VALIDATOR___INBOUND_OUTBOUND_INDICATOR as INBOUND_OUTBOUND_INDICATOR", \
	"EXP_UPD_VALIDATOR___TC_ORDER_ID as TC_ORDER_ID", \
	"EXP_UPD_VALIDATOR___TC_PARENT_LPN_ID as TC_PARENT_LPN_ID", \
	"EXP_UPD_VALIDATOR___TC_LPN_ID as TC_LPN_ID", \
	"EXP_UPD_VALIDATOR___TC_SHIPMENT_ID as TC_SHIPMENT_ID", \
	"EXP_UPD_VALIDATOR___STOP_SEQ as STOP_SEQ", \
	"EXP_UPD_VALIDATOR___STATIC_ROUTE_ID as STATIC_ROUTE_ID", \
	"EXP_UPD_VALIDATOR___INVENTORY_TYPE as INVENTORY_TYPE", \
	"EXP_UPD_VALIDATOR___ITEM_ID as ITEM_ID", \
	"EXP_UPD_VALIDATOR___GTIN as GTIN", \
	"EXP_UPD_VALIDATOR___CNTRY_OF_ORGN as CNTRY_OF_ORGN", \
	"EXP_UPD_VALIDATOR___PRODUCT_STATUS as PRODUCT_STATUS", \
	"EXP_UPD_VALIDATOR___QA_FLAG as QA_FLAG", \
	"EXP_UPD_VALIDATOR___COUNT_QUANTITY as COUNT_QUANTITY", \
	"EXP_UPD_VALIDATOR___EXPECTED_QUANTITY as EXPECTED_QUANTITY", \
	"EXP_UPD_VALIDATOR___BATCH_NBR as BATCH_NBR", \
	"EXP_UPD_VALIDATOR___AUDITOR_USERID as AUDITOR_USERID", \
	"EXP_UPD_VALIDATOR___PICKER_USERID as PICKER_USERID", \
	"EXP_UPD_VALIDATOR___PACKER_USERID as PACKER_USERID", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_1 as ITEM_ATTR_1", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_2 as ITEM_ATTR_2", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_3 as ITEM_ATTR_3", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_4 as ITEM_ATTR_4", \
	"EXP_UPD_VALIDATOR___ITEM_ATTR_5 as ITEM_ATTR_5", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", \
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_UPD_VALIDATOR___LOAD_TSTMP_EXP as LOAD_TSTMP_EXP", \
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR", \
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP_EXP as UPDATE_TSTMP_EXP") \
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)), lit(0)).when(col('o_UPDATE_VALIDATOR') ==(lit(2)), lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_LPN_AUDIT_RESULTS1, type TARGET 
# COLUMN COUNT: 43


Shortcut_to_WM_LPN_AUDIT_RESULTS1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(LPN_AUDIT_RESULTS_ID AS INT) as WM_LPN_AUDIT_RESULTS_ID",
	"CAST(TC_COMPANY_ID AS INT) as WM_TC_COMPANY_ID",
	"CAST(FACILITY_ID AS BIGINT) as WM_FACILITY_ID",
	"CAST(FACILITY_ALIAS_ID AS STRING) as WM_FACILITY_ALIAS_ID",
	"CAST(DEST_FACILITY_ALIAS_ID AS STRING) as WM_DEST_FACILITY_ALIAS_ID",
	"CAST(AUDIT_TRANSACTION_ID AS INT) as WM_AUDIT_TRANSACTION_ID",
	"CAST(AUDIT_COUNT AS INT) as AUDIT_CNT",
	"CAST(QUAL_AUD_STAT_CODE AS INT) as WM_QUAL_AUD_STAT_CD",
	"CAST(VALIDATION_LEVEL AS STRING) as VALIDATION_LEVEL",
	"CAST(TRAN_NAME AS STRING) as WM_TRAN_NAME",
	"CAST(INBOUND_OUTBOUND_INDICATOR AS STRING) as INBOUND_OUTBOUND_IND",
	"CAST(TC_ORDER_ID AS STRING) as WM_TC_ORDER_ID",
	"CAST(TC_PARENT_LPN_ID AS STRING) as WM_TC_PARENT_LPN_ID",
	"CAST(TC_LPN_ID AS STRING) as WM_TC_LPN_ID",
	"CAST(TC_SHIPMENT_ID AS STRING) as WM_TC_SHIPMENT_ID",
	"CAST(STOP_SEQ AS INT) as WM_STOP_SEQ",
	"CAST(STATIC_ROUTE_ID AS INT) as WM_STATIC_ROUTE_ID",
	"CAST(INVENTORY_TYPE AS STRING) as WM_INVENTORY_TYPE",
	"CAST(ITEM_ID AS INT) as WM_ITEM_ID",
	"CAST(GTIN AS STRING) as WM_GTIN",
	"CAST(CNTRY_OF_ORGN AS STRING) as COUNTRY_OF_ORIGIN",
	"CAST(PRODUCT_STATUS AS STRING) as WM_PRODUCT_STATUS",
	"CAST(QA_FLAG AS STRING) as QA_FLAG",
	"CAST(COUNT_QUANTITY AS DECIMAL(13,5)) as CNT_QTY",
	"CAST(EXPECTED_QUANTITY AS DECIMAL(13,5)) as EXPECTED_QTY",
	"CAST(BATCH_NBR AS STRING) as WM_BATCH_NBR",
	"CAST(AUDITOR_USERID AS STRING) as WM_AUDITOR_USER_ID",
	"CAST(PICKER_USERID AS STRING) as WM_PICKER_USER_ID",
	"CAST(PACKER_USERID AS STRING) as WM_PACKER_USER_ID",
	"CAST(ITEM_ATTR_1 AS STRING) as ITEM_ATTR_1",
	"CAST(ITEM_ATTR_2 AS STRING) as ITEM_ATTR_2",
	"CAST(ITEM_ATTR_3 AS STRING) as ITEM_ATTR_3",
	"CAST(ITEM_ATTR_4 AS STRING) as ITEM_ATTR_4",
	"CAST(ITEM_ATTR_5 AS STRING) as ITEM_ATTR_5",
	"CAST(CREATED_SOURCE_TYPE AS INT) as WM_CREATED_SOURCE_TYPE",
	"CAST(CREATED_SOURCE AS STRING) as WM_CREATED_SOURCE",
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_SOURCE_TYPE AS INT) as WM_LAST_UPDATED_SOURCE_TYPE",
	"CAST(LAST_UPDATED_SOURCE AS STRING) as WM_LAST_UPDATED_SOURCE",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_WM_LAST_UPDATED_TSTMP",
	"CAST(UPDATE_TSTMP_EXP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_LPN_AUDIT_RESULTS_ID = target.WM_LPN_AUDIT_RESULTS_ID"""
#   refined_perf_table = "WM_LPN_AUDIT_RESULTS"
  executeMerge(Shortcut_to_WM_LPN_AUDIT_RESULTS1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_LPN_AUDIT_RESULTS", "WM_LPN_AUDIT_RESULTS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_LPN_AUDIT_RESULTS", "WM_LPN_AUDIT_RESULTS","Failed",str(e), f"{raw}.log_run_details", )
  raise e
