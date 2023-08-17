#Code converted on 2023-06-22 21:03:33
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
refined_perf_table = f"{refine}.WM_UN_NUMBER"
raw_perf_table = f"{raw}.WM_UN_NUMBER_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_UN_NUMBER_PRE, type SOURCE 
# COLUMN COUNT: 73

SQ_Shortcut_to_WM_UN_NUMBER_PRE = spark.sql(f"""SELECT
DC_NBR,
UN_NUMBER_ID,
TC_COMPANY_ID,
UN_NUMBER_VALUE,
UN_NUMBER_DESCRIPTION,
UNN_CLASS_DIV,
UNN_SUB_RISK,
UNN_HAZARD_LABEL,
UNN_PG,
UNN_SP,
UNN_AIR_BOTH_INST_LTD,
UNN_AIR_BOTH_QTY_LTD,
UNN_AIR_BOTH_INST,
UNN_AIR_BOTH_QTY,
UNN_AIR_CARGO_INST,
UNN_AIR_CARGO_QTY,
UN_NUMBER,
RQ_FIELD_1,
RQ_FIELD_2,
RQ_FIELD_3,
RQ_FIELD_4,
RQ_FIELD_5,
HZC_FIELD_1,
HZC_FIELD_2,
HZC_FIELD_3,
HZC_FIELD_4,
HZC_FIELD_5,
MARK_FOR_DELETION,
CREATED_SOURCE_TYPE,
CREATED_SOURCE,
CREATED_DTTM,
LAST_UPDATED_SOURCE_TYPE,
LAST_UPDATED_SOURCE,
LAST_UPDATED_DTTM,
UN_NUMBER_VERSION,
HAZMAT_UOM,
MAX_HAZMAT_QTY,
REGULATION_SET,
TRANS_MODE,
REPORTABLE_QTY,
TECHNICAL_NAME,
ADDL_DESC,
PHONE_NUMBER,
CONTACT_NAME,
IS_ACCESSIBLE,
RESIDUE_INDICATOR_CODE,
HAZMAT_CLASS_QUAL,
NOS_INDICATOR_CODE,
HAZMAT_MATERIAL_QUAL,
FLASH_POINT_TEMPERATURE,
FLASH_POINT_TEMPERATURE_UOM,
HAZARD_ZONE_CODE,
RADIOACTIVE_QTY_CALC_FACTOR,
RADIOACTIVE_QUANTITY_UOM,
EPAWASTESTREAM_NUMBER,
WASTE_CHARACTERISTIC_CODE,
NET_EXPLOSIVE_QUANTITY_FACTOR,
HAZMAT_EXEMPTION_REF_NUMBR,
RQ_FIELD_6,
RQ_FIELD_7,
RQ_FIELD_8,
RQ_FIELD_9,
RQ_FIELD_10,
RQ_FIELD_11,
RQ_FIELD_12,
RQ_FIELD_13,
RQ_FIELD_14,
RQ_FIELD_15,
RQ_FIELD_16,
RQ_FIELD_17,
RQ_FIELD_18,
RQ_FIELD_19,
RQ_FIELD_20
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 73

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_UN_NUMBER_PRE_temp = SQ_Shortcut_to_WM_UN_NUMBER_PRE.toDF(*["SQ_Shortcut_to_WM_UN_NUMBER_PRE___" + col for col in SQ_Shortcut_to_WM_UN_NUMBER_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_UN_NUMBER_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_UN_NUMBER_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UN_NUMBER_ID as UN_NUMBER_ID", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___TC_COMPANY_ID as TC_COMPANY_ID", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UN_NUMBER_VALUE as UN_NUMBER_VALUE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UN_NUMBER_DESCRIPTION as UN_NUMBER_DESCRIPTION", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_CLASS_DIV as UNN_CLASS_DIV", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_SUB_RISK as UNN_SUB_RISK", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_HAZARD_LABEL as UNN_HAZARD_LABEL", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_PG as UNN_PG", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_SP as UNN_SP", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_AIR_BOTH_INST_LTD as UNN_AIR_BOTH_INST_LTD", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_AIR_BOTH_QTY_LTD as UNN_AIR_BOTH_QTY_LTD", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_AIR_BOTH_INST as UNN_AIR_BOTH_INST", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_AIR_BOTH_QTY as UNN_AIR_BOTH_QTY", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_AIR_CARGO_INST as UNN_AIR_CARGO_INST", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UNN_AIR_CARGO_QTY as UNN_AIR_CARGO_QTY", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UN_NUMBER as UN_NUMBER", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_1 as RQ_FIELD_1", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_2 as RQ_FIELD_2", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_3 as RQ_FIELD_3", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_4 as RQ_FIELD_4", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_5 as RQ_FIELD_5", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HZC_FIELD_1 as HZC_FIELD_1", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HZC_FIELD_2 as HZC_FIELD_2", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HZC_FIELD_3 as HZC_FIELD_3", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HZC_FIELD_4 as HZC_FIELD_4", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HZC_FIELD_5 as HZC_FIELD_5", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___CREATED_SOURCE as CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___UN_NUMBER_VERSION as UN_NUMBER_VERSION", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HAZMAT_UOM as HAZMAT_UOM", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___MAX_HAZMAT_QTY as MAX_HAZMAT_QTY", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___REGULATION_SET as REGULATION_SET", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___TRANS_MODE as TRANS_MODE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___REPORTABLE_QTY as REPORTABLE_QTY", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___TECHNICAL_NAME as TECHNICAL_NAME", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___ADDL_DESC as ADDL_DESC", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___PHONE_NUMBER as PHONE_NUMBER", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___CONTACT_NAME as CONTACT_NAME", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___IS_ACCESSIBLE as IS_ACCESSIBLE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RESIDUE_INDICATOR_CODE as RESIDUE_INDICATOR_CODE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HAZMAT_CLASS_QUAL as HAZMAT_CLASS_QUAL", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___NOS_INDICATOR_CODE as NOS_INDICATOR_CODE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HAZMAT_MATERIAL_QUAL as HAZMAT_MATERIAL_QUAL", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___FLASH_POINT_TEMPERATURE as FLASH_POINT_TEMPERATURE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___FLASH_POINT_TEMPERATURE_UOM as FLASH_POINT_TEMPERATURE_UOM", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HAZARD_ZONE_CODE as HAZARD_ZONE_CODE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RADIOACTIVE_QTY_CALC_FACTOR as RADIOACTIVE_QTY_CALC_FACTOR", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RADIOACTIVE_QUANTITY_UOM as RADIOACTIVE_QUANTITY_UOM", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___EPAWASTESTREAM_NUMBER as EPAWASTESTREAM_NUMBER", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___WASTE_CHARACTERISTIC_CODE as WASTE_CHARACTERISTIC_CODE", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___NET_EXPLOSIVE_QUANTITY_FACTOR as NET_EXPLOSIVE_QUANTITY_FACTOR", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___HAZMAT_EXEMPTION_REF_NUMBR as HAZMAT_EXEMPTION_REF_NUMBR", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_6 as RQ_FIELD_6", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_7 as RQ_FIELD_7", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_8 as RQ_FIELD_8", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_9 as RQ_FIELD_9", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_10 as RQ_FIELD_10", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_11 as RQ_FIELD_11", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_12 as RQ_FIELD_12", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_13 as RQ_FIELD_13", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_14 as RQ_FIELD_14", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_15 as RQ_FIELD_15", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_16 as RQ_FIELD_16", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_17 as RQ_FIELD_17", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_18 as RQ_FIELD_18", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_19 as RQ_FIELD_19", 
	"SQ_Shortcut_to_WM_UN_NUMBER_PRE___RQ_FIELD_20 as RQ_FIELD_20" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_UN_NUMBER, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_WM_UN_NUMBER = spark.sql(f"""SELECT
LOCATION_ID,
WM_UN_NBR_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_UN_NBR_ID IN (SELECT UN_NUMBER_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 75

JNR_SITE_PROFILE = EXP_INT_CONV.join(SQ_Shortcut_to_SITE_PROFILE,[EXP_INT_CONV.o_DC_NBR == SQ_Shortcut_to_SITE_PROFILE.STORE_NBR],'inner')

# COMMAND ----------
# Processing node JNR_WM_UN_NUMBER, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 78

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_UN_NUMBER_temp = SQ_Shortcut_to_WM_UN_NUMBER.toDF(*["SQ_Shortcut_to_WM_UN_NUMBER___" + col for col in SQ_Shortcut_to_WM_UN_NUMBER.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_WM_UN_NUMBER = SQ_Shortcut_to_WM_UN_NUMBER_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_UN_NUMBER_temp.SQ_Shortcut_to_WM_UN_NUMBER___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_UN_NUMBER_temp.SQ_Shortcut_to_WM_UN_NUMBER___WM_UN_NBR_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___UN_NUMBER_ID],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___UN_NUMBER_ID as UN_NUMBER_ID", 
	"JNR_SITE_PROFILE___TC_COMPANY_ID as TC_COMPANY_ID", 
	"JNR_SITE_PROFILE___UN_NUMBER_VALUE as UN_NUMBER_VALUE", 
	"JNR_SITE_PROFILE___UN_NUMBER_DESCRIPTION as UN_NUMBER_DESCRIPTION", 
	"JNR_SITE_PROFILE___UNN_CLASS_DIV as UNN_CLASS_DIV", 
	"JNR_SITE_PROFILE___UNN_SUB_RISK as UNN_SUB_RISK", 
	"JNR_SITE_PROFILE___UNN_HAZARD_LABEL as UNN_HAZARD_LABEL", 
	"JNR_SITE_PROFILE___UNN_PG as UNN_PG", 
	"JNR_SITE_PROFILE___UNN_SP as UNN_SP", 
	"JNR_SITE_PROFILE___UNN_AIR_BOTH_INST_LTD as UNN_AIR_BOTH_INST_LTD", 
	"JNR_SITE_PROFILE___UNN_AIR_BOTH_QTY_LTD as UNN_AIR_BOTH_QTY_LTD", 
	"JNR_SITE_PROFILE___UNN_AIR_BOTH_INST as UNN_AIR_BOTH_INST", 
	"JNR_SITE_PROFILE___UNN_AIR_BOTH_QTY as UNN_AIR_BOTH_QTY", 
	"JNR_SITE_PROFILE___UNN_AIR_CARGO_INST as UNN_AIR_CARGO_INST", 
	"JNR_SITE_PROFILE___UNN_AIR_CARGO_QTY as UNN_AIR_CARGO_QTY", 
	"JNR_SITE_PROFILE___UN_NUMBER as UN_NUMBER", 
	"JNR_SITE_PROFILE___RQ_FIELD_1 as RQ_FIELD_1", 
	"JNR_SITE_PROFILE___RQ_FIELD_2 as RQ_FIELD_2", 
	"JNR_SITE_PROFILE___RQ_FIELD_3 as RQ_FIELD_3", 
	"JNR_SITE_PROFILE___RQ_FIELD_4 as RQ_FIELD_4", 
	"JNR_SITE_PROFILE___RQ_FIELD_5 as RQ_FIELD_5", 
	"JNR_SITE_PROFILE___HZC_FIELD_1 as HZC_FIELD_1", 
	"JNR_SITE_PROFILE___HZC_FIELD_2 as HZC_FIELD_2", 
	"JNR_SITE_PROFILE___HZC_FIELD_3 as HZC_FIELD_3", 
	"JNR_SITE_PROFILE___HZC_FIELD_4 as HZC_FIELD_4", 
	"JNR_SITE_PROFILE___HZC_FIELD_5 as HZC_FIELD_5", 
	"JNR_SITE_PROFILE___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE_PROFILE___UN_NUMBER_VERSION as UN_NUMBER_VERSION", 
	"JNR_SITE_PROFILE___HAZMAT_UOM as HAZMAT_UOM", 
	"JNR_SITE_PROFILE___MAX_HAZMAT_QTY as MAX_HAZMAT_QTY", 
	"JNR_SITE_PROFILE___REGULATION_SET as REGULATION_SET", 
	"JNR_SITE_PROFILE___TRANS_MODE as TRANS_MODE", 
	"JNR_SITE_PROFILE___REPORTABLE_QTY as REPORTABLE_QTY", 
	"JNR_SITE_PROFILE___TECHNICAL_NAME as TECHNICAL_NAME", 
	"JNR_SITE_PROFILE___ADDL_DESC as ADDL_DESC", 
	"JNR_SITE_PROFILE___PHONE_NUMBER as PHONE_NUMBER", 
	"JNR_SITE_PROFILE___CONTACT_NAME as CONTACT_NAME", 
	"JNR_SITE_PROFILE___IS_ACCESSIBLE as IS_ACCESSIBLE", 
	"JNR_SITE_PROFILE___RESIDUE_INDICATOR_CODE as RESIDUE_INDICATOR_CODE", 
	"JNR_SITE_PROFILE___HAZMAT_CLASS_QUAL as HAZMAT_CLASS_QUAL", 
	"JNR_SITE_PROFILE___NOS_INDICATOR_CODE as NOS_INDICATOR_CODE", 
	"JNR_SITE_PROFILE___HAZMAT_MATERIAL_QUAL as HAZMAT_MATERIAL_QUAL", 
	"JNR_SITE_PROFILE___FLASH_POINT_TEMPERATURE as FLASH_POINT_TEMPERATURE", 
	"JNR_SITE_PROFILE___FLASH_POINT_TEMPERATURE_UOM as FLASH_POINT_TEMPERATURE_UOM", 
	"JNR_SITE_PROFILE___HAZARD_ZONE_CODE as HAZARD_ZONE_CODE", 
	"JNR_SITE_PROFILE___RADIOACTIVE_QTY_CALC_FACTOR as RADIOACTIVE_QTY_CALC_FACTOR", 
	"JNR_SITE_PROFILE___RADIOACTIVE_QUANTITY_UOM as RADIOACTIVE_QUANTITY_UOM", 
	"JNR_SITE_PROFILE___EPAWASTESTREAM_NUMBER as EPAWASTESTREAM_NUMBER", 
	"JNR_SITE_PROFILE___WASTE_CHARACTERISTIC_CODE as WASTE_CHARACTERISTIC_CODE", 
	"JNR_SITE_PROFILE___NET_EXPLOSIVE_QUANTITY_FACTOR as NET_EXPLOSIVE_QUANTITY_FACTOR", 
	"JNR_SITE_PROFILE___HAZMAT_EXEMPTION_REF_NUMBR as HAZMAT_EXEMPTION_REF_NUMBR", 
	"JNR_SITE_PROFILE___RQ_FIELD_6 as RQ_FIELD_6", 
	"JNR_SITE_PROFILE___RQ_FIELD_7 as RQ_FIELD_7", 
	"JNR_SITE_PROFILE___RQ_FIELD_8 as RQ_FIELD_8", 
	"JNR_SITE_PROFILE___RQ_FIELD_9 as RQ_FIELD_9", 
	"JNR_SITE_PROFILE___RQ_FIELD_10 as RQ_FIELD_10", 
	"JNR_SITE_PROFILE___RQ_FIELD_11 as RQ_FIELD_11", 
	"JNR_SITE_PROFILE___RQ_FIELD_12 as RQ_FIELD_12", 
	"JNR_SITE_PROFILE___RQ_FIELD_13 as RQ_FIELD_13", 
	"JNR_SITE_PROFILE___RQ_FIELD_14 as RQ_FIELD_14", 
	"JNR_SITE_PROFILE___RQ_FIELD_15 as RQ_FIELD_15", 
	"JNR_SITE_PROFILE___RQ_FIELD_16 as RQ_FIELD_16", 
	"JNR_SITE_PROFILE___RQ_FIELD_17 as RQ_FIELD_17", 
	"JNR_SITE_PROFILE___RQ_FIELD_18 as RQ_FIELD_18", 
	"JNR_SITE_PROFILE___RQ_FIELD_19 as RQ_FIELD_19", 
	"JNR_SITE_PROFILE___RQ_FIELD_20 as RQ_FIELD_20", 
	"SQ_Shortcut_to_WM_UN_NUMBER___LOCATION_ID as i_LOCATION_ID1", 
	"SQ_Shortcut_to_WM_UN_NUMBER___WM_UN_NBR_ID as i_WM_UN_NBR_ID", 
	"SQ_Shortcut_to_WM_UN_NUMBER___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_UN_NUMBER___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_UN_NUMBER___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 77

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_UN_NUMBER_temp = JNR_WM_UN_NUMBER.toDF(*["JNR_WM_UN_NUMBER___" + col for col in JNR_WM_UN_NUMBER.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_UN_NUMBER_temp.selectExpr( 
	"JNR_WM_UN_NUMBER___LOCATION_ID as LOCATION_ID", 
	"JNR_WM_UN_NUMBER___UN_NUMBER_ID as UN_NUMBER_ID", 
	"JNR_WM_UN_NUMBER___TC_COMPANY_ID as TC_COMPANY_ID", 
	"JNR_WM_UN_NUMBER___UN_NUMBER_VALUE as UN_NUMBER_VALUE", 
	"JNR_WM_UN_NUMBER___UN_NUMBER_DESCRIPTION as UN_NUMBER_DESCRIPTION", 
	"JNR_WM_UN_NUMBER___UNN_CLASS_DIV as UNN_CLASS_DIV", 
	"JNR_WM_UN_NUMBER___UNN_SUB_RISK as UNN_SUB_RISK", 
	"JNR_WM_UN_NUMBER___UNN_HAZARD_LABEL as UNN_HAZARD_LABEL", 
	"JNR_WM_UN_NUMBER___UNN_PG as UNN_PG", 
	"JNR_WM_UN_NUMBER___UNN_SP as UNN_SP", 
	"JNR_WM_UN_NUMBER___UNN_AIR_BOTH_INST_LTD as UNN_AIR_BOTH_INST_LTD", 
	"JNR_WM_UN_NUMBER___UNN_AIR_BOTH_QTY_LTD as UNN_AIR_BOTH_QTY_LTD", 
	"JNR_WM_UN_NUMBER___UNN_AIR_BOTH_INST as UNN_AIR_BOTH_INST", 
	"JNR_WM_UN_NUMBER___UNN_AIR_BOTH_QTY as UNN_AIR_BOTH_QTY", 
	"JNR_WM_UN_NUMBER___UNN_AIR_CARGO_INST as UNN_AIR_CARGO_INST", 
	"JNR_WM_UN_NUMBER___UNN_AIR_CARGO_QTY as UNN_AIR_CARGO_QTY", 
	"JNR_WM_UN_NUMBER___UN_NUMBER as UN_NUMBER", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_1 as RQ_FIELD_1", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_2 as RQ_FIELD_2", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_3 as RQ_FIELD_3", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_4 as RQ_FIELD_4", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_5 as RQ_FIELD_5", 
	"JNR_WM_UN_NUMBER___HZC_FIELD_1 as HZC_FIELD_1", 
	"JNR_WM_UN_NUMBER___HZC_FIELD_2 as HZC_FIELD_2", 
	"JNR_WM_UN_NUMBER___HZC_FIELD_3 as HZC_FIELD_3", 
	"JNR_WM_UN_NUMBER___HZC_FIELD_4 as HZC_FIELD_4", 
	"JNR_WM_UN_NUMBER___HZC_FIELD_5 as HZC_FIELD_5", 
	"JNR_WM_UN_NUMBER___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"JNR_WM_UN_NUMBER___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"JNR_WM_UN_NUMBER___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_WM_UN_NUMBER___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_UN_NUMBER___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"JNR_WM_UN_NUMBER___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_WM_UN_NUMBER___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_WM_UN_NUMBER___UN_NUMBER_VERSION as UN_NUMBER_VERSION", 
	"JNR_WM_UN_NUMBER___HAZMAT_UOM as HAZMAT_UOM", 
	"JNR_WM_UN_NUMBER___MAX_HAZMAT_QTY as MAX_HAZMAT_QTY", 
	"JNR_WM_UN_NUMBER___REGULATION_SET as REGULATION_SET", 
	"JNR_WM_UN_NUMBER___TRANS_MODE as TRANS_MODE", 
	"JNR_WM_UN_NUMBER___REPORTABLE_QTY as REPORTABLE_QTY", 
	"JNR_WM_UN_NUMBER___TECHNICAL_NAME as TECHNICAL_NAME", 
	"JNR_WM_UN_NUMBER___ADDL_DESC as ADDL_DESC", 
	"JNR_WM_UN_NUMBER___PHONE_NUMBER as PHONE_NUMBER", 
	"JNR_WM_UN_NUMBER___CONTACT_NAME as CONTACT_NAME", 
	"JNR_WM_UN_NUMBER___IS_ACCESSIBLE as IS_ACCESSIBLE", 
	"JNR_WM_UN_NUMBER___RESIDUE_INDICATOR_CODE as RESIDUE_INDICATOR_CODE", 
	"JNR_WM_UN_NUMBER___HAZMAT_CLASS_QUAL as HAZMAT_CLASS_QUAL", 
	"JNR_WM_UN_NUMBER___NOS_INDICATOR_CODE as NOS_INDICATOR_CODE", 
	"JNR_WM_UN_NUMBER___HAZMAT_MATERIAL_QUAL as HAZMAT_MATERIAL_QUAL", 
	"JNR_WM_UN_NUMBER___FLASH_POINT_TEMPERATURE as FLASH_POINT_TEMPERATURE", 
	"JNR_WM_UN_NUMBER___FLASH_POINT_TEMPERATURE_UOM as FLASH_POINT_TEMPERATURE_UOM", 
	"JNR_WM_UN_NUMBER___HAZARD_ZONE_CODE as HAZARD_ZONE_CODE", 
	"JNR_WM_UN_NUMBER___RADIOACTIVE_QTY_CALC_FACTOR as RADIOACTIVE_QTY_CALC_FACTOR", 
	"JNR_WM_UN_NUMBER___RADIOACTIVE_QUANTITY_UOM as RADIOACTIVE_QUANTITY_UOM", 
	"JNR_WM_UN_NUMBER___EPAWASTESTREAM_NUMBER as EPAWASTESTREAM_NUMBER", 
	"JNR_WM_UN_NUMBER___WASTE_CHARACTERISTIC_CODE as WASTE_CHARACTERISTIC_CODE", 
	"JNR_WM_UN_NUMBER___NET_EXPLOSIVE_QUANTITY_FACTOR as NET_EXPLOSIVE_QUANTITY_FACTOR", 
	"JNR_WM_UN_NUMBER___HAZMAT_EXEMPTION_REF_NUMBR as HAZMAT_EXEMPTION_REF_NUMBR", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_6 as RQ_FIELD_6", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_7 as RQ_FIELD_7", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_8 as RQ_FIELD_8", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_9 as RQ_FIELD_9", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_10 as RQ_FIELD_10", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_11 as RQ_FIELD_11", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_12 as RQ_FIELD_12", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_13 as RQ_FIELD_13", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_14 as RQ_FIELD_14", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_15 as RQ_FIELD_15", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_16 as RQ_FIELD_16", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_17 as RQ_FIELD_17", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_18 as RQ_FIELD_18", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_19 as RQ_FIELD_19", 
	"JNR_WM_UN_NUMBER___RQ_FIELD_20 as RQ_FIELD_20", 
	"JNR_WM_UN_NUMBER___i_WM_UN_NBR_ID as i_WM_UN_NBR_ID", 
	"JNR_WM_UN_NUMBER___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"JNR_WM_UN_NUMBER___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_UN_NUMBER___i_LOAD_TSTMP as i_LOAD_TSTMP").filter(expr("i_WM_UN_NBR_ID IS NULL OR (NOT i_WM_UN_NBR_ID IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_OUTPUT_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 76

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_OUTPUT_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID as LOCATION_ID", 
	"FIL_UNCHANGED_RECORDS___UN_NUMBER_ID as UN_NUMBER_ID", 
	"FIL_UNCHANGED_RECORDS___TC_COMPANY_ID as TC_COMPANY_ID", 
	"FIL_UNCHANGED_RECORDS___UN_NUMBER_VALUE as UN_NUMBER_VALUE", 
	"FIL_UNCHANGED_RECORDS___UN_NUMBER_DESCRIPTION as UN_NUMBER_DESCRIPTION", 
	"FIL_UNCHANGED_RECORDS___UNN_CLASS_DIV as UNN_CLASS_DIV", 
	"FIL_UNCHANGED_RECORDS___UNN_SUB_RISK as UNN_SUB_RISK", 
	"FIL_UNCHANGED_RECORDS___UNN_HAZARD_LABEL as UNN_HAZARD_LABEL", 
	"FIL_UNCHANGED_RECORDS___UNN_PG as UNN_PG", 
	"FIL_UNCHANGED_RECORDS___UNN_SP as UNN_SP", 
	"FIL_UNCHANGED_RECORDS___UNN_AIR_BOTH_INST_LTD as UNN_AIR_BOTH_INST_LTD", 
	"FIL_UNCHANGED_RECORDS___UNN_AIR_BOTH_QTY_LTD as UNN_AIR_BOTH_QTY_LTD", 
	"FIL_UNCHANGED_RECORDS___UNN_AIR_BOTH_INST as UNN_AIR_BOTH_INST", 
	"FIL_UNCHANGED_RECORDS___UNN_AIR_BOTH_QTY as UNN_AIR_BOTH_QTY", 
	"FIL_UNCHANGED_RECORDS___UNN_AIR_CARGO_INST as UNN_AIR_CARGO_INST", 
	"FIL_UNCHANGED_RECORDS___UNN_AIR_CARGO_QTY as UNN_AIR_CARGO_QTY", 
	"FIL_UNCHANGED_RECORDS___UN_NUMBER as UN_NUMBER", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_1 as RQ_FIELD_1", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_2 as RQ_FIELD_2", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_3 as RQ_FIELD_3", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_4 as RQ_FIELD_4", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_5 as RQ_FIELD_5", 
	"FIL_UNCHANGED_RECORDS___HZC_FIELD_1 as HZC_FIELD_1", 
	"FIL_UNCHANGED_RECORDS___HZC_FIELD_2 as HZC_FIELD_2", 
	"FIL_UNCHANGED_RECORDS___HZC_FIELD_3 as HZC_FIELD_3", 
	"FIL_UNCHANGED_RECORDS___HZC_FIELD_4 as HZC_FIELD_4", 
	"FIL_UNCHANGED_RECORDS___HZC_FIELD_5 as HZC_FIELD_5", 
	"FIL_UNCHANGED_RECORDS___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___UN_NUMBER_VERSION as UN_NUMBER_VERSION", 
	"FIL_UNCHANGED_RECORDS___HAZMAT_UOM as HAZMAT_UOM", 
	"FIL_UNCHANGED_RECORDS___MAX_HAZMAT_QTY as MAX_HAZMAT_QTY", 
	"FIL_UNCHANGED_RECORDS___REGULATION_SET as REGULATION_SET", 
	"FIL_UNCHANGED_RECORDS___TRANS_MODE as TRANS_MODE", 
	"FIL_UNCHANGED_RECORDS___REPORTABLE_QTY as REPORTABLE_QTY", 
	"FIL_UNCHANGED_RECORDS___TECHNICAL_NAME as TECHNICAL_NAME", 
	"FIL_UNCHANGED_RECORDS___ADDL_DESC as ADDL_DESC", 
	"FIL_UNCHANGED_RECORDS___PHONE_NUMBER as PHONE_NUMBER", 
	"FIL_UNCHANGED_RECORDS___CONTACT_NAME as CONTACT_NAME", 
	"FIL_UNCHANGED_RECORDS___IS_ACCESSIBLE as IS_ACCESSIBLE", 
	"FIL_UNCHANGED_RECORDS___RESIDUE_INDICATOR_CODE as RESIDUE_INDICATOR_CODE", 
	"FIL_UNCHANGED_RECORDS___HAZMAT_CLASS_QUAL as HAZMAT_CLASS_QUAL", 
	"FIL_UNCHANGED_RECORDS___NOS_INDICATOR_CODE as NOS_INDICATOR_CODE", 
	"FIL_UNCHANGED_RECORDS___HAZMAT_MATERIAL_QUAL as HAZMAT_MATERIAL_QUAL", 
	"FIL_UNCHANGED_RECORDS___FLASH_POINT_TEMPERATURE as FLASH_POINT_TEMPERATURE", 
	"FIL_UNCHANGED_RECORDS___FLASH_POINT_TEMPERATURE_UOM as FLASH_POINT_TEMPERATURE_UOM", 
	"FIL_UNCHANGED_RECORDS___HAZARD_ZONE_CODE as HAZARD_ZONE_CODE", 
	"FIL_UNCHANGED_RECORDS___RADIOACTIVE_QTY_CALC_FACTOR as RADIOACTIVE_QTY_CALC_FACTOR", 
	"FIL_UNCHANGED_RECORDS___RADIOACTIVE_QUANTITY_UOM as RADIOACTIVE_QUANTITY_UOM", 
	"FIL_UNCHANGED_RECORDS___EPAWASTESTREAM_NUMBER as EPAWASTESTREAM_NUMBER", 
	"FIL_UNCHANGED_RECORDS___WASTE_CHARACTERISTIC_CODE as WASTE_CHARACTERISTIC_CODE", 
	"FIL_UNCHANGED_RECORDS___NET_EXPLOSIVE_QUANTITY_FACTOR as NET_EXPLOSIVE_QUANTITY_FACTOR", 
	"FIL_UNCHANGED_RECORDS___HAZMAT_EXEMPTION_REF_NUMBR as HAZMAT_EXEMPTION_REF_NUMBR", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_6 as RQ_FIELD_6", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_7 as RQ_FIELD_7", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_8 as RQ_FIELD_8", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_9 as RQ_FIELD_9", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_10 as RQ_FIELD_10", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_11 as RQ_FIELD_11", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_12 as RQ_FIELD_12", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_13 as RQ_FIELD_13", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_14 as RQ_FIELD_14", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_15 as RQ_FIELD_15", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_16 as RQ_FIELD_16", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_17 as RQ_FIELD_17", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_18 as RQ_FIELD_18", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_19 as RQ_FIELD_19", 
	"FIL_UNCHANGED_RECORDS___RQ_FIELD_20 as RQ_FIELD_20", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_WM_UN_NBR_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 76

# for each involved DataFrame, append the dataframe name to each column
EXP_OUTPUT_VALIDATOR_temp = EXP_OUTPUT_VALIDATOR.toDF(*["EXP_OUTPUT_VALIDATOR___" + col for col in EXP_OUTPUT_VALIDATOR.columns])

UPD_INS_UPD = EXP_OUTPUT_VALIDATOR_temp.selectExpr( 
	"EXP_OUTPUT_VALIDATOR___LOCATION_ID as LOCATION_ID", 
	"EXP_OUTPUT_VALIDATOR___UN_NUMBER_ID as UN_NUMBER_ID", 
	"EXP_OUTPUT_VALIDATOR___TC_COMPANY_ID as TC_COMPANY_ID", 
	"EXP_OUTPUT_VALIDATOR___UN_NUMBER_VALUE as UN_NUMBER_VALUE", 
	"EXP_OUTPUT_VALIDATOR___UN_NUMBER_DESCRIPTION as UN_NUMBER_DESCRIPTION", 
	"EXP_OUTPUT_VALIDATOR___UNN_CLASS_DIV as UNN_CLASS_DIV", 
	"EXP_OUTPUT_VALIDATOR___UNN_SUB_RISK as UNN_SUB_RISK", 
	"EXP_OUTPUT_VALIDATOR___UNN_HAZARD_LABEL as UNN_HAZARD_LABEL", 
	"EXP_OUTPUT_VALIDATOR___UNN_PG as UNN_PG", 
	"EXP_OUTPUT_VALIDATOR___UNN_SP as UNN_SP", 
	"EXP_OUTPUT_VALIDATOR___UNN_AIR_BOTH_INST_LTD as UNN_AIR_BOTH_INST_LTD", 
	"EXP_OUTPUT_VALIDATOR___UNN_AIR_BOTH_QTY_LTD as UNN_AIR_BOTH_QTY_LTD", 
	"EXP_OUTPUT_VALIDATOR___UNN_AIR_BOTH_INST as UNN_AIR_BOTH_INST", 
	"EXP_OUTPUT_VALIDATOR___UNN_AIR_BOTH_QTY as UNN_AIR_BOTH_QTY", 
	"EXP_OUTPUT_VALIDATOR___UNN_AIR_CARGO_INST as UNN_AIR_CARGO_INST", 
	"EXP_OUTPUT_VALIDATOR___UNN_AIR_CARGO_QTY as UNN_AIR_CARGO_QTY", 
	"EXP_OUTPUT_VALIDATOR___UN_NUMBER as UN_NUMBER", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_1 as RQ_FIELD_1", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_2 as RQ_FIELD_2", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_3 as RQ_FIELD_3", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_4 as RQ_FIELD_4", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_5 as RQ_FIELD_5", 
	"EXP_OUTPUT_VALIDATOR___HZC_FIELD_1 as HZC_FIELD_1", 
	"EXP_OUTPUT_VALIDATOR___HZC_FIELD_2 as HZC_FIELD_2", 
	"EXP_OUTPUT_VALIDATOR___HZC_FIELD_3 as HZC_FIELD_3", 
	"EXP_OUTPUT_VALIDATOR___HZC_FIELD_4 as HZC_FIELD_4", 
	"EXP_OUTPUT_VALIDATOR___HZC_FIELD_5 as HZC_FIELD_5", 
	"EXP_OUTPUT_VALIDATOR___MARK_FOR_DELETION as MARK_FOR_DELETION", 
	"EXP_OUTPUT_VALIDATOR___CREATED_SOURCE_TYPE as CREATED_SOURCE_TYPE", 
	"EXP_OUTPUT_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", 
	"EXP_OUTPUT_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_OUTPUT_VALIDATOR___LAST_UPDATED_SOURCE_TYPE as LAST_UPDATED_SOURCE_TYPE", 
	"EXP_OUTPUT_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"EXP_OUTPUT_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_OUTPUT_VALIDATOR___UN_NUMBER_VERSION as UN_NUMBER_VERSION", 
	"EXP_OUTPUT_VALIDATOR___HAZMAT_UOM as HAZMAT_UOM", 
	"EXP_OUTPUT_VALIDATOR___MAX_HAZMAT_QTY as MAX_HAZMAT_QTY", 
	"EXP_OUTPUT_VALIDATOR___REGULATION_SET as REGULATION_SET", 
	"EXP_OUTPUT_VALIDATOR___TRANS_MODE as TRANS_MODE", 
	"EXP_OUTPUT_VALIDATOR___REPORTABLE_QTY as REPORTABLE_QTY", 
	"EXP_OUTPUT_VALIDATOR___TECHNICAL_NAME as TECHNICAL_NAME", 
	"EXP_OUTPUT_VALIDATOR___ADDL_DESC as ADDL_DESC", 
	"EXP_OUTPUT_VALIDATOR___PHONE_NUMBER as PHONE_NUMBER", 
	"EXP_OUTPUT_VALIDATOR___CONTACT_NAME as CONTACT_NAME", 
	"EXP_OUTPUT_VALIDATOR___IS_ACCESSIBLE as IS_ACCESSIBLE", 
	"EXP_OUTPUT_VALIDATOR___RESIDUE_INDICATOR_CODE as RESIDUE_INDICATOR_CODE", 
	"EXP_OUTPUT_VALIDATOR___HAZMAT_CLASS_QUAL as HAZMAT_CLASS_QUAL", 
	"EXP_OUTPUT_VALIDATOR___NOS_INDICATOR_CODE as NOS_INDICATOR_CODE", 
	"EXP_OUTPUT_VALIDATOR___HAZMAT_MATERIAL_QUAL as HAZMAT_MATERIAL_QUAL", 
	"EXP_OUTPUT_VALIDATOR___FLASH_POINT_TEMPERATURE as FLASH_POINT_TEMPERATURE", 
	"EXP_OUTPUT_VALIDATOR___FLASH_POINT_TEMPERATURE_UOM as FLASH_POINT_TEMPERATURE_UOM", 
	"EXP_OUTPUT_VALIDATOR___HAZARD_ZONE_CODE as HAZARD_ZONE_CODE", 
	"EXP_OUTPUT_VALIDATOR___RADIOACTIVE_QTY_CALC_FACTOR as RADIOACTIVE_QTY_CALC_FACTOR", 
	"EXP_OUTPUT_VALIDATOR___RADIOACTIVE_QUANTITY_UOM as RADIOACTIVE_QUANTITY_UOM", 
	"EXP_OUTPUT_VALIDATOR___EPAWASTESTREAM_NUMBER as EPAWASTESTREAM_NUMBER", 
	"EXP_OUTPUT_VALIDATOR___WASTE_CHARACTERISTIC_CODE as WASTE_CHARACTERISTIC_CODE", 
	"EXP_OUTPUT_VALIDATOR___NET_EXPLOSIVE_QUANTITY_FACTOR as NET_EXPLOSIVE_QUANTITY_FACTOR", 
	"EXP_OUTPUT_VALIDATOR___HAZMAT_EXEMPTION_REF_NUMBR as HAZMAT_EXEMPTION_REF_NUMBR", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_6 as RQ_FIELD_6", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_7 as RQ_FIELD_7", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_8 as RQ_FIELD_8", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_9 as RQ_FIELD_9", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_10 as RQ_FIELD_10", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_11 as RQ_FIELD_11", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_12 as RQ_FIELD_12", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_13 as RQ_FIELD_13", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_14 as RQ_FIELD_14", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_15 as RQ_FIELD_15", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_16 as RQ_FIELD_16", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_17 as RQ_FIELD_17", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_18 as RQ_FIELD_18", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_19 as RQ_FIELD_19", 
	"EXP_OUTPUT_VALIDATOR___RQ_FIELD_20 as RQ_FIELD_20", 
	"EXP_OUTPUT_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_OUTPUT_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_OUTPUT_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)),lit(0)).when(col('o_UPDATE_VALIDATOR') ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_UN_NUMBER1, type TARGET 
# COLUMN COUNT: 75


Shortcut_to_WM_UN_NUMBER1 = UPD_INS_UPD.selectExpr(
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(UN_NUMBER_ID AS BIGINT) as WM_UN_NBR_ID",
	"CAST(TC_COMPANY_ID AS INT) as WM_TC_COMPANY_ID",
	"CAST(UN_NUMBER_VALUE AS STRING) as WM_UN_NBR_VALUE",
	"CAST(UN_NUMBER_DESCRIPTION AS STRING) as WM_UN_NBR_DESC",
	"CAST(ADDL_DESC AS STRING) as ADDL_DESC",
	"CAST(TECHNICAL_NAME AS STRING) as TECHNICAL_NAME",
	"CAST(UN_NUMBER AS BIGINT) as WM_UN_NBR",
	"CAST(UN_NUMBER_VERSION AS STRING) as WM_UN_NBR_VERSION",
	"CAST(HAZMAT_UOM AS INT) as WM_HAZMAT_UOM",
	"CAST(MAX_HAZMAT_QTY AS DECIMAL(13,4)) as MAX_HAZMAT_QTY",
	"CAST(REGULATION_SET AS STRING) as REGULATION_SET",
	"CAST(TRANS_MODE AS STRING) as WM_TRANS_MODE",
	"CAST(REPORTABLE_QTY AS TINYINT) as REPORTABLE_QTY_FLAG",
	"CAST(IS_ACCESSIBLE AS TINYINT) as ACCESSIBLE_FLAG",
	"CAST(RESIDUE_INDICATOR_CODE AS STRING) as RESIDUE_INDICATOR_CD",
	"CAST(HAZMAT_CLASS_QUAL AS STRING) as HAZMAT_CLASS_QUAL",
	"CAST(NOS_INDICATOR_CODE AS TINYINT) as NOS_INDICATOR_CD",
	"CAST(HAZMAT_MATERIAL_QUAL AS STRING) as HAZMAT_MATERIAL_QUAL",
	"CAST(FLASH_POINT_TEMPERATURE AS DECIMAL(13,4)) as FLASH_POINT_TEMPERATURE",
	"CAST(FLASH_POINT_TEMPERATURE_UOM AS STRING) as FLASH_POINT_TEMPERATURE_UOM",
	"CAST(HAZARD_ZONE_CODE AS STRING) as HAZARD_ZONE_CD",
	"CAST(RADIOACTIVE_QTY_CALC_FACTOR AS DECIMAL(13,4)) as RADIOACTIVE_QTY_CALC_FACTOR",
	"CAST(RADIOACTIVE_QUANTITY_UOM AS STRING) as RADIOACTIVE_QUANTITY_UOM",
	"CAST(EPAWASTESTREAM_NUMBER AS STRING) as EPA_WASTE_STREAM_NBR",
	"CAST(WASTE_CHARACTERISTIC_CODE AS STRING) as WASTE_CHARACTERISTIC_CODE",
	"CAST(NET_EXPLOSIVE_QUANTITY_FACTOR AS DECIMAL(13,4)) as NET_EXPLOSIVE_QUANTITY_FACTOR",
	"CAST(HAZMAT_EXEMPTION_REF_NUMBR AS STRING) as HAZMAT_EXEMPTION_REF_NBR",
	"CAST(UNN_CLASS_DIV AS STRING) as UNN_CLASS_DIV",
	"CAST(UNN_SUB_RISK AS STRING) as UNN_SUB_RISK",
	"CAST(UNN_HAZARD_LABEL AS STRING) as UNN_HAZARD_LABEL",
	"CAST(UNN_PG AS STRING) as UNN_PG",
	"CAST(UNN_SP AS STRING) as UNN_SP",
	"CAST(UNN_AIR_BOTH_INST_LTD AS STRING) as UNN_AIR_BOTH_LTD_INSTRUCTIONS",
	"CAST(UNN_AIR_BOTH_QTY_LTD AS STRING) as UNN_AIR_BOTH_LTD_QTY",
	"CAST(UNN_AIR_BOTH_INST AS STRING) as UNN_AIR_BOTH_INSTRUCTIONS",
	"CAST(UNN_AIR_BOTH_QTY AS STRING) as UNN_AIR_BOTH_QTY",
	"CAST(UNN_AIR_CARGO_INST AS STRING) as UNN_AIR_CARGO_INSTRUCTIONS",
	"CAST(UNN_AIR_CARGO_QTY AS STRING) as UNN_AIR_CARGO_QTY",
	"CAST(CONTACT_NAME AS STRING) as CONTACT_NAME",
	"CAST(PHONE_NUMBER AS STRING) as PHONE_NBR",
	"CAST(HZC_FIELD_1 AS STRING) as HZC_FIELD_1",
	"CAST(HZC_FIELD_2 AS STRING) as HZC_FIELD_2",
	"CAST(HZC_FIELD_3 AS STRING) as HZC_FIELD_3",
	"CAST(HZC_FIELD_4 AS STRING) as HZC_FIELD_4",
	"CAST(HZC_FIELD_5 AS STRING) as HZC_FIELD_5",
	"CAST(RQ_FIELD_1 AS STRING) as RQ_FIELD_1",
	"CAST(RQ_FIELD_2 AS STRING) as RQ_FIELD_2",
	"CAST(RQ_FIELD_3 AS STRING) as RQ_FIELD_3",
	"CAST(RQ_FIELD_4 AS STRING) as RQ_FIELD_4",
	"CAST(RQ_FIELD_5 AS STRING) as RQ_FIELD_5",
	"CAST(RQ_FIELD_6 AS STRING) as RQ_FIELD_6",
	"CAST(RQ_FIELD_7 AS STRING) as RQ_FIELD_7",
	"CAST(RQ_FIELD_8 AS STRING) as RQ_FIELD_8",
	"CAST(RQ_FIELD_9 AS STRING) as RQ_FIELD_9",
	"CAST(RQ_FIELD_10 AS STRING) as RQ_FIELD_10",
	"CAST(RQ_FIELD_11 AS STRING) as RQ_FIELD_11",
	"CAST(RQ_FIELD_12 AS STRING) as RQ_FIELD_12",
	"CAST(RQ_FIELD_13 AS STRING) as RQ_FIELD_13",
	"CAST(RQ_FIELD_14 AS STRING) as RQ_FIELD_14",
	"CAST(RQ_FIELD_15 AS STRING) as RQ_FIELD_15",
	"CAST(RQ_FIELD_16 AS STRING) as RQ_FIELD_16",
	"CAST(RQ_FIELD_17 AS STRING) as RQ_FIELD_17",
	"CAST(RQ_FIELD_18 AS STRING) as RQ_FIELD_18",
	"CAST(RQ_FIELD_19 AS STRING) as RQ_FIELD_19",
	"CAST(RQ_FIELD_20 AS STRING) as RQ_FIELD_20",
	"CAST(MARK_FOR_DELETION AS TINYINT) as MARK_FOR_DELETION_FLAG",
	"CAST(CREATED_SOURCE_TYPE AS TINYINT) as WM_CREATED_SOURCE_TYPE",
	"CAST(CREATED_SOURCE AS STRING) as WM_CREATED_SOURCE",
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP",
	"CAST(LAST_UPDATED_SOURCE_TYPE AS TINYINT) as WM_LAST_UPDATED_SOURCE_TYPE",
	"CAST(LAST_UPDATED_SOURCE AS STRING) as WM_LAST_UPDATED_SOURCE",
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_UN_NBR_ID = target.WM_UN_NBR_ID"""
  # refined_perf_table = "WM_UN_NUMBER"
  executeMerge(Shortcut_to_WM_UN_NUMBER1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_UN_NUMBER", "WM_UN_NUMBER", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_UN_NUMBER", "WM_UN_NUMBER","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	