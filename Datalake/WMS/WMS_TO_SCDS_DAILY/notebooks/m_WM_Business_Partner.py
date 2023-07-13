#Code converted on 2023-06-20 18:37:41
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
refined_perf_table = f"{refine}.WM_BUSINESS_PARTNER"
raw_perf_table = f"{raw}.WM_BUSINESS_PARTNER_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"



# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE, type SOURCE 
# COLUMN COUNT: 31

SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE = spark.sql(f"""SELECT
DC_NBR,
TC_COMPANY_ID,
BUSINESS_PARTNER_ID,
DESCRIPTION,
MARK_FOR_DELETION,
BP_ID,
BP_COMPANY_ID,
ACCREDITED_BP,
BUSINESS_NUMBER,
ADDRESS_1,
CITY,
STATE_PROV,
POSTAL_CODE,
COUNTY,
COUNTRY_CODE,
COMMENTS,
TEL_NBR,
LAST_UPDATED_SOURCE,
CREATED_DTTM,
LAST_UPDATED_DTTM,
CREATED_SOURCE,
ADDRESS_2,
ADDRESS_3,
HIBERNATE_VERSION,
PREFIX,
ATTRIBUTE_1,
ATTRIBUTE_2,
ATTRIBUTE_3,
ATTRIBUTE_4,
ATTRIBUTE_5,
LOAD_TSTMP
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_BUSINESS_PARTNER, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_WM_BUSINESS_PARTNER = spark.sql(f"""SELECT
LOCATION_ID,
WM_TC_COMPANY_ID,
WM_BUSINESS_PARTNER_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP
FROM {refined_perf_table}
WHERE WM_BUSINESS_PARTNER_ID IN (SELECT BUSINESS_PARTNER_ID FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE_temp = SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE.toDF(*["SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___" + col for col in SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE.columns])

EXP_INT_CONV = SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE_temp.selectExpr( \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___sys_row_id as sys_row_id", \
	"cast(SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___DC_NBR as int) as o_DC_NBR", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___DESCRIPTION as DESCRIPTION", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___BP_ID as BP_ID", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___BP_COMPANY_ID as BP_COMPANY_ID", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___ACCREDITED_BP as ACCREDITED_BP", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___BUSINESS_NUMBER as BUSINESS_NUMBER", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___ADDRESS_1 as ADDRESS_1", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___CITY as CITY", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___STATE_PROV as STATE_PROV", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___POSTAL_CODE as POSTAL_CODE", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___COUNTY as COUNTY", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___COUNTRY_CODE as COUNTRY_CODE", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___COMMENTS as COMMENTS", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___TEL_NBR as TEL_NBR", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___CREATED_DTTM as CREATED_DTTM", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___CREATED_SOURCE as CREATED_SOURCE", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___ADDRESS_2 as ADDRESS_2", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___ADDRESS_3 as ADDRESS_3", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___PREFIX as PREFIX", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___ATTRIBUTE_1 as ATTRIBUTE_1", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___ATTRIBUTE_2 as ATTRIBUTE_2", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___ATTRIBUTE_3 as ATTRIBUTE_3", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___ATTRIBUTE_4 as ATTRIBUTE_4", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___ATTRIBUTE_5 as ATTRIBUTE_5", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER_PRE___LOAD_TSTMP as LOAD_TSTMP" \
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
EXP_INT_CONV_temp = EXP_INT_CONV.toDF(*["EXP_INT_CONV___" + col for col in EXP_INT_CONV.columns])
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE_temp.join(EXP_INT_CONV_temp,[SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR == EXP_INT_CONV_temp.EXP_INT_CONV___o_DC_NBR],'inner').selectExpr( \
	"EXP_INT_CONV___o_DC_NBR as DC_NBR", \
	"EXP_INT_CONV___TC_COMPANY_ID as TC_COMPANY_ID", \
	"EXP_INT_CONV___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
	"EXP_INT_CONV___DESCRIPTION as DESCRIPTION", \
	"EXP_INT_CONV___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"EXP_INT_CONV___BP_ID as BP_ID", \
	"EXP_INT_CONV___BP_COMPANY_ID as BP_COMPANY_ID", \
	"EXP_INT_CONV___ACCREDITED_BP as ACCREDITED_BP", \
	"EXP_INT_CONV___BUSINESS_NUMBER as BUSINESS_NUMBER", \
	"EXP_INT_CONV___ADDRESS_1 as ADDRESS_1", \
	"EXP_INT_CONV___CITY as CITY", \
	"EXP_INT_CONV___STATE_PROV as STATE_PROV", \
	"EXP_INT_CONV___POSTAL_CODE as POSTAL_CODE", \
	"EXP_INT_CONV___COUNTY as COUNTY", \
	"EXP_INT_CONV___COUNTRY_CODE as COUNTRY_CODE", \
	"EXP_INT_CONV___COMMENTS as COMMENTS", \
	"EXP_INT_CONV___TEL_NBR as TEL_NBR", \
	"EXP_INT_CONV___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"EXP_INT_CONV___CREATED_DTTM as CREATED_DTTM", \
	"EXP_INT_CONV___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"EXP_INT_CONV___CREATED_SOURCE as CREATED_SOURCE", \
	"EXP_INT_CONV___ADDRESS_2 as ADDRESS_2", \
	"EXP_INT_CONV___ADDRESS_3 as ADDRESS_3", \
	"EXP_INT_CONV___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"EXP_INT_CONV___PREFIX as PREFIX", \
	"EXP_INT_CONV___ATTRIBUTE_1 as ATTRIBUTE_1", \
	"EXP_INT_CONV___ATTRIBUTE_2 as ATTRIBUTE_2", \
	"EXP_INT_CONV___ATTRIBUTE_3 as ATTRIBUTE_3", \
	"EXP_INT_CONV___ATTRIBUTE_4 as ATTRIBUTE_4", \
	"EXP_INT_CONV___ATTRIBUTE_5 as ATTRIBUTE_5", \
	"EXP_INT_CONV___LOAD_TSTMP as LOAD_TSTMP", \
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR")

# COMMAND ----------
# Processing node JNR_WM_BUSINESS_PARTNER, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 36

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_BUSINESS_PARTNER_temp = SQ_Shortcut_to_WM_BUSINESS_PARTNER.toDF(*["SQ_Shortcut_to_WM_BUSINESS_PARTNER___" + col for col in SQ_Shortcut_to_WM_BUSINESS_PARTNER.columns])

JNR_WM_BUSINESS_PARTNER = SQ_Shortcut_to_WM_BUSINESS_PARTNER_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_BUSINESS_PARTNER_temp.SQ_Shortcut_to_WM_BUSINESS_PARTNER___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID, SQ_Shortcut_to_WM_BUSINESS_PARTNER_temp.SQ_Shortcut_to_WM_BUSINESS_PARTNER___WM_TC_COMPANY_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___TC_COMPANY_ID, SQ_Shortcut_to_WM_BUSINESS_PARTNER_temp.SQ_Shortcut_to_WM_BUSINESS_PARTNER___WM_BUSINESS_PARTNER_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___BUSINESS_PARTNER_ID],'right_outer').selectExpr( \
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", \
	"JNR_SITE_PROFILE___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_SITE_PROFILE___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
	"JNR_SITE_PROFILE___DESCRIPTION as DESCRIPTION", \
	"JNR_SITE_PROFILE___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"JNR_SITE_PROFILE___BP_ID as BP_ID", \
	"JNR_SITE_PROFILE___BP_COMPANY_ID as BP_COMPANY_ID", \
	"JNR_SITE_PROFILE___ACCREDITED_BP as ACCREDITED_BP", \
	"JNR_SITE_PROFILE___BUSINESS_NUMBER as BUSINESS_NUMBER", \
	"JNR_SITE_PROFILE___ADDRESS_1 as ADDRESS_1", \
	"JNR_SITE_PROFILE___CITY as CITY", \
	"JNR_SITE_PROFILE___STATE_PROV as STATE_PROV", \
	"JNR_SITE_PROFILE___POSTAL_CODE as POSTAL_CODE", \
	"JNR_SITE_PROFILE___COUNTY as COUNTY", \
	"JNR_SITE_PROFILE___COUNTRY_CODE as COUNTRY_CODE", \
	"JNR_SITE_PROFILE___COMMENTS as COMMENTS", \
	"JNR_SITE_PROFILE___TEL_NBR as TEL_NBR", \
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", \
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_SITE_PROFILE___ADDRESS_2 as ADDRESS_2", \
	"JNR_SITE_PROFILE___ADDRESS_3 as ADDRESS_3", \
	"JNR_SITE_PROFILE___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"JNR_SITE_PROFILE___PREFIX as PREFIX", \
	"JNR_SITE_PROFILE___ATTRIBUTE_1 as ATTRIBUTE_1", \
	"JNR_SITE_PROFILE___ATTRIBUTE_2 as ATTRIBUTE_2", \
	"JNR_SITE_PROFILE___ATTRIBUTE_3 as ATTRIBUTE_3", \
	"JNR_SITE_PROFILE___ATTRIBUTE_4 as ATTRIBUTE_4", \
	"JNR_SITE_PROFILE___ATTRIBUTE_5 as ATTRIBUTE_5", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER___LOAD_TSTMP as in_LOAD_TSTMP", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER___LOCATION_ID as in_LOCATION_ID", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER___WM_TC_COMPANY_ID as in_WM_TC_COMPANY_ID", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER___WM_BUSINESS_PARTNER_ID as in_WM_BUSINESS_PARTNER_ID", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER___WM_CREATED_TSTMP as in_WM_CREATED_TSTMP", \
	"SQ_Shortcut_to_WM_BUSINESS_PARTNER___WM_LAST_UPDATED_TSTMP as in_WM_LAST_UPDATED_TSTMP")

# COMMAND ----------
# Processing node FIL_NO_CHANGE_REC, type FILTER 
# COLUMN COUNT: 36

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_BUSINESS_PARTNER_temp = JNR_WM_BUSINESS_PARTNER.toDF(*["JNR_WM_BUSINESS_PARTNER___" + col for col in JNR_WM_BUSINESS_PARTNER.columns])

FIL_NO_CHANGE_REC = JNR_WM_BUSINESS_PARTNER_temp.selectExpr( \
	"JNR_WM_BUSINESS_PARTNER___LOCATION_ID as LOCATION_ID", \
	"JNR_WM_BUSINESS_PARTNER___TC_COMPANY_ID as TC_COMPANY_ID", \
	"JNR_WM_BUSINESS_PARTNER___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
	"JNR_WM_BUSINESS_PARTNER___DESCRIPTION as DESCRIPTION", \
	"JNR_WM_BUSINESS_PARTNER___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"JNR_WM_BUSINESS_PARTNER___BP_ID as BP_ID", \
	"JNR_WM_BUSINESS_PARTNER___BP_COMPANY_ID as BP_COMPANY_ID", \
	"JNR_WM_BUSINESS_PARTNER___ACCREDITED_BP as ACCREDITED_BP", \
	"JNR_WM_BUSINESS_PARTNER___BUSINESS_NUMBER as BUSINESS_NUMBER", \
	"JNR_WM_BUSINESS_PARTNER___ADDRESS_1 as ADDRESS_1", \
	"JNR_WM_BUSINESS_PARTNER___CITY as CITY", \
	"JNR_WM_BUSINESS_PARTNER___STATE_PROV as STATE_PROV", \
	"JNR_WM_BUSINESS_PARTNER___POSTAL_CODE as POSTAL_CODE", \
	"JNR_WM_BUSINESS_PARTNER___COUNTY as COUNTY", \
	"JNR_WM_BUSINESS_PARTNER___COUNTRY_CODE as COUNTRY_CODE", \
	"JNR_WM_BUSINESS_PARTNER___COMMENTS as COMMENTS", \
	"JNR_WM_BUSINESS_PARTNER___TEL_NBR as TEL_NBR", \
	"JNR_WM_BUSINESS_PARTNER___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"JNR_WM_BUSINESS_PARTNER___CREATED_DTTM as CREATED_DTTM", \
	"JNR_WM_BUSINESS_PARTNER___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"JNR_WM_BUSINESS_PARTNER___CREATED_SOURCE as CREATED_SOURCE", \
	"JNR_WM_BUSINESS_PARTNER___ADDRESS_2 as ADDRESS_2", \
	"JNR_WM_BUSINESS_PARTNER___ADDRESS_3 as ADDRESS_3", \
	"JNR_WM_BUSINESS_PARTNER___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"JNR_WM_BUSINESS_PARTNER___PREFIX as PREFIX", \
	"JNR_WM_BUSINESS_PARTNER___ATTRIBUTE_1 as ATTRIBUTE_1", \
	"JNR_WM_BUSINESS_PARTNER___ATTRIBUTE_2 as ATTRIBUTE_2", \
	"JNR_WM_BUSINESS_PARTNER___ATTRIBUTE_3 as ATTRIBUTE_3", \
	"JNR_WM_BUSINESS_PARTNER___ATTRIBUTE_4 as ATTRIBUTE_4", \
	"JNR_WM_BUSINESS_PARTNER___ATTRIBUTE_5 as ATTRIBUTE_5", \
	"JNR_WM_BUSINESS_PARTNER___in_LOAD_TSTMP as in_LOAD_TSTMP", \
	"JNR_WM_BUSINESS_PARTNER___in_LOCATION_ID as in_LOCATION_ID", \
	"JNR_WM_BUSINESS_PARTNER___in_WM_TC_COMPANY_ID as in_WM_TC_COMPANY_ID", \
	"JNR_WM_BUSINESS_PARTNER___in_WM_BUSINESS_PARTNER_ID as in_WM_BUSINESS_PARTNER_ID", \
	"JNR_WM_BUSINESS_PARTNER___in_WM_CREATED_TSTMP as in_WM_CREATED_TSTMP", \
	"JNR_WM_BUSINESS_PARTNER___in_WM_LAST_UPDATED_TSTMP as in_WM_LAST_UPDATED_TSTMP") \
    .filter("( in_WM_TC_COMPANY_ID is Null OR in_WM_BUSINESS_PARTNER_ID is Null ) OR ( (  in_WM_TC_COMPANY_ID isnot Null OR  in_WM_BUSINESS_PARTNER_ID is not Null ) AND \
             ( COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(in_WM_CREATED_TSTMP, date'1900-01-01') \
             OR COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(in_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))").withColumn("sys_row_id", monotonically_increasing_id())



# COMMAND ----------
# Processing node EXP_EVAL_VALUES, type EXPRESSION 
# COLUMN COUNT: 37

# for each involved DataFrame, append the dataframe name to each column
FIL_NO_CHANGE_REC_temp = FIL_NO_CHANGE_REC.toDF(*["FIL_NO_CHANGE_REC___" + col for col in FIL_NO_CHANGE_REC.columns])

EXP_EVAL_VALUES = FIL_NO_CHANGE_REC_temp.selectExpr( \
	"FIL_NO_CHANGE_REC___sys_row_id as sys_row_id", \
	"FIL_NO_CHANGE_REC___LOCATION_ID as LOCATION_ID", \
	"FIL_NO_CHANGE_REC___TC_COMPANY_ID as TC_COMPANY_ID", \
	"FIL_NO_CHANGE_REC___BUSINESS_PARTNER_ID as BUSINESS_PARTNER_ID", \
	"FIL_NO_CHANGE_REC___DESCRIPTION as DESCRIPTION", \
	"FIL_NO_CHANGE_REC___MARK_FOR_DELETION as MARK_FOR_DELETION", \
	"FIL_NO_CHANGE_REC___BP_ID as BP_ID", \
	"FIL_NO_CHANGE_REC___BP_COMPANY_ID as BP_COMPANY_ID", \
	"FIL_NO_CHANGE_REC___ACCREDITED_BP as ACCREDITED_BP", \
	"FIL_NO_CHANGE_REC___BUSINESS_NUMBER as BUSINESS_NUMBER", \
	"FIL_NO_CHANGE_REC___ADDRESS_1 as ADDRESS_1", \
	"FIL_NO_CHANGE_REC___CITY as CITY", \
	"FIL_NO_CHANGE_REC___STATE_PROV as STATE_PROV", \
	"FIL_NO_CHANGE_REC___POSTAL_CODE as POSTAL_CODE", \
	"FIL_NO_CHANGE_REC___COUNTY as COUNTY", \
	"FIL_NO_CHANGE_REC___COUNTRY_CODE as COUNTRY_CODE", \
	"FIL_NO_CHANGE_REC___COMMENTS as COMMENTS", \
	"FIL_NO_CHANGE_REC___TEL_NBR as TEL_NBR", \
	"FIL_NO_CHANGE_REC___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", \
	"FIL_NO_CHANGE_REC___CREATED_DTTM as CREATED_DTTM", \
	"FIL_NO_CHANGE_REC___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", \
	"FIL_NO_CHANGE_REC___CREATED_SOURCE as CREATED_SOURCE", \
	"FIL_NO_CHANGE_REC___ADDRESS_2 as ADDRESS_2", \
	"FIL_NO_CHANGE_REC___ADDRESS_3 as ADDRESS_3", \
	"FIL_NO_CHANGE_REC___HIBERNATE_VERSION as HIBERNATE_VERSION", \
	"FIL_NO_CHANGE_REC___PREFIX as PREFIX", \
	"FIL_NO_CHANGE_REC___ATTRIBUTE_1 as ATTRIBUTE_1", \
	"FIL_NO_CHANGE_REC___ATTRIBUTE_2 as ATTRIBUTE_2", \
	"FIL_NO_CHANGE_REC___ATTRIBUTE_3 as ATTRIBUTE_3", \
	"FIL_NO_CHANGE_REC___ATTRIBUTE_4 as ATTRIBUTE_4", \
	"FIL_NO_CHANGE_REC___ATTRIBUTE_5 as ATTRIBUTE_5", \
	"IF(FIL_NO_CHANGE_REC___in_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_NO_CHANGE_REC___in_LOAD_TSTMP) as LOAD_TSTMP", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"FIL_NO_CHANGE_REC___in_LOCATION_ID as in_LOCATION_ID", \
	"FIL_NO_CHANGE_REC___in_WM_TC_COMPANY_ID as in_WM_TC_COMPANY_ID", \
	"FIL_NO_CHANGE_REC___in_WM_BUSINESS_PARTNER_ID as in_WM_BUSINESS_PARTNER_ID", \
	"FIL_NO_CHANGE_REC___in_WM_CREATED_TSTMP as in_WM_CREATED_TSTMP", \
	"FIL_NO_CHANGE_REC___in_WM_LAST_UPDATED_TSTMP as in_WM_LAST_UPDATED_TSTMP" \
)

# COMMAND ----------
# Processing node UPD_VALIDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
EXP_EVAL_VALUES_temp = EXP_EVAL_VALUES.toDF(*["EXP_EVAL_VALUES___" + col for col in EXP_EVAL_VALUES.columns])

UPD_VALIDATE = EXP_EVAL_VALUES_temp.selectExpr( \
	"EXP_EVAL_VALUES___LOCATION_ID as LOCATION_ID", \
	"EXP_EVAL_VALUES___TC_COMPANY_ID as WM_TC_COMPANY_ID1", \
	"EXP_EVAL_VALUES___BUSINESS_PARTNER_ID as WM_BUSINESS_PARTNER_ID", \
	"EXP_EVAL_VALUES___DESCRIPTION as WM_BUSINESSS_PARTNER_DESC", \
	"EXP_EVAL_VALUES___MARK_FOR_DELETION as MARK_FOR_DELETION_FLAG", \
	"EXP_EVAL_VALUES___BP_ID as WM_BP_ID", \
	"EXP_EVAL_VALUES___BP_COMPANY_ID as WM_BP_COMPANY_ID", \
	"EXP_EVAL_VALUES___ACCREDITED_BP as ACCREDITED_BP_FLAG", \
	"EXP_EVAL_VALUES___BUSINESS_NUMBER as WM_BUSINESS_NBR", \
	"EXP_EVAL_VALUES___ADDRESS_1 as ADDR_1", \
	"EXP_EVAL_VALUES___CITY as CITY", \
	"EXP_EVAL_VALUES___STATE_PROV as STATE_PROV", \
	"EXP_EVAL_VALUES___POSTAL_CODE as POSTAL_CD", \
	"EXP_EVAL_VALUES___COUNTY as COUNTY", \
	"EXP_EVAL_VALUES___COUNTRY_CODE as COUNTRY_CD", \
	"EXP_EVAL_VALUES___COMMENTS as COMMENTS", \
	"EXP_EVAL_VALUES___TEL_NBR as PHONE_NBR", \
	"EXP_EVAL_VALUES___LAST_UPDATED_SOURCE as WM_LAST_UPDATED_SOURCE", \
	"EXP_EVAL_VALUES___CREATED_DTTM as WM_CREATED_TSTMP", \
	"EXP_EVAL_VALUES___LAST_UPDATED_DTTM as WM_LAST_UPDATED_TSTMP", \
	"EXP_EVAL_VALUES___CREATED_SOURCE as WM_CREATED_SOURCE", \
	"EXP_EVAL_VALUES___ADDRESS_2 as ADDR_2", \
	"EXP_EVAL_VALUES___ADDRESS_3 as ADDR_3", \
	"EXP_EVAL_VALUES___HIBERNATE_VERSION as WM_HIBERNATE_VERSION", \
	"EXP_EVAL_VALUES___PREFIX as PREFIX", \
	"EXP_EVAL_VALUES___ATTRIBUTE_1 as ATTRIBUTE_1", \
	"EXP_EVAL_VALUES___ATTRIBUTE_2 as ATTRIBUTE_2", \
	"EXP_EVAL_VALUES___ATTRIBUTE_3 as ATTRIBUTE_3", \
	"EXP_EVAL_VALUES___ATTRIBUTE_4 as ATTRIBUTE_4", \
	"EXP_EVAL_VALUES___ATTRIBUTE_5 as ATTRIBUTE_5", \
	"EXP_EVAL_VALUES___LOAD_TSTMP as LOAD_TSTMP", \
	"EXP_EVAL_VALUES___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_EVAL_VALUES___in_WM_TC_COMPANY_ID as in_WM_TC_COMPANY_ID") \
	.withColumn('pyspark_data_action', when((in_WM_TC_COMPANY_ID.isNull()) ,(lit(0))).otherwise(lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_BUSINESS_PARTNER, type TARGET 
# COLUMN COUNT: 32

Shortcut_to_WM_BUSINESS_PARTNER = UPD_VALIDATE.selectExpr( 
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", 
	"CAST(WM_TC_COMPANY_ID1 AS BIGINT) as WM_TC_COMPANY_ID", 
	"CAST(WM_BUSINESS_PARTNER_ID AS STRING) as WM_BUSINESS_PARTNER_ID", 
	"CAST(WM_BUSINESSS_PARTNER_DESC AS STRING) as WM_BUSINESSS_PARTNER_DESC", 
	"CAST(WM_BP_ID AS BIGINT) as WM_BP_ID", 
	"CAST(WM_BP_COMPANY_ID AS BIGINT) as WM_BP_COMPANY_ID", 
	"CAST(WM_BUSINESS_NBR AS STRING) as WM_BUSINESS_NBR", 
	"CAST(ACCREDITED_BP_FLAG AS BIGINT) as ACCREDITED_BP_FLAG", 
	"CAST(PREFIX AS STRING) as PREFIX", 
	"CAST(ADDR_1 AS STRING) as ADDR_1", 
	"CAST(ADDR_2 AS STRING) as ADDR_2", 
	"CAST(ADDR_3 AS STRING) as ADDR_3", 
	"CAST(CITY AS STRING) as CITY", 
	"CAST(STATE_PROV AS STRING) as STATE_PROV", 
	"CAST(POSTAL_CD AS STRING) as POSTAL_CD", 
	"CAST(COUNTY AS STRING) as COUNTY", 
	"CAST(COUNTRY_CD AS STRING) as COUNTRY_CD", 
	"CAST(PHONE_NBR AS STRING) as PHONE_NBR", 
	"CAST(ATTRIBUTE_1 AS BIGINT) as ATTR_1", 
	"CAST(ATTRIBUTE_2 AS STRING) as ATTR_2", 
	"CAST(ATTRIBUTE_3 AS STRING) as ATTR_3", 
	"CAST(ATTRIBUTE_4 AS STRING) as ATTR_4", 
	"CAST(ATTRIBUTE_5 AS STRING) as ATTR_5", 
	"CAST(COMMENTS AS STRING) as WM_COMMENT", 
	"CAST(MARK_FOR_DELETION_FLAG AS BIGINT) as MARK_FOR_DELETION_FLAG", 
	"CAST(WM_HIBERNATE_VERSION AS BIGINT) as WM_HIBERNATE_VERSION", 
	"CAST(WM_CREATED_SOURCE AS STRING) as WM_CREATED_SOURCE", 
	"CAST(WM_CREATED_TSTMP AS TIMESTAMP) as WM_CREATED_TSTMP", 
	"CAST(WM_LAST_UPDATED_SOURCE AS STRING) as WM_LAST_UPDATED_SOURCE", 
	"CAST(WM_LAST_UPDATED_TSTMP AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", 
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", 
    "pyspark_data_action" 
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.WM_TC_COMPANY_ID = target.WM_TC_COMPANY_ID AND source.WM_BUSINESS_PARTNER_ID = target.WM_BUSINESS_PARTNER_ID"""
#   refined_perf_table = "WM_BUSINESS_PARTNER"
  executeMerge(Shortcut_to_WM_BUSINESS_PARTNER, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_BUSINESS_PARTNER", "WM_BUSINESS_PARTNER", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_BUSINESS_PARTNER", "WM_BUSINESS_PARTNER","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	