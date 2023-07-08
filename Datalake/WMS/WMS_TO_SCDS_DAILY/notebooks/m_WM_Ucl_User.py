#Code converted on 2023-06-22 21:03:38
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
refined_perf_table = f"{refine}.WM_UCL_USER"
raw_perf_table = f"{raw}.WM_UCL_USER_PRE"
site_profile_table = f"{legacy}.SITE_PROFILE"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_UCL_USER_PRE, type SOURCE 
# COLUMN COUNT: 53

SQ_Shortcut_to_WM_UCL_USER_PRE = spark.sql(f"""SELECT
DC_NBR,
UCL_USER_ID,
COMPANY_ID,
USER_NAME,
USER_PASSWORD,
IS_ACTIVE,
CREATED_SOURCE_TYPE_ID,
CREATED_SOURCE,
CREATED_DTTM,
LAST_UPDATED_SOURCE_TYPE_ID,
LAST_UPDATED_SOURCE,
LAST_UPDATED_DTTM,
USER_TYPE_ID,
LOCALE_ID,
LOCATION_ID,
USER_FIRST_NAME,
USER_MIDDLE_NAME,
USER_LAST_NAME,
USER_PREFIX,
USER_TITLE,
TELEPHONE_NUMBER,
FAX_NUMBER,
ADDRESS_1,
ADDRESS_2,
CITY,
STATE_PROV_CODE,
POSTAL_CODE,
COUNTRY_CODE,
USER_EMAIL_1,
USER_EMAIL_2,
COMM_METHOD_ID_DURING_BH_1,
COMM_METHOD_ID_DURING_BH_2,
COMM_METHOD_ID_AFTER_BH_1,
COMM_METHOD_ID_AFTER_BH_2,
COMMON_NAME,
LAST_PASSWORD_CHANGE_DTTM,
LOGGED_IN,
LAST_LOGIN_DTTM,
DEFAULT_BUSINESS_UNIT_ID,
DEFAULT_WHSE_REGION_ID,
CHANNEL_ID,
HIBERNATE_VERSION,
NUMBER_OF_INVALID_LOGINS,
TAX_ID_NBR,
EMP_START_DATE,
BIRTH_DATE,
GENDER_ID,
PASSWORD_RESET_DATE_TIME,
PASSWORD_TOKEN,
ISPASSWORDMANAGEDINTERNALLY,
COPY_FROM_USER,
EXTERNAL_USER_ID,
SECURITY_POLICY_GROUP_ID
FROM {raw_perf_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_UCL_USER, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_WM_UCL_USER = spark.sql(f"""SELECT
LOCATION_ID,
WM_UCL_USER_ID,
WM_CREATED_TSTMP,
WM_LAST_UPDATED_TSTMP,
LOAD_TSTMP,
USER_NAME
FROM {refined_perf_table}
WHERE USER_NAME IN (SELECT USER_NAME FROM {raw_perf_table})""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 53

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_UCL_USER_PRE_temp = SQ_Shortcut_to_WM_UCL_USER_PRE.toDF(*["SQ_Shortcut_to_WM_UCL_USER_PRE___" + col for col in SQ_Shortcut_to_WM_UCL_USER_PRE.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_UCL_USER_PRE_temp.selectExpr( 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___sys_row_id as sys_row_id", 
	"cast(SQ_Shortcut_to_WM_UCL_USER_PRE___DC_NBR as int) as o_DC_NBR", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___UCL_USER_ID as UCL_USER_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___COMPANY_ID as COMPANY_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_NAME as USER_NAME", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_PASSWORD as USER_PASSWORD", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___IS_ACTIVE as IS_ACTIVE", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___CREATED_SOURCE_TYPE_ID as CREATED_SOURCE_TYPE_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___CREATED_SOURCE as CREATED_SOURCE", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___LAST_UPDATED_SOURCE_TYPE_ID as LAST_UPDATED_SOURCE_TYPE_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_TYPE_ID as USER_TYPE_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___LOCALE_ID as LOCALE_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___LOCATION_ID as LOCATION_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_FIRST_NAME as USER_FIRST_NAME", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_MIDDLE_NAME as USER_MIDDLE_NAME", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_LAST_NAME as USER_LAST_NAME", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_PREFIX as USER_PREFIX", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_TITLE as USER_TITLE", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___TELEPHONE_NUMBER as TELEPHONE_NUMBER", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___FAX_NUMBER as FAX_NUMBER", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___ADDRESS_1 as ADDRESS_1", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___ADDRESS_2 as ADDRESS_2", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___CITY as CITY", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___STATE_PROV_CODE as STATE_PROV_CODE", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___POSTAL_CODE as POSTAL_CODE", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___COUNTRY_CODE as COUNTRY_CODE", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_EMAIL_1 as USER_EMAIL_1", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___USER_EMAIL_2 as USER_EMAIL_2", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___COMM_METHOD_ID_DURING_BH_1 as COMM_METHOD_ID_DURING_BH_1", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___COMM_METHOD_ID_DURING_BH_2 as COMM_METHOD_ID_DURING_BH_2", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___COMM_METHOD_ID_AFTER_BH_1 as COMM_METHOD_ID_AFTER_BH_1", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___COMM_METHOD_ID_AFTER_BH_2 as COMM_METHOD_ID_AFTER_BH_2", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___COMMON_NAME as COMMON_NAME", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___LAST_PASSWORD_CHANGE_DTTM as LAST_PASSWORD_CHANGE_DTTM", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___LOGGED_IN as LOGGED_IN", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___LAST_LOGIN_DTTM as LAST_LOGIN_DTTM", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___DEFAULT_BUSINESS_UNIT_ID as DEFAULT_BUSINESS_UNIT_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___DEFAULT_WHSE_REGION_ID as DEFAULT_WHSE_REGION_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___CHANNEL_ID as CHANNEL_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___HIBERNATE_VERSION as HIBERNATE_VERSION", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___NUMBER_OF_INVALID_LOGINS as NUMBER_OF_INVALID_LOGINS", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___TAX_ID_NBR as TAX_ID_NBR", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___EMP_START_DATE as EMP_START_DATE", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___BIRTH_DATE as BIRTH_DATE", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___GENDER_ID as GENDER_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___PASSWORD_RESET_DATE_TIME as PASSWORD_RESET_DATE_TIME", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___PASSWORD_TOKEN as PASSWORD_TOKEN", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___ISPASSWORDMANAGEDINTERNALLY as ISPASSWORDMANAGEDINTERNALLY", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___COPY_FROM_USER as COPY_FROM_USER", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___EXTERNAL_USER_ID as EXTERNAL_USER_ID", 
	"SQ_Shortcut_to_WM_UCL_USER_PRE___SECURITY_POLICY_GROUP_ID as SECURITY_POLICY_GROUP_ID" 
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT LOCATION_ID, STORE_NBR FROM {site_profile_table}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 55

# for each involved DataFrame, append the dataframe name to each column
EXP_INT_CONVERSION_temp = EXP_INT_CONVERSION.toDF(*["EXP_INT_CONVERSION___" + col for col in EXP_INT_CONVERSION.columns])
SQ_Shortcut_to_SITE_PROFILE_temp = SQ_Shortcut_to_SITE_PROFILE.toDF(*["SQ_Shortcut_to_SITE_PROFILE___" + col for col in SQ_Shortcut_to_SITE_PROFILE.columns])

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE_temp.join(EXP_INT_CONVERSION_temp,[SQ_Shortcut_to_SITE_PROFILE_temp.SQ_Shortcut_to_SITE_PROFILE___STORE_NBR == EXP_INT_CONVERSION_temp.EXP_INT_CONVERSION___o_DC_NBR],'inner').selectExpr( 
	"EXP_INT_CONVERSION___o_DC_NBR as o_DC_NBR", 
	"EXP_INT_CONVERSION___UCL_USER_ID as UCL_USER_ID", 
	"EXP_INT_CONVERSION___COMPANY_ID as COMPANY_ID", 
	"EXP_INT_CONVERSION___USER_NAME as USER_NAME", 
	"EXP_INT_CONVERSION___USER_PASSWORD as USER_PASSWORD", 
	"EXP_INT_CONVERSION___IS_ACTIVE as IS_ACTIVE", 
	"EXP_INT_CONVERSION___CREATED_SOURCE_TYPE_ID as CREATED_SOURCE_TYPE_ID", 
	"EXP_INT_CONVERSION___CREATED_SOURCE as CREATED_SOURCE", 
	"EXP_INT_CONVERSION___CREATED_DTTM as CREATED_DTTM", 
	"EXP_INT_CONVERSION___LAST_UPDATED_SOURCE_TYPE_ID as LAST_UPDATED_SOURCE_TYPE_ID", 
	"EXP_INT_CONVERSION___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"EXP_INT_CONVERSION___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_INT_CONVERSION___USER_TYPE_ID as USER_TYPE_ID", 
	"EXP_INT_CONVERSION___LOCALE_ID as LOCALE_ID", 
	"EXP_INT_CONVERSION___LOCATION_ID as LOCATION_ID", 
	"EXP_INT_CONVERSION___USER_FIRST_NAME as USER_FIRST_NAME", 
	"EXP_INT_CONVERSION___USER_MIDDLE_NAME as USER_MIDDLE_NAME", 
	"EXP_INT_CONVERSION___USER_LAST_NAME as USER_LAST_NAME", 
	"EXP_INT_CONVERSION___USER_PREFIX as USER_PREFIX", 
	"EXP_INT_CONVERSION___USER_TITLE as USER_TITLE", 
	"EXP_INT_CONVERSION___TELEPHONE_NUMBER as TELEPHONE_NUMBER", 
	"EXP_INT_CONVERSION___FAX_NUMBER as FAX_NUMBER", 
	"EXP_INT_CONVERSION___ADDRESS_1 as ADDRESS_1", 
	"EXP_INT_CONVERSION___ADDRESS_2 as ADDRESS_2", 
	"EXP_INT_CONVERSION___CITY as CITY", 
	"EXP_INT_CONVERSION___STATE_PROV_CODE as STATE_PROV_CODE", 
	"EXP_INT_CONVERSION___POSTAL_CODE as POSTAL_CODE", 
	"EXP_INT_CONVERSION___COUNTRY_CODE as COUNTRY_CODE", 
	"EXP_INT_CONVERSION___USER_EMAIL_1 as USER_EMAIL_1", 
	"EXP_INT_CONVERSION___USER_EMAIL_2 as USER_EMAIL_2", 
	"EXP_INT_CONVERSION___COMM_METHOD_ID_DURING_BH_1 as COMM_METHOD_ID_DURING_BH_1", 
	"EXP_INT_CONVERSION___COMM_METHOD_ID_DURING_BH_2 as COMM_METHOD_ID_DURING_BH_2", 
	"EXP_INT_CONVERSION___COMM_METHOD_ID_AFTER_BH_1 as COMM_METHOD_ID_AFTER_BH_1", 
	"EXP_INT_CONVERSION___COMM_METHOD_ID_AFTER_BH_2 as COMM_METHOD_ID_AFTER_BH_2", 
	"EXP_INT_CONVERSION___COMMON_NAME as COMMON_NAME", 
	"EXP_INT_CONVERSION___LAST_PASSWORD_CHANGE_DTTM as LAST_PASSWORD_CHANGE_DTTM", 
	"EXP_INT_CONVERSION___LOGGED_IN as LOGGED_IN", 
	"EXP_INT_CONVERSION___LAST_LOGIN_DTTM as LAST_LOGIN_DTTM", 
	"EXP_INT_CONVERSION___DEFAULT_BUSINESS_UNIT_ID as DEFAULT_BUSINESS_UNIT_ID", 
	"EXP_INT_CONVERSION___DEFAULT_WHSE_REGION_ID as DEFAULT_WHSE_REGION_ID", 
	"EXP_INT_CONVERSION___CHANNEL_ID as CHANNEL_ID", 
	"EXP_INT_CONVERSION___HIBERNATE_VERSION as HIBERNATE_VERSION", 
	"EXP_INT_CONVERSION___NUMBER_OF_INVALID_LOGINS as NUMBER_OF_INVALID_LOGINS", 
	"EXP_INT_CONVERSION___TAX_ID_NBR as TAX_ID_NBR", 
	"EXP_INT_CONVERSION___EMP_START_DATE as EMP_START_DATE", 
	"EXP_INT_CONVERSION___BIRTH_DATE as BIRTH_DATE", 
	"EXP_INT_CONVERSION___GENDER_ID as GENDER_ID", 
	"EXP_INT_CONVERSION___PASSWORD_RESET_DATE_TIME as PASSWORD_RESET_DATE_TIME", 
	"EXP_INT_CONVERSION___PASSWORD_TOKEN as PASSWORD_TOKEN", 
	"EXP_INT_CONVERSION___ISPASSWORDMANAGEDINTERNALLY as ISPASSWORDMANAGEDINTERNALLY", 
	"EXP_INT_CONVERSION___COPY_FROM_USER as COPY_FROM_USER", 
	"EXP_INT_CONVERSION___EXTERNAL_USER_ID as EXTERNAL_USER_ID", 
	"EXP_INT_CONVERSION___SECURITY_POLICY_GROUP_ID as SECURITY_POLICY_GROUP_ID", 
	"SQ_Shortcut_to_SITE_PROFILE___LOCATION_ID as LOCATION_ID1", 
	"SQ_Shortcut_to_SITE_PROFILE___STORE_NBR as STORE_NBR")

# COMMAND ----------
# Processing node JNR_WM_UCL_USER, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 59

# for each involved DataFrame, append the dataframe name to each column
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])
SQ_Shortcut_to_WM_UCL_USER_temp = SQ_Shortcut_to_WM_UCL_USER.toDF(*["SQ_Shortcut_to_WM_UCL_USER___" + col for col in SQ_Shortcut_to_WM_UCL_USER.columns])

JNR_WM_UCL_USER = SQ_Shortcut_to_WM_UCL_USER_temp.join(JNR_SITE_PROFILE_temp,[SQ_Shortcut_to_WM_UCL_USER_temp.SQ_Shortcut_to_WM_UCL_USER___LOCATION_ID == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___LOCATION_ID1, SQ_Shortcut_to_WM_UCL_USER_temp.SQ_Shortcut_to_WM_UCL_USER___USER_NAME == JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___USER_NAME],'right_outer').selectExpr( 
	"JNR_SITE_PROFILE___LOCATION_ID1 as LOCATION_ID1", 
	"JNR_SITE_PROFILE___UCL_USER_ID as UCL_USER_ID", 
	"JNR_SITE_PROFILE___COMPANY_ID as COMPANY_ID", 
	"JNR_SITE_PROFILE___USER_NAME as USER_NAME", 
	"JNR_SITE_PROFILE___USER_PASSWORD as USER_PASSWORD", 
	"JNR_SITE_PROFILE___IS_ACTIVE as IS_ACTIVE", 
	"JNR_SITE_PROFILE___CREATED_SOURCE_TYPE_ID as CREATED_SOURCE_TYPE_ID", 
	"JNR_SITE_PROFILE___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_SITE_PROFILE___CREATED_DTTM as CREATED_DTTM", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE_TYPE_ID as LAST_UPDATED_SOURCE_TYPE_ID", 
	"JNR_SITE_PROFILE___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_SITE_PROFILE___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_SITE_PROFILE___USER_TYPE_ID as USER_TYPE_ID", 
	"JNR_SITE_PROFILE___LOCALE_ID as LOCALE_ID", 
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID", 
	"JNR_SITE_PROFILE___USER_FIRST_NAME as USER_FIRST_NAME", 
	"JNR_SITE_PROFILE___USER_MIDDLE_NAME as USER_MIDDLE_NAME", 
	"JNR_SITE_PROFILE___USER_LAST_NAME as USER_LAST_NAME", 
	"JNR_SITE_PROFILE___USER_PREFIX as USER_PREFIX", 
	"JNR_SITE_PROFILE___USER_TITLE as USER_TITLE", 
	"JNR_SITE_PROFILE___TELEPHONE_NUMBER as TELEPHONE_NUMBER", 
	"JNR_SITE_PROFILE___FAX_NUMBER as FAX_NUMBER", 
	"JNR_SITE_PROFILE___ADDRESS_1 as ADDRESS_1", 
	"JNR_SITE_PROFILE___ADDRESS_2 as ADDRESS_2", 
	"JNR_SITE_PROFILE___CITY as CITY", 
	"JNR_SITE_PROFILE___STATE_PROV_CODE as STATE_PROV_CODE", 
	"JNR_SITE_PROFILE___POSTAL_CODE as POSTAL_CODE", 
	"JNR_SITE_PROFILE___COUNTRY_CODE as COUNTRY_CODE", 
	"JNR_SITE_PROFILE___USER_EMAIL_1 as USER_EMAIL_1", 
	"JNR_SITE_PROFILE___USER_EMAIL_2 as USER_EMAIL_2", 
	"JNR_SITE_PROFILE___COMM_METHOD_ID_DURING_BH_1 as COMM_METHOD_ID_DURING_BH_1", 
	"JNR_SITE_PROFILE___COMM_METHOD_ID_DURING_BH_2 as COMM_METHOD_ID_DURING_BH_2", 
	"JNR_SITE_PROFILE___COMM_METHOD_ID_AFTER_BH_1 as COMM_METHOD_ID_AFTER_BH_1", 
	"JNR_SITE_PROFILE___COMM_METHOD_ID_AFTER_BH_2 as COMM_METHOD_ID_AFTER_BH_2", 
	"JNR_SITE_PROFILE___COMMON_NAME as COMMON_NAME", 
	"JNR_SITE_PROFILE___LAST_PASSWORD_CHANGE_DTTM as LAST_PASSWORD_CHANGE_DTTM", 
	"JNR_SITE_PROFILE___LOGGED_IN as LOGGED_IN", 
	"JNR_SITE_PROFILE___LAST_LOGIN_DTTM as LAST_LOGIN_DTTM", 
	"JNR_SITE_PROFILE___DEFAULT_BUSINESS_UNIT_ID as DEFAULT_BUSINESS_UNIT_ID", 
	"JNR_SITE_PROFILE___DEFAULT_WHSE_REGION_ID as DEFAULT_WHSE_REGION_ID", 
	"JNR_SITE_PROFILE___CHANNEL_ID as CHANNEL_ID", 
	"JNR_SITE_PROFILE___HIBERNATE_VERSION as HIBERNATE_VERSION", 
	"JNR_SITE_PROFILE___NUMBER_OF_INVALID_LOGINS as NUMBER_OF_INVALID_LOGINS", 
	"JNR_SITE_PROFILE___TAX_ID_NBR as TAX_ID_NBR", 
	"JNR_SITE_PROFILE___EMP_START_DATE as EMP_START_DATE", 
	"JNR_SITE_PROFILE___BIRTH_DATE as BIRTH_DATE", 
	"JNR_SITE_PROFILE___GENDER_ID as GENDER_ID", 
	"JNR_SITE_PROFILE___PASSWORD_RESET_DATE_TIME as PASSWORD_RESET_DATE_TIME", 
	"JNR_SITE_PROFILE___PASSWORD_TOKEN as PASSWORD_TOKEN", 
	"JNR_SITE_PROFILE___ISPASSWORDMANAGEDINTERNALLY as ISPASSWORDMANAGEDINTERNALLY", 
	"JNR_SITE_PROFILE___COPY_FROM_USER as COPY_FROM_USER", 
	"JNR_SITE_PROFILE___EXTERNAL_USER_ID as EXTERNAL_USER_ID", 
	"JNR_SITE_PROFILE___SECURITY_POLICY_GROUP_ID as SECURITY_POLICY_GROUP_ID", 
	"SQ_Shortcut_to_WM_UCL_USER___LOCATION_ID as i_LOCATION_ID", 
	"SQ_Shortcut_to_WM_UCL_USER___WM_UCL_USER_ID as i_WM_UCL_USER_ID", 
	"SQ_Shortcut_to_WM_UCL_USER___WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"SQ_Shortcut_to_WM_UCL_USER___WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"SQ_Shortcut_to_WM_UCL_USER___LOAD_TSTMP as i_LOAD_TSTMP", 
	"SQ_Shortcut_to_WM_UCL_USER___USER_NAME as i_USER_NAME")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 59

# for each involved DataFrame, append the dataframe name to each column
JNR_WM_UCL_USER_temp = JNR_WM_UCL_USER.toDF(*["JNR_WM_UCL_USER___" + col for col in JNR_WM_UCL_USER.columns])

FIL_UNCHANGED_RECORDS = JNR_WM_UCL_USER_temp.selectExpr( 
	"JNR_WM_UCL_USER___LOCATION_ID1 as LOCATION_ID1", 
	"JNR_WM_UCL_USER___UCL_USER_ID as UCL_USER_ID", 
	"JNR_WM_UCL_USER___COMPANY_ID as COMPANY_ID", 
	"JNR_WM_UCL_USER___USER_NAME as USER_NAME1", 
	"JNR_WM_UCL_USER___USER_PASSWORD as USER_PASSWORD", 
	"JNR_WM_UCL_USER___IS_ACTIVE as IS_ACTIVE", 
	"JNR_WM_UCL_USER___CREATED_SOURCE_TYPE_ID as CREATED_SOURCE_TYPE_ID", 
	"JNR_WM_UCL_USER___CREATED_SOURCE as CREATED_SOURCE", 
	"JNR_WM_UCL_USER___CREATED_DTTM as CREATED_DTTM", 
	"JNR_WM_UCL_USER___LAST_UPDATED_SOURCE_TYPE_ID as LAST_UPDATED_SOURCE_TYPE_ID", 
	"JNR_WM_UCL_USER___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"JNR_WM_UCL_USER___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"JNR_WM_UCL_USER___USER_TYPE_ID as USER_TYPE_ID", 
	"JNR_WM_UCL_USER___LOCALE_ID as LOCALE_ID", 
	"JNR_WM_UCL_USER___LOCATION_ID as LOCATION_ID2", 
	"JNR_WM_UCL_USER___USER_FIRST_NAME as USER_FIRST_NAME1", 
	"JNR_WM_UCL_USER___USER_MIDDLE_NAME as USER_MIDDLE_NAME1", 
	"JNR_WM_UCL_USER___USER_LAST_NAME as USER_LAST_NAME1", 
	"JNR_WM_UCL_USER___USER_PREFIX as USER_PREFIX1", 
	"JNR_WM_UCL_USER___USER_TITLE as USER_TITLE1", 
	"JNR_WM_UCL_USER___TELEPHONE_NUMBER as TELEPHONE_NUMBER", 
	"JNR_WM_UCL_USER___FAX_NUMBER as FAX_NUMBER", 
	"JNR_WM_UCL_USER___ADDRESS_1 as ADDRESS_11", 
	"JNR_WM_UCL_USER___ADDRESS_2 as ADDRESS_21", 
	"JNR_WM_UCL_USER___CITY as CITY1", 
	"JNR_WM_UCL_USER___STATE_PROV_CODE as STATE_PROV_CODE", 
	"JNR_WM_UCL_USER___POSTAL_CODE as POSTAL_CODE", 
	"JNR_WM_UCL_USER___COUNTRY_CODE as COUNTRY_CODE", 
	"JNR_WM_UCL_USER___USER_EMAIL_1 as USER_EMAIL_11", 
	"JNR_WM_UCL_USER___USER_EMAIL_2 as USER_EMAIL_21", 
	"JNR_WM_UCL_USER___COMM_METHOD_ID_DURING_BH_1 as COMM_METHOD_ID_DURING_BH_1", 
	"JNR_WM_UCL_USER___COMM_METHOD_ID_DURING_BH_2 as COMM_METHOD_ID_DURING_BH_2", 
	"JNR_WM_UCL_USER___COMM_METHOD_ID_AFTER_BH_1 as COMM_METHOD_ID_AFTER_BH_1", 
	"JNR_WM_UCL_USER___COMM_METHOD_ID_AFTER_BH_2 as COMM_METHOD_ID_AFTER_BH_2", 
	"JNR_WM_UCL_USER___COMMON_NAME as COMMON_NAME1", 
	"JNR_WM_UCL_USER___LAST_PASSWORD_CHANGE_DTTM as LAST_PASSWORD_CHANGE_DTTM", 
	"JNR_WM_UCL_USER___LOGGED_IN as LOGGED_IN1", 
	"JNR_WM_UCL_USER___LAST_LOGIN_DTTM as LAST_LOGIN_DTTM", 
	"JNR_WM_UCL_USER___DEFAULT_BUSINESS_UNIT_ID as DEFAULT_BUSINESS_UNIT_ID", 
	"JNR_WM_UCL_USER___DEFAULT_WHSE_REGION_ID as DEFAULT_WHSE_REGION_ID", 
	"JNR_WM_UCL_USER___CHANNEL_ID as CHANNEL_ID", 
	"JNR_WM_UCL_USER___HIBERNATE_VERSION as HIBERNATE_VERSION1", 
	"JNR_WM_UCL_USER___NUMBER_OF_INVALID_LOGINS as NUMBER_OF_INVALID_LOGINS1", 
	"JNR_WM_UCL_USER___TAX_ID_NBR as TAX_ID_NBR", 
	"JNR_WM_UCL_USER___EMP_START_DATE as EMP_START_DATE", 
	"JNR_WM_UCL_USER___BIRTH_DATE as BIRTH_DATE", 
	"JNR_WM_UCL_USER___GENDER_ID as GENDER_ID1", 
	"JNR_WM_UCL_USER___PASSWORD_RESET_DATE_TIME as PASSWORD_RESET_DATE_TIME", 
	"JNR_WM_UCL_USER___PASSWORD_TOKEN as PASSWORD_TOKEN", 
	"JNR_WM_UCL_USER___ISPASSWORDMANAGEDINTERNALLY as ISPASSWORDMANAGEDINTERNALLY", 
	"JNR_WM_UCL_USER___COPY_FROM_USER as COPY_FROM_USER1", 
	"JNR_WM_UCL_USER___EXTERNAL_USER_ID as EXTERNAL_USER_ID", 
	"JNR_WM_UCL_USER___SECURITY_POLICY_GROUP_ID as SECURITY_POLICY_GROUP_ID", 
	"JNR_WM_UCL_USER___i_WM_UCL_USER_ID as i_WM_UCL_USER_ID", 
	"JNR_WM_UCL_USER___i_WM_CREATED_TSTMP as i_WM_CREATED_TSTMP", 
	"JNR_WM_UCL_USER___i_WM_LAST_UPDATED_TSTMP as i_WM_LAST_UPDATED_TSTMP", 
	"JNR_WM_UCL_USER___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"JNR_WM_UCL_USER___i_USER_NAME as i_USER_NAME1", 
	"JNR_WM_UCL_USER___i_LOCATION_ID as i_LOCATION_ID").filter(expr("i_USER_NAME1 IS NULL OR (NOT i_USER_NAME1 IS NULL AND (COALESCE(CREATED_DTTM, date'1900-01-01') != COALESCE(i_WM_CREATED_TSTMP, date'1900-01-01')) OR (COALESCE(LAST_UPDATED_DTTM, date'1900-01-01') != COALESCE(i_WM_LAST_UPDATED_TSTMP, date'1900-01-01')))")).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPD_VALIDATOR, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 58

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID1 as LOCATION_ID1", 
	"FIL_UNCHANGED_RECORDS___UCL_USER_ID as UCL_USER_ID", 
	"FIL_UNCHANGED_RECORDS___COMPANY_ID as COMPANY_ID", 
	"FIL_UNCHANGED_RECORDS___USER_NAME1 as USER_NAME1", 
	"FIL_UNCHANGED_RECORDS___USER_PASSWORD as USER_PASSWORD", 
	"FIL_UNCHANGED_RECORDS___IS_ACTIVE as IS_ACTIVE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE_ID as CREATED_SOURCE_TYPE_ID", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE_ID as LAST_UPDATED_SOURCE_TYPE_ID", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___USER_TYPE_ID as USER_TYPE_ID", 
	"FIL_UNCHANGED_RECORDS___LOCALE_ID as LOCALE_ID", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID2 as LOCATION_ID2", 
	"FIL_UNCHANGED_RECORDS___USER_FIRST_NAME1 as USER_FIRST_NAME1", 
	"FIL_UNCHANGED_RECORDS___USER_MIDDLE_NAME1 as USER_MIDDLE_NAME1", 
	"FIL_UNCHANGED_RECORDS___USER_LAST_NAME1 as USER_LAST_NAME1", 
	"FIL_UNCHANGED_RECORDS___USER_PREFIX1 as USER_PREFIX1", 
	"FIL_UNCHANGED_RECORDS___USER_TITLE1 as USER_TITLE1", 
	"FIL_UNCHANGED_RECORDS___TELEPHONE_NUMBER as TELEPHONE_NUMBER", 
	"FIL_UNCHANGED_RECORDS___FAX_NUMBER as FAX_NUMBER", 
	"FIL_UNCHANGED_RECORDS___ADDRESS_11 as ADDRESS_11", 
	"FIL_UNCHANGED_RECORDS___ADDRESS_21 as ADDRESS_21", 
	"FIL_UNCHANGED_RECORDS___CITY1 as CITY1", 
	"FIL_UNCHANGED_RECORDS___STATE_PROV_CODE as STATE_PROV_CODE", 
	"FIL_UNCHANGED_RECORDS___POSTAL_CODE as POSTAL_CODE", 
	"FIL_UNCHANGED_RECORDS___COUNTRY_CODE as COUNTRY_CODE", 
	"FIL_UNCHANGED_RECORDS___USER_EMAIL_11 as USER_EMAIL_11", 
	"FIL_UNCHANGED_RECORDS___USER_EMAIL_21 as USER_EMAIL_21", 
	"FIL_UNCHANGED_RECORDS___COMM_METHOD_ID_DURING_BH_1 as COMM_METHOD_ID_DURING_BH_1", 
	"FIL_UNCHANGED_RECORDS___COMM_METHOD_ID_DURING_BH_2 as COMM_METHOD_ID_DURING_BH_2", 
	"FIL_UNCHANGED_RECORDS___COMM_METHOD_ID_AFTER_BH_1 as COMM_METHOD_ID_AFTER_BH_1", 
	"FIL_UNCHANGED_RECORDS___COMM_METHOD_ID_AFTER_BH_2 as COMM_METHOD_ID_AFTER_BH_2", 
	"FIL_UNCHANGED_RECORDS___COMMON_NAME1 as COMMON_NAME1", 
	"FIL_UNCHANGED_RECORDS___LAST_PASSWORD_CHANGE_DTTM as LAST_PASSWORD_CHANGE_DTTM", 
	"FIL_UNCHANGED_RECORDS___LOGGED_IN1 as LOGGED_IN1", 
	"FIL_UNCHANGED_RECORDS___LAST_LOGIN_DTTM as LAST_LOGIN_DTTM", 
	"FIL_UNCHANGED_RECORDS___DEFAULT_BUSINESS_UNIT_ID as DEFAULT_BUSINESS_UNIT_ID", 
	"FIL_UNCHANGED_RECORDS___DEFAULT_WHSE_REGION_ID as DEFAULT_WHSE_REGION_ID", 
	"FIL_UNCHANGED_RECORDS___CHANNEL_ID as CHANNEL_ID", 
	"FIL_UNCHANGED_RECORDS___HIBERNATE_VERSION1 as HIBERNATE_VERSION1", 
	"FIL_UNCHANGED_RECORDS___NUMBER_OF_INVALID_LOGINS1 as NUMBER_OF_INVALID_LOGINS1", 
	"FIL_UNCHANGED_RECORDS___TAX_ID_NBR as TAX_ID_NBR", 
	"FIL_UNCHANGED_RECORDS___EMP_START_DATE as EMP_START_DATE", 
	"FIL_UNCHANGED_RECORDS___BIRTH_DATE as BIRTH_DATE", 
	"FIL_UNCHANGED_RECORDS___GENDER_ID1 as GENDER_ID1", 
	"FIL_UNCHANGED_RECORDS___PASSWORD_RESET_DATE_TIME as PASSWORD_RESET_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___PASSWORD_TOKEN as PASSWORD_TOKEN", 
	"FIL_UNCHANGED_RECORDS___ISPASSWORDMANAGEDINTERNALLY as ISPASSWORDMANAGEDINTERNALLY", 
	"FIL_UNCHANGED_RECORDS___COPY_FROM_USER1 as COPY_FROM_USER1", 
	"FIL_UNCHANGED_RECORDS___EXTERNAL_USER_ID as EXTERNAL_USER_ID", 
	"FIL_UNCHANGED_RECORDS___SECURITY_POLICY_GROUP_ID as SECURITY_POLICY_GROUP_ID", 
	"FIL_UNCHANGED_RECORDS___i_WM_UCL_USER_ID as i_WM_UCL_USER_ID", 
	"FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP as i_LOAD_TSTMP", 
	"FIL_UNCHANGED_RECORDS___i_USER_NAME1 as i_USER_NAME1", 
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID as i_LOCATION_ID1").selectExpr( 
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID1 as LOCATION_ID1", 
	"FIL_UNCHANGED_RECORDS___UCL_USER_ID as UCL_USER_ID", 
	"FIL_UNCHANGED_RECORDS___COMPANY_ID as COMPANY_ID", 
	"FIL_UNCHANGED_RECORDS___USER_NAME1 as USER_NAME1", 
	"FIL_UNCHANGED_RECORDS___USER_PASSWORD as USER_PASSWORD", 
	"FIL_UNCHANGED_RECORDS___IS_ACTIVE as IS_ACTIVE", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE_TYPE_ID as CREATED_SOURCE_TYPE_ID", 
	"FIL_UNCHANGED_RECORDS___CREATED_SOURCE as CREATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___CREATED_DTTM as CREATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE_TYPE_ID as LAST_UPDATED_SOURCE_TYPE_ID", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"FIL_UNCHANGED_RECORDS___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"FIL_UNCHANGED_RECORDS___USER_TYPE_ID as USER_TYPE_ID", 
	"FIL_UNCHANGED_RECORDS___LOCALE_ID as LOCALE_ID", 
	"FIL_UNCHANGED_RECORDS___LOCATION_ID2 as LOCATION_ID2", 
	"FIL_UNCHANGED_RECORDS___USER_FIRST_NAME1 as USER_FIRST_NAME1", 
	"FIL_UNCHANGED_RECORDS___USER_MIDDLE_NAME1 as USER_MIDDLE_NAME1", 
	"FIL_UNCHANGED_RECORDS___USER_LAST_NAME1 as USER_LAST_NAME1", 
	"FIL_UNCHANGED_RECORDS___USER_PREFIX1 as USER_PREFIX1", 
	"FIL_UNCHANGED_RECORDS___USER_TITLE1 as USER_TITLE1", 
	"FIL_UNCHANGED_RECORDS___TELEPHONE_NUMBER as TELEPHONE_NUMBER", 
	"FIL_UNCHANGED_RECORDS___FAX_NUMBER as FAX_NUMBER", 
	"FIL_UNCHANGED_RECORDS___ADDRESS_11 as ADDRESS_11", 
	"FIL_UNCHANGED_RECORDS___ADDRESS_21 as ADDRESS_21", 
	"FIL_UNCHANGED_RECORDS___CITY1 as CITY1", 
	"FIL_UNCHANGED_RECORDS___STATE_PROV_CODE as STATE_PROV_CODE", 
	"FIL_UNCHANGED_RECORDS___POSTAL_CODE as POSTAL_CODE", 
	"FIL_UNCHANGED_RECORDS___COUNTRY_CODE as COUNTRY_CODE", 
	"FIL_UNCHANGED_RECORDS___USER_EMAIL_11 as USER_EMAIL_11", 
	"FIL_UNCHANGED_RECORDS___USER_EMAIL_21 as USER_EMAIL_21", 
	"FIL_UNCHANGED_RECORDS___COMM_METHOD_ID_DURING_BH_1 as COMM_METHOD_ID_DURING_BH_1", 
	"FIL_UNCHANGED_RECORDS___COMM_METHOD_ID_DURING_BH_2 as COMM_METHOD_ID_DURING_BH_2", 
	"FIL_UNCHANGED_RECORDS___COMM_METHOD_ID_AFTER_BH_1 as COMM_METHOD_ID_AFTER_BH_1", 
	"FIL_UNCHANGED_RECORDS___COMM_METHOD_ID_AFTER_BH_2 as COMM_METHOD_ID_AFTER_BH_2", 
	"FIL_UNCHANGED_RECORDS___COMMON_NAME1 as COMMON_NAME1", 
	"FIL_UNCHANGED_RECORDS___LAST_PASSWORD_CHANGE_DTTM as LAST_PASSWORD_CHANGE_DTTM", 
	"FIL_UNCHANGED_RECORDS___LOGGED_IN1 as LOGGED_IN1", 
	"FIL_UNCHANGED_RECORDS___LAST_LOGIN_DTTM as LAST_LOGIN_DTTM", 
	"FIL_UNCHANGED_RECORDS___DEFAULT_BUSINESS_UNIT_ID as DEFAULT_BUSINESS_UNIT_ID", 
	"FIL_UNCHANGED_RECORDS___DEFAULT_WHSE_REGION_ID as DEFAULT_WHSE_REGION_ID", 
	"FIL_UNCHANGED_RECORDS___CHANNEL_ID as CHANNEL_ID", 
	"FIL_UNCHANGED_RECORDS___HIBERNATE_VERSION1 as HIBERNATE_VERSION1", 
	"FIL_UNCHANGED_RECORDS___NUMBER_OF_INVALID_LOGINS1 as NUMBER_OF_INVALID_LOGINS1", 
	"FIL_UNCHANGED_RECORDS___TAX_ID_NBR as TAX_ID_NBR", 
	"FIL_UNCHANGED_RECORDS___EMP_START_DATE as EMP_START_DATE", 
	"FIL_UNCHANGED_RECORDS___BIRTH_DATE as BIRTH_DATE", 
	"FIL_UNCHANGED_RECORDS___GENDER_ID1 as GENDER_ID1", 
	"FIL_UNCHANGED_RECORDS___PASSWORD_RESET_DATE_TIME as PASSWORD_RESET_DATE_TIME", 
	"FIL_UNCHANGED_RECORDS___PASSWORD_TOKEN as PASSWORD_TOKEN", 
	"FIL_UNCHANGED_RECORDS___ISPASSWORDMANAGEDINTERNALLY as ISPASSWORDMANAGEDINTERNALLY", 
	"FIL_UNCHANGED_RECORDS___COPY_FROM_USER1 as COPY_FROM_USER1", 
	"FIL_UNCHANGED_RECORDS___EXTERNAL_USER_ID as EXTERNAL_USER_ID", 
	"FIL_UNCHANGED_RECORDS___SECURITY_POLICY_GROUP_ID as SECURITY_POLICY_GROUP_ID", 
	"FIL_UNCHANGED_RECORDS___i_USER_NAME1 as i_USER_NAME1", 
	"FIL_UNCHANGED_RECORDS___i_LOCATION_ID1 as i_LOCATION_ID1", 
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", 
	"IF(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP", 
	"IF(( FIL_UNCHANGED_RECORDS___i_USER_NAME1 IS NULL AND FIL_UNCHANGED_RECORDS___i_LOCATION_ID1 IS NULL ), 1, 2) as o_UPDATE_VALIDATOR" 
)

# COMMAND ----------
# Processing node UPD_INS_UPD, type UPDATE_STRATEGY 
# COLUMN COUNT: 56

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_VALIDATOR_temp = EXP_UPD_VALIDATOR.toDF(*["EXP_UPD_VALIDATOR___" + col for col in EXP_UPD_VALIDATOR.columns])

UPD_INS_UPD = EXP_UPD_VALIDATOR_temp.selectExpr( 
	"EXP_UPD_VALIDATOR___LOCATION_ID1 as LOCATION_ID1", 
	"EXP_UPD_VALIDATOR___UCL_USER_ID as UCL_USER_ID", 
	"EXP_UPD_VALIDATOR___COMPANY_ID as COMPANY_ID", 
	"EXP_UPD_VALIDATOR___USER_NAME1 as USER_NAME1", 
	"EXP_UPD_VALIDATOR___USER_PASSWORD as USER_PASSWORD", 
	"EXP_UPD_VALIDATOR___IS_ACTIVE as IS_ACTIVE", 
	"EXP_UPD_VALIDATOR___CREATED_SOURCE_TYPE_ID as CREATED_SOURCE_TYPE_ID", 
	"EXP_UPD_VALIDATOR___CREATED_SOURCE as CREATED_SOURCE", 
	"EXP_UPD_VALIDATOR___CREATED_DTTM as CREATED_DTTM", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE_TYPE_ID as LAST_UPDATED_SOURCE_TYPE_ID", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"EXP_UPD_VALIDATOR___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"EXP_UPD_VALIDATOR___USER_TYPE_ID as USER_TYPE_ID", 
	"EXP_UPD_VALIDATOR___LOCALE_ID as LOCALE_ID", 
	"EXP_UPD_VALIDATOR___LOCATION_ID2 as LOCATION_ID2", 
	"EXP_UPD_VALIDATOR___USER_FIRST_NAME1 as USER_FIRST_NAME1", 
	"EXP_UPD_VALIDATOR___USER_MIDDLE_NAME1 as USER_MIDDLE_NAME1", 
	"EXP_UPD_VALIDATOR___USER_LAST_NAME1 as USER_LAST_NAME1", 
	"EXP_UPD_VALIDATOR___USER_PREFIX1 as USER_PREFIX1", 
	"EXP_UPD_VALIDATOR___USER_TITLE1 as USER_TITLE1", 
	"EXP_UPD_VALIDATOR___TELEPHONE_NUMBER as TELEPHONE_NUMBER", 
	"EXP_UPD_VALIDATOR___FAX_NUMBER as FAX_NUMBER", 
	"EXP_UPD_VALIDATOR___ADDRESS_11 as ADDRESS_11", 
	"EXP_UPD_VALIDATOR___ADDRESS_21 as ADDRESS_21", 
	"EXP_UPD_VALIDATOR___CITY1 as CITY1", 
	"EXP_UPD_VALIDATOR___STATE_PROV_CODE as STATE_PROV_CODE", 
	"EXP_UPD_VALIDATOR___POSTAL_CODE as POSTAL_CODE", 
	"EXP_UPD_VALIDATOR___COUNTRY_CODE as COUNTRY_CODE", 
	"EXP_UPD_VALIDATOR___USER_EMAIL_11 as USER_EMAIL_11", 
	"EXP_UPD_VALIDATOR___USER_EMAIL_21 as USER_EMAIL_21", 
	"EXP_UPD_VALIDATOR___COMM_METHOD_ID_DURING_BH_1 as COMM_METHOD_ID_DURING_BH_1", 
	"EXP_UPD_VALIDATOR___COMM_METHOD_ID_DURING_BH_2 as COMM_METHOD_ID_DURING_BH_2", 
	"EXP_UPD_VALIDATOR___COMM_METHOD_ID_AFTER_BH_1 as COMM_METHOD_ID_AFTER_BH_1", 
	"EXP_UPD_VALIDATOR___COMM_METHOD_ID_AFTER_BH_2 as COMM_METHOD_ID_AFTER_BH_2", 
	"EXP_UPD_VALIDATOR___COMMON_NAME1 as COMMON_NAME1", 
	"EXP_UPD_VALIDATOR___LAST_PASSWORD_CHANGE_DTTM as LAST_PASSWORD_CHANGE_DTTM", 
	"EXP_UPD_VALIDATOR___LOGGED_IN1 as LOGGED_IN1", 
	"EXP_UPD_VALIDATOR___LAST_LOGIN_DTTM as LAST_LOGIN_DTTM", 
	"EXP_UPD_VALIDATOR___DEFAULT_BUSINESS_UNIT_ID as DEFAULT_BUSINESS_UNIT_ID", 
	"EXP_UPD_VALIDATOR___DEFAULT_WHSE_REGION_ID as DEFAULT_WHSE_REGION_ID", 
	"EXP_UPD_VALIDATOR___CHANNEL_ID as CHANNEL_ID", 
	"EXP_UPD_VALIDATOR___HIBERNATE_VERSION1 as HIBERNATE_VERSION1", 
	"EXP_UPD_VALIDATOR___NUMBER_OF_INVALID_LOGINS1 as NUMBER_OF_INVALID_LOGINS1", 
	"EXP_UPD_VALIDATOR___TAX_ID_NBR as TAX_ID_NBR", 
	"EXP_UPD_VALIDATOR___EMP_START_DATE as EMP_START_DATE", 
	"EXP_UPD_VALIDATOR___BIRTH_DATE as BIRTH_DATE", 
	"EXP_UPD_VALIDATOR___GENDER_ID1 as GENDER_ID1", 
	"EXP_UPD_VALIDATOR___PASSWORD_RESET_DATE_TIME as PASSWORD_RESET_DATE_TIME", 
	"EXP_UPD_VALIDATOR___PASSWORD_TOKEN as PASSWORD_TOKEN", 
	"EXP_UPD_VALIDATOR___ISPASSWORDMANAGEDINTERNALLY as ISPASSWORDMANAGEDINTERNALLY", 
	"EXP_UPD_VALIDATOR___COPY_FROM_USER1 as COPY_FROM_USER1", 
	"EXP_UPD_VALIDATOR___EXTERNAL_USER_ID as EXTERNAL_USER_ID", 
	"EXP_UPD_VALIDATOR___SECURITY_POLICY_GROUP_ID as SECURITY_POLICY_GROUP_ID", 
	"EXP_UPD_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", 
	"EXP_UPD_VALIDATOR___LOAD_TSTMP as LOAD_TSTMP", 
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR"
).withColumn('pyspark_data_action', when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(1)),lit(0)).when(EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR ==(lit(2)),lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WM_UCL_USER, type TARGET 
# COLUMN COUNT: 53


Shortcut_to_WM_UCL_USER = UPD_INS_UPD.selectExpr( 
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID", 
	"CAST(UCL_USER_ID AS BIGINT) as WM_UCL_USER_ID", 
	"CAST(COMPANY_ID AS BIGINT) as WM_COMPANY_ID", 
	"CAST(LOCATION_ID2 AS BIGINT) as WM_LOCATION_ID", 
	"CAST(LOCALE_ID AS BIGINT) as WM_LOCALE_ID", 
	"CAST(USER_TYPE_ID AS BIGINT) as WM_USER_TYPE_ID", 
	"CAST(IS_ACTIVE AS BIGINT) as ACTIVE_FLAG", 
	"CAST(USER_NAME1 AS STRING) as USER_NAME", 
	"CAST(TAX_ID_NBR AS STRING) as TAX_ID_NBR", 
	"CAST(COMMON_NAME1 AS STRING) as COMMON_NAME", 
	"CAST(USER_PREFIX1 AS STRING) as USER_PREFIX", 
	"CAST(USER_TITLE1 AS STRING) as USER_TITLE", 
	"CAST(USER_FIRST_NAME1 AS STRING) as USER_FIRST_NAME", 
	"CAST(USER_MIDDLE_NAME1 AS STRING) as USER_MIDDLE_NAME", 
	"CAST(USER_LAST_NAME1 AS STRING) as USER_LAST_NAME", 
	"CAST(BIRTH_DATE AS DATE) as BIRTH_DT", 
	"CAST(GENDER_ID1 AS STRING) as GENDER_ID", 
	"CAST(EMP_START_DATE AS DATE) as EMPLOYEE_START_DT", 
	"CAST(ADDRESS_11 AS STRING) as ADDR_1", 
	"CAST(ADDRESS_21 AS STRING) as ADDR_2", 
	"CAST(CITY1 AS STRING) as CITY", 
	"CAST(STATE_PROV_CODE AS STRING) as STATE_PROV_CD", 
	"CAST(POSTAL_CODE AS STRING) as POSTAL_CD", 
	"CAST(COUNTRY_CODE AS STRING) as COUNTRY_CD", 
	"CAST(USER_EMAIL_11 AS STRING) as USER_EMAIL_1", 
	"CAST(USER_EMAIL_21 AS STRING) as USER_EMAIL_2", 
	"CAST(TELEPHONE_NUMBER AS STRING) as PHONE_NBR", 
	"CAST(FAX_NUMBER AS STRING) as FAX_NBR", 
	"CAST(EXTERNAL_USER_ID AS STRING) as WM_EXTERNAL_USER_ID", 
	"CAST(COPY_FROM_USER1 AS STRING) as COPY_FROM_USER", 
	"CAST(SECURITY_POLICY_GROUP_ID AS BIGINT) as WM_SECURITY_POLICY_GROUP_ID", 
	"CAST(DEFAULT_BUSINESS_UNIT_ID AS BIGINT) as DEFAULT_WM_BUSINESS_UNIT_ID", 
	"CAST(DEFAULT_WHSE_REGION_ID AS BIGINT) as DEFAULT_WM_WHSE_REGION_ID", 
	"CAST(CHANNEL_ID AS BIGINT) as WM_CHANNEL_ID", 
	"CAST(COMM_METHOD_ID_DURING_BH_1 AS BIGINT) as WM_COMM_METHOD_ID_DURING_BH_1", 
	"CAST(COMM_METHOD_ID_DURING_BH_2 AS BIGINT) as WM_COMM_METHOD_ID_DURING_BH_2", 
	"CAST(COMM_METHOD_ID_AFTER_BH_1 AS BIGINT) as WM_COMM_METHOD_ID_AFTER_BH_1", 
	"CAST(COMM_METHOD_ID_AFTER_BH_2 AS BIGINT) as WM_COMM_METHOD_ID_AFTER_BH_2", 
	"CAST(ISPASSWORDMANAGEDINTERNALLY AS BIGINT) as PASSWORD_MANAGED_INTERNALLY_FLAG", 
	"CAST(LOGGED_IN1 AS BIGINT) as LOGGED_IN_FLAG", 
	"CAST(LAST_LOGIN_DTTM AS TIMESTAMP) as LAST_LOGIN_TSTMP", 
	"CAST(NUMBER_OF_INVALID_LOGINS1 AS BIGINT) as NUMBER_OF_INVALID_LOGINS", 
	"CAST(PASSWORD_RESET_DATE_TIME AS TIMESTAMP) as PASSWORD_RESET_TSTMP", 
	"CAST(LAST_PASSWORD_CHANGE_DTTM AS TIMESTAMP) as LAST_PASSWORD_CHANGE_TSTMP", 
	"CAST(HIBERNATE_VERSION1 AS BIGINT) as WM_HIBERNATE_VERSION", 
	"CAST(CREATED_SOURCE_TYPE_ID AS BIGINT) as WM_CREATED_SOURCE_TYPE_ID", 
	"CAST(CREATED_SOURCE AS STRING) as WM_CREATED_SOURCE", 
	"CAST(CREATED_DTTM AS TIMESTAMP) as WM_CREATED_TSTMP", 
	"CAST(LAST_UPDATED_SOURCE_TYPE_ID AS BIGINT) as WM_LAST_UPDATED_SOURCE_TYPE_ID", 
	"CAST(LAST_UPDATED_SOURCE AS STRING) as WM_LAST_UPDATED_SOURCE", 
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as WM_LAST_UPDATED_TSTMP", 
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", 
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP" , 
    "pyspark_data_action"
)

try:
  primary_key = """source.LOCATION_ID = target.LOCATION_ID AND source.USER_NAME = target.USER_NAME"""
  # refined_perf_table = "WM_UCL_USER"
  executeMerge(Shortcut_to_WM_UCL_USER, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("WM_UCL_USER", "WM_UCL_USER", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("WM_UCL_USER", "WM_UCL_USER","Failed",str(e), f"{raw}.log_run_details", )
  raise e
