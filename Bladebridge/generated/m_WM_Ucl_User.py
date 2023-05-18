#Code converted on 2023-05-18 09:46:46
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from dbruntime import dbutils

# COMMAND ----------

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in job variables
# read_infa_paramfile('', 'm_WM_Ucl_User') ProcessingUtils

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_UCL_USER_PRE, type SOURCE 
# COLUMN COUNT: 53

SQ_Shortcut_to_WM_UCL_USER_PRE = spark.read.jdbc(os.environ.get('NZ_SCDS_CONNECT_STRING'), f"""SELECT
WM_UCL_USER_PRE.DC_NBR,
WM_UCL_USER_PRE.UCL_USER_ID,
WM_UCL_USER_PRE.COMPANY_ID,
WM_UCL_USER_PRE.USER_NAME,
WM_UCL_USER_PRE.USER_PASSWORD,
WM_UCL_USER_PRE.IS_ACTIVE,
WM_UCL_USER_PRE.CREATED_SOURCE_TYPE_ID,
WM_UCL_USER_PRE.CREATED_SOURCE,
WM_UCL_USER_PRE.CREATED_DTTM,
WM_UCL_USER_PRE.LAST_UPDATED_SOURCE_TYPE_ID,
WM_UCL_USER_PRE.LAST_UPDATED_SOURCE,
WM_UCL_USER_PRE.LAST_UPDATED_DTTM,
WM_UCL_USER_PRE.USER_TYPE_ID,
WM_UCL_USER_PRE.LOCALE_ID,
WM_UCL_USER_PRE.LOCATION_ID,
WM_UCL_USER_PRE.USER_FIRST_NAME,
WM_UCL_USER_PRE.USER_MIDDLE_NAME,
WM_UCL_USER_PRE.USER_LAST_NAME,
WM_UCL_USER_PRE.USER_PREFIX,
WM_UCL_USER_PRE.USER_TITLE,
WM_UCL_USER_PRE.TELEPHONE_NUMBER,
WM_UCL_USER_PRE.FAX_NUMBER,
WM_UCL_USER_PRE.ADDRESS_1,
WM_UCL_USER_PRE.ADDRESS_2,
WM_UCL_USER_PRE.CITY,
WM_UCL_USER_PRE.STATE_PROV_CODE,
WM_UCL_USER_PRE.POSTAL_CODE,
WM_UCL_USER_PRE.COUNTRY_CODE,
WM_UCL_USER_PRE.USER_EMAIL_1,
WM_UCL_USER_PRE.USER_EMAIL_2,
WM_UCL_USER_PRE.COMM_METHOD_ID_DURING_BH_1,
WM_UCL_USER_PRE.COMM_METHOD_ID_DURING_BH_2,
WM_UCL_USER_PRE.COMM_METHOD_ID_AFTER_BH_1,
WM_UCL_USER_PRE.COMM_METHOD_ID_AFTER_BH_2,
WM_UCL_USER_PRE.COMMON_NAME,
WM_UCL_USER_PRE.LAST_PASSWORD_CHANGE_DTTM,
WM_UCL_USER_PRE.LOGGED_IN,
WM_UCL_USER_PRE.LAST_LOGIN_DTTM,
WM_UCL_USER_PRE.DEFAULT_BUSINESS_UNIT_ID,
WM_UCL_USER_PRE.DEFAULT_WHSE_REGION_ID,
WM_UCL_USER_PRE.CHANNEL_ID,
WM_UCL_USER_PRE.HIBERNATE_VERSION,
WM_UCL_USER_PRE.NUMBER_OF_INVALID_LOGINS,
WM_UCL_USER_PRE.TAX_ID_NBR,
WM_UCL_USER_PRE.EMP_START_DATE,
WM_UCL_USER_PRE.BIRTH_DATE,
WM_UCL_USER_PRE.GENDER_ID,
WM_UCL_USER_PRE.PASSWORD_RESET_DATE_TIME,
WM_UCL_USER_PRE.PASSWORD_TOKEN,
WM_UCL_USER_PRE.ISPASSWORDMANAGEDINTERNALLY,
WM_UCL_USER_PRE.COPY_FROM_USER,
WM_UCL_USER_PRE.EXTERNAL_USER_ID,
WM_UCL_USER_PRE.SECURITY_POLICY_GROUP_ID
FROM WM_UCL_USER_PRE""", 
properties={
'user': os.environ.get('NZ_SCDS_LOGIN'),
'password': os.environ.get('NZ_SCDS_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WM_UCL_USER, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_WM_UCL_USER = spark.read.jdbc(os.environ.get('NZ_SCDS_CONNECT_STRING'), f"""SELECT
WM_UCL_USER.LOCATION_ID,
WM_UCL_USER.WM_UCL_USER_ID,
WM_UCL_USER.WM_CREATED_TSTMP,
WM_UCL_USER.WM_LAST_UPDATED_TSTMP,
WM_UCL_USER.LOAD_TSTMP,
WM_UCL_USER.USER_NAME
FROM WM_UCL_USER
WHERE USER_NAME IN (SELECT USER_NAME FROM WM_UCL_USER_PRE)""", 
properties={
'user': os.environ.get('NZ_SCDS_LOGIN'),
'password': os.environ.get('NZ_SCDS_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_INT_CONVERSION, type EXPRESSION 
# COLUMN COUNT: 53

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WM_UCL_USER_PRE_temp = SQ_Shortcut_to_WM_UCL_USER_PRE.toDF(*["SQ_Shortcut_to_WM_UCL_USER_PRE___" + col for col in SQ_Shortcut_to_WM_UCL_USER_PRE.columns])
DC_NBR_temp = DC_NBR.toDF(*["DC_NBR___" + col for col in DC_NBR.columns])

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_UCL_USER_PRE_temp.selectExpr(
	"SQ_Shortcut_to_WM_UCL_USER_PRE___sys_row_id as sys_row_id",
	"DC_NBR___cast(IntegerType()) as o_DC_NBR",
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

SQ_Shortcut_to_SITE_PROFILE = spark.read.jdbc(os.environ.get('NZ_SCDS_CONNECT_STRING'), f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM SITE_PROFILE""", 
properties={
'user': os.environ.get('NZ_SCDS_LOGIN'),
'password': os.environ.get('NZ_SCDS_PASSWORD'),
'driver': os.environ.get('_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())

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
	"JNR_WM_UCL_USER___i_LOCATION_ID as i_LOCATION_ID").filter(f"i_USER_NAME1.isNull() OR ( i_USER_NAME1.isNotNull()  &  ( when((CREATED_DTTM.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(CREATED_DTTM) != when((i_WM_CREATED_TSTMP.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(i_WM_CREATED_TSTMP) OR when((LAST_UPDATED_DTTM.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(LAST_UPDATED_DTTM) != when((i_WM_LAST_UPDATED_TSTMP.isNull()),(to_date ( '01/01/1900' , 'MM/DD/YYYY' ))).otherwise(i_WM_LAST_UPDATED_TSTMP) ) )").withColumn("sys_row_id", monotonically_increasing_id())

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
	"current_date() as UPDATE_TSTMP",
	"when((ISNULL ( FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP )),(current_date())).otherwise(FIL_UNCHANGED_RECORDS___i_LOAD_TSTMP) as LOAD_TSTMP",
	"when((( ISNULL ( FIL_UNCHANGED_RECORDS___i_USER_NAME1 )  & ISNULL ( FIL_UNCHANGED_RECORDS___i_LOCATION_ID1 ) )),(1)).otherwise(2) as o_UPDATE_VALIDATOR"
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
	"EXP_UPD_VALIDATOR___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR")
	.withColumn('pyspark_data_action', lit())

# COMMAND ----------
# Processing node Shortcut_to_WM_UCL_USER, type TARGET 
# COLUMN COUNT: 53

Shortcut_to_WM_UCL_USER = DeltaTable.forPath(spark, '/tmp/delta/WM_UCL_USER')
Shortcut_to_WM_UCL_USER.alias('WM_UCL_USER').merge(UPD_INS_UPD.alias('UPD_INS_UPD'),
'WM_UCL_USER.LOCATION_ID = UPD_INS_UPD.LOCATION_ID1 and WM_UCL_USER.USER_NAME = UPD_INS_UPD.USER_NAME1')
.whenMatchedUpdate(set = {
	'LOCATION_ID' : 'UPD_INS_UPD.LOCATION_ID1',
	'WM_UCL_USER_ID' : 'UPD_INS_UPD.UCL_USER_ID',
	'WM_COMPANY_ID' : 'UPD_INS_UPD.COMPANY_ID',
	'WM_LOCATION_ID' : 'UPD_INS_UPD.LOCATION_ID2',
	'WM_LOCALE_ID' : 'UPD_INS_UPD.LOCALE_ID',
	'WM_USER_TYPE_ID' : 'UPD_INS_UPD.USER_TYPE_ID',
	'ACTIVE_FLAG' : 'UPD_INS_UPD.IS_ACTIVE',
	'USER_NAME' : 'UPD_INS_UPD.USER_NAME1',
	'TAX_ID_NBR' : 'UPD_INS_UPD.TAX_ID_NBR',
	'COMMON_NAME' : 'UPD_INS_UPD.COMMON_NAME1',
	'USER_PREFIX' : 'UPD_INS_UPD.USER_PREFIX1',
	'USER_TITLE' : 'UPD_INS_UPD.USER_TITLE1',
	'USER_FIRST_NAME' : 'UPD_INS_UPD.USER_FIRST_NAME1',
	'USER_MIDDLE_NAME' : 'UPD_INS_UPD.USER_MIDDLE_NAME1',
	'USER_LAST_NAME' : 'UPD_INS_UPD.USER_LAST_NAME1',
	'BIRTH_DT' : 'UPD_INS_UPD.BIRTH_DATE',
	'GENDER_ID' : 'UPD_INS_UPD.GENDER_ID1',
	'EMPLOYEE_START_DT' : 'UPD_INS_UPD.EMP_START_DATE',
	'ADDR_1' : 'UPD_INS_UPD.ADDRESS_11',
	'ADDR_2' : 'UPD_INS_UPD.ADDRESS_21',
	'CITY' : 'UPD_INS_UPD.CITY1',
	'STATE_PROV_CD' : 'UPD_INS_UPD.STATE_PROV_CODE',
	'POSTAL_CD' : 'UPD_INS_UPD.POSTAL_CODE',
	'COUNTRY_CD' : 'UPD_INS_UPD.COUNTRY_CODE',
	'USER_EMAIL_1' : 'UPD_INS_UPD.USER_EMAIL_11',
	'USER_EMAIL_2' : 'UPD_INS_UPD.USER_EMAIL_21',
	'PHONE_NBR' : 'UPD_INS_UPD.TELEPHONE_NUMBER',
	'FAX_NBR' : 'UPD_INS_UPD.FAX_NUMBER',
	'WM_EXTERNAL_USER_ID' : 'UPD_INS_UPD.EXTERNAL_USER_ID',
	'COPY_FROM_USER' : 'UPD_INS_UPD.COPY_FROM_USER1',
	'WM_SECURITY_POLICY_GROUP_ID' : 'UPD_INS_UPD.SECURITY_POLICY_GROUP_ID',
	'DEFAULT_WM_BUSINESS_UNIT_ID' : 'UPD_INS_UPD.DEFAULT_BUSINESS_UNIT_ID',
	'DEFAULT_WM_WHSE_REGION_ID' : 'UPD_INS_UPD.DEFAULT_WHSE_REGION_ID',
	'WM_CHANNEL_ID' : 'UPD_INS_UPD.CHANNEL_ID',
	'WM_COMM_METHOD_ID_DURING_BH_1' : 'UPD_INS_UPD.COMM_METHOD_ID_DURING_BH_1',
	'WM_COMM_METHOD_ID_DURING_BH_2' : 'UPD_INS_UPD.COMM_METHOD_ID_DURING_BH_2',
	'WM_COMM_METHOD_ID_AFTER_BH_1' : 'UPD_INS_UPD.COMM_METHOD_ID_AFTER_BH_1',
	'WM_COMM_METHOD_ID_AFTER_BH_2' : 'UPD_INS_UPD.COMM_METHOD_ID_AFTER_BH_2',
	'PASSWORD_MANAGED_INTERNALLY_FLAG' : 'UPD_INS_UPD.ISPASSWORDMANAGEDINTERNALLY',
	'LOGGED_IN_FLAG' : 'UPD_INS_UPD.LOGGED_IN1',
	'LAST_LOGIN_TSTMP' : 'UPD_INS_UPD.LAST_LOGIN_DTTM',
	'NUMBER_OF_INVALID_LOGINS' : 'UPD_INS_UPD.NUMBER_OF_INVALID_LOGINS1',
	'PASSWORD_RESET_TSTMP' : 'UPD_INS_UPD.PASSWORD_RESET_DATE_TIME',
	'LAST_PASSWORD_CHANGE_TSTMP' : 'UPD_INS_UPD.LAST_PASSWORD_CHANGE_DTTM',
	'WM_HIBERNATE_VERSION' : 'UPD_INS_UPD.HIBERNATE_VERSION1',
	'WM_CREATED_SOURCE_TYPE_ID' : 'UPD_INS_UPD.CREATED_SOURCE_TYPE_ID',
	'WM_CREATED_SOURCE' : 'UPD_INS_UPD.CREATED_SOURCE',
	'WM_CREATED_TSTMP' : 'UPD_INS_UPD.CREATED_DTTM',
	'WM_LAST_UPDATED_SOURCE_TYPE_ID' : 'UPD_INS_UPD.LAST_UPDATED_SOURCE_TYPE_ID',
	'WM_LAST_UPDATED_SOURCE' : 'UPD_INS_UPD.LAST_UPDATED_SOURCE',
	'WM_LAST_UPDATED_TSTMP' : 'UPD_INS_UPD.LAST_UPDATED_DTTM',
	'UPDATE_TSTMP' : 'UPD_INS_UPD.UPDATE_TSTMP',
	'LOAD_TSTMP' : 'UPD_INS_UPD.LOAD_TSTMP'} )
.whenNotMatchedInsert(values = {
	'LOCATION_ID' : 'UPD_INS_UPD.LOCATION_ID1',
	'WM_UCL_USER_ID' : 'UPD_INS_UPD.UCL_USER_ID',
	'WM_COMPANY_ID' : 'UPD_INS_UPD.COMPANY_ID',
	'WM_LOCATION_ID' : 'UPD_INS_UPD.LOCATION_ID2',
	'WM_LOCALE_ID' : 'UPD_INS_UPD.LOCALE_ID',
	'WM_USER_TYPE_ID' : 'UPD_INS_UPD.USER_TYPE_ID',
	'ACTIVE_FLAG' : 'UPD_INS_UPD.IS_ACTIVE',
	'USER_NAME' : 'UPD_INS_UPD.USER_NAME1',
	'TAX_ID_NBR' : 'UPD_INS_UPD.TAX_ID_NBR',
	'COMMON_NAME' : 'UPD_INS_UPD.COMMON_NAME1',
	'USER_PREFIX' : 'UPD_INS_UPD.USER_PREFIX1',
	'USER_TITLE' : 'UPD_INS_UPD.USER_TITLE1',
	'USER_FIRST_NAME' : 'UPD_INS_UPD.USER_FIRST_NAME1',
	'USER_MIDDLE_NAME' : 'UPD_INS_UPD.USER_MIDDLE_NAME1',
	'USER_LAST_NAME' : 'UPD_INS_UPD.USER_LAST_NAME1',
	'BIRTH_DT' : 'UPD_INS_UPD.BIRTH_DATE',
	'GENDER_ID' : 'UPD_INS_UPD.GENDER_ID1',
	'EMPLOYEE_START_DT' : 'UPD_INS_UPD.EMP_START_DATE',
	'ADDR_1' : 'UPD_INS_UPD.ADDRESS_11',
	'ADDR_2' : 'UPD_INS_UPD.ADDRESS_21',
	'CITY' : 'UPD_INS_UPD.CITY1',
	'STATE_PROV_CD' : 'UPD_INS_UPD.STATE_PROV_CODE',
	'POSTAL_CD' : 'UPD_INS_UPD.POSTAL_CODE',
	'COUNTRY_CD' : 'UPD_INS_UPD.COUNTRY_CODE',
	'USER_EMAIL_1' : 'UPD_INS_UPD.USER_EMAIL_11',
	'USER_EMAIL_2' : 'UPD_INS_UPD.USER_EMAIL_21',
	'PHONE_NBR' : 'UPD_INS_UPD.TELEPHONE_NUMBER',
	'FAX_NBR' : 'UPD_INS_UPD.FAX_NUMBER',
	'WM_EXTERNAL_USER_ID' : 'UPD_INS_UPD.EXTERNAL_USER_ID',
	'COPY_FROM_USER' : 'UPD_INS_UPD.COPY_FROM_USER1',
	'WM_SECURITY_POLICY_GROUP_ID' : 'UPD_INS_UPD.SECURITY_POLICY_GROUP_ID',
	'DEFAULT_WM_BUSINESS_UNIT_ID' : 'UPD_INS_UPD.DEFAULT_BUSINESS_UNIT_ID',
	'DEFAULT_WM_WHSE_REGION_ID' : 'UPD_INS_UPD.DEFAULT_WHSE_REGION_ID',
	'WM_CHANNEL_ID' : 'UPD_INS_UPD.CHANNEL_ID',
	'WM_COMM_METHOD_ID_DURING_BH_1' : 'UPD_INS_UPD.COMM_METHOD_ID_DURING_BH_1',
	'WM_COMM_METHOD_ID_DURING_BH_2' : 'UPD_INS_UPD.COMM_METHOD_ID_DURING_BH_2',
	'WM_COMM_METHOD_ID_AFTER_BH_1' : 'UPD_INS_UPD.COMM_METHOD_ID_AFTER_BH_1',
	'WM_COMM_METHOD_ID_AFTER_BH_2' : 'UPD_INS_UPD.COMM_METHOD_ID_AFTER_BH_2',
	'PASSWORD_MANAGED_INTERNALLY_FLAG' : 'UPD_INS_UPD.ISPASSWORDMANAGEDINTERNALLY',
	'LOGGED_IN_FLAG' : 'UPD_INS_UPD.LOGGED_IN1',
	'LAST_LOGIN_TSTMP' : 'UPD_INS_UPD.LAST_LOGIN_DTTM',
	'NUMBER_OF_INVALID_LOGINS' : 'UPD_INS_UPD.NUMBER_OF_INVALID_LOGINS1',
	'PASSWORD_RESET_TSTMP' : 'UPD_INS_UPD.PASSWORD_RESET_DATE_TIME',
	'LAST_PASSWORD_CHANGE_TSTMP' : 'UPD_INS_UPD.LAST_PASSWORD_CHANGE_DTTM',
	'WM_HIBERNATE_VERSION' : 'UPD_INS_UPD.HIBERNATE_VERSION1',
	'WM_CREATED_SOURCE_TYPE_ID' : 'UPD_INS_UPD.CREATED_SOURCE_TYPE_ID',
	'WM_CREATED_SOURCE' : 'UPD_INS_UPD.CREATED_SOURCE',
	'WM_CREATED_TSTMP' : 'UPD_INS_UPD.CREATED_DTTM',
	'WM_LAST_UPDATED_SOURCE_TYPE_ID' : 'UPD_INS_UPD.LAST_UPDATED_SOURCE_TYPE_ID',
	'WM_LAST_UPDATED_SOURCE' : 'UPD_INS_UPD.LAST_UPDATED_SOURCE',
	'WM_LAST_UPDATED_TSTMP' : 'UPD_INS_UPD.LAST_UPDATED_DTTM',
	'UPDATE_TSTMP' : 'UPD_INS_UPD.UPDATE_TSTMP',
	'LOAD_TSTMP' : 'UPD_INS_UPD.LOAD_TSTMP'}).execute()

quit()