from logging import *
from pyspark.dbutils import DBUtils
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import current_timestamp,lit,monotonically_increasing_id,col,when
from pyspark.sql.types import StringType,DecimalType,TimestampType,DateType,LongType

from Datalake.WMS.notebooks.utils.genericUtilities import getEnvPrefix
from Datalake.WMS.notebooks.utils.logger import logPrevRunDt
from Datalake.WMS.notebooks.utils.mergeUtils import executeMerge

spark: SparkSession = SparkSession.getActiveSession()
dbutils: DBUtils = DBUtils(spark)

dcnbr = dbutils.jobs.taskValue.get(key='DC_NBR', defaultValue='')
env = dbutils.jobs.taskValue.get(key='env', defaultValue='')

if dcnbr is None or dcnbr == "":
    raise ValueError("DC_NBR is not set")

if env is None or env == "":
    raise Exception("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"

pre_user_table=f'{raw}.WM_UCL_USER_PRE'
refined_user_table=f'{refine}.WM_UCL_USER'
site_profile_table=f'{legacy}.SITE_PROFILE'

logger=getLogger()
logger.setLevel(INFO)

SQ_Shortcut_to_WM_UCL_USER_PRE_query="""SELECT
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
FROM """+pre_user_table

SQ_Shortcut_to_WM_UCL_USER_PRE=spark.sql(SQ_Shortcut_to_WM_UCL_USER_PRE_query).withColumn("sys_row_id", monotonically_increasing_id())
logger.info('Query to extract data from'+pre_user_table+' executed successfully')



SQ_Shortcut_to_WM_UCL_USER_query="""SELECT
WM_UCL_USER.LOCATION_ID,
WM_UCL_USER.WM_UCL_USER_ID,
WM_UCL_USER.WM_CREATED_TSTMP,
WM_UCL_USER.WM_LAST_UPDATED_TSTMP,
WM_UCL_USER.LOAD_TSTMP,
WM_UCL_USER.USER_NAME
FROM """+refined_user_table+"""
WHERE USER_NAME IN (SELECT USER_NAME FROM """+pre_user_table+ """)"""
SQ_Shortcut_to_WM_UCL_USER=spark.sql(SQ_Shortcut_to_WM_UCL_USER_query).withColumn("sys_row_id", monotonically_increasing_id())

EXP_INT_CONVERSION = SQ_Shortcut_to_WM_UCL_USER_PRE.select( \
	SQ_Shortcut_to_WM_UCL_USER_PRE.sys_row_id.alias('sys_row_id'), \
	col('DC_NBR').cast(DecimalType(3,0)).alias('o_DC_NBR'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.UCL_USER_ID.alias('UCL_USER_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.COMPANY_ID.alias('COMPANY_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_NAME.alias('USER_NAME'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_PASSWORD.alias('USER_PASSWORD'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.IS_ACTIVE.alias('IS_ACTIVE'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.CREATED_SOURCE_TYPE_ID.alias('CREATED_SOURCE_TYPE_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.CREATED_SOURCE.alias('CREATED_SOURCE'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.CREATED_DTTM.alias('CREATED_DTTM'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.LAST_UPDATED_SOURCE_TYPE_ID.alias('LAST_UPDATED_SOURCE_TYPE_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_TYPE_ID.alias('USER_TYPE_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.LOCALE_ID.alias('LOCALE_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.LOCATION_ID.alias('LOCATION_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_FIRST_NAME.alias('USER_FIRST_NAME'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_MIDDLE_NAME.alias('USER_MIDDLE_NAME'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_LAST_NAME.alias('USER_LAST_NAME'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_PREFIX.alias('USER_PREFIX'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_TITLE.alias('USER_TITLE'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.TELEPHONE_NUMBER.alias('TELEPHONE_NUMBER'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.FAX_NUMBER.alias('FAX_NUMBER'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.ADDRESS_1.alias('ADDRESS_1'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.ADDRESS_2.alias('ADDRESS_2'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.CITY.alias('CITY'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.STATE_PROV_CODE.alias('STATE_PROV_CODE'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.POSTAL_CODE.alias('POSTAL_CODE'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.COUNTRY_CODE.alias('COUNTRY_CODE'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_EMAIL_1.alias('USER_EMAIL_1'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.USER_EMAIL_2.alias('USER_EMAIL_2'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.COMM_METHOD_ID_DURING_BH_1.alias('COMM_METHOD_ID_DURING_BH_1'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.COMM_METHOD_ID_DURING_BH_2.alias('COMM_METHOD_ID_DURING_BH_2'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.COMM_METHOD_ID_AFTER_BH_1.alias('COMM_METHOD_ID_AFTER_BH_1'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.COMM_METHOD_ID_AFTER_BH_2.alias('COMM_METHOD_ID_AFTER_BH_2'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.COMMON_NAME.alias('COMMON_NAME'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.LAST_PASSWORD_CHANGE_DTTM.alias('LAST_PASSWORD_CHANGE_DTTM'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.LOGGED_IN.alias('LOGGED_IN'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.LAST_LOGIN_DTTM.alias('LAST_LOGIN_DTTM'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.DEFAULT_BUSINESS_UNIT_ID.alias('DEFAULT_BUSINESS_UNIT_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.DEFAULT_WHSE_REGION_ID.alias('DEFAULT_WHSE_REGION_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.CHANNEL_ID.alias('CHANNEL_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.HIBERNATE_VERSION.alias('HIBERNATE_VERSION'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.NUMBER_OF_INVALID_LOGINS.alias('NUMBER_OF_INVALID_LOGINS'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.TAX_ID_NBR.alias('TAX_ID_NBR'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.EMP_START_DATE.alias('EMP_START_DATE'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.BIRTH_DATE.alias('BIRTH_DATE'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.GENDER_ID.alias('GENDER_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.PASSWORD_RESET_DATE_TIME.alias('PASSWORD_RESET_DATE_TIME'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.PASSWORD_TOKEN.alias('PASSWORD_TOKEN'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.ISPASSWORDMANAGEDINTERNALLY.alias('ISPASSWORDMANAGEDINTERNALLY'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.COPY_FROM_USER.alias('COPY_FROM_USER'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.EXTERNAL_USER_ID.alias('EXTERNAL_USER_ID'), \
	SQ_Shortcut_to_WM_UCL_USER_PRE.SECURITY_POLICY_GROUP_ID.alias('SECURITY_POLICY_GROUP_ID') \
)

SQ_Shortcut_to_SITE_PROFILE = spark.sql("""SELECT SITE_PROFILE.LOCATION_ID,SITE_PROFILE.STORE_NBR FROM """+site_profile_table).withColumn("sys_row_id", monotonically_increasing_id())
logger.info('Site profile table query executed successfully!')

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONVERSION,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONVERSION.o_DC_NBR],'inner').select( \
	EXP_INT_CONVERSION.sys_row_id.alias('sys_row_id'), \
	EXP_INT_CONVERSION.o_DC_NBR.alias('o_DC_NBR'), \
	EXP_INT_CONVERSION.UCL_USER_ID.alias('UCL_USER_ID'), \
	EXP_INT_CONVERSION.COMPANY_ID.alias('COMPANY_ID'), \
	EXP_INT_CONVERSION.USER_NAME.alias('USER_NAME'), \
	EXP_INT_CONVERSION.USER_PASSWORD.alias('USER_PASSWORD'), \
	EXP_INT_CONVERSION.IS_ACTIVE.alias('IS_ACTIVE'), \
	EXP_INT_CONVERSION.CREATED_SOURCE_TYPE_ID.alias('CREATED_SOURCE_TYPE_ID'), \
	EXP_INT_CONVERSION.CREATED_SOURCE.alias('CREATED_SOURCE'), \
	EXP_INT_CONVERSION.CREATED_DTTM.alias('CREATED_DTTM'), \
	EXP_INT_CONVERSION.LAST_UPDATED_SOURCE_TYPE_ID.alias('LAST_UPDATED_SOURCE_TYPE_ID'), \
	EXP_INT_CONVERSION.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE'), \
	EXP_INT_CONVERSION.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	EXP_INT_CONVERSION.USER_TYPE_ID.alias('USER_TYPE_ID'), \
	EXP_INT_CONVERSION.LOCALE_ID.alias('LOCALE_ID'), \
	EXP_INT_CONVERSION.LOCATION_ID.alias('LOCATION_ID'), \
	EXP_INT_CONVERSION.USER_FIRST_NAME.alias('USER_FIRST_NAME'), \
	EXP_INT_CONVERSION.USER_MIDDLE_NAME.alias('USER_MIDDLE_NAME'), \
	EXP_INT_CONVERSION.USER_LAST_NAME.alias('USER_LAST_NAME'), \
	EXP_INT_CONVERSION.USER_PREFIX.alias('USER_PREFIX'), \
	EXP_INT_CONVERSION.USER_TITLE.alias('USER_TITLE'), \
	EXP_INT_CONVERSION.TELEPHONE_NUMBER.alias('TELEPHONE_NUMBER'), \
	EXP_INT_CONVERSION.FAX_NUMBER.alias('FAX_NUMBER'), \
	EXP_INT_CONVERSION.ADDRESS_1.alias('ADDRESS_1'), \
	EXP_INT_CONVERSION.ADDRESS_2.alias('ADDRESS_2'), \
	EXP_INT_CONVERSION.CITY.alias('CITY'), \
	EXP_INT_CONVERSION.STATE_PROV_CODE.alias('STATE_PROV_CODE'), \
	EXP_INT_CONVERSION.POSTAL_CODE.alias('POSTAL_CODE'), \
	EXP_INT_CONVERSION.COUNTRY_CODE.alias('COUNTRY_CODE'), \
	EXP_INT_CONVERSION.USER_EMAIL_1.alias('USER_EMAIL_1'), \
	EXP_INT_CONVERSION.USER_EMAIL_2.alias('USER_EMAIL_2'), \
	EXP_INT_CONVERSION.COMM_METHOD_ID_DURING_BH_1.alias('COMM_METHOD_ID_DURING_BH_1'), \
	EXP_INT_CONVERSION.COMM_METHOD_ID_DURING_BH_2.alias('COMM_METHOD_ID_DURING_BH_2'), \
	EXP_INT_CONVERSION.COMM_METHOD_ID_AFTER_BH_1.alias('COMM_METHOD_ID_AFTER_BH_1'), \
	EXP_INT_CONVERSION.COMM_METHOD_ID_AFTER_BH_2.alias('COMM_METHOD_ID_AFTER_BH_2'), \
	EXP_INT_CONVERSION.COMMON_NAME.alias('COMMON_NAME'), \
	EXP_INT_CONVERSION.LAST_PASSWORD_CHANGE_DTTM.alias('LAST_PASSWORD_CHANGE_DTTM'), \
	EXP_INT_CONVERSION.LOGGED_IN.alias('LOGGED_IN'), \
	EXP_INT_CONVERSION.LAST_LOGIN_DTTM.alias('LAST_LOGIN_DTTM'), \
	EXP_INT_CONVERSION.DEFAULT_BUSINESS_UNIT_ID.alias('DEFAULT_BUSINESS_UNIT_ID'), \
	EXP_INT_CONVERSION.DEFAULT_WHSE_REGION_ID.alias('DEFAULT_WHSE_REGION_ID'), \
	EXP_INT_CONVERSION.CHANNEL_ID.alias('CHANNEL_ID'), \
	EXP_INT_CONVERSION.HIBERNATE_VERSION.alias('HIBERNATE_VERSION'), \
	EXP_INT_CONVERSION.NUMBER_OF_INVALID_LOGINS.alias('NUMBER_OF_INVALID_LOGINS'), \
	EXP_INT_CONVERSION.TAX_ID_NBR.alias('TAX_ID_NBR'), \
	EXP_INT_CONVERSION.EMP_START_DATE.alias('EMP_START_DATE'), \
	EXP_INT_CONVERSION.BIRTH_DATE.alias('BIRTH_DATE'), \
	EXP_INT_CONVERSION.GENDER_ID.alias('GENDER_ID'), \
	EXP_INT_CONVERSION.PASSWORD_RESET_DATE_TIME.alias('PASSWORD_RESET_DATE_TIME'), \
	EXP_INT_CONVERSION.PASSWORD_TOKEN.alias('PASSWORD_TOKEN'), \
	EXP_INT_CONVERSION.ISPASSWORDMANAGEDINTERNALLY.alias('ISPASSWORDMANAGEDINTERNALLY'), \
	EXP_INT_CONVERSION.COPY_FROM_USER.alias('COPY_FROM_USER'), \
	EXP_INT_CONVERSION.EXTERNAL_USER_ID.alias('EXTERNAL_USER_ID'), \
	EXP_INT_CONVERSION.SECURITY_POLICY_GROUP_ID.alias('SECURITY_POLICY_GROUP_ID'), \
	SQ_Shortcut_to_SITE_PROFILE.LOCATION_ID.alias('LOCATION_ID1'), \
	SQ_Shortcut_to_SITE_PROFILE.STORE_NBR.alias('STORE_NBR'))

JNR_WM_UCL_USER = SQ_Shortcut_to_WM_UCL_USER.join(JNR_SITE_PROFILE,[SQ_Shortcut_to_WM_UCL_USER.LOCATION_ID == JNR_SITE_PROFILE.LOCATION_ID1, SQ_Shortcut_to_WM_UCL_USER.USER_NAME == JNR_SITE_PROFILE.USER_NAME],'right_outer').select( \
	SQ_Shortcut_to_WM_UCL_USER.sys_row_id.alias('sys_row_id'), \
	JNR_SITE_PROFILE.LOCATION_ID1.alias('LOCATION_ID1'), \
	JNR_SITE_PROFILE.UCL_USER_ID.alias('UCL_USER_ID'), \
	JNR_SITE_PROFILE.COMPANY_ID.alias('COMPANY_ID'),\
	JNR_SITE_PROFILE.USER_NAME.alias('USER_NAME'),JNR_SITE_PROFILE.USER_PASSWORD.alias('USER_PASSWORD'), \
	JNR_SITE_PROFILE.IS_ACTIVE.alias('IS_ACTIVE'), \
	JNR_SITE_PROFILE.CREATED_SOURCE_TYPE_ID.alias('CREATED_SOURCE_TYPE_ID'), \
	JNR_SITE_PROFILE.CREATED_SOURCE.alias('CREATED_SOURCE'), \
	JNR_SITE_PROFILE.CREATED_DTTM.alias('CREATED_DTTM'), \
	JNR_SITE_PROFILE.LAST_UPDATED_SOURCE_TYPE_ID.alias('LAST_UPDATED_SOURCE_TYPE_ID'), \
	JNR_SITE_PROFILE.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE'), \
	JNR_SITE_PROFILE.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	JNR_SITE_PROFILE.USER_TYPE_ID.alias('USER_TYPE_ID'), \
	JNR_SITE_PROFILE.LOCALE_ID.alias('LOCALE_ID'), \
	JNR_SITE_PROFILE.LOCATION_ID.alias('LOCATION_ID'), \
	JNR_SITE_PROFILE.USER_FIRST_NAME.alias('USER_FIRST_NAME'), \
	JNR_SITE_PROFILE.USER_MIDDLE_NAME.alias('USER_MIDDLE_NAME'), \
	JNR_SITE_PROFILE.USER_LAST_NAME.alias('USER_LAST_NAME'), \
	JNR_SITE_PROFILE.USER_PREFIX.alias('USER_PREFIX'), \
	JNR_SITE_PROFILE.USER_TITLE.alias('USER_TITLE'), \
	JNR_SITE_PROFILE.TELEPHONE_NUMBER.alias('TELEPHONE_NUMBER'), \
	JNR_SITE_PROFILE.FAX_NUMBER.alias('FAX_NUMBER'), \
	JNR_SITE_PROFILE.ADDRESS_1.alias('ADDRESS_1'), \
	JNR_SITE_PROFILE.ADDRESS_2.alias('ADDRESS_2'), \
	JNR_SITE_PROFILE.CITY.alias('CITY'), \
	JNR_SITE_PROFILE.STATE_PROV_CODE.alias('STATE_PROV_CODE'), \
	JNR_SITE_PROFILE.POSTAL_CODE.alias('POSTAL_CODE'), \
	JNR_SITE_PROFILE.COUNTRY_CODE.alias('COUNTRY_CODE'), \
	JNR_SITE_PROFILE.USER_EMAIL_1.alias('USER_EMAIL_1'), \
	JNR_SITE_PROFILE.USER_EMAIL_2.alias('USER_EMAIL_2'), \
	JNR_SITE_PROFILE.COMM_METHOD_ID_DURING_BH_1.alias('COMM_METHOD_ID_DURING_BH_1'), \
	JNR_SITE_PROFILE.COMM_METHOD_ID_DURING_BH_2.alias('COMM_METHOD_ID_DURING_BH_2'), \
	JNR_SITE_PROFILE.COMM_METHOD_ID_AFTER_BH_1.alias('COMM_METHOD_ID_AFTER_BH_1'), \
	JNR_SITE_PROFILE.COMM_METHOD_ID_AFTER_BH_2.alias('COMM_METHOD_ID_AFTER_BH_2'), \
	JNR_SITE_PROFILE.COMMON_NAME.alias('COMMON_NAME'),JNR_SITE_PROFILE.LAST_PASSWORD_CHANGE_DTTM.alias('LAST_PASSWORD_CHANGE_DTTM'), \
	JNR_SITE_PROFILE.LOGGED_IN.alias('LOGGED_IN'), \
	JNR_SITE_PROFILE.LAST_LOGIN_DTTM.alias('LAST_LOGIN_DTTM'), \
	JNR_SITE_PROFILE.DEFAULT_BUSINESS_UNIT_ID.alias('DEFAULT_BUSINESS_UNIT_ID'), \
	JNR_SITE_PROFILE.DEFAULT_WHSE_REGION_ID.alias('DEFAULT_WHSE_REGION_ID'), \
	JNR_SITE_PROFILE.CHANNEL_ID.alias('CHANNEL_ID'), \
	JNR_SITE_PROFILE.HIBERNATE_VERSION.alias('HIBERNATE_VERSION'), \
	JNR_SITE_PROFILE.NUMBER_OF_INVALID_LOGINS.alias('NUMBER_OF_INVALID_LOGINS'), \
	JNR_SITE_PROFILE.TAX_ID_NBR.alias('TAX_ID_NBR'), \
	JNR_SITE_PROFILE.EMP_START_DATE.alias('EMP_START_DATE'), \
	JNR_SITE_PROFILE.BIRTH_DATE.alias('BIRTH_DATE'), \
	JNR_SITE_PROFILE.GENDER_ID.alias('GENDER_ID'), \
	JNR_SITE_PROFILE.PASSWORD_RESET_DATE_TIME.alias('PASSWORD_RESET_DATE_TIME'), \
	JNR_SITE_PROFILE.PASSWORD_TOKEN.alias('PASSWORD_TOKEN'), \
	JNR_SITE_PROFILE.ISPASSWORDMANAGEDINTERNALLY.alias('ISPASSWORDMANAGEDINTERNALLY'), \
	JNR_SITE_PROFILE.COPY_FROM_USER.alias('COPY_FROM_USER'), \
	JNR_SITE_PROFILE.EXTERNAL_USER_ID.alias('EXTERNAL_USER_ID'), \
	JNR_SITE_PROFILE.SECURITY_POLICY_GROUP_ID.alias('SECURITY_POLICY_GROUP_ID'), \
	SQ_Shortcut_to_WM_UCL_USER.LOCATION_ID.alias('i_LOCATION_ID'), \
	SQ_Shortcut_to_WM_UCL_USER.WM_UCL_USER_ID.alias('i_WM_UCL_USER_ID'), \
	SQ_Shortcut_to_WM_UCL_USER.WM_CREATED_TSTMP.alias('i_WM_CREATED_TSTMP'), \
	SQ_Shortcut_to_WM_UCL_USER.WM_LAST_UPDATED_TSTMP.alias('i_WM_LAST_UPDATED_TSTMP'), \
	SQ_Shortcut_to_WM_UCL_USER.LOAD_TSTMP.alias('i_LOAD_TSTMP'), \
	SQ_Shortcut_to_WM_UCL_USER.USER_NAME.alias('i_USER_NAME'))

FIL_UNCHANGED_RECORDS = JNR_WM_UCL_USER.select( \
	JNR_WM_UCL_USER.sys_row_id.alias('sys_row_id'), \
	JNR_WM_UCL_USER.LOCATION_ID1.alias('LOCATION_ID1'), \
	JNR_WM_UCL_USER.UCL_USER_ID.alias('UCL_USER_ID'), \
	JNR_WM_UCL_USER.COMPANY_ID.alias('COMPANY_ID'), \
	JNR_WM_UCL_USER.USER_NAME.alias('USER_NAME1'), \
	JNR_WM_UCL_USER.USER_PASSWORD.alias('USER_PASSWORD'), \
	JNR_WM_UCL_USER.IS_ACTIVE.alias('IS_ACTIVE'), \
	JNR_WM_UCL_USER.CREATED_SOURCE_TYPE_ID.alias('CREATED_SOURCE_TYPE_ID'), \
	JNR_WM_UCL_USER.CREATED_SOURCE.alias('CREATED_SOURCE'), \
	JNR_WM_UCL_USER.CREATED_DTTM.alias('CREATED_DTTM'), \
	JNR_WM_UCL_USER.LAST_UPDATED_SOURCE_TYPE_ID.alias('LAST_UPDATED_SOURCE_TYPE_ID'), \
	JNR_WM_UCL_USER.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE'), \
	JNR_WM_UCL_USER.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	JNR_WM_UCL_USER.USER_TYPE_ID.alias('USER_TYPE_ID'), \
	JNR_WM_UCL_USER.LOCALE_ID.alias('LOCALE_ID'), \
	JNR_WM_UCL_USER.LOCATION_ID.alias('LOCATION_ID2'), \
	JNR_WM_UCL_USER.USER_FIRST_NAME.alias('USER_FIRST_NAME1'), \
	JNR_WM_UCL_USER.USER_MIDDLE_NAME.alias('USER_MIDDLE_NAME1'), \
	JNR_WM_UCL_USER.USER_LAST_NAME.alias('USER_LAST_NAME1'), \
	JNR_WM_UCL_USER.USER_PREFIX.alias('USER_PREFIX1'), \
	JNR_WM_UCL_USER.USER_TITLE.alias('USER_TITLE1'), \
	JNR_WM_UCL_USER.TELEPHONE_NUMBER.alias('TELEPHONE_NUMBER'), \
	JNR_WM_UCL_USER.FAX_NUMBER.alias('FAX_NUMBER'), \
	JNR_WM_UCL_USER.ADDRESS_1.alias('ADDRESS_11'), \
	JNR_WM_UCL_USER.ADDRESS_2.alias('ADDRESS_21'), \
	JNR_WM_UCL_USER.CITY.alias('CITY1'), \
	JNR_WM_UCL_USER.STATE_PROV_CODE.alias('STATE_PROV_CODE'), \
	JNR_WM_UCL_USER.POSTAL_CODE.alias('POSTAL_CODE'), \
	JNR_WM_UCL_USER.COUNTRY_CODE.alias('COUNTRY_CODE'), \
	JNR_WM_UCL_USER.USER_EMAIL_1.alias('USER_EMAIL_11'), \
	JNR_WM_UCL_USER.USER_EMAIL_2.alias('USER_EMAIL_21'), \
	JNR_WM_UCL_USER.COMM_METHOD_ID_DURING_BH_1.alias('COMM_METHOD_ID_DURING_BH_1'), \
	JNR_WM_UCL_USER.COMM_METHOD_ID_DURING_BH_2.alias('COMM_METHOD_ID_DURING_BH_2'), \
	JNR_WM_UCL_USER.COMM_METHOD_ID_AFTER_BH_1.alias('COMM_METHOD_ID_AFTER_BH_1'), \
	JNR_WM_UCL_USER.COMM_METHOD_ID_AFTER_BH_2.alias('COMM_METHOD_ID_AFTER_BH_2'), \
	JNR_WM_UCL_USER.COMMON_NAME.alias('COMMON_NAME1'), \
	JNR_WM_UCL_USER.LAST_PASSWORD_CHANGE_DTTM.alias('LAST_PASSWORD_CHANGE_DTTM'), \
	JNR_WM_UCL_USER.LOGGED_IN.alias('LOGGED_IN1'), \
	JNR_WM_UCL_USER.LAST_LOGIN_DTTM.alias('LAST_LOGIN_DTTM'), \
	JNR_WM_UCL_USER.DEFAULT_BUSINESS_UNIT_ID.alias('DEFAULT_BUSINESS_UNIT_ID'), \
	JNR_WM_UCL_USER.DEFAULT_WHSE_REGION_ID.alias('DEFAULT_WHSE_REGION_ID'), \
	JNR_WM_UCL_USER.CHANNEL_ID.alias('CHANNEL_ID'), \
	JNR_WM_UCL_USER.HIBERNATE_VERSION.alias('HIBERNATE_VERSION1'), \
	JNR_WM_UCL_USER.NUMBER_OF_INVALID_LOGINS.alias('NUMBER_OF_INVALID_LOGINS1'), \
	JNR_WM_UCL_USER.TAX_ID_NBR.alias('TAX_ID_NBR'), \
	JNR_WM_UCL_USER.EMP_START_DATE.alias('EMP_START_DATE'), \
	JNR_WM_UCL_USER.BIRTH_DATE.alias('BIRTH_DATE'), \
	JNR_WM_UCL_USER.GENDER_ID.alias('GENDER_ID1'), \
	JNR_WM_UCL_USER.PASSWORD_RESET_DATE_TIME.alias('PASSWORD_RESET_DATE_TIME'), \
	JNR_WM_UCL_USER.PASSWORD_TOKEN.alias('PASSWORD_TOKEN'), \
	JNR_WM_UCL_USER.ISPASSWORDMANAGEDINTERNALLY.alias('ISPASSWORDMANAGEDINTERNALLY'), \
	JNR_WM_UCL_USER.COPY_FROM_USER.alias('COPY_FROM_USER1'), \
	JNR_WM_UCL_USER.EXTERNAL_USER_ID.alias('EXTERNAL_USER_ID'), \
	JNR_WM_UCL_USER.SECURITY_POLICY_GROUP_ID.alias('SECURITY_POLICY_GROUP_ID'), \
	JNR_WM_UCL_USER.i_WM_UCL_USER_ID.alias('i_WM_UCL_USER_ID'), \
	JNR_WM_UCL_USER.i_WM_CREATED_TSTMP.alias('i_WM_CREATED_TSTMP'), \
	JNR_WM_UCL_USER.i_WM_LAST_UPDATED_TSTMP.alias('i_WM_LAST_UPDATED_TSTMP'), \
	JNR_WM_UCL_USER.i_LOAD_TSTMP.alias('i_LOAD_TSTMP'), \
	JNR_WM_UCL_USER.i_USER_NAME.alias('i_USER_NAME1'), \
	JNR_WM_UCL_USER.i_LOCATION_ID.alias('i_LOCATION_ID'))\
		.filter(" (i_USER_NAME1 is null) OR ( NOT (i_USER_NAME1 is null) AND ( ((case when CREATED_DTTM is null then TO_DATE('01/01/1900','M/d/y') else CREATED_DTTM end ) != (case when i_WM_CREATED_TSTMP is null then TO_DATE('01/01/1900','M/d/y') else i_WM_CREATED_TSTMP end )) or ((case when LAST_UPDATED_DTTM is null then TO_DATE('01/01/1900','M/d/y') else LAST_UPDATED_DTTM end )!= (case when i_WM_LAST_UPDATED_TSTMP is null then TO_DATE('01/01/1900','M/d/y') else i_WM_LAST_UPDATED_TSTMP end )) ) )").withColumn("sys_row_id", monotonically_increasing_id())

EXP_UPD_VALIDATOR = FIL_UNCHANGED_RECORDS.select( \
	FIL_UNCHANGED_RECORDS.sys_row_id.alias('sys_row_id'), \
	FIL_UNCHANGED_RECORDS.LOCATION_ID1.alias('LOCATION_ID1'), \
	FIL_UNCHANGED_RECORDS.UCL_USER_ID.alias('UCL_USER_ID'), \
	FIL_UNCHANGED_RECORDS.COMPANY_ID.alias('COMPANY_ID'), \
	FIL_UNCHANGED_RECORDS.USER_NAME1.alias('USER_NAME1'), \
	FIL_UNCHANGED_RECORDS.USER_PASSWORD.alias('USER_PASSWORD'), \
	FIL_UNCHANGED_RECORDS.IS_ACTIVE.alias('IS_ACTIVE'), \
	FIL_UNCHANGED_RECORDS.CREATED_SOURCE_TYPE_ID.alias('CREATED_SOURCE_TYPE_ID'), \
	FIL_UNCHANGED_RECORDS.CREATED_SOURCE.alias('CREATED_SOURCE'), \
	FIL_UNCHANGED_RECORDS.CREATED_DTTM.alias('CREATED_DTTM'), \
	FIL_UNCHANGED_RECORDS.LAST_UPDATED_SOURCE_TYPE_ID.alias('LAST_UPDATED_SOURCE_TYPE_ID'), \
	FIL_UNCHANGED_RECORDS.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE'), \
	FIL_UNCHANGED_RECORDS.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	FIL_UNCHANGED_RECORDS.USER_TYPE_ID.alias('USER_TYPE_ID'), \
	FIL_UNCHANGED_RECORDS.LOCALE_ID.alias('LOCALE_ID'), \
	FIL_UNCHANGED_RECORDS.LOCATION_ID2.alias('LOCATION_ID2'), \
	FIL_UNCHANGED_RECORDS.USER_FIRST_NAME1.alias('USER_FIRST_NAME1'), \
	FIL_UNCHANGED_RECORDS.USER_MIDDLE_NAME1.alias('USER_MIDDLE_NAME1'), \
	FIL_UNCHANGED_RECORDS.USER_LAST_NAME1.alias('USER_LAST_NAME1'), \
	FIL_UNCHANGED_RECORDS.USER_PREFIX1.alias('USER_PREFIX1'), \
	FIL_UNCHANGED_RECORDS.USER_TITLE1.alias('USER_TITLE1'), \
	FIL_UNCHANGED_RECORDS.TELEPHONE_NUMBER.alias('TELEPHONE_NUMBER'), \
	FIL_UNCHANGED_RECORDS.FAX_NUMBER.alias('FAX_NUMBER'), \
	FIL_UNCHANGED_RECORDS.ADDRESS_11.alias('ADDRESS_11'), \
	FIL_UNCHANGED_RECORDS.ADDRESS_21.alias('ADDRESS_21'), \
	FIL_UNCHANGED_RECORDS.CITY1.alias('CITY1'), \
	FIL_UNCHANGED_RECORDS.STATE_PROV_CODE.alias('STATE_PROV_CODE'), \
	FIL_UNCHANGED_RECORDS.POSTAL_CODE.alias('POSTAL_CODE'), \
	FIL_UNCHANGED_RECORDS.COUNTRY_CODE.alias('COUNTRY_CODE'), \
	FIL_UNCHANGED_RECORDS.USER_EMAIL_11.alias('USER_EMAIL_11'), \
	FIL_UNCHANGED_RECORDS.USER_EMAIL_21.alias('USER_EMAIL_21'), \
	FIL_UNCHANGED_RECORDS.COMM_METHOD_ID_DURING_BH_1.alias('COMM_METHOD_ID_DURING_BH_1'), \
	FIL_UNCHANGED_RECORDS.COMM_METHOD_ID_DURING_BH_2.alias('COMM_METHOD_ID_DURING_BH_2'), \
	FIL_UNCHANGED_RECORDS.COMM_METHOD_ID_AFTER_BH_1.alias('COMM_METHOD_ID_AFTER_BH_1'), \
	FIL_UNCHANGED_RECORDS.COMM_METHOD_ID_AFTER_BH_2.alias('COMM_METHOD_ID_AFTER_BH_2'), \
	FIL_UNCHANGED_RECORDS.COMMON_NAME1.alias('COMMON_NAME1'), \
	FIL_UNCHANGED_RECORDS.LAST_PASSWORD_CHANGE_DTTM.alias('LAST_PASSWORD_CHANGE_DTTM'), \
	FIL_UNCHANGED_RECORDS.LOGGED_IN1.alias('LOGGED_IN1'), \
	FIL_UNCHANGED_RECORDS.LAST_LOGIN_DTTM.alias('LAST_LOGIN_DTTM'), \
	FIL_UNCHANGED_RECORDS.DEFAULT_BUSINESS_UNIT_ID.alias('DEFAULT_BUSINESS_UNIT_ID'), \
	FIL_UNCHANGED_RECORDS.DEFAULT_WHSE_REGION_ID.alias('DEFAULT_WHSE_REGION_ID'), \
	FIL_UNCHANGED_RECORDS.CHANNEL_ID.alias('CHANNEL_ID'), \
	FIL_UNCHANGED_RECORDS.HIBERNATE_VERSION1.alias('HIBERNATE_VERSION1'), \
	FIL_UNCHANGED_RECORDS.NUMBER_OF_INVALID_LOGINS1.alias('NUMBER_OF_INVALID_LOGINS1'), \
	FIL_UNCHANGED_RECORDS.TAX_ID_NBR.alias('TAX_ID_NBR'), \
	FIL_UNCHANGED_RECORDS.EMP_START_DATE.alias('EMP_START_DATE'), \
	FIL_UNCHANGED_RECORDS.BIRTH_DATE.alias('BIRTH_DATE'), \
	FIL_UNCHANGED_RECORDS.GENDER_ID1.alias('GENDER_ID1'), \
	FIL_UNCHANGED_RECORDS.PASSWORD_RESET_DATE_TIME.alias('PASSWORD_RESET_DATE_TIME'), \
	FIL_UNCHANGED_RECORDS.PASSWORD_TOKEN.alias('PASSWORD_TOKEN'), \
	FIL_UNCHANGED_RECORDS.ISPASSWORDMANAGEDINTERNALLY.alias('ISPASSWORDMANAGEDINTERNALLY'), \
	FIL_UNCHANGED_RECORDS.COPY_FROM_USER1.alias('COPY_FROM_USER1'), \
	FIL_UNCHANGED_RECORDS.EXTERNAL_USER_ID.alias('EXTERNAL_USER_ID'), \
	FIL_UNCHANGED_RECORDS.SECURITY_POLICY_GROUP_ID.alias('SECURITY_POLICY_GROUP_ID'), \
	FIL_UNCHANGED_RECORDS.i_WM_UCL_USER_ID.alias('i_WM_UCL_USER_ID'), \
	FIL_UNCHANGED_RECORDS.i_LOAD_TSTMP.alias('i_LOAD_TSTMP'), \
	FIL_UNCHANGED_RECORDS.i_USER_NAME1.alias('i_USER_NAME1'), \
	FIL_UNCHANGED_RECORDS.i_LOCATION_ID.alias('i_LOCATION_ID1')).select( \
	(col('sys_row_id')).alias('sys_row_id'), \
	col('LOCATION_ID1'), \
	col('UCL_USER_ID'), \
	col('COMPANY_ID'), \
	col('USER_NAME1'), \
	col('USER_PASSWORD'), \
	col('IS_ACTIVE'), \
	col('CREATED_SOURCE_TYPE_ID'), \
	col('CREATED_SOURCE'), \
	col('CREATED_DTTM'), \
	col('LAST_UPDATED_SOURCE_TYPE_ID'), \
	col('LAST_UPDATED_SOURCE'), \
	col('LAST_UPDATED_DTTM'), \
	col('USER_TYPE_ID'), \
	col('LOCALE_ID'), \
	col('LOCATION_ID2'), \
	col('USER_FIRST_NAME1'), \
	col('USER_MIDDLE_NAME1'), \
	col('USER_LAST_NAME1'), \
	col('USER_PREFIX1'), \
	col('USER_TITLE1'), \
	col('TELEPHONE_NUMBER'), \
	col('FAX_NUMBER'), \
	col('ADDRESS_11'), \
	col('ADDRESS_21'), \
	col('CITY1'), \
	col('STATE_PROV_CODE'), \
	col('POSTAL_CODE'), \
	col('COUNTRY_CODE'), \
	col('USER_EMAIL_11'), \
	col('USER_EMAIL_21'), \
	col('COMM_METHOD_ID_DURING_BH_1'), \
	col('COMM_METHOD_ID_DURING_BH_2'), \
	col('COMM_METHOD_ID_AFTER_BH_1'), \
	col('COMM_METHOD_ID_AFTER_BH_2'), \
	col('COMMON_NAME1'), \
	col('LAST_PASSWORD_CHANGE_DTTM'), \
	col('LOGGED_IN1'), \
	col('LAST_LOGIN_DTTM'), \
	col('DEFAULT_BUSINESS_UNIT_ID'), \
	col('DEFAULT_WHSE_REGION_ID'), \
	col('CHANNEL_ID'), \
	col('HIBERNATE_VERSION1'), \
	col('NUMBER_OF_INVALID_LOGINS1'), \
	col('TAX_ID_NBR'), \
	col('EMP_START_DATE'), \
	col('BIRTH_DATE'), \
	col('GENDER_ID1'), \
	col('PASSWORD_RESET_DATE_TIME'), \
	col('PASSWORD_TOKEN'), \
	col('ISPASSWORDMANAGEDINTERNALLY'), \
	col('COPY_FROM_USER1'), \
	col('EXTERNAL_USER_ID'), \
	col('SECURITY_POLICY_GROUP_ID'), \
	col('i_USER_NAME1'), \
	col('i_LOCATION_ID1'), \
	(current_timestamp()).alias('UPDATE_TSTMP'), \
	(when((col('i_LOAD_TSTMP').isNull()) ,(current_timestamp())).otherwise(col('i_LOAD_TSTMP'))).alias('LOAD_TSTMP'), \
	(when((((col('i_USER_NAME1').isNull()) &(col('i_LOCATION_ID1').isNull()))) ,(lit(1))).otherwise(lit(2))).alias('o_UPDATE_VALIDATOR') \
)  ## no i_WM_UCL_USER_ID & i_LOAD_TSTMP in code 

UPD_INS_UPD = EXP_UPD_VALIDATOR.select( \
	EXP_UPD_VALIDATOR.LOCATION_ID1.alias('LOCATION_ID1'), \
	EXP_UPD_VALIDATOR.UCL_USER_ID.alias('UCL_USER_ID'), \
	EXP_UPD_VALIDATOR.COMPANY_ID.alias('COMPANY_ID'), \
	EXP_UPD_VALIDATOR.USER_NAME1.alias('USER_NAME1'), \
	EXP_UPD_VALIDATOR.USER_PASSWORD.alias('USER_PASSWORD'), \
	EXP_UPD_VALIDATOR.IS_ACTIVE.alias('IS_ACTIVE'), \
	EXP_UPD_VALIDATOR.CREATED_SOURCE_TYPE_ID.alias('CREATED_SOURCE_TYPE_ID'), \
	EXP_UPD_VALIDATOR.CREATED_SOURCE.alias('CREATED_SOURCE'), \
	EXP_UPD_VALIDATOR.CREATED_DTTM.alias('CREATED_DTTM'), \
	EXP_UPD_VALIDATOR.LAST_UPDATED_SOURCE_TYPE_ID.alias('LAST_UPDATED_SOURCE_TYPE_ID'), \
	EXP_UPD_VALIDATOR.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE'), \
	EXP_UPD_VALIDATOR.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	EXP_UPD_VALIDATOR.USER_TYPE_ID.alias('USER_TYPE_ID'), \
	EXP_UPD_VALIDATOR.LOCALE_ID.alias('LOCALE_ID'), \
	EXP_UPD_VALIDATOR.LOCATION_ID2.alias('LOCATION_ID2'), \
	EXP_UPD_VALIDATOR.USER_FIRST_NAME1.alias('USER_FIRST_NAME1'), \
	EXP_UPD_VALIDATOR.USER_MIDDLE_NAME1.alias('USER_MIDDLE_NAME1'), \
	EXP_UPD_VALIDATOR.USER_LAST_NAME1.alias('USER_LAST_NAME1'), \
	EXP_UPD_VALIDATOR.USER_PREFIX1.alias('USER_PREFIX1'), \
	EXP_UPD_VALIDATOR.USER_TITLE1.alias('USER_TITLE1'), \
	EXP_UPD_VALIDATOR.TELEPHONE_NUMBER.alias('TELEPHONE_NUMBER'), \
	EXP_UPD_VALIDATOR.FAX_NUMBER.alias('FAX_NUMBER'), \
	EXP_UPD_VALIDATOR.ADDRESS_11.alias('ADDRESS_11'), \
	EXP_UPD_VALIDATOR.ADDRESS_21.alias('ADDRESS_21'), \
	EXP_UPD_VALIDATOR.CITY1.alias('CITY1'), \
	EXP_UPD_VALIDATOR.STATE_PROV_CODE.alias('STATE_PROV_CODE'), \
	EXP_UPD_VALIDATOR.POSTAL_CODE.alias('POSTAL_CODE'), \
	EXP_UPD_VALIDATOR.COUNTRY_CODE.alias('COUNTRY_CODE'), \
	EXP_UPD_VALIDATOR.USER_EMAIL_11.alias('USER_EMAIL_11'), \
	EXP_UPD_VALIDATOR.USER_EMAIL_21.alias('USER_EMAIL_21'), \
	EXP_UPD_VALIDATOR.COMM_METHOD_ID_DURING_BH_1.alias('COMM_METHOD_ID_DURING_BH_1'), \
	EXP_UPD_VALIDATOR.COMM_METHOD_ID_DURING_BH_2.alias('COMM_METHOD_ID_DURING_BH_2'), \
	EXP_UPD_VALIDATOR.COMM_METHOD_ID_AFTER_BH_1.alias('COMM_METHOD_ID_AFTER_BH_1'), \
	EXP_UPD_VALIDATOR.COMM_METHOD_ID_AFTER_BH_2.alias('COMM_METHOD_ID_AFTER_BH_2'), \
	EXP_UPD_VALIDATOR.COMMON_NAME1.alias('COMMON_NAME1'), \
	EXP_UPD_VALIDATOR.LAST_PASSWORD_CHANGE_DTTM.alias('LAST_PASSWORD_CHANGE_DTTM'), \
	EXP_UPD_VALIDATOR.LOGGED_IN1.alias('LOGGED_IN1'), \
	EXP_UPD_VALIDATOR.LAST_LOGIN_DTTM.alias('LAST_LOGIN_DTTM'), \
	EXP_UPD_VALIDATOR.DEFAULT_BUSINESS_UNIT_ID.alias('DEFAULT_BUSINESS_UNIT_ID'), \
	EXP_UPD_VALIDATOR.DEFAULT_WHSE_REGION_ID.alias('DEFAULT_WHSE_REGION_ID'), \
	EXP_UPD_VALIDATOR.CHANNEL_ID.alias('CHANNEL_ID'), \
	EXP_UPD_VALIDATOR.HIBERNATE_VERSION1.alias('HIBERNATE_VERSION1'), \
	EXP_UPD_VALIDATOR.NUMBER_OF_INVALID_LOGINS1.alias('NUMBER_OF_INVALID_LOGINS1'), \
	EXP_UPD_VALIDATOR.TAX_ID_NBR.alias('TAX_ID_NBR'), \
	EXP_UPD_VALIDATOR.EMP_START_DATE.alias('EMP_START_DATE'), \
	EXP_UPD_VALIDATOR.BIRTH_DATE.alias('BIRTH_DATE'), \
	EXP_UPD_VALIDATOR.GENDER_ID1.alias('GENDER_ID1'), \
	EXP_UPD_VALIDATOR.PASSWORD_RESET_DATE_TIME.alias('PASSWORD_RESET_DATE_TIME'), \
	EXP_UPD_VALIDATOR.PASSWORD_TOKEN.alias('PASSWORD_TOKEN'), \
	EXP_UPD_VALIDATOR.ISPASSWORDMANAGEDINTERNALLY.alias('ISPASSWORDMANAGEDINTERNALLY'), \
	EXP_UPD_VALIDATOR.COPY_FROM_USER1.alias('COPY_FROM_USER1'), \
	EXP_UPD_VALIDATOR.EXTERNAL_USER_ID.alias('EXTERNAL_USER_ID'), \
	EXP_UPD_VALIDATOR.SECURITY_POLICY_GROUP_ID.alias('SECURITY_POLICY_GROUP_ID'), \
	EXP_UPD_VALIDATOR.UPDATE_TSTMP.alias('UPDATE_TSTMP'), \
	EXP_UPD_VALIDATOR.LOAD_TSTMP.alias('LOAD_TSTMP'), \
	EXP_UPD_VALIDATOR.o_UPDATE_VALIDATOR.alias('o_UPDATE_VALIDATOR')) 
	
UPD_INS_UPD=UPD_INS_UPD.withColumn('pyspark_data_action', when(UPD_INS_UPD.o_UPDATE_VALIDATOR ==(lit(1)) , lit(0)) .when(UPD_INS_UPD.o_UPDATE_VALIDATOR ==(lit(2)) , lit(1)))

Shortcut_to_WM_UCL_USER = UPD_INS_UPD.select( \
	UPD_INS_UPD.LOCATION_ID1.cast(LongType()).alias('LOCATION_ID'), \
	UPD_INS_UPD.UCL_USER_ID.cast(DecimalType(18,0)).alias('WM_UCL_USER_ID'), \
	UPD_INS_UPD.COMPANY_ID.cast(DecimalType(9,0)).alias('WM_COMPANY_ID'), \
	UPD_INS_UPD.LOCATION_ID2.cast(DecimalType(18,0)).alias('WM_LOCATION_ID'), \
	UPD_INS_UPD.LOCALE_ID.cast(DecimalType(4,0)).alias('WM_LOCALE_ID'), \
	UPD_INS_UPD.USER_TYPE_ID.cast(DecimalType(4,0)).alias('WM_USER_TYPE_ID'), \
	UPD_INS_UPD.IS_ACTIVE.cast(DecimalType(1,0)).alias('ACTIVE_FLAG'), \
	UPD_INS_UPD.USER_NAME1.cast(StringType()).alias('USER_NAME'), \
	UPD_INS_UPD.TAX_ID_NBR.cast(StringType()).alias('TAX_ID_NBR'), \
	UPD_INS_UPD.COMMON_NAME1.cast(StringType()).alias('COMMON_NAME'), \
	UPD_INS_UPD.USER_PREFIX1.cast(StringType()).alias('USER_PREFIX'), \
	UPD_INS_UPD.USER_TITLE1.cast(StringType()).alias('USER_TITLE'), \
	UPD_INS_UPD.USER_FIRST_NAME1.cast(StringType()).alias('USER_FIRST_NAME'), \
	UPD_INS_UPD.USER_MIDDLE_NAME1.cast(StringType()).alias('USER_MIDDLE_NAME'), \
	UPD_INS_UPD.USER_LAST_NAME1.cast(StringType()).alias('USER_LAST_NAME'), \
	UPD_INS_UPD.BIRTH_DATE.cast(DateType()).alias('BIRTH_DT'), \
	UPD_INS_UPD.GENDER_ID1.cast(StringType()).alias('GENDER_ID'), \
	UPD_INS_UPD.EMP_START_DATE.cast(DateType()).alias('EMPLOYEE_START_DT'), \
	UPD_INS_UPD.ADDRESS_11.cast(StringType()).alias('ADDR_1'), \
	UPD_INS_UPD.ADDRESS_21.cast(StringType()).alias('ADDR_2'), \
	UPD_INS_UPD.CITY1.cast(StringType()).alias('CITY'), \
	UPD_INS_UPD.STATE_PROV_CODE.cast(StringType()).alias('STATE_PROV_CD'), \
	UPD_INS_UPD.POSTAL_CODE.cast(StringType()).alias('POSTAL_CD'), \
	UPD_INS_UPD.COUNTRY_CODE.cast(StringType()).alias('COUNTRY_CD'), \
	UPD_INS_UPD.USER_EMAIL_11.cast(StringType()).alias('USER_EMAIL_1'), \
	UPD_INS_UPD.USER_EMAIL_21.cast(StringType()).alias('USER_EMAIL_2'), \
	UPD_INS_UPD.TELEPHONE_NUMBER.cast(StringType()).alias('PHONE_NBR'), \
	UPD_INS_UPD.FAX_NUMBER.cast(StringType()).alias('FAX_NBR'), \
	UPD_INS_UPD.EXTERNAL_USER_ID.cast(StringType()).alias('WM_EXTERNAL_USER_ID'), \
	UPD_INS_UPD.COPY_FROM_USER1.cast(StringType()).alias('COPY_FROM_USER'), \
	UPD_INS_UPD.SECURITY_POLICY_GROUP_ID.cast(DecimalType(10,0)).alias('WM_SECURITY_POLICY_GROUP_ID'), \
	UPD_INS_UPD.DEFAULT_BUSINESS_UNIT_ID.cast(DecimalType(9,0)).alias('DEFAULT_WM_BUSINESS_UNIT_ID'), \
	UPD_INS_UPD.DEFAULT_WHSE_REGION_ID.cast(DecimalType(9,0)).alias('DEFAULT_WM_WHSE_REGION_ID'), \
	UPD_INS_UPD.CHANNEL_ID.cast(DecimalType(18,0)).alias('WM_CHANNEL_ID'), \
	UPD_INS_UPD.COMM_METHOD_ID_DURING_BH_1.cast(DecimalType(4,0)).alias('WM_COMM_METHOD_ID_DURING_BH_1'), \
	UPD_INS_UPD.COMM_METHOD_ID_DURING_BH_2.cast(DecimalType(4,0)).alias('WM_COMM_METHOD_ID_DURING_BH_2'), \
	UPD_INS_UPD.COMM_METHOD_ID_AFTER_BH_1.cast(DecimalType(4,0)).alias('WM_COMM_METHOD_ID_AFTER_BH_1'), \
	UPD_INS_UPD.COMM_METHOD_ID_AFTER_BH_2.cast(DecimalType(4,0)).alias('WM_COMM_METHOD_ID_AFTER_BH_2'), \
	UPD_INS_UPD.ISPASSWORDMANAGEDINTERNALLY.cast(DecimalType(1,0)).alias('PASSWORD_MANAGED_INTERNALLY_FLAG'), \
	UPD_INS_UPD.LOGGED_IN1.cast(DecimalType(1,0)).alias('LOGGED_IN_FLAG'), \
	UPD_INS_UPD.LAST_LOGIN_DTTM.cast(TimestampType()).alias('LAST_LOGIN_TSTMP'), \
	UPD_INS_UPD.NUMBER_OF_INVALID_LOGINS1.cast(DecimalType(4,0)).alias('NUMBER_OF_INVALID_LOGINS'), \
	UPD_INS_UPD.PASSWORD_RESET_DATE_TIME.cast(TimestampType()).alias('PASSWORD_RESET_TSTMP'), \
	UPD_INS_UPD.LAST_PASSWORD_CHANGE_DTTM.cast(TimestampType()).alias('LAST_PASSWORD_CHANGE_TSTMP'), \
	UPD_INS_UPD.HIBERNATE_VERSION1.cast(DecimalType(10,0)).alias('WM_HIBERNATE_VERSION'), \
	UPD_INS_UPD.CREATED_SOURCE_TYPE_ID.cast(DecimalType(4,0)).alias('WM_CREATED_SOURCE_TYPE_ID'), \
	UPD_INS_UPD.CREATED_SOURCE.cast(StringType()).alias('WM_CREATED_SOURCE'), \
	UPD_INS_UPD.CREATED_DTTM.cast(TimestampType()).alias('WM_CREATED_TSTMP'), \
	UPD_INS_UPD.LAST_UPDATED_SOURCE_TYPE_ID.cast(DecimalType(4,0)).alias('WM_LAST_UPDATED_SOURCE_TYPE_ID'), \
	UPD_INS_UPD.LAST_UPDATED_SOURCE.cast(StringType()).alias('WM_LAST_UPDATED_SOURCE'), \
	UPD_INS_UPD.LAST_UPDATED_DTTM.cast(TimestampType()).alias('WM_LAST_UPDATED_TSTMP'), \
	UPD_INS_UPD.UPDATE_TSTMP.cast(TimestampType()).alias('UPDATE_TSTMP'), \
	UPD_INS_UPD.LOAD_TSTMP.cast(TimestampType()).alias('LOAD_TSTMP'), \
	UPD_INS_UPD.pyspark_data_action.alias('pyspark_data_action') \
)

logger.info('Input data for '+refined_user_table+' generated!')

#Final Merge 
try:
    primary_key = "source.LOCATION_ID = target.LOCATION_ID AND source.USER_NAME = target.USER_NAME"
    executeMerge(Shortcut_to_WM_UCL_USER,refined_user_table, primary_key)
    logger.info('Merge with'+refined_user_table+'completed]')
    logPrevRunDt('WM_UCL_USER','WM_UCL_USER','Completed','N/A',f"{raw}.log_run_details")
except Exception as e:
    logPrevRunDt('WM_UCL_USER','WM_UCL_USER','Failed',str(e),f"{raw}.log_run_details")
    raise e

