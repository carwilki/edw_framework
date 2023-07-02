#Code converted on 2023-06-22 21:03:35
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

# Read in relation source variables
(username, password, connection_string) = getConfig(DC_NBR, env)

# COMMAND ----------
# Variable_declaration_comment
DC_NBR=args.DC_NBR
Prev_Run_Dt=args.Prev_Run_Dt
Initial_Load=args.Initial_Load

# COMMAND ----------
# Processing node SQ_Shortcut_to_UCL_USER, type SOURCE 
# COLUMN COUNT: 52

SQ_Shortcut_to_UCL_USER = (spark.read.format('jdbc').option('url', connection_string).option(
    'query', 
    f"""SELECT
            UCL_USER.UCL_USER_ID,
            UCL_USER.COMPANY_ID,
            UCL_USER.USER_NAME,
            UCL_USER.USER_PASSWORD,
            UCL_USER.IS_ACTIVE,
            UCL_USER.CREATED_SOURCE_TYPE_ID,
            UCL_USER.CREATED_SOURCE,
            UCL_USER.CREATED_DTTM,
            UCL_USER.LAST_UPDATED_SOURCE_TYPE_ID,
            UCL_USER.LAST_UPDATED_SOURCE,
            UCL_USER.LAST_UPDATED_DTTM,
            UCL_USER.USER_TYPE_ID,
            UCL_USER.LOCALE_ID,
            UCL_USER.LOCATION_ID,
            UCL_USER.USER_FIRST_NAME,
            UCL_USER.USER_MIDDLE_NAME,
            UCL_USER.USER_LAST_NAME,
            UCL_USER.USER_PREFIX,
            UCL_USER.USER_TITLE,
            UCL_USER.TELEPHONE_NUMBER,
            UCL_USER.FAX_NUMBER,
            UCL_USER.ADDRESS_1,
            UCL_USER.ADDRESS_2,
            UCL_USER.CITY,
            UCL_USER.STATE_PROV_CODE,
            UCL_USER.POSTAL_CODE,
            UCL_USER.COUNTRY_CODE,
            UCL_USER.USER_EMAIL_1,
            UCL_USER.USER_EMAIL_2,
            UCL_USER.COMM_METHOD_ID_DURING_BH_1,
            UCL_USER.COMM_METHOD_ID_DURING_BH_2,
            UCL_USER.COMM_METHOD_ID_AFTER_BH_1,
            UCL_USER.COMM_METHOD_ID_AFTER_BH_2,
            UCL_USER.COMMON_NAME,
            UCL_USER.LAST_PASSWORD_CHANGE_DTTM,
            UCL_USER.LOGGED_IN,
            UCL_USER.LAST_LOGIN_DTTM,
            UCL_USER.DEFAULT_BUSINESS_UNIT_ID,
            UCL_USER.DEFAULT_WHSE_REGION_ID,
            UCL_USER.CHANNEL_ID,
            UCL_USER.HIBERNATE_VERSION,
            UCL_USER.NUMBER_OF_INVALID_LOGINS,
            UCL_USER.TAX_ID_NBR,
            UCL_USER.EMP_START_DATE,
            UCL_USER.BIRTH_DATE,
            UCL_USER.GENDER_ID,
            UCL_USER.PASSWORD_RESET_DATE_TIME,
            UCL_USER.PASSWORD_TOKEN,
            UCL_USER.ISPASSWORDMANAGEDINTERNALLY,
            UCL_USER.COPY_FROM_USER,
            UCL_USER.EXTERNAL_USER_ID,
            UCL_USER.SECURITY_POLICY_GROUP_ID
        FROM UCL_USER
        WHERE {Initial_Load} (TRUNC(UCL_USER.CREATED_DTTM)>= TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (TRUNC(UCL_USER.LAST_UPDATED_DTTM)>= TRUNC(to_date('{Prev_Run_Dt}','MM/DD/YYYY HH24:MI:SS')) - 1)""") \
     .option('user', username) \
     .option('password', password) \
     .option('numPartitions', 3) \
     .option('driver', 'oracle.jdbc.OracleDriver') \
     .option('fetchsize', 10000) \
     .option('oracle.jdbc.timezoneAsRegion', 'false') \
	 .option(
	 	'sessionInitStatement',
	 	"begin execute immediate 'alter session set time_zone=''-07:00'''; end;"
	 ).load()).withColumn("sys_row_id", monotonically_increasing_id())	

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 54

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_UCL_USER_temp = SQ_Shortcut_to_UCL_USER.toDF(*["SQ_Shortcut_to_UCL_USER___" + col for col in SQ_Shortcut_to_UCL_USER.columns])

EXPTRANS = SQ_Shortcut_to_UCL_USER_temp.selectExpr( 
	"SQ_Shortcut_to_UCL_USER___sys_row_id as sys_row_id", 
	f"{DC_NBR} as DC_NBR_EXP", 
	"SQ_Shortcut_to_UCL_USER___UCL_USER_ID as UCL_USER_ID", 
	"SQ_Shortcut_to_UCL_USER___COMPANY_ID as COMPANY_ID", 
	"SQ_Shortcut_to_UCL_USER___USER_NAME as USER_NAME", 
	"SQ_Shortcut_to_UCL_USER___USER_PASSWORD as USER_PASSWORD", 
	"SQ_Shortcut_to_UCL_USER___IS_ACTIVE as IS_ACTIVE", 
	"SQ_Shortcut_to_UCL_USER___CREATED_SOURCE_TYPE_ID as CREATED_SOURCE_TYPE_ID", 
	"SQ_Shortcut_to_UCL_USER___CREATED_SOURCE as CREATED_SOURCE", 
	"SQ_Shortcut_to_UCL_USER___CREATED_DTTM as CREATED_DTTM", 
	"SQ_Shortcut_to_UCL_USER___LAST_UPDATED_SOURCE_TYPE_ID as LAST_UPDATED_SOURCE_TYPE_ID", 
	"SQ_Shortcut_to_UCL_USER___LAST_UPDATED_SOURCE as LAST_UPDATED_SOURCE", 
	"SQ_Shortcut_to_UCL_USER___LAST_UPDATED_DTTM as LAST_UPDATED_DTTM", 
	"SQ_Shortcut_to_UCL_USER___USER_TYPE_ID as USER_TYPE_ID", 
	"SQ_Shortcut_to_UCL_USER___LOCALE_ID as LOCALE_ID", 
	"SQ_Shortcut_to_UCL_USER___LOCATION_ID as LOCATION_ID", 
	"SQ_Shortcut_to_UCL_USER___USER_FIRST_NAME as USER_FIRST_NAME", 
	"SQ_Shortcut_to_UCL_USER___USER_MIDDLE_NAME as USER_MIDDLE_NAME", 
	"SQ_Shortcut_to_UCL_USER___USER_LAST_NAME as USER_LAST_NAME", 
	"SQ_Shortcut_to_UCL_USER___USER_PREFIX as USER_PREFIX", 
	"SQ_Shortcut_to_UCL_USER___USER_TITLE as USER_TITLE", 
	"SQ_Shortcut_to_UCL_USER___TELEPHONE_NUMBER as TELEPHONE_NUMBER", 
	"SQ_Shortcut_to_UCL_USER___FAX_NUMBER as FAX_NUMBER", 
	"SQ_Shortcut_to_UCL_USER___ADDRESS_1 as ADDRESS_1", 
	"SQ_Shortcut_to_UCL_USER___ADDRESS_2 as ADDRESS_2", 
	"SQ_Shortcut_to_UCL_USER___CITY as CITY", 
	"SQ_Shortcut_to_UCL_USER___STATE_PROV_CODE as STATE_PROV_CODE", 
	"SQ_Shortcut_to_UCL_USER___POSTAL_CODE as POSTAL_CODE", 
	"SQ_Shortcut_to_UCL_USER___COUNTRY_CODE as COUNTRY_CODE", 
	"SQ_Shortcut_to_UCL_USER___USER_EMAIL_1 as USER_EMAIL_1", 
	"SQ_Shortcut_to_UCL_USER___USER_EMAIL_2 as USER_EMAIL_2", 
	"SQ_Shortcut_to_UCL_USER___COMM_METHOD_ID_DURING_BH_1 as COMM_METHOD_ID_DURING_BH_1", 
	"SQ_Shortcut_to_UCL_USER___COMM_METHOD_ID_DURING_BH_2 as COMM_METHOD_ID_DURING_BH_2", 
	"SQ_Shortcut_to_UCL_USER___COMM_METHOD_ID_AFTER_BH_1 as COMM_METHOD_ID_AFTER_BH_1", 
	"SQ_Shortcut_to_UCL_USER___COMM_METHOD_ID_AFTER_BH_2 as COMM_METHOD_ID_AFTER_BH_2", 
	"SQ_Shortcut_to_UCL_USER___COMMON_NAME as COMMON_NAME", 
	"SQ_Shortcut_to_UCL_USER___LAST_PASSWORD_CHANGE_DTTM as LAST_PASSWORD_CHANGE_DTTM", 
	"SQ_Shortcut_to_UCL_USER___LOGGED_IN as LOGGED_IN", 
	"SQ_Shortcut_to_UCL_USER___LAST_LOGIN_DTTM as LAST_LOGIN_DTTM", 
	"SQ_Shortcut_to_UCL_USER___DEFAULT_BUSINESS_UNIT_ID as DEFAULT_BUSINESS_UNIT_ID", 
	"SQ_Shortcut_to_UCL_USER___DEFAULT_WHSE_REGION_ID as DEFAULT_WHSE_REGION_ID", 
	"SQ_Shortcut_to_UCL_USER___CHANNEL_ID as CHANNEL_ID", 
	"SQ_Shortcut_to_UCL_USER___HIBERNATE_VERSION as HIBERNATE_VERSION", 
	"SQ_Shortcut_to_UCL_USER___NUMBER_OF_INVALID_LOGINS as NUMBER_OF_INVALID_LOGINS", 
	"SQ_Shortcut_to_UCL_USER___TAX_ID_NBR as TAX_ID_NBR", 
	"SQ_Shortcut_to_UCL_USER___EMP_START_DATE as EMP_START_DATE", 
	"SQ_Shortcut_to_UCL_USER___BIRTH_DATE as BIRTH_DATE", 
	"SQ_Shortcut_to_UCL_USER___GENDER_ID as GENDER_ID", 
	"SQ_Shortcut_to_UCL_USER___PASSWORD_RESET_DATE_TIME as PASSWORD_RESET_DATE_TIME", 
	"SQ_Shortcut_to_UCL_USER___PASSWORD_TOKEN as PASSWORD_TOKEN", 
	"SQ_Shortcut_to_UCL_USER___ISPASSWORDMANAGEDINTERNALLY as ISPASSWORDMANAGEDINTERNALLY", 
	"SQ_Shortcut_to_UCL_USER___COPY_FROM_USER as COPY_FROM_USER", 
	"SQ_Shortcut_to_UCL_USER___EXTERNAL_USER_ID as EXTERNAL_USER_ID", 
	"SQ_Shortcut_to_UCL_USER___SECURITY_POLICY_GROUP_ID as SECURITY_POLICY_GROUP_ID", 
	"CURRENT_TIMESTAMP() as LOAD_TSTMP_EXP" 
)

# COMMAND ----------
# Processing node Shortcut_to_WM_UCL_USER_PRE, type TARGET 
# COLUMN COUNT: 54


Shortcut_to_WM_UCL_USER_PRE = EXPTRANS.selectExpr( 
	"CAST(DC_NBR_EXP AS BIGINT) as DC_NBR", 
	"CAST(UCL_USER_ID AS BIGINT) as UCL_USER_ID", 
	"CAST(COMPANY_ID AS BIGINT) as COMPANY_ID", 
	"CAST(USER_NAME AS STRING) as USER_NAME", 
	"CAST(USER_PASSWORD AS STRING) as USER_PASSWORD", 
	"CAST(IS_ACTIVE AS BIGINT) as IS_ACTIVE", 
	"CAST(CREATED_SOURCE_TYPE_ID AS BIGINT) as CREATED_SOURCE_TYPE_ID", 
	"CAST(CREATED_SOURCE AS STRING) as CREATED_SOURCE", 
	"CAST(CREATED_DTTM AS TIMESTAMP) as CREATED_DTTM", 
	"CAST(LAST_UPDATED_SOURCE_TYPE_ID AS BIGINT) as LAST_UPDATED_SOURCE_TYPE_ID", 
	"CAST(LAST_UPDATED_SOURCE AS STRING) as LAST_UPDATED_SOURCE", 
	"CAST(LAST_UPDATED_DTTM AS TIMESTAMP) as LAST_UPDATED_DTTM", 
	"CAST(USER_TYPE_ID AS BIGINT) as USER_TYPE_ID", 
	"CAST(LOCALE_ID AS BIGINT) as LOCALE_ID", 
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID", 
	"CAST(USER_FIRST_NAME AS STRING) as USER_FIRST_NAME", 
	"CAST(USER_MIDDLE_NAME AS STRING) as USER_MIDDLE_NAME", 
	"CAST(USER_LAST_NAME AS STRING) as USER_LAST_NAME", 
	"CAST(USER_PREFIX AS STRING) as USER_PREFIX", 
	"CAST(USER_TITLE AS STRING) as USER_TITLE", 
	"CAST(TELEPHONE_NUMBER AS STRING) as TELEPHONE_NUMBER", 
	"CAST(FAX_NUMBER AS STRING) as FAX_NUMBER", 
	"CAST(ADDRESS_1 AS STRING) as ADDRESS_1", 
	"CAST(ADDRESS_2 AS STRING) as ADDRESS_2", 
	"CAST(CITY AS STRING) as CITY", 
	"CAST(STATE_PROV_CODE AS STRING) as STATE_PROV_CODE", 
	"CAST(POSTAL_CODE AS STRING) as POSTAL_CODE", 
	"CAST(COUNTRY_CODE AS STRING) as COUNTRY_CODE", 
	"CAST(USER_EMAIL_1 AS STRING) as USER_EMAIL_1", 
	"CAST(USER_EMAIL_2 AS STRING) as USER_EMAIL_2", 
	"CAST(COMM_METHOD_ID_DURING_BH_1 AS BIGINT) as COMM_METHOD_ID_DURING_BH_1", 
	"CAST(COMM_METHOD_ID_DURING_BH_2 AS BIGINT) as COMM_METHOD_ID_DURING_BH_2", 
	"CAST(COMM_METHOD_ID_AFTER_BH_1 AS BIGINT) as COMM_METHOD_ID_AFTER_BH_1", 
	"CAST(COMM_METHOD_ID_AFTER_BH_2 AS BIGINT) as COMM_METHOD_ID_AFTER_BH_2", 
	"CAST(COMMON_NAME AS STRING) as COMMON_NAME", 
	"CAST(LAST_PASSWORD_CHANGE_DTTM AS TIMESTAMP) as LAST_PASSWORD_CHANGE_DTTM", 
	"CAST(LOGGED_IN AS BIGINT) as LOGGED_IN", 
	"CAST(LAST_LOGIN_DTTM AS TIMESTAMP) as LAST_LOGIN_DTTM", 
	"CAST(DEFAULT_BUSINESS_UNIT_ID AS BIGINT) as DEFAULT_BUSINESS_UNIT_ID", 
	"CAST(DEFAULT_WHSE_REGION_ID AS BIGINT) as DEFAULT_WHSE_REGION_ID", 
	"CAST(CHANNEL_ID AS BIGINT) as CHANNEL_ID", 
	"CAST(HIBERNATE_VERSION AS BIGINT) as HIBERNATE_VERSION", 
	"CAST(NUMBER_OF_INVALID_LOGINS AS BIGINT) as NUMBER_OF_INVALID_LOGINS", 
	"CAST(TAX_ID_NBR AS STRING) as TAX_ID_NBR", 
	"CAST(EMP_START_DATE AS TIMESTAMP) as EMP_START_DATE", 
	"CAST(BIRTH_DATE AS TIMESTAMP) as BIRTH_DATE", 
	"CAST(GENDER_ID AS STRING) as GENDER_ID", 
	"CAST(PASSWORD_RESET_DATE_TIME AS TIMESTAMP) as PASSWORD_RESET_DATE_TIME", 
	"CAST(PASSWORD_TOKEN AS STRING) as PASSWORD_TOKEN", 
	"CAST(ISPASSWORDMANAGEDINTERNALLY AS BIGINT) as ISPASSWORDMANAGEDINTERNALLY", 
	"CAST(COPY_FROM_USER AS STRING) as COPY_FROM_USER", 
	"CAST(EXTERNAL_USER_ID AS STRING) as EXTERNAL_USER_ID", 
	"CAST(SECURITY_POLICY_GROUP_ID AS BIGINT) as SECURITY_POLICY_GROUP_ID", 
	"CAST(LOAD_TSTMP_EXP AS TIMESTAMP) as LOAD_TSTMP" 
)
Shortcut_to_WM_UCL_USER_PRE.write.saveAsTable(f'{raw}.WM_UCL_USER_PRE')