#Code converted on 2023-05-03 09:47:08
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import SparkContext;
from pyspark import SparkConf
from pyspark.sql.session import SparkSession
from datetime import datetime
from dbruntime import dbutils
#from PySparkBQWriter import *
#import ProcessingUtils;
#bqw = PySparkBQWriter()
#bqw.setDebug(True)

# COMMAND ----------

conf = SparkConf().setMaster('local')
sc = SparkContext.getOrCreate(conf = conf)
spark = SparkSession(sc)

# Set global variables
starttime = datetime.now() #start timestamp of the script

# Read in job variables
# read_infa_paramfile('', 'm_WM_Ucl_User_PRE') ProcessingUtils

# COMMAND ----------
# Variable_declaration_comment
dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='Prev_Run_Dt', defaultValue='01/01/1901')
dbutils.widgets.text(name='Initial_Load', defaultValue='')

# COMMAND ----------
# Processing node SQ_Shortcut_to_UCL_USER, type SOURCE 
# COLUMN COUNT: 52

SQ_Shortcut_to_UCL_USER = spark.read.jdbc(os.environ.get('DBConnection_Source_CONNECT_STRING'), f"""SELECT
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
WHERE $$Initial_Load (date_trunc('DD', UCL_USER.CREATED_DTTM)>= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS')) - 1) OR (date_trunc('DD', UCL_USER.LAST_UPDATED_DTTM)>= date_trunc('DD', to_date('$$Prev_Run_Dt','MM/DD/YYYY HH24:MI:SS')) - 1) AND 



1=1""", 
properties={
'user': os.environ.get('DBConnection_Source_LOGIN'),
'password': os.environ.get('DBConnection_Source_PASSWORD'),
'driver': os.environ.get('ORACLE_DRIVER')}).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_UCL_USER = SQ_Shortcut_to_UCL_USER \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[0],'UCL_USER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[1],'COMPANY_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[2],'USER_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[3],'USER_PASSWORD') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[4],'IS_ACTIVE') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[5],'CREATED_SOURCE_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[6],'CREATED_SOURCE') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[7],'CREATED_DTTM') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[8],'LAST_UPDATED_SOURCE_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[9],'LAST_UPDATED_SOURCE') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[10],'LAST_UPDATED_DTTM') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[11],'USER_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[12],'LOCALE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[13],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[14],'USER_FIRST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[15],'USER_MIDDLE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[16],'USER_LAST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[17],'USER_PREFIX') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[18],'USER_TITLE') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[19],'TELEPHONE_NUMBER') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[20],'FAX_NUMBER') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[21],'ADDRESS_1') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[22],'ADDRESS_2') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[23],'CITY') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[24],'STATE_PROV_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[25],'POSTAL_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[26],'COUNTRY_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[27],'USER_EMAIL_1') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[28],'USER_EMAIL_2') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[29],'COMM_METHOD_ID_DURING_BH_1') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[30],'COMM_METHOD_ID_DURING_BH_2') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[31],'COMM_METHOD_ID_AFTER_BH_1') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[32],'COMM_METHOD_ID_AFTER_BH_2') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[33],'COMMON_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[34],'LAST_PASSWORD_CHANGE_DTTM') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[35],'LOGGED_IN') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[36],'LAST_LOGIN_DTTM') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[37],'DEFAULT_BUSINESS_UNIT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[38],'DEFAULT_WHSE_REGION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[39],'CHANNEL_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[40],'HIBERNATE_VERSION') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[41],'NUMBER_OF_INVALID_LOGINS') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[42],'TAX_ID_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[43],'EMP_START_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[44],'BIRTH_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[45],'GENDER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[46],'PASSWORD_RESET_DATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[47],'PASSWORD_TOKEN') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[48],'ISPASSWORDMANAGEDINTERNALLY') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[49],'COPY_FROM_USER') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[50],'EXTERNAL_USER_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UCL_USER.columns[51],'SECURITY_POLICY_GROUP_ID')

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 54

EXPTRANS = SQ_Shortcut_to_UCL_USER.select( \
	SQ_Shortcut_to_UCL_USER.sys_row_id.alias('sys_row_id'), \
	(os.environ.get(lit('DC_NBR'))).alias('DC_NBR_EXP'), \
	SQ_Shortcut_to_UCL_USER.UCL_USER_ID.alias('UCL_USER_ID'), \
	SQ_Shortcut_to_UCL_USER.COMPANY_ID.alias('COMPANY_ID'), \
	SQ_Shortcut_to_UCL_USER.USER_NAME.alias('USER_NAME'), \
	SQ_Shortcut_to_UCL_USER.USER_PASSWORD.alias('USER_PASSWORD'), \
	SQ_Shortcut_to_UCL_USER.IS_ACTIVE.alias('IS_ACTIVE'), \
	SQ_Shortcut_to_UCL_USER.CREATED_SOURCE_TYPE_ID.alias('CREATED_SOURCE_TYPE_ID'), \
	SQ_Shortcut_to_UCL_USER.CREATED_SOURCE.alias('CREATED_SOURCE'), \
	SQ_Shortcut_to_UCL_USER.CREATED_DTTM.alias('CREATED_DTTM'), \
	SQ_Shortcut_to_UCL_USER.LAST_UPDATED_SOURCE_TYPE_ID.alias('LAST_UPDATED_SOURCE_TYPE_ID'), \
	SQ_Shortcut_to_UCL_USER.LAST_UPDATED_SOURCE.alias('LAST_UPDATED_SOURCE'), \
	SQ_Shortcut_to_UCL_USER.LAST_UPDATED_DTTM.alias('LAST_UPDATED_DTTM'), \
	SQ_Shortcut_to_UCL_USER.USER_TYPE_ID.alias('USER_TYPE_ID'), \
	SQ_Shortcut_to_UCL_USER.LOCALE_ID.alias('LOCALE_ID'), \
	SQ_Shortcut_to_UCL_USER.LOCATION_ID.alias('LOCATION_ID'), \
	SQ_Shortcut_to_UCL_USER.USER_FIRST_NAME.alias('USER_FIRST_NAME'), \
	SQ_Shortcut_to_UCL_USER.USER_MIDDLE_NAME.alias('USER_MIDDLE_NAME'), \
	SQ_Shortcut_to_UCL_USER.USER_LAST_NAME.alias('USER_LAST_NAME'), \
	SQ_Shortcut_to_UCL_USER.USER_PREFIX.alias('USER_PREFIX'), \
	SQ_Shortcut_to_UCL_USER.USER_TITLE.alias('USER_TITLE'), \
	SQ_Shortcut_to_UCL_USER.TELEPHONE_NUMBER.alias('TELEPHONE_NUMBER'), \
	SQ_Shortcut_to_UCL_USER.FAX_NUMBER.alias('FAX_NUMBER'), \
	SQ_Shortcut_to_UCL_USER.ADDRESS_1.alias('ADDRESS_1'), \
	SQ_Shortcut_to_UCL_USER.ADDRESS_2.alias('ADDRESS_2'), \
	SQ_Shortcut_to_UCL_USER.CITY.alias('CITY'), \
	SQ_Shortcut_to_UCL_USER.STATE_PROV_CODE.alias('STATE_PROV_CODE'), \
	SQ_Shortcut_to_UCL_USER.POSTAL_CODE.alias('POSTAL_CODE'), \
	SQ_Shortcut_to_UCL_USER.COUNTRY_CODE.alias('COUNTRY_CODE'), \
	SQ_Shortcut_to_UCL_USER.USER_EMAIL_1.alias('USER_EMAIL_1'), \
	SQ_Shortcut_to_UCL_USER.USER_EMAIL_2.alias('USER_EMAIL_2'), \
	SQ_Shortcut_to_UCL_USER.COMM_METHOD_ID_DURING_BH_1.alias('COMM_METHOD_ID_DURING_BH_1'), \
	SQ_Shortcut_to_UCL_USER.COMM_METHOD_ID_DURING_BH_2.alias('COMM_METHOD_ID_DURING_BH_2'), \
	SQ_Shortcut_to_UCL_USER.COMM_METHOD_ID_AFTER_BH_1.alias('COMM_METHOD_ID_AFTER_BH_1'), \
	SQ_Shortcut_to_UCL_USER.COMM_METHOD_ID_AFTER_BH_2.alias('COMM_METHOD_ID_AFTER_BH_2'), \
	SQ_Shortcut_to_UCL_USER.COMMON_NAME.alias('COMMON_NAME'), \
	SQ_Shortcut_to_UCL_USER.LAST_PASSWORD_CHANGE_DTTM.alias('LAST_PASSWORD_CHANGE_DTTM'), \
	SQ_Shortcut_to_UCL_USER.LOGGED_IN.alias('LOGGED_IN'), \
	SQ_Shortcut_to_UCL_USER.LAST_LOGIN_DTTM.alias('LAST_LOGIN_DTTM'), \
	SQ_Shortcut_to_UCL_USER.DEFAULT_BUSINESS_UNIT_ID.alias('DEFAULT_BUSINESS_UNIT_ID'), \
	SQ_Shortcut_to_UCL_USER.DEFAULT_WHSE_REGION_ID.alias('DEFAULT_WHSE_REGION_ID'), \
	SQ_Shortcut_to_UCL_USER.CHANNEL_ID.alias('CHANNEL_ID'), \
	SQ_Shortcut_to_UCL_USER.HIBERNATE_VERSION.alias('HIBERNATE_VERSION'), \
	SQ_Shortcut_to_UCL_USER.NUMBER_OF_INVALID_LOGINS.alias('NUMBER_OF_INVALID_LOGINS'), \
	SQ_Shortcut_to_UCL_USER.TAX_ID_NBR.alias('TAX_ID_NBR'), \
	SQ_Shortcut_to_UCL_USER.EMP_START_DATE.alias('EMP_START_DATE'), \
	SQ_Shortcut_to_UCL_USER.BIRTH_DATE.alias('BIRTH_DATE'), \
	SQ_Shortcut_to_UCL_USER.GENDER_ID.alias('GENDER_ID'), \
	SQ_Shortcut_to_UCL_USER.PASSWORD_RESET_DATE_TIME.alias('PASSWORD_RESET_DATE_TIME'), \
	SQ_Shortcut_to_UCL_USER.PASSWORD_TOKEN.alias('PASSWORD_TOKEN'), \
	SQ_Shortcut_to_UCL_USER.ISPASSWORDMANAGEDINTERNALLY.alias('ISPASSWORDMANAGEDINTERNALLY'), \
	SQ_Shortcut_to_UCL_USER.COPY_FROM_USER.alias('COPY_FROM_USER'), \
	SQ_Shortcut_to_UCL_USER.EXTERNAL_USER_ID.alias('EXTERNAL_USER_ID'), \
	SQ_Shortcut_to_UCL_USER.SECURITY_POLICY_GROUP_ID.alias('SECURITY_POLICY_GROUP_ID'), \
	(current_timestamp()()).alias('LOAD_TSTMP_EXP') \
)

# COMMAND ----------
# Processing node Shortcut_to_WM_UCL_USER_PRE, type TARGET 
# COLUMN COUNT: 54


Shortcut_to_WM_UCL_USER_PRE = EXPTRANS.select( \
	EXPTRANS.DC_NBR_EXP.cast(LongType()).alias('DC_NBR'), \
	EXPTRANS.UCL_USER_ID.cast(LongType()).alias('UCL_USER_ID'), \
	EXPTRANS.COMPANY_ID.cast(LongType()).alias('COMPANY_ID'), \
	EXPTRANS.USER_NAME.cast(StringType()).alias('USER_NAME'), \
	EXPTRANS.USER_PASSWORD.cast(StringType()).alias('USER_PASSWORD'), \
	EXPTRANS.IS_ACTIVE.cast(LongType()).alias('IS_ACTIVE'), \
	EXPTRANS.CREATED_SOURCE_TYPE_ID.cast(LongType()).alias('CREATED_SOURCE_TYPE_ID'), \
	EXPTRANS.CREATED_SOURCE.cast(StringType()).alias('CREATED_SOURCE'), \
	EXPTRANS.CREATED_DTTM.cast(TimestampType()).alias('CREATED_DTTM'), \
	EXPTRANS.LAST_UPDATED_SOURCE_TYPE_ID.cast(LongType()).alias('LAST_UPDATED_SOURCE_TYPE_ID'), \
	EXPTRANS.LAST_UPDATED_SOURCE.cast(StringType()).alias('LAST_UPDATED_SOURCE'), \
	EXPTRANS.LAST_UPDATED_DTTM.cast(TimestampType()).alias('LAST_UPDATED_DTTM'), \
	EXPTRANS.USER_TYPE_ID.cast(LongType()).alias('USER_TYPE_ID'), \
	EXPTRANS.LOCALE_ID.cast(LongType()).alias('LOCALE_ID'), \
	EXPTRANS.LOCATION_ID.cast(LongType()).alias('LOCATION_ID'), \
	EXPTRANS.USER_FIRST_NAME.cast(StringType()).alias('USER_FIRST_NAME'), \
	EXPTRANS.USER_MIDDLE_NAME.cast(StringType()).alias('USER_MIDDLE_NAME'), \
	EXPTRANS.USER_LAST_NAME.cast(StringType()).alias('USER_LAST_NAME'), \
	EXPTRANS.USER_PREFIX.cast(StringType()).alias('USER_PREFIX'), \
	EXPTRANS.USER_TITLE.cast(StringType()).alias('USER_TITLE'), \
	EXPTRANS.TELEPHONE_NUMBER.cast(StringType()).alias('TELEPHONE_NUMBER'), \
	EXPTRANS.FAX_NUMBER.cast(StringType()).alias('FAX_NUMBER'), \
	EXPTRANS.ADDRESS_1.cast(StringType()).alias('ADDRESS_1'), \
	EXPTRANS.ADDRESS_2.cast(StringType()).alias('ADDRESS_2'), \
	EXPTRANS.CITY.cast(StringType()).alias('CITY'), \
	EXPTRANS.STATE_PROV_CODE.cast(StringType()).alias('STATE_PROV_CODE'), \
	EXPTRANS.POSTAL_CODE.cast(StringType()).alias('POSTAL_CODE'), \
	EXPTRANS.COUNTRY_CODE.cast(StringType()).alias('COUNTRY_CODE'), \
	EXPTRANS.USER_EMAIL_1.cast(StringType()).alias('USER_EMAIL_1'), \
	EXPTRANS.USER_EMAIL_2.cast(StringType()).alias('USER_EMAIL_2'), \
	EXPTRANS.COMM_METHOD_ID_DURING_BH_1.cast(LongType()).alias('COMM_METHOD_ID_DURING_BH_1'), \
	EXPTRANS.COMM_METHOD_ID_DURING_BH_2.cast(LongType()).alias('COMM_METHOD_ID_DURING_BH_2'), \
	EXPTRANS.COMM_METHOD_ID_AFTER_BH_1.cast(LongType()).alias('COMM_METHOD_ID_AFTER_BH_1'), \
	EXPTRANS.COMM_METHOD_ID_AFTER_BH_2.cast(LongType()).alias('COMM_METHOD_ID_AFTER_BH_2'), \
	EXPTRANS.COMMON_NAME.cast(StringType()).alias('COMMON_NAME'), \
	EXPTRANS.LAST_PASSWORD_CHANGE_DTTM.cast(TimestampType()).alias('LAST_PASSWORD_CHANGE_DTTM'), \
	EXPTRANS.LOGGED_IN.cast(LongType()).alias('LOGGED_IN'), \
	EXPTRANS.LAST_LOGIN_DTTM.cast(TimestampType()).alias('LAST_LOGIN_DTTM'), \
	EXPTRANS.DEFAULT_BUSINESS_UNIT_ID.cast(LongType()).alias('DEFAULT_BUSINESS_UNIT_ID'), \
	EXPTRANS.DEFAULT_WHSE_REGION_ID.cast(LongType()).alias('DEFAULT_WHSE_REGION_ID'), \
	EXPTRANS.CHANNEL_ID.cast(LongType()).alias('CHANNEL_ID'), \
	EXPTRANS.HIBERNATE_VERSION.cast(LongType()).alias('HIBERNATE_VERSION'), \
	EXPTRANS.NUMBER_OF_INVALID_LOGINS.cast(LongType()).alias('NUMBER_OF_INVALID_LOGINS'), \
	EXPTRANS.TAX_ID_NBR.cast(StringType()).alias('TAX_ID_NBR'), \
	EXPTRANS.EMP_START_DATE.cast(TimestampType()).alias('EMP_START_DATE'), \
	EXPTRANS.BIRTH_DATE.cast(TimestampType()).alias('BIRTH_DATE'), \
	EXPTRANS.GENDER_ID.cast(StringType()).alias('GENDER_ID'), \
	EXPTRANS.PASSWORD_RESET_DATE_TIME.cast(TimestampType()).alias('PASSWORD_RESET_DATE_TIME'), \
	EXPTRANS.PASSWORD_TOKEN.cast(StringType()).alias('PASSWORD_TOKEN'), \
	EXPTRANS.ISPASSWORDMANAGEDINTERNALLY.cast(LongType()).alias('ISPASSWORDMANAGEDINTERNALLY'), \
	EXPTRANS.COPY_FROM_USER.cast(StringType()).alias('COPY_FROM_USER'), \
	EXPTRANS.EXTERNAL_USER_ID.cast(StringType()).alias('EXTERNAL_USER_ID'), \
	EXPTRANS.SECURITY_POLICY_GROUP_ID.cast(LongType()).alias('SECURITY_POLICY_GROUP_ID'), \
	EXPTRANS.LOAD_TSTMP_EXP.cast(TimestampType()).alias('LOAD_TSTMP') \
)
Shortcut_to_WM_UCL_USER_PRE.write.saveAsTable('WM_UCL_USER_PRE', mode = 'overwrite')