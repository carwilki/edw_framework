# Databricks notebook source
import os
from pyspark.dbutils import DBUtils
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import SparkContext;
from pyspark.sql.session import SparkSession
from datetime import datetime


# COMMAND ----------

# MAGIC %run ./utils/configs 

# COMMAND ----------


dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='env', defaultValue='')
dcnbr = dbutils.widgets.get('DC_NBR')
env = dbutils.widgets.get('env')

tableName='WM_UCL_USER_PRE'
schemaName=env+'_raw'
target_table_name = schemaName+'.'+tableName

refine_table_name='WM_UCL_USER'



prev_run_dt = spark.sql(f"""select max(prev_run_date) from {env}_raw.log_run_details where table_name='{refine_table_name}' and lower(status)= 'completed'""").collect()[0][0]

if prev_run_dt is None:
    print("Prev_run_dt is none so getting maxdate")
    #prev_run_dt = getMaxDate(refine_table_name,env)
    prev_run_dt = "2023-05-12"
else:
    prev_run_dt = datetime.strptime(prev_run_dt, "%Y-%m-%d %H:%M:%S")
    prev_run_dt = prev_run_dt.strftime('%Y-%m-%d')

print('The prev run date is ' + prev_run_dt)


# COMMAND ----------

 (username,password,connection_string)= getConfig(dcnbr,env)


# COMMAND ----------

#Extract dc number 
dcnbr=dcnbr.strip()[2:]

# COMMAND ----------

user_query=f"""SELECT
UCL_USER.UCL_USER_ID,
UCL_USER.COMPANY_ID,
UCL_USER.USER_NAME,
UCL_USER.USER_PASSWORD,
UCL_USER.IS_ACTIVE,
UCL_USER.CREATED_SOURCE_TYPE_ID,
UCL_USER.CREATED_SOURCE,
cast(UCL_USER.CREATED_DTTM as Timestamp) as CREATED_DTTM,
UCL_USER.LAST_UPDATED_SOURCE_TYPE_ID,
UCL_USER.LAST_UPDATED_SOURCE,
cast(UCL_USER.LAST_UPDATED_DTTM as timestamp) as LAST_UPDATED_DTTM,
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
cast(UCL_USER.LAST_PASSWORD_CHANGE_DTTM as Timestamp) as LAST_PASSWORD_CHANGE_DTTM,
UCL_USER.LOGGED_IN,
cast(UCL_USER.LAST_LOGIN_DTTM as Timestamp) as LAST_LOGIN_DTTM,
UCL_USER.DEFAULT_BUSINESS_UNIT_ID,
UCL_USER.DEFAULT_WHSE_REGION_ID,
UCL_USER.CHANNEL_ID,
UCL_USER.HIBERNATE_VERSION,
UCL_USER.NUMBER_OF_INVALID_LOGINS,
UCL_USER.TAX_ID_NBR,
cast(UCL_USER.EMP_START_DATE as Timestamp) as EMP_START_DATE,
cast(UCL_USER.BIRTH_DATE as Timestamp) as BIRTH_DATE,
UCL_USER.GENDER_ID,
cast(UCL_USER.PASSWORD_RESET_DATE_TIME as Timestamp) as PASSWORD_RESET_DATE_TIME,
UCL_USER.PASSWORD_TOKEN,
UCL_USER.ISPASSWORDMANAGEDINTERNALLY,
UCL_USER.COPY_FROM_USER,
UCL_USER.EXTERNAL_USER_ID,
UCL_USER.SECURITY_POLICY_GROUP_ID
FROM UCL_USER
WHERE 
(trunc(UCL_USER.CREATED_DTTM)>= trunc(to_date('{prev_run_dt}','YYYY-MM-DD')) - 1) 
OR
(trunc(UCL_USER.LAST_UPDATED_DTTM)>= trunc(to_date('{prev_run_dt}','YYYY-MM-DD')) - 1) AND 1=1
"""

# COMMAND ----------

SQ_Shortcut_to_UCL_USER = spark.read \
  .format("jdbc") \
  .option("url", connection_string) \
  .option("query", user_query) \
  .option("user", username) \
  .option("password", password) \
  .option("numPartitions", 3)\
  .option("driver","oracle.jdbc.driver.OracleDriver")
  .option("sessionInitStatement","""begin 
  		execute immediate 'alter session set time_zone=''-07:00''';
	end;
 """) \
  .load()

SQ_Shortcut_to_UCL_USER.createOrReplaceTempView('SQ_Shortcut_to_UCL_USER_Temp')

# COMMAND ----------

EXPTRANS=SQ_Shortcut_to_UCL_USER.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

Shortcut_to_WM_UCL_USER_PRE = EXPTRANS.select( \
	lit(f'{dcnbr}').cast(DecimalType(3,0)).alias('DC_NBR'), \
	EXPTRANS.UCL_USER_ID.cast(DecimalType(18,0)).alias('UCL_USER_ID'), \
	EXPTRANS.COMPANY_ID.cast(DecimalType(9,0)).alias('COMPANY_ID'), \
	EXPTRANS.USER_NAME.cast(StringType()).alias('USER_NAME'), \
	EXPTRANS.USER_PASSWORD.cast(StringType()).alias('USER_PASSWORD'), \
	EXPTRANS.IS_ACTIVE.cast(DecimalType(4,0)).alias('IS_ACTIVE'), \
	EXPTRANS.CREATED_SOURCE_TYPE_ID.cast(DecimalType(4,0)).alias('CREATED_SOURCE_TYPE_ID'), \
	EXPTRANS.CREATED_SOURCE.cast(StringType()).alias('CREATED_SOURCE'), \
	EXPTRANS.CREATED_DTTM.cast(TimestampType()).alias('CREATED_DTTM'), \
	EXPTRANS.LAST_UPDATED_SOURCE_TYPE_ID.cast(DecimalType(4,0)).alias('LAST_UPDATED_SOURCE_TYPE_ID'), \
	EXPTRANS.LAST_UPDATED_SOURCE.cast(StringType()).alias('LAST_UPDATED_SOURCE'), \
	EXPTRANS.LAST_UPDATED_DTTM.cast(TimestampType()).alias('LAST_UPDATED_DTTM'), \
	EXPTRANS.USER_TYPE_ID.cast(DecimalType(4,0)).alias('USER_TYPE_ID'), \
	EXPTRANS.LOCALE_ID.cast(DecimalType(4,0)).alias('LOCALE_ID'), \
	EXPTRANS.LOCATION_ID.cast(DecimalType(18,0)).alias('LOCATION_ID'), \
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
	EXPTRANS.COMM_METHOD_ID_DURING_BH_1.cast(DecimalType(4,0)).alias('COMM_METHOD_ID_DURING_BH_1'), \
	EXPTRANS.COMM_METHOD_ID_DURING_BH_2.cast(DecimalType(4,0)).alias('COMM_METHOD_ID_DURING_BH_2'), \
	EXPTRANS.COMM_METHOD_ID_AFTER_BH_1.cast(DecimalType(4,0)).alias('COMM_METHOD_ID_AFTER_BH_1'), \
	EXPTRANS.COMM_METHOD_ID_AFTER_BH_2.cast(DecimalType(4,0)).alias('COMM_METHOD_ID_AFTER_BH_2'), \
	EXPTRANS.COMMON_NAME.cast(StringType()).alias('COMMON_NAME'), \
	EXPTRANS.LAST_PASSWORD_CHANGE_DTTM.cast(TimestampType()).alias('LAST_PASSWORD_CHANGE_DTTM'), \
	EXPTRANS.LOGGED_IN.cast(DecimalType(9,0)).alias('LOGGED_IN'), \
	EXPTRANS.LAST_LOGIN_DTTM.cast(TimestampType()).alias('LAST_LOGIN_DTTM'), \
	EXPTRANS.DEFAULT_BUSINESS_UNIT_ID.cast(DecimalType(9,0)).alias('DEFAULT_BUSINESS_UNIT_ID'), \
	EXPTRANS.DEFAULT_WHSE_REGION_ID.cast(DecimalType(9,0)).alias('DEFAULT_WHSE_REGION_ID'), \
	EXPTRANS.CHANNEL_ID.cast(DecimalType(18,0)).alias('CHANNEL_ID'), \
	EXPTRANS.HIBERNATE_VERSION.cast(DecimalType(10,0)).alias('HIBERNATE_VERSION'), \
	EXPTRANS.NUMBER_OF_INVALID_LOGINS.cast(DecimalType(4,0)).alias('NUMBER_OF_INVALID_LOGINS'), \
	EXPTRANS.TAX_ID_NBR.cast(StringType()).alias('TAX_ID_NBR'), \
	EXPTRANS.EMP_START_DATE.cast(TimestampType()).alias('EMP_START_DATE'), \
	EXPTRANS.BIRTH_DATE.cast(TimestampType()).alias('BIRTH_DATE'), \
	EXPTRANS.GENDER_ID.cast(StringType()).alias('GENDER_ID'), \
	EXPTRANS.PASSWORD_RESET_DATE_TIME.cast(TimestampType()).alias('PASSWORD_RESET_DATE_TIME'), \
	EXPTRANS.PASSWORD_TOKEN.cast(StringType()).alias('PASSWORD_TOKEN'), \
	EXPTRANS.ISPASSWORDMANAGEDINTERNALLY.cast(DecimalType(1,0)).alias('ISPASSWORDMANAGEDINTERNALLY'), \
	EXPTRANS.COPY_FROM_USER.cast(StringType()).alias('COPY_FROM_USER'), \
	EXPTRANS.EXTERNAL_USER_ID.cast(StringType()).alias('EXTERNAL_USER_ID'), \
	EXPTRANS.SECURITY_POLICY_GROUP_ID.cast(DecimalType(10,0)).alias('SECURITY_POLICY_GROUP_ID'), \
	current_timestamp().cast(TimestampType()).alias('LOAD_TSTMP') \
)



# COMMAND ----------

Shortcut_to_WM_UCL_USER_PRE.write.partitionBy('DC_NBR') \
  .mode("overwrite") \
  .option("replaceWhere", f'DC_NBR={dcnbr}') \
  .saveAsTable(target_table_name)

