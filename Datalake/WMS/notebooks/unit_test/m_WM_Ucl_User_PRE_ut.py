#
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


dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='Prev_Run_Dt', defaultValue='01/01/1901')
dbutils.widgets.text(name='Initial_Load', defaultValue='')


# Set global variables
starttime = datetime.now() #start timestamp of the script
dcnbr = dbutils.widgets.get('DC_NBR')
prev_run_dt = dbutils.widgets.get('Prev_Run_Dt')	


# COMMAND ----------

user_query=f"""SELECT
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
WHERE (date_trunc('DD', UCL_USER.CREATED_DTTM)>= date_trunc('DD', to_date('{prev_run_dt}','MM/DD/YYYY HH24:MI:SS')) - 1) OR (date_trunc('DD', UCL_USER.LAST_UPDATED_DTTM)>= date_trunc('DD', to_date('{prev_run_dt}','MM/DD/YYYY HH24:MI:SS')) - 1) AND 1=1"""

# COMMAND ----------

SQ_Shortcut_to_UCL_USER = spark.read \
  .format("jdbc") \
  .option("url", connection_string) \
  .option("query", user_query) \
  .option("user", username) \
  .option("password", password) \
  .load()

  EXPTRANS=SQ_Shortcut_to_UCL_USER.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

EXPTRANS = spark.sql("""
SELECT
123.12 as UCL_USER_ID,
123.12 as COMPANY_ID,
"qwerty12" as USER_NAME,
"qwerty12" as USER_PASSWORD,
1.0 as IS_ACTIVE,
12.1 as CREATED_SOURCE_TYPE_ID,
"qwerty12" as CREATED_SOURCE,
TIMESTAMP "2003-01-01 2:00:00" as CREATED_DTTM,
2.0 as LAST_UPDATED_SOURCE_TYPE_ID,
"abcd" as LAST_UPDATED_SOURCE,
TIMESTAMP "2003-01-01 2:00:00" as LAST_UPDATED_DTTM,
123.12 as USER_TYPE_ID,
123.12 as LOCALE_ID,
123.12 as LOCATION_ID,
"qwerty12" as USER_FIRST_NAME,
"qwerty12" as USER_MIDDLE_NAME,
"qwerty12" as USER_LAST_NAME,
"qwerty12" as USER_PREFIX,
"qwerty12" as USER_TITLE,
"384832938" as TELEPHONE_NUMBER,
"3723483" as FAX_NUMBER,
"qwerty12" as ADDRESS_1,
"qwerty12" as ADDRESS_2,
"Riverside" as CITY,
"Calf" as STATE_PROV_CODE,
"us-8293" as POSTAL_CODE,
"USA" as COUNTRY_CODE,
"qwerty12@xyz1.com" as USER_EMAIL_1,
"qwerty12@xyz.com" as USER_EMAIL_2,
12.1 as COMM_METHOD_ID_DURING_BH_1,
1.2 as COMM_METHOD_ID_DURING_BH_2,
1.2 as COMM_METHOD_ID_AFTER_BH_1,
1.2 as COMM_METHOD_ID_AFTER_BH_2,
"qwerty12" as COMMON_NAME,
TIMESTAMP "2003-01-01 2:00:00" as LAST_PASSWORD_CHANGE_DTTM,
2.0 as LOGGED_IN,
TIMESTAMP "2003-01-01 2:00:00" as LAST_LOGIN_DTTM,
12312 as DEFAULT_BUSINESS_UNIT_ID,
12312 as DEFAULT_WHSE_REGION_ID,
12312 as CHANNEL_ID,
12312 as HIBERNATE_VERSION,
12312 as NUMBER_OF_INVALID_LOGINS,
"qwerty12" as TAX_ID_NBR,
TIMESTAMP "2003-01-01 2:00:00" as EMP_START_DATE,
TIMESTAMP "1993-01-01 2:00:00" as BIRTH_DATE,
"M" as GENDER_ID,
TIMESTAMP "2003-01-01 2:00:00" as PASSWORD_RESET_DATE_TIME,
"qwerty12" as PASSWORD_TOKEN,
2.0 as ISPASSWORDMANAGEDINTERNALLY,
"qwerty12" as COPY_FROM_USER,
"qwerty12" as EXTERNAL_USER_ID,
123.12 as SECURITY_POLICY_GROUP_ID""")

# COMMAND ----------

EXPTRANS.display()

# COMMAND ----------

Shortcut_to_WM_UCL_USER_PRE = EXPTRANS.select( \
	lit(f'{dcnbr}').cast(LongType()).alias('DC_NBR'), \
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
	current_timestamp().cast(TimestampType()).alias('LOAD_TSTMP') \
)



# COMMAND ----------

# checking the row count
assert Shortcut_to_WM_UCL_USER_PRE.count() == EXPTRANS.count()

# COMMAND ----------

# checking the long data type columns
assert EXPTRANS.select(EXPTRANS.COMM_METHOD_ID_DURING_BH_1.cast(LongType())).first() == Shortcut_to_WM_UCL_USER_PRE.select(["COMM_METHOD_ID_DURING_BH_1"]).first()

# COMMAND ----------

# checking the Timestamp data type column
assert Shortcut_to_WM_UCL_USER_PRE.select(["LAST_LOGIN_DTTM"]).first() == EXPTRANS.select(["LAST_LOGIN_DTTM"]).first()

# COMMAND ----------

# checking the string data type column
assert Shortcut_to_WM_UCL_USER_PRE.select(["USER_FIRST_NAME"]).first() == EXPTRANS.select(["USER_FIRST_NAME"]).first()

# COMMAND ----------

Shortcut_to_WM_UCL_USER_PRE.write.partitionBy('DC_NBR') \
  .mode("overwrite") \
  .option("replaceWhere", f'DC_NBR={dcnbr}') \
  .saveAsTable("WM_UCL_USER_PRE")

