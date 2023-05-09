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


dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='Prev_Run_Dt', defaultValue='01/01/1901')
dbutils.widgets.text(name='Initial_Load', defaultValue='')


# Set global variables
starttime = datetime.now() #start timestamp of the script
dcnbr = dbutils.widgets.get('DC_NBR')
prev_run_dt = dbutils.widgets.get('Prev_Run_Dt')	


# COMMAND ----------

dept_query=f"""SELECT
E_DEPT.DEPT_ID,
E_DEPT.DEPT_CODE,
E_DEPT.DESCRIPTION,
E_DEPT.CREATE_DATE_TIME,
E_DEPT.MOD_DATE_TIME,
E_DEPT.USER_ID,
E_DEPT.WHSE,
E_DEPT.MISC_TXT_1,
E_DEPT.MISC_TXT_2,
E_DEPT.MISC_NUM_1,
E_DEPT.MISC_NUM_2,
E_DEPT.PERF_GOAL,
E_DEPT.VERSION_ID,
E_DEPT.CREATED_DTTM,
E_DEPT.LAST_UPDATED_DTTM
FROM E_DEPT
WHERE 
(date_trunc('DD', CREATE_DATE_TIME) >= date_trunc('DD', to_date('{prev_run_dt}','MM/DD/YYYY HH24:MI:SS'))-1) 
OR (date_trunc('DD', MOD_DATE_TIME) >=  date_trunc('DD', to_date('{prev_run_dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (date_trunc('DD', CREATED_DTTM) >= date_trunc('DD', to_date('{prev_run_dt}','MM/DD/YYYY HH24:MI:SS'))-1) OR (date_trunc('DD', LAST_UPDATED_DTTM) >=  date_trunc('DD', to_date('{prev_run_dt}','MM/DD/YYYY HH24:MI:SS'))-1) 
AND 1=1"""

# COMMAND ----------

SQ_Shortcut_to_E_DEPT =spark.sql("""select 
123.12 as DEPT_ID,
"qwerty" as DEPT_CODE,
"qwerty" as DESCRIPTION,
TIMESTAMP "2003-01-01 2:00:00" as CREATE_DATE_TIME,
TIMESTAMP "2003-01-01 2:00:00" as MOD_DATE_TIME,
"qwerty" as USER_ID,
"qwerty" as WHSE,
"qwerty" as MISC_TXT_1,
"qwerty" as MISC_TXT_2,
123.12 as MISC_NUM_1,
123.12 as MISC_NUM_2,
123.12 as PERF_GOAL,
1.01 as VERSION_ID,
TIMESTAMP "2003-01-01 2:00:00" as CREATED_DTTM,
TIMESTAMP "2003-01-01 2:00:00" as LAST_UPDATED_DTTM""")

# COMMAND ----------

SQ_Shortcut_to_E_DEPT.display()

# COMMAND ----------

SQ_Shortcut_to_E_DEPT = spark.read \
  .format("jdbc") \
  .option("url", connection_string) \
  .option("query", dept_query) \
  .option("user", username) \
  .option("password", password) \
  .load()

# COMMAND ----------

EXPTRANS = SQ_Shortcut_to_E_DEPT.select( \
	lit(f'{dcnbr}').cast(LongType()).alias('DC_NBR'), \
	SQ_Shortcut_to_E_DEPT.DEPT_ID.cast(LongType()).alias('DEPT_ID'), \
	SQ_Shortcut_to_E_DEPT.DEPT_CODE.cast(StringType()).alias('DEPT_CODE'), \
	SQ_Shortcut_to_E_DEPT.DESCRIPTION.cast(StringType()).alias('DESCRIPTION'), \
	SQ_Shortcut_to_E_DEPT.CREATE_DATE_TIME.cast(TimestampType()).alias('CREATE_DATE_TIME'), \
	SQ_Shortcut_to_E_DEPT.MOD_DATE_TIME.cast(TimestampType()).alias('MOD_DATE_TIME'), \
	SQ_Shortcut_to_E_DEPT.USER_ID.cast(StringType()).alias('USER_ID'), \
	SQ_Shortcut_to_E_DEPT.WHSE.cast(StringType()).alias('WHSE'), \
	SQ_Shortcut_to_E_DEPT.MISC_TXT_1.cast(StringType()).alias('MISC_TXT_1'), \
	SQ_Shortcut_to_E_DEPT.MISC_TXT_2.cast(StringType()).alias('MISC_TXT_2'), \
	SQ_Shortcut_to_E_DEPT.MISC_NUM_1.cast(LongType()).alias('MISC_NUM_1'), \
	SQ_Shortcut_to_E_DEPT.MISC_NUM_2.cast(LongType()).alias('MISC_NUM_2'), \
	SQ_Shortcut_to_E_DEPT.PERF_GOAL.cast(LongType()).alias('PERF_GOAL'), \
	SQ_Shortcut_to_E_DEPT.VERSION_ID.cast(LongType()).alias('VERSION_ID'), \
	SQ_Shortcut_to_E_DEPT.CREATED_DTTM.cast(TimestampType()).alias('CREATED_DTTM'), \
	SQ_Shortcut_to_E_DEPT.LAST_UPDATED_DTTM.cast(TimestampType()).alias('LAST_UPDATED_DTTM'), 
	current_timestamp().cast(TimestampType()).alias('LOAD_TSTMP') \
)

# COMMAND ----------

# checking the row count
assert SQ_Shortcut_to_E_DEPT.count() == EXPTRANS.count()

# COMMAND ----------

# checking the long data type columns
assert SQ_Shortcut_to_E_DEPT.select(SQ_Shortcut_to_E_DEPT.DEPT_ID.cast(LongType())).first() == EXPTRANS.select(["DEPT_ID"]).first()

# COMMAND ----------

# checking the string data type column
assert SQ_Shortcut_to_E_DEPT.select(["WHSE"]).first() == EXPTRANS.select(["WHSE"]).first()

# COMMAND ----------

# checking the Timestamp data type column
assert SQ_Shortcut_to_E_DEPT.select(["USER_ID"]).first() == EXPTRANS.select(["USER_ID"]).first()

# COMMAND ----------

EXPTRANS.write.partitionBy('DC_NBR') \
  .mode("overwrite") \
  .option("replaceWhere", f'DC_NBR={dcnbr}') \
  .saveAsTable("WM_E_DEPT_PRE")

