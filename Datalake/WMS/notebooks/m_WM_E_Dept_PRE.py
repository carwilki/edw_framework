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

tableName='WM_E_DEPT_PRE'
schemaName=env+'_raw'

target_table_name = schemaName+'.'+tableName

refine_table_name='WM_E_DEPT'



prev_run_dt = spark.sql(f"""select max(prev_run_date) from {env}_raw.log_run_details where table_name='{refine_table_name}' and lower(status)= 'completed'""").collect()[0][0]

if prev_run_dt is None:
    #prev_run_dt = getMaxDate(refine_table_name,env)
    prev_run_dt = "2000-01-01"
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
where
(trunc(E_DEPT.CREATE_DATE_TIME) >= trunc(to_date('{prev_run_dt}','YYYY-MM-DD')) - 1 ) 
OR (trunc(E_DEPT.MOD_DATE_TIME) >= trunc(to_date('{prev_run_dt}','YYYY-MM-DD')) - 1)
OR (trunc(E_DEPT.CREATED_DTTM) >= trunc(to_date('{prev_run_dt}','YYYY-MM-DD')) - 1) 
AND 1=1"""

# COMMAND ----------

SQ_Shortcut_to_E_DEPT = spark.read \
  .format("jdbc") \
  .option("url", connection_string) \
  .option("query", dept_query) \
  .option("user", username) \
  .option("password", password)\
  .option("numPartitions", 3)\
  .option("driver", "oracle.jdbc.OracleDriver")\
  .load()

SQ_Shortcut_to_E_DEPT.createOrReplaceTempView('SQ_Shortcut_to_E_DEPT_Temp')

# COMMAND ----------

EXPTRANS = SQ_Shortcut_to_E_DEPT.select( \
	lit(f'{dcnbr}').cast(DecimalType(3,0)).alias('DC_NBR'), \
	SQ_Shortcut_to_E_DEPT.DEPT_ID.cast(DecimalType(9,0)).alias('DEPT_ID'), \
	SQ_Shortcut_to_E_DEPT.DEPT_CODE.cast(StringType()).alias('DEPT_CODE'), \
	SQ_Shortcut_to_E_DEPT.DESCRIPTION.cast(StringType()).alias('DESCRIPTION'), \
	SQ_Shortcut_to_E_DEPT.CREATE_DATE_TIME.cast(TimestampType()).alias('CREATE_DATE_TIME'), \
	SQ_Shortcut_to_E_DEPT.MOD_DATE_TIME.cast(TimestampType()).alias('MOD_DATE_TIME'), \
	SQ_Shortcut_to_E_DEPT.USER_ID.cast(StringType()).alias('USER_ID'), \
	SQ_Shortcut_to_E_DEPT.WHSE.cast(StringType()).alias('WHSE'), \
	SQ_Shortcut_to_E_DEPT.MISC_TXT_1.cast(StringType()).alias('MISC_TXT_1'), \
	SQ_Shortcut_to_E_DEPT.MISC_TXT_2.cast(StringType()).alias('MISC_TXT_2'), \
	SQ_Shortcut_to_E_DEPT.MISC_NUM_1.cast(DecimalType(20,7)).alias('MISC_NUM_1'), \
	SQ_Shortcut_to_E_DEPT.MISC_NUM_2.cast(DecimalType(20,7)).alias('MISC_NUM_2'), \
	SQ_Shortcut_to_E_DEPT.PERF_GOAL.cast(DecimalType(9,2)).alias('PERF_GOAL'), \
	SQ_Shortcut_to_E_DEPT.VERSION_ID.cast(DecimalType(6,0)).alias('VERSION_ID'), \
	SQ_Shortcut_to_E_DEPT.CREATED_DTTM.cast(TimestampType()).alias('CREATED_DTTM'), \
	SQ_Shortcut_to_E_DEPT.LAST_UPDATED_DTTM.cast(TimestampType()).alias('LAST_UPDATED_DTTM'), 
	current_timestamp().cast(TimestampType()).alias('LOAD_TSTMP') \
)

# COMMAND ----------

EXPTRANS.write.partitionBy('DC_NBR') \
  .mode("overwrite") \
  .option("replaceWhere", f'DC_NBR={dcnbr}') \
  .saveAsTable(target_table_name)


# COMMAND ----------


