# Databricks notebook source
import os
from pyspark.dbutils import DBUtils
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from datetime import datetime

# COMMAND ----------


dbutils.widgets.text(name='DC_NBR', defaultValue='')
dbutils.widgets.text(name='env', defaultValue='')

dcnbr = dbutils.widgets.get('DC_NBR')
env = dbutils.widgets.get('env')

tableName='WM_E_CONSOL_PERF_SMRY_PRE'
schemaName=env+'_raw'

target_table_name = schemaName+'.'+tableName

refine_table_name='WM_E_CONSOL_PERF_SMRY'


prev_run_dt = str(spark.sql(f"""select max(prev_run_date) from {env}_raw.log_run_details where table_name='{refine_table_name}' and lower(status)= 'completed'""").collect()[0][0])

if prev_run_dt =="None":
    
    prev_run_dt='1990-01-01'
else:
    prev_run_dt = datetime.strptime(prev_run_dt, "%Y-%m-%d %H:%M:%S.%f")
    print('The prev run date is ' + prev_run_dt.strftime('%Y-%m-%d'))

# COMMAND ----------

# MAGIC %run ./utils/configs 

# COMMAND ----------

#get Configs for JDBC Credentials
(username,password,connection_string)= getConfig(dcnbr,env)

#Extract dc number 
dcnbr=dcnbr.strip()[2:]


# COMMAND ----------

perf_summary_query=f"""SELECT
E_CONSOL_PERF_SMRY.PERF_SMRY_TRAN_ID,
E_CONSOL_PERF_SMRY.WHSE,
E_CONSOL_PERF_SMRY.LOGIN_USER_ID,
E_CONSOL_PERF_SMRY.JOB_FUNCTION_NAME,
E_CONSOL_PERF_SMRY.SPVSR_LOGIN_USER_ID,
E_CONSOL_PERF_SMRY.DEPT_CODE,
cast(E_CONSOL_PERF_SMRY.CLOCK_IN_DATE as timestamp) as CLOCK_IN_DATE,
E_CONSOL_PERF_SMRY.CLOCK_IN_STATUS,
E_CONSOL_PERF_SMRY.TOTAL_SAM,
E_CONSOL_PERF_SMRY.TOTAL_PAM,
E_CONSOL_PERF_SMRY.TOTAL_TIME,
E_CONSOL_PERF_SMRY.OSDL,
E_CONSOL_PERF_SMRY.OSIL,
E_CONSOL_PERF_SMRY.NSDL,
E_CONSOL_PERF_SMRY.SIL,
E_CONSOL_PERF_SMRY.UDIL,
E_CONSOL_PERF_SMRY.UIL,
E_CONSOL_PERF_SMRY.ADJ_OSDL,
E_CONSOL_PERF_SMRY.ADJ_OSIL,
E_CONSOL_PERF_SMRY.ADJ_UDIL,
E_CONSOL_PERF_SMRY.ADJ_NSDL,
E_CONSOL_PERF_SMRY.PAID_BRK,
E_CONSOL_PERF_SMRY.UNPAID_BRK,
E_CONSOL_PERF_SMRY.REF_OSDL,
E_CONSOL_PERF_SMRY.REF_OSIL,
E_CONSOL_PERF_SMRY.REF_UDIL,
E_CONSOL_PERF_SMRY.REF_NSDL,
E_CONSOL_PERF_SMRY.REF_ADJ_OSDL,
E_CONSOL_PERF_SMRY.REF_ADJ_OSIL,
E_CONSOL_PERF_SMRY.REF_ADJ_UDIL,
E_CONSOL_PERF_SMRY.REF_ADJ_NSDL,
E_CONSOL_PERF_SMRY.MISC_NUMBER_1,
E_CONSOL_PERF_SMRY.CREATE_DATE_TIME,
E_CONSOL_PERF_SMRY.MOD_DATE_TIME,
E_CONSOL_PERF_SMRY.USER_ID,
E_CONSOL_PERF_SMRY.MISC_1,
E_CONSOL_PERF_SMRY.MISC_2,
cast(E_CONSOL_PERF_SMRY.CLOCK_OUT_DATE as timestamp) as CLOCK_OUT_DATE,
E_CONSOL_PERF_SMRY.SHIFT_CODE,
E_CONSOL_PERF_SMRY.EVENT_COUNT,
cast(E_CONSOL_PERF_SMRY.START_DATE_TIME as timestamp) as START_DATE_TIME ,
cast(E_CONSOL_PERF_SMRY.END_DATE_TIME as timestamp) as END_DATE_TIME,
E_CONSOL_PERF_SMRY.LEVEL_1,
E_CONSOL_PERF_SMRY.LEVEL_2,
E_CONSOL_PERF_SMRY.LEVEL_3,
E_CONSOL_PERF_SMRY.LEVEL_4,
E_CONSOL_PERF_SMRY.LEVEL_5,
cast(E_CONSOL_PERF_SMRY.WHSE_DATE as timestamp) as WHSE_DATE,
E_CONSOL_PERF_SMRY.OPS_CODE,
E_CONSOL_PERF_SMRY.REF_SAM,
E_CONSOL_PERF_SMRY.REF_PAM,
E_CONSOL_PERF_SMRY.REPORT_SHIFT,
E_CONSOL_PERF_SMRY.MISC_TXT_1,
E_CONSOL_PERF_SMRY.MISC_TXT_2,
E_CONSOL_PERF_SMRY.MISC_NUM_1,
E_CONSOL_PERF_SMRY.MISC_NUM_2,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_1,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_2,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_3,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_4,
E_CONSOL_PERF_SMRY.EVNT_CTGRY_5,
E_CONSOL_PERF_SMRY.LABOR_COST_RATE,
E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSDL,
E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSDL,
E_CONSOL_PERF_SMRY.PAID_OVERLAP_NSDL,
E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_NSDL,
E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSIL,
E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSIL,
E_CONSOL_PERF_SMRY.PAID_OVERLAP_UDIL,
E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_UDIL,
E_CONSOL_PERF_SMRY.VERSION_ID,
E_CONSOL_PERF_SMRY.TEAM_CODE,
E_CONSOL_PERF_SMRY.DEFAULT_JF_FLAG,
E_CONSOL_PERF_SMRY.EMP_PERF_SMRY_ID,
E_CONSOL_PERF_SMRY.TOTAL_QTY,
E_CONSOL_PERF_SMRY.REF_NBR,
cast(E_CONSOL_PERF_SMRY.TEAM_BEGIN_TIME as timestamp) as TEAM_BEGIN_TIME,
E_CONSOL_PERF_SMRY.THRUPUT_MIN,
E_CONSOL_PERF_SMRY.DISPLAY_UOM_QTY,
E_CONSOL_PERF_SMRY.DISPLAY_UOM,
E_CONSOL_PERF_SMRY.LOCN_GRP_ATTR,
E_CONSOL_PERF_SMRY.RESOURCE_GROUP_ID,
E_CONSOL_PERF_SMRY.COMP_ASSIGNMENT_ID,
E_CONSOL_PERF_SMRY.REFLECTIVE_CODE
FROM E_CONSOL_PERF_SMRY
WHERE 
(trunc(E_CONSOL_PERF_SMRY.CREATE_DATE_TIME) >= trunc(to_date('{prev_run_dt}','YYYY-MM-DD')) - 1 ) 
OR (trunc(E_CONSOL_PERF_SMRY.MOD_DATE_TIME) >= trunc(to_date('{prev_run_dt}','YYYY-MM-DD')) - 1) 
AND 1=1
"""


# COMMAND ----------

SQ_Shortcut_to_E_CONSOL_PERF_SMRY = spark.read \
  .format("jdbc") \
  .option("url", connection_string) \
  .option("query", perf_summary_query) \
  .option("user", username) \
  .option("password", password)\
  .option("numPartitions", 3)\
  .option("driver", "oracle.jdbc.OracleDriver")\
  .load()
SQ_Shortcut_to_E_CONSOL_PERF_SMRY.createOrReplaceTempView("SQ_Shortcut_to_E_CONSOL_PERF_SMRY_Temp")

# COMMAND ----------

Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE = SQ_Shortcut_to_E_CONSOL_PERF_SMRY.select( \
	lit(f'{dcnbr}').cast(DecimalType(3,0)).alias('DC_NBR'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PERF_SMRY_TRAN_ID.cast(DecimalType(20,0)).alias('PERF_SMRY_TRAN_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.WHSE.cast(StringType()).alias('WHSE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LOGIN_USER_ID.cast(StringType()).alias('LOGIN_USER_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.JOB_FUNCTION_NAME.cast(StringType()).alias('JOB_FUNCTION_NAME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.SPVSR_LOGIN_USER_ID.cast(StringType()).alias('SPVSR_LOGIN_USER_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DEPT_CODE.cast(StringType()).alias('DEPT_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CLOCK_IN_DATE.cast(TimestampType()).alias('CLOCK_IN_DATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CLOCK_IN_STATUS.cast(DecimalType(3,0)).alias('CLOCK_IN_STATUS'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_SAM.cast(DecimalType(20,7)).alias('TOTAL_SAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_PAM.cast(DecimalType(13,5)).alias('TOTAL_PAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_TIME.cast(DecimalType(13,5)).alias('TOTAL_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.OSDL.cast(DecimalType(13,5)).alias('OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.OSIL.cast(DecimalType(13,5)).alias('OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.NSDL.cast(DecimalType(13,5)).alias('NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.SIL.cast(DecimalType(13,5)).alias('SIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UDIL.cast(DecimalType(13,5)).alias('UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UIL.cast(DecimalType(13,5)).alias('UIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_OSDL.cast(DecimalType(13,5)).alias('ADJ_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_OSIL.cast(DecimalType(13,5)).alias('ADJ_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_UDIL.cast(DecimalType(13,5)).alias('ADJ_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.ADJ_NSDL.cast(DecimalType(13,5)).alias('ADJ_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_BRK.cast(DecimalType(13,5)).alias('PAID_BRK'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_BRK.cast(DecimalType(13,5)).alias('UNPAID_BRK'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_OSDL.cast(DecimalType(13,5)).alias('REF_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_OSIL.cast(DecimalType(13,5)).alias('REF_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_UDIL.cast(DecimalType(13,5)).alias('REF_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_NSDL.cast(DecimalType(13,5)).alias('REF_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_OSDL.cast(DecimalType(13,5)).alias('REF_ADJ_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_OSIL.cast(DecimalType(13,5)).alias('REF_ADJ_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_UDIL.cast(DecimalType(13,5)).alias('REF_ADJ_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_ADJ_NSDL.cast(DecimalType(13,5)).alias('REF_ADJ_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_NUMBER_1.cast(DecimalType(13,5)).alias('MISC_NUMBER_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CREATE_DATE_TIME.cast(TimestampType()).alias('CREATE_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MOD_DATE_TIME.cast(TimestampType()).alias('MOD_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.USER_ID.cast(StringType()).alias('USER_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_1.cast(StringType()).alias('MISC_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_2.cast(StringType()).alias('MISC_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.CLOCK_OUT_DATE.cast(TimestampType()).alias('CLOCK_OUT_DATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.SHIFT_CODE.cast(StringType()).alias('SHIFT_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVENT_COUNT.cast(DecimalType(9,0)).alias('EVENT_COUNT'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.START_DATE_TIME.cast(TimestampType()).alias('START_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.END_DATE_TIME.cast(TimestampType()).alias('END_DATE_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_1.cast(StringType()).alias('LEVEL_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_2.cast(StringType()).alias('LEVEL_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_3.cast(StringType()).alias('LEVEL_3'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_4.cast(StringType()).alias('LEVEL_4'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LEVEL_5.cast(StringType()).alias('LEVEL_5'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.WHSE_DATE.cast(TimestampType()).alias('WHSE_DATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.OPS_CODE.cast(StringType()).alias('OPS_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_SAM.cast(DecimalType(13,5)).alias('REF_SAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_PAM.cast(DecimalType(13,5)).alias('REF_PAM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REPORT_SHIFT.cast(StringType()).alias('REPORT_SHIFT'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_TXT_1.cast(StringType()).alias('MISC_TXT_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_TXT_2.cast(StringType()).alias('MISC_TXT_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_NUM_1.cast(DecimalType(20,7)).alias('MISC_NUM_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.MISC_NUM_2.cast(DecimalType(20,7)).alias('MISC_NUM_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_1.cast(StringType()).alias('EVNT_CTGRY_1'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_2.cast(StringType()).alias('EVNT_CTGRY_2'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_3.cast(StringType()).alias('EVNT_CTGRY_3'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_4.cast(StringType()).alias('EVNT_CTGRY_4'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EVNT_CTGRY_5.cast(StringType()).alias('EVNT_CTGRY_5'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LABOR_COST_RATE.cast(DecimalType(20,7)).alias('LABOR_COST_RATE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSDL.cast(DecimalType(20,7)).alias('PAID_OVERLAP_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSDL.cast(DecimalType(20,7)).alias('UNPAID_OVERLAP_OSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_NSDL.cast(DecimalType(20,7)).alias('PAID_OVERLAP_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_NSDL.cast(DecimalType(20,7)).alias('UNPAID_OVERLAP_NSDL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_OSIL.cast(DecimalType(20,7)).alias('PAID_OVERLAP_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_OSIL.cast(DecimalType(20,7)).alias('UNPAID_OVERLAP_OSIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.PAID_OVERLAP_UDIL.cast(DecimalType(20,7)).alias('PAID_OVERLAP_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.UNPAID_OVERLAP_UDIL.cast(DecimalType(20,7)).alias('UNPAID_OVERLAP_UDIL'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.VERSION_ID.cast(DecimalType(6,0)).alias('VERSION_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TEAM_CODE.cast(StringType()).alias('TEAM_CODE'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DEFAULT_JF_FLAG.cast(DecimalType(9,0)).alias('DEFAULT_JF_FLAG'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.EMP_PERF_SMRY_ID.cast(DecimalType(20,0)).alias('EMP_PERF_SMRY_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TOTAL_QTY.cast(DecimalType(13,5)).alias('TOTAL_QTY'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REF_NBR.cast(StringType()).alias('REF_NBR'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.TEAM_BEGIN_TIME.cast(TimestampType()).alias('TEAM_BEGIN_TIME'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.THRUPUT_MIN.cast(DecimalType(20,7)).alias('THRUPUT_MIN'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DISPLAY_UOM_QTY.cast(DecimalType(20,7)).alias('DISPLAY_UOM_QTY'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.DISPLAY_UOM.cast(StringType()).alias('DISPLAY_UOM'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.LOCN_GRP_ATTR.cast(StringType()).alias('LOCN_GRP_ATTR'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.RESOURCE_GROUP_ID.cast(StringType()).alias('RESOURCE_GROUP_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.COMP_ASSIGNMENT_ID.cast(StringType()).alias('COMP_ASSIGNMENT_ID'), \
	SQ_Shortcut_to_E_CONSOL_PERF_SMRY.REFLECTIVE_CODE.cast(StringType()).alias('REFLECTIVE_CODE'), \
	current_timestamp().cast(TimestampType()).alias('LOAD_TSTMP') \
)




# COMMAND ----------

Shortcut_to_WM_E_CONSOL_PERF_SMRY_PRE.write.partitionBy('DC_NBR') \
  .mode("overwrite") \
  .option("replaceWhere", f'DC_NBR={dcnbr}') \
  .saveAsTable(target_table_name)

