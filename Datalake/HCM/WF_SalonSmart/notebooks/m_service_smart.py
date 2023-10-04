# Databricks notebook source
#Code converted on 2023-09-06 12:10:13
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

# COMMAND ----------

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='qa')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

# Processing node LKP_SITE_PROFILE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_SITE_PROFILE_SRC = spark.sql(f"""SELECT
LOCATION_ID,
COUNTRY_CD,
STORE_NBR
FROM {legacy}.SITE_PROFILE""")


# COMMAND ----------

# Processing node LKP_SERVICE_SMART_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_SERVICE_SMART_SRC = spark.sql(f"""SELECT
LOCATION_ID,
LOAD_DT,
WEEK_DT
FROM {legacy}.SERVICE_SMART""")


# COMMAND ----------

# Processing node SQ_Shortcut_To_SALONSMART, type SOURCE 
# COLUMN COUNT: 82

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-qa-raw-p1-gcs-gbl/nas/employee/salonsmart/')
# source_bucket = dbutils.widgets.get('source_bucket')

source_bucket = getParameterValue(raw,'BA_HCM.prm','BA_HCM.WF:wf_SalonSmart','source_bucket')

# def get_source_file(key, _bucket):
#   import builtins
#   lst = dbutils.fs.ls(_bucket)
#   fldr = builtins.max(lst, key=lambda x: x.name).name
#   lst = dbutils.fs.ls(_bucket + fldr)
#   files = [x.path for x in lst if x.name.startswith(key)]
#   return files[0] if files else None
  
source_file = get_src_file('salonsmart', source_bucket)

SQ_Shortcut_To_SALONSMART = spark.read.csv(source_file, sep=',', header=True, quote='"').withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node LKP_SITE_PROFILE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4


LKP_SITE_PROFILE_lookup_result = SQ_Shortcut_To_SALONSMART.selectExpr(
	"we","Store as i_Storenumber", "sys_row_id").join(LKP_SITE_PROFILE_SRC, (col('i_Storenumber') == col('STORE_NBR')), 'left')  
    
LKP_SITE_PROFILE = LKP_SITE_PROFILE_lookup_result.select(
	LKP_SITE_PROFILE_lookup_result.sys_row_id,
	col('we'),
	col('i_Storenumber'),
	col('LOCATION_ID'),
	col('COUNTRY_CD')
)

# COMMAND ----------

# Processing node FIL_LOCATION_ID, type FILTER 
# COLUMN COUNT: 83

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_SALONSMART_temp = SQ_Shortcut_To_SALONSMART.toDF(*["SQ_Shortcut_To_SALONSMART___" + col for col in SQ_Shortcut_To_SALONSMART.columns])


# COMMAND ----------

# Joining dataframes SQ_Shortcut_To_SALONSMART, LKP_SITE_PROFILE to form FIL_LOCATION_ID
#FIL_LOCATION_ID_joined = SQ_Shortcut_To_SALONSMART_temp.join(LKP_SITE_PROFILE, col("SQ_Shortcut_To_SALONSMART___sys_row_id") == col("sys_row_id"), 'inner')
FIL_LOCATION_ID_joined = SQ_Shortcut_To_SALONSMART_temp.join(LKP_SITE_PROFILE, (col("SQ_Shortcut_To_SALONSMART___WE") == col("we")) & (col("SQ_Shortcut_To_SALONSMART___Store") == col("i_Storenumber")), 'inner')

# COMMAND ----------



FIL_LOCATION_ID = FIL_LOCATION_ID_joined.selectExpr(
	"`LOCATION_ID` as LOCATION_ID",
	"`SQ_Shortcut_To_SALONSMART___WE` as WE",
	"`SQ_Shortcut_To_SALONSMART___CSM Act Hrs` as C_Actual_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Ernd Hrs` as C_Earned_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Hrs Efficiency` as C_Hours_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___CSM Act Spec Hrs` as C_Actual_Spec_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Ernd Spec Hrs` as C_Earned_Spec_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Spec Hrs Efficiency` as C_Spec_Hours_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___CSM Act $` as C_Actual_Amt",
	"`SQ_Shortcut_To_SALONSMART___CSM Ernd $` as C_Earn_Amt",
	"`SQ_Shortcut_To_SALONSMART___CSM $ Efficiency` as C_Amt_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___CSM Act OT $` as C_Actual_OT_Amt",
	"`SQ_Shortcut_To_SALONSMART___CSM Ernd OT $` as C_Earn_OT_Amt",
	"`SQ_Shortcut_To_SALONSMART___CSM OT $ Efficiency` as C_OT_Amt_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___CSM Act Pyrl %` as C_Actual_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___CSM Ernd Pyrl %` as C_Earn_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___SSM Act FSG Hrs` as S_Actual_FSG_Hrs",
	"`SQ_Shortcut_To_SALONSMART___SSM Ernd FSG Hrs` as S_Earn_FSG_Hrs",
	"`SQ_Shortcut_To_SALONSMART___SSM FSG Hrs Efficiency` as S_FSG_Hrs_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___SSM Act FSG $` as S_Actual_FSG_Amt",
	"`SQ_Shortcut_To_SALONSMART___SSM Ernd FSG $` as S_Earn_FSG_Amt",
	"`SQ_Shortcut_To_SALONSMART___SSM FSG $ Efficiency` as S_FSG_Amt_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___SSM Act FSG Pyrl %` as S_Actual_FSG_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___SSM Ernd FSG Pyrl %` as S_Earn_FSG_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___SSM Act BB Hrs` as S_Actual_BB_Hrs",
	"`SQ_Shortcut_To_SALONSMART___SSM Ernd BB Hrs` as S_Earn_BB_Hrs",
	"`SQ_Shortcut_To_SALONSMART___SSM BB Hrs Efficiency` as S_BB_Hrs_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___SSM Act BB $` as S_Actual_BB_Amt",
	"`SQ_Shortcut_To_SALONSMART___SSM Ernd BB $` as S_Earn_BB_Amt",
	"`SQ_Shortcut_To_SALONSMART___SSM BB $ Efficiency` as S_BB_Amt_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___SSM Act BB Pyrl %` as S_Actual_BB_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___SSM Ernd BB Pyrl %` as S_Earn_BB_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___SSM Act Ttl Hrs` as S_Actual_Ttl_Hrs",
	"`SQ_Shortcut_To_SALONSMART___SSM Ernd Ttl Hrs` as S_Earn_Ttl_Hrs",
	"`SQ_Shortcut_To_SALONSMART___SSM Ttl Hrs Efficiency` as S_Ttl_Hrs_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___SSM Act Ttl $` as S_Actual_Ttl_Amt",
	"`SQ_Shortcut_To_SALONSMART___SSM Ernd Ttl $` as S_Earn_Ttl_Amt",
	"`SQ_Shortcut_To_SALONSMART___SSM Ttl $ Efficiency` as S_Ttl_Amt_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___SSM Act Ttl Pyrl %` as S_Actual_Ttl_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___SSM Ernd Ttl Pyrl %` as S_Earn_Ttl_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___TSM Act Hrs` as T_Actual_Hrs",
	"`SQ_Shortcut_To_SALONSMART___TSM Ernd Hrs` as T_Earn_Hrs",
	"`SQ_Shortcut_To_SALONSMART___TSM Hrs Efficiency` as T_Hrs_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___TSM Act $` as T_Actual_Amt",
	"`SQ_Shortcut_To_SALONSMART___TSM Ernd $` as T_Earn_Amt",
	"`SQ_Shortcut_To_SALONSMART___TSM $ Efficiency` as T_Amt_Efficiency",
	"`SQ_Shortcut_To_SALONSMART___TSM Act Pyrl %` as T_Actual_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___TSM Ernd Pyrl %` as T_Earn_Prcnt",
	"`SQ_Shortcut_To_SALONSMART___CSM Plan Pyrl` as C_Plan_Amt",
	"`SQ_Shortcut_To_SALONSMART___SSM Plan Pyrl` as S_Plan_Amt",
	"`SQ_Shortcut_To_SALONSMART___TSM Plan Pyrl` as T_Plan_Amt",
	"`SQ_Shortcut_To_SALONSMART___CSM Act Salary $` as CSM_Act_Salary_AMT",
	"`SQ_Shortcut_To_SALONSMART___CSM Act Salary Hrs` as CSM_Act_Salary_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Pln Salary $` as CSM_Pln_Salary_AMT",
	"`SQ_Shortcut_To_SALONSMART___CSM Pln Salary Hrs` as CSM_Pln_Salary_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Act Hrly Mgr Pay` as CSM_Act_Hrly_Mgr_Pay",
	"`SQ_Shortcut_To_SALONSMART___CSM Act Hrly Mgr Hrs` as CSM_Act_Hrly_Mgr_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Pln Hrly Mgr Pay` as CSM_Pln_Hrly_Mgr_Pay",
	"`SQ_Shortcut_To_SALONSMART___CSM Pln Hrly Mgr Hrs` as CSM_Pln_Hrly_Mgr_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Act Hrly Pay` as CSM_Act_Hrly_Pay",
	"`SQ_Shortcut_To_SALONSMART___CSM Act Hrly Hrs` as CSM_Act_Hrly_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Pln Hrly Pay` as CSM_Pln_Hrly_Pay",
	"`SQ_Shortcut_To_SALONSMART___CSM Pln Hrly Hrs` as CSM_Pln_Hrly_Hrs",
	"`SQ_Shortcut_To_SALONSMART___CSM Ttl Pln Pay` as CSM_Ttl_Pln_Pay",
	"`SQ_Shortcut_To_SALONSMART___CSM Pln Ttl Hrs` as CSM_Pln_Ttl_Hrs",
	"`COUNTRY_CD` as COUNTRY_CD",
	"`SQ_Shortcut_To_SALONSMART___C_ACTUAL_OT_HRS` as C_ACTUAL_OT_HRS",
	"`SQ_Shortcut_To_SALONSMART___C_INCREMENTAL_HRS` as C_INCREMENTAL_HRS",
	"`SQ_Shortcut_To_SALONSMART___H_HOURLY_ACTUAL_HRS` as H_HOURLY_ACTUAL_HRS",
	"`SQ_Shortcut_To_SALONSMART___H_HOURLY_ACTUAL_OT_HRS` as H_HOURLY_ACTUAL_OT_HRS",
	"`SQ_Shortcut_To_SALONSMART___H_HOURLY_ACTUAL_AMT` as H_HOURLY_ACTUAL_AMT",
	"`SQ_Shortcut_To_SALONSMART___H_HOURLY_ACTUAL_OT_AMT` as H_HOURLY_ACTUAL_OT_AMT",
	"`SQ_Shortcut_To_SALONSMART___H_SALARY_ACTUAL_HRS` as H_SALARY_ACTUAL_HRS",
	"`SQ_Shortcut_To_SALONSMART___H_SALARY_ACTUAL_AMT` as H_SALARY_ACTUAL_AMT",
	"`SQ_Shortcut_To_SALONSMART___H_HOURLY_EARN_AMT` as H_HOURLY_EARN_AMT",
	"`SQ_Shortcut_To_SALONSMART___H_SALARY_EARN_AMT` as H_SALARY_EARN_AMT",
	"`SQ_Shortcut_To_SALONSMART___H_HOURLY_EARN_HRS` as H_HOURLY_EARN_HRS",
	"`SQ_Shortcut_To_SALONSMART___H_SALARY_EARN_HRS` as H_SALARY_EARN_HRS",
	"`SQ_Shortcut_To_SALONSMART___H_HOURLY_OT_EARN_AMT` as H_HOURLY_OT_EARN_AMT",
	"`SQ_Shortcut_To_SALONSMART___H_HOURLY_OT_EARN_HRS` as H_HOURLY_OT_EARN_HRS",
	"`SQ_Shortcut_To_SALONSMART___S_FCast_FSG_Hrs` as S_Fcst_BB_Hrs",
	"`SQ_Shortcut_To_SALONSMART___S_Fcast_BB_Hrs` as S_Fcst_FSG_Hrs",
	"`SQ_Shortcut_To_SALONSMART___T_Fcast_Hrs` as T_Fcst_Hrs") \
    .filter("LOCATION_ID IS NOT NULL") \
    .withColumn("sys_row_id", monotonically_increasing_id()) \
    .withColumn("T_Actual_Prcnt_VAR", expr("ROUND(T_Actual_Prcnt, 1) / 10.00")) \
    .withColumn("T_Earn_Prcnt_VAR", expr("ROUND(T_Earn_Prcnt, 1) / 10.00"))

# COMMAND ----------

FIL_LOCATION_ID.select(col("CSM_Act_Hrly_Pay")).filter("CSM_Act_Hrly_Pay!='0.00'").show()

# COMMAND ----------

# Processing node EXP_SALON_SMART, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 85

# for each involved DataFrame, append the dataframe name to each column
FIL_LOCATION_ID_temp = FIL_LOCATION_ID.toDF(*["FIL_LOCATION_ID___" + col for col in FIL_LOCATION_ID.columns])

EXP_SALON_SMART = FIL_LOCATION_ID_temp.selectExpr(
	"FIL_LOCATION_ID___sys_row_id as sys_row_id",
	"FIL_LOCATION_ID___Location_ID as Location_ID",
	"COALESCE(to_date(FIL_LOCATION_ID___WE, 'MM/dd/yy HH:mm:ss'), to_date(FIL_LOCATION_ID___WE, 'MM/dd/yyyy HH:mm:ss'), to_date(FIL_LOCATION_ID___WE, 'MM/dd/yyyy')) AS WEEK_DT",
	"CASE WHEN FIL_LOCATION_ID___T_Actual_Prcnt_VAR > 9999.99 THEN 9999.99 WHEN FIL_LOCATION_ID___T_Actual_Prcnt_VAR < -9999.99 THEN -9999.99 ELSE FIL_LOCATION_ID___T_Actual_Prcnt_VAR END AS T_Actual_Prcnt1",
	"CASE WHEN FIL_LOCATION_ID___T_Earn_Prcnt_VAR > 9999.99 THEN 9999.99 WHEN FIL_LOCATION_ID___T_Earn_Prcnt_VAR < -9999.99 THEN -9999.99 ELSE FIL_LOCATION_ID___T_Earn_Prcnt_VAR END AS T_Earn_Prcnt1",
	"CASE WHEN ROUND(FIL_LOCATION_ID___T_Amt_Efficiency, 1) / 1000 > 9999.99 THEN 9999.99 WHEN ROUND(FIL_LOCATION_ID___T_Amt_Efficiency, 1) / 1000 < -9999.99 THEN -9999.99 ELSE ROUND(FIL_LOCATION_ID___T_Amt_Efficiency, 1) / 1000 END AS T_Amt_Efficiency1",
 	"CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Act_Hrly_Pay AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Act_Hrly_Pay AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Act_Hrly_Pay AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___CSM_Act_Hrly_Pay AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___CSM_Act_Hrly_Pay AS FLOAT) END AS CSM_Act_Hrly_Pay_Out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Act_Salary_AMT AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Act_Salary_AMT AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Act_Salary_AMT AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___CSM_Act_Salary_AMT AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___CSM_Act_Salary_AMT AS FLOAT) END AS CSM_Act_Salary_AMT_Out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Pln_Salary_AMT AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Pln_Salary_AMT AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Pln_Salary_AMT AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___CSM_Pln_Salary_AMT AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___CSM_Pln_Salary_AMT AS FLOAT) END AS CSM_Pln_Salary_AMT_Out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Ttl_Pln_Pay AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Ttl_Pln_Pay AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___CSM_Ttl_Pln_Pay AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___CSM_Ttl_Pln_Pay AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___CSM_Ttl_Pln_Pay AS FLOAT) END AS CSM_Ttl_Pln_Pay_Out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Actual_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Actual_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Actual_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___C_Actual_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___C_Actual_Amt AS FLOAT) END AS C_Actual_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Actual_OT_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Actual_OT_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Actual_OT_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___C_Actual_OT_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___C_Actual_OT_Amt AS FLOAT) END AS C_Actual_OT_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Earn_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Earn_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Earn_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___C_Earn_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___C_Earn_Amt AS FLOAT) END AS C_Earn_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Earn_OT_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Earn_OT_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Earn_OT_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___C_Earn_OT_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___C_Earn_OT_Amt AS FLOAT) END AS C_Earn_OT_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Plan_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Plan_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___C_Plan_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___C_Plan_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___C_Plan_Amt AS FLOAT) END AS o_C_Plan_Amt",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_AMT AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_AMT AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_AMT AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_AMT AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_AMT AS FLOAT) END AS H_HOURLY_ACTUAL_AMT_out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_OT_AMT AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_OT_AMT AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_OT_AMT AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_OT_AMT AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___H_HOURLY_ACTUAL_OT_AMT AS FLOAT) END AS H_HOURLY_ACTUAL_OT_AMT_out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_EARN_AMT AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_EARN_AMT AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_EARN_AMT AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_EARN_AMT AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___H_HOURLY_EARN_AMT AS FLOAT) END AS H_HOURLY_EARN_AMT_out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_OT_EARN_AMT AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_OT_EARN_AMT AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_OT_EARN_AMT AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___H_HOURLY_OT_EARN_AMT AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___H_HOURLY_OT_EARN_AMT AS FLOAT) END AS H_HOURLY_OT_EARN_AMT_out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_SALARY_ACTUAL_AMT AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_SALARY_ACTUAL_AMT AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_SALARY_ACTUAL_AMT AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___H_SALARY_ACTUAL_AMT AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___H_SALARY_ACTUAL_AMT AS FLOAT) END AS H_SALARY_ACTUAL_AMT_out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_SALARY_EARN_AMT AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_SALARY_EARN_AMT AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___H_SALARY_EARN_AMT AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___H_SALARY_EARN_AMT AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___H_SALARY_EARN_AMT AS FLOAT) END AS H_SALARY_EARN_AMT_out",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_BB_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_BB_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_BB_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_BB_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___S_Actual_BB_Amt AS FLOAT) END AS S_Actual_BB_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_FSG_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_FSG_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_FSG_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_FSG_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___S_Actual_FSG_Amt AS FLOAT) END AS S_Actual_FSG_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_Ttl_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_Ttl_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_Ttl_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___S_Actual_Ttl_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___S_Actual_Ttl_Amt AS FLOAT) END AS S_Actual_Ttl_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_BB_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_BB_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_BB_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_BB_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___S_Earn_BB_Amt AS FLOAT) END AS S_Earn_BB_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_FSG_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_FSG_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_FSG_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_FSG_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___S_Earn_FSG_Amt AS FLOAT) END AS S_Earn_FSG_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_Ttl_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_Ttl_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_Ttl_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___S_Earn_Ttl_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___S_Earn_Ttl_Amt AS FLOAT) END AS S_Earn_Ttl_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___T_Actual_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___T_Actual_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___T_Actual_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___T_Actual_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___T_Actual_Amt AS FLOAT) END AS T_Actual_Amt1",
  "CASE WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___T_Earn_Amt AS STRING)), 1, 1) = CHR(36) THEN CAST(SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___T_Earn_Amt AS STRING)), 2) AS FLOAT) WHEN SUBSTR(LTRIM(CAST(FIL_LOCATION_ID___T_Earn_Amt AS STRING)), 2, 1) = CHR(36) THEN -CAST(CONCAT(SUBSTR(replace(replace(LTRIM(CAST(FIL_LOCATION_ID___T_Earn_Amt AS STRING)), ')', ''), CHR(36), ''), 3)) AS FLOAT) ELSE CAST(FIL_LOCATION_ID___T_Earn_Amt AS FLOAT) END AS T_Earn_Amt1",
  "FIL_LOCATION_ID___CSM_Act_Hrly_Hrs as CSM_Act_Hrly_Hrs",
	"FIL_LOCATION_ID___CSM_Act_Hrly_Mgr_Hrs as CSM_Act_Hrly_Mgr_Hrs",
	"FIL_LOCATION_ID___CSM_Act_Hrly_Mgr_Pay as CSM_Act_Hrly_Mgr_Pay",
	"FIL_LOCATION_ID___CSM_Act_Salary_Hrs as CSM_Act_Salary_Hrs",
	"FIL_LOCATION_ID___CSM_Pln_Hrly_Hrs as CSM_Pln_Hrly_Hrs",
	"FIL_LOCATION_ID___CSM_Pln_Hrly_Mgr_Hrs as CSM_Pln_Hrly_Mgr_Hrs",
	"FIL_LOCATION_ID___CSM_Pln_Hrly_Mgr_Pay as CSM_Pln_Hrly_Mgr_Pay",
	"FIL_LOCATION_ID___CSM_Pln_Hrly_Pay as CSM_Pln_Hrly_Pay",
	"FIL_LOCATION_ID___CSM_Pln_Salary_Hrs as CSM_Pln_Salary_Hrs",
	"FIL_LOCATION_ID___CSM_Pln_Ttl_Hrs as CSM_Pln_Ttl_Hrs",
	"FIL_LOCATION_ID___C_ACTUAL_OT_HRS as C_ACTUAL_OT_HRS",
	"FIL_LOCATION_ID___C_Actual_Hrs as C_Actual_Hrs",
	"FIL_LOCATION_ID___C_Actual_Spec_Hrs as C_Actual_Spec_Hrs",
	"FIL_LOCATION_ID___C_Earned_Hrs as C_Earned_Hrs",
	"FIL_LOCATION_ID___C_Earned_Spec_Hrs as C_Earned_Spec_Hrs",
	"FIL_LOCATION_ID___C_INCREMENTAL_HRS as C_INCREMENTAL_HRS",
	"FIL_LOCATION_ID___H_HOURLY_ACTUAL_HRS as H_HOURLY_ACTUAL_HRS",
	"FIL_LOCATION_ID___H_HOURLY_ACTUAL_OT_HRS as H_HOURLY_ACTUAL_OT_HRS",
	"FIL_LOCATION_ID___H_HOURLY_EARN_HRS as H_HOURLY_EARN_HRS",
	"FIL_LOCATION_ID___H_HOURLY_OT_EARN_HRS as H_HOURLY_OT_EARN_HRS",
	"FIL_LOCATION_ID___H_SALARY_ACTUAL_HRS as H_SALARY_ACTUAL_HRS",
	"FIL_LOCATION_ID___H_SALARY_EARN_HRS as H_SALARY_EARN_HRS",
	"FIL_LOCATION_ID___S_Actual_BB_Hrs as S_Actual_BB_Hrs",
	"FIL_LOCATION_ID___S_Actual_FSG_Hrs as S_Actual_FSG_Hrs",
	"FIL_LOCATION_ID___S_Actual_Ttl_Hrs as S_Actual_Ttl_Hrs",
	"FIL_LOCATION_ID___S_Earn_BB_Hrs as S_Earn_BB_Hrs",
	"FIL_LOCATION_ID___S_Earn_FSG_Hrs as S_Earn_FSG_Hrs",
	"FIL_LOCATION_ID___S_Earn_Ttl_Hrs as S_Earn_Ttl_Hrs",
	"FIL_LOCATION_ID___S_Fcst_BB_Hrs as S_Fcst_BB_Hrs",
	"FIL_LOCATION_ID___S_Fcst_FSG_Hrs as S_Fcst_FSG_Hrs",
	"FIL_LOCATION_ID___S_Plan_Amt as S_Plan_Amt",
	"FIL_LOCATION_ID___T_Actual_Hrs as T_Actual_Hrs",
	"FIL_LOCATION_ID___T_Earn_Hrs as T_Earn_Hrs",
	"FIL_LOCATION_ID___T_Fcst_Hrs as T_Fcst_Hrs",
	"FIL_LOCATION_ID___T_Plan_Amt as T_Plan_Amt",
	"TRIM(FIL_LOCATION_ID___COUNTRY_CD) as o_COUNTRY_CD",
	"ROUND(FIL_LOCATION_ID___C_Actual_Prcnt, 1)/ 10.00 as C_Actual_Prcnt1",
	"ROUND(FIL_LOCATION_ID___C_Amt_Efficiency, 1)/ 10.00 as C_Amt_Efficiency1",
	"ROUND(FIL_LOCATION_ID___C_Earn_Prcnt, 1)/ 10.00 as C_Earn_Prcnt1",
	"ROUND(FIL_LOCATION_ID___C_Hours_Efficiency, 1)/ 10.00 as C_Hours_Efficiency1",
	"ROUND(FIL_LOCATION_ID___C_OT_Amt_Efficiency, 1)/ 10.00 as C_OT_Amt_Efficiency1",
	"ROUND(FIL_LOCATION_ID___C_Spec_Hours_Efficiency, 1)/ 10.00 as C_Spec_Hours_Efficiency1",
	"ROUND(FIL_LOCATION_ID___S_Actual_BB_Prcnt, 1)/ 10.00 as S_Actual_BB_Prcnt1",
	"ROUND(FIL_LOCATION_ID___S_Actual_FSG_Prcnt, 1)/ 10.00 as S_Actual_FSG_Prcnt1",
	"ROUND(FIL_LOCATION_ID___S_Actual_Ttl_Prcnt, 1)/ 10.00 as S_Actual_Ttl_Prcnt1",
	"ROUND(FIL_LOCATION_ID___S_BB_Amt_Efficiency, 1)/ 10.00 as S_BB_Amt_Efficiency1",
	"ROUND(FIL_LOCATION_ID___S_BB_Hrs_Efficiency, 1)/ 10.00 as S_BB_Hrs_Efficiency1",
	"ROUND(FIL_LOCATION_ID___S_Earn_BB_Prcnt, 1)/ 10.00 as S_Earn_BB_Prcnt1",
	"ROUND(FIL_LOCATION_ID___S_Earn_FSG_Prcnt, 1)/ 10.00 as S_Earn_FSG_Prcnt1",
	"ROUND(FIL_LOCATION_ID___S_Earn_Ttl_Prcnt, 1)/ 10.00 as S_Earn_Ttl_Prcnt1",
	"ROUND(FIL_LOCATION_ID___S_FSG_Amt_Efficiency, 1)/ 10.00 as S_FSG_Amt_Efficiency1",
	"ROUND(FIL_LOCATION_ID___S_FSG_Hrs_Efficiency, 1)/ 10.00 as S_FSG_Hrs_Efficiency1",
	"ROUND(FIL_LOCATION_ID___S_Ttl_Amt_Efficiency, 1)/ 10.00 as S_Ttl_Amt_Efficiency1",
	"ROUND(FIL_LOCATION_ID___S_Ttl_Hrs_Efficiency, 1)/ 10.00 as S_Ttl_Hrs_Efficiency1",
	"ROUND(FIL_LOCATION_ID___T_Hrs_Efficiency, 1)/ 10.00 as T_Hrs_Efficiency1",
	"date_trunc('day', CURRENT_TIMESTAMP) as ADD_DT",
	"date_trunc('day', CURRENT_TIMESTAMP) as UPDATE_DT"
)


# COMMAND ----------

# Processing node LKP_SERVICE_SMART, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5


LKP_SERVICE_SMART_lookup_result = EXP_SALON_SMART.selectExpr(
	"WEEK_DT as i_WEEK_DT",
	"Location_ID as i_LOCATION_ID","sys_row_id").join(LKP_SERVICE_SMART_SRC, (col('i_WEEK_DT') == col('WEEK_DT')) & (col('i_LOCATION_ID') == col('LOCATION_ID') ), 'left') 
    
LKP_SERVICE_SMART = LKP_SERVICE_SMART_lookup_result.select(
	col('sys_row_id'),
	col('i_WEEK_DT'),
	col('i_LOCATION_ID'),
	col('LOCATION_ID'),
	col('LOAD_DT')
)

# COMMAND ----------

# Processing node EXP_LOAD_FLAG, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 89

# for each involved DataFrame, append the dataframe name to each column
EXP_SALON_SMART_temp = EXP_SALON_SMART.toDF(*["EXP_SALON_SMART___" + col for col in EXP_SALON_SMART.columns])
LKP_SERVICE_SMART_temp = LKP_SERVICE_SMART.toDF(*["LKP_SERVICE_SMART___" + col for col in LKP_SERVICE_SMART.columns])

# Joining dataframes EXP_SALON_SMART, LKP_SERVICE_SMART to form EXP_LOAD_FLAG
EXP_LOAD_FLAG_joined = EXP_SALON_SMART_temp.join(LKP_SERVICE_SMART_temp, (col("EXP_SALON_SMART___Location_ID") == col("LKP_SERVICE_SMART___i_LOCATION_ID")) & (col("EXP_SALON_SMART___WEEK_DT")== col("LKP_SERVICE_SMART___i_WEEK_DT")), 'left')

# COMMAND ----------



EXP_LOAD_FLAG = EXP_LOAD_FLAG_joined.selectExpr(
	"LKP_SERVICE_SMART___sys_row_id as sys_row_id",
	"EXP_SALON_SMART___Location_ID as Location_ID",
	"EXP_SALON_SMART___WEEK_DT as WEEK_DT",
	"EXP_SALON_SMART___C_Actual_Hrs as C_Actual_Hrs",
	"EXP_SALON_SMART___C_Earned_Hrs as C_Earned_Hrs",
	"EXP_SALON_SMART___C_Hours_Efficiency1 as C_Hours_Efficiency1",
	"EXP_SALON_SMART___C_Actual_Spec_Hrs as C_Actual_Spec_Hrs",
	"EXP_SALON_SMART___C_Earned_Spec_Hrs as C_Earned_Spec_Hrs",
	"EXP_SALON_SMART___C_Spec_Hours_Efficiency1 as C_Spec_Hours_Efficiency1",
	"EXP_SALON_SMART___C_Actual_Amt1 as C_Actual_Amt1",
	"EXP_SALON_SMART___C_Earn_Amt1 as C_Earn_Amt1",
	"EXP_SALON_SMART___C_Amt_Efficiency1 as C_Amt_Efficiency1",
	"EXP_SALON_SMART___C_Actual_OT_Amt1 as C_Actual_OT_Amt1",
	"EXP_SALON_SMART___C_Earn_OT_Amt1 as C_Earn_OT_Amt1",
	"EXP_SALON_SMART___C_OT_Amt_Efficiency1 as C_OT_Amt_Efficiency1",
	"EXP_SALON_SMART___C_Actual_Prcnt1 as C_Actual_Prcnt1",
	"EXP_SALON_SMART___C_Earn_Prcnt1 as C_Earn_Prcnt1",
	"EXP_SALON_SMART___S_Actual_FSG_Hrs as S_Actual_FSG_Hrs",
	"EXP_SALON_SMART___S_Earn_FSG_Hrs as S_Earn_FSG_Hrs",
	"EXP_SALON_SMART___S_FSG_Hrs_Efficiency1 as S_FSG_Hrs_Efficiency1",
	"EXP_SALON_SMART___S_Actual_FSG_Amt1 as S_Actual_FSG_Amt1",
	"EXP_SALON_SMART___S_Earn_FSG_Amt1 as S_Earn_FSG_Amt1",
	"EXP_SALON_SMART___S_FSG_Amt_Efficiency1 as S_FSG_Amt_Efficiency1",
	"EXP_SALON_SMART___S_Actual_FSG_Prcnt1 as S_Actual_FSG_Prcnt1",
	"EXP_SALON_SMART___S_Earn_FSG_Prcnt1 as S_Earn_FSG_Prcnt1",
	"EXP_SALON_SMART___S_Actual_BB_Hrs as S_Actual_BB_Hrs",
	"EXP_SALON_SMART___S_Earn_BB_Hrs as S_Earn_BB_Hrs",
	"EXP_SALON_SMART___S_BB_Hrs_Efficiency1 as S_BB_Hrs_Efficiency1",
	"EXP_SALON_SMART___S_Actual_BB_Amt1 as S_Actual_BB_Amt1",
	"EXP_SALON_SMART___S_Earn_BB_Amt1 as S_Earn_BB_Amt1",
	"EXP_SALON_SMART___S_BB_Amt_Efficiency1 as S_BB_Amt_Efficiency1",
	"EXP_SALON_SMART___S_Actual_BB_Prcnt1 as S_Actual_BB_Prcnt1",
	"EXP_SALON_SMART___S_Earn_BB_Prcnt1 as S_Earn_BB_Prcnt1",
	"EXP_SALON_SMART___S_Actual_Ttl_Hrs as S_Actual_Ttl_Hrs",
	"EXP_SALON_SMART___S_Earn_Ttl_Hrs as S_Earn_Ttl_Hrs",
	"EXP_SALON_SMART___S_Ttl_Hrs_Efficiency1 as S_Ttl_Hrs_Efficiency1",
	"EXP_SALON_SMART___S_Actual_Ttl_Amt1 as S_Actual_Ttl_Amt1",
	"EXP_SALON_SMART___S_Earn_Ttl_Amt1 as S_Earn_Ttl_Amt1",
	"EXP_SALON_SMART___S_Ttl_Amt_Efficiency1 as S_Ttl_Amt_Efficiency1",
	"EXP_SALON_SMART___S_Actual_Ttl_Prcnt1 as S_Actual_Ttl_Prcnt1",
	"EXP_SALON_SMART___S_Earn_Ttl_Prcnt1 as S_Earn_Ttl_Prcnt1",
	"EXP_SALON_SMART___T_Actual_Hrs as T_Actual_Hrs",
	"EXP_SALON_SMART___T_Earn_Hrs as T_Earn_Hrs",
	"EXP_SALON_SMART___T_Hrs_Efficiency1 as T_Hrs_Efficiency1",
	"EXP_SALON_SMART___T_Actual_Amt1 as T_Actual_Amt1",
	"EXP_SALON_SMART___T_Earn_Amt1 as T_Earn_Amt1",
	"EXP_SALON_SMART___T_Amt_Efficiency1 as T_Amt_Efficiency1",
	"EXP_SALON_SMART___T_Actual_Prcnt1 as T_Actual_Prcnt1",
	"EXP_SALON_SMART___T_Earn_Prcnt1 as T_Earn_Prcnt1",
	"EXP_SALON_SMART___ADD_DT as ADD_DT",
	"EXP_SALON_SMART___UPDATE_DT as UPDATE_DT",
	"EXP_SALON_SMART___o_C_Plan_Amt as o_C_Plan_Amt",
	"EXP_SALON_SMART___S_Plan_Amt as S_Plan_Amt",
	"EXP_SALON_SMART___T_Plan_Amt as T_Plan_Amt",
	"EXP_SALON_SMART___CSM_Act_Salary_AMT_Out as CSM_Act_Salary_AMT_Out",
	"EXP_SALON_SMART___CSM_Act_Salary_Hrs as CSM_Act_Salary_Hrs",
	"EXP_SALON_SMART___CSM_Pln_Salary_AMT_Out as CSM_Pln_Salary_AMT_Out",
	"EXP_SALON_SMART___CSM_Pln_Salary_Hrs as CSM_Pln_Salary_Hrs",
	"EXP_SALON_SMART___CSM_Act_Hrly_Mgr_Pay as CSM_Act_Hrly_Mgr_Pay",
	"EXP_SALON_SMART___CSM_Act_Hrly_Mgr_Hrs as CSM_Act_Hrly_Mgr_Hrs",
	"EXP_SALON_SMART___CSM_Pln_Hrly_Mgr_Pay as CSM_Pln_Hrly_Mgr_Pay",
	"EXP_SALON_SMART___CSM_Pln_Hrly_Mgr_Hrs as CSM_Pln_Hrly_Mgr_Hrs",
	"EXP_SALON_SMART___CSM_Act_Hrly_Pay_Out as CSM_Act_Hrly_Pay_Out",
	"EXP_SALON_SMART___CSM_Act_Hrly_Hrs as CSM_Act_Hrly_Hrs",
	"EXP_SALON_SMART___CSM_Pln_Hrly_Pay as CSM_Pln_Hrly_Pay",
	"EXP_SALON_SMART___CSM_Pln_Hrly_Hrs as CSM_Pln_Hrly_Hrs",
	"EXP_SALON_SMART___CSM_Ttl_Pln_Pay_Out as CSM_Ttl_Pln_Pay_Out",
	"EXP_SALON_SMART___CSM_Pln_Ttl_Hrs as CSM_Pln_Ttl_Hrs",
	"EXP_SALON_SMART___o_COUNTRY_CD as o_COUNTRY_CD",
	"EXP_SALON_SMART___C_ACTUAL_OT_HRS as C_ACTUAL_OT_HRS",
	"EXP_SALON_SMART___C_INCREMENTAL_HRS as C_INCREMENTAL_HRS",
	"EXP_SALON_SMART___H_HOURLY_ACTUAL_HRS as H_HOURLY_ACTUAL_HRS",
	"EXP_SALON_SMART___H_HOURLY_ACTUAL_OT_HRS as H_HOURLY_ACTUAL_OT_HRS",
	"EXP_SALON_SMART___H_HOURLY_ACTUAL_AMT_out as H_HOURLY_ACTUAL_AMT_out",
	"EXP_SALON_SMART___H_HOURLY_ACTUAL_OT_AMT_out as H_HOURLY_ACTUAL_OT_AMT_out",
	"EXP_SALON_SMART___H_SALARY_ACTUAL_HRS as H_SALARY_ACTUAL_HRS",
	"EXP_SALON_SMART___H_SALARY_ACTUAL_AMT_out as H_SALARY_ACTUAL_AMT_out",
	"EXP_SALON_SMART___H_HOURLY_EARN_AMT_out as H_HOURLY_EARN_AMT_out",
	"EXP_SALON_SMART___H_SALARY_EARN_AMT_out as H_SALARY_EARN_AMT_out",
	"EXP_SALON_SMART___H_HOURLY_EARN_HRS as H_HOURLY_EARN_HRS",
	"EXP_SALON_SMART___H_SALARY_EARN_HRS as H_SALARY_EARN_HRS",
	"EXP_SALON_SMART___H_HOURLY_OT_EARN_AMT_out as H_HOURLY_OT_EARN_AMT_out",
	"EXP_SALON_SMART___H_HOURLY_OT_EARN_HRS as H_HOURLY_OT_EARN_HRS",
	"EXP_SALON_SMART___H_HOURLY_OT_EARN_AMT_out as H_HOURLY_OT_EARN_AMT_out1",
	"EXP_SALON_SMART___H_HOURLY_OT_EARN_HRS as H_HOURLY_OT_EARN_HRS1",
	"EXP_SALON_SMART___S_Fcst_BB_Hrs as S_Fcst_BB_Hrs",
	"EXP_SALON_SMART___S_Fcst_FSG_Hrs as S_Fcst_FSG_Hrs",
	"EXP_SALON_SMART___T_Fcst_Hrs as T_Fcst_Hrs",	
	"IF (LKP_SERVICE_SMART___Location_ID IS NULL, 'I', 'U') as LOAD_FLAG",
	"IF (LKP_SERVICE_SMART___LOAD_DT IS NULL, CURRENT_TIMESTAMP, LKP_SERVICE_SMART___LOAD_DT) as LOAD_DT"
)

# COMMAND ----------

# Processing node UPD_SERVICE_SMART, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 87

# for each involved DataFrame, append the dataframe name to each column
EXP_LOAD_FLAG_temp = EXP_LOAD_FLAG.toDF(*["EXP_LOAD_FLAG___" + col for col in EXP_LOAD_FLAG.columns])

UPD_SERVICE_SMART = EXP_LOAD_FLAG_temp.selectExpr(
	"EXP_LOAD_FLAG___Location_ID as Location_ID1",
	"EXP_LOAD_FLAG___WEEK_DT as WEEK_DT1",
	"EXP_LOAD_FLAG___C_Actual_Hrs as C_Actual_Hrs1",
	"EXP_LOAD_FLAG___C_Earned_Hrs as C_Earned_Hrs1",
	"EXP_LOAD_FLAG___C_Hours_Efficiency1 as C_Hours_Efficiency11",
	"EXP_LOAD_FLAG___C_Actual_Spec_Hrs as C_Actual_Spec_Hrs1",
	"EXP_LOAD_FLAG___C_Earned_Spec_Hrs as C_Earned_Spec_Hrs1",
	"EXP_LOAD_FLAG___C_Spec_Hours_Efficiency1 as C_Spec_Hours_Efficiency11",
	"EXP_LOAD_FLAG___C_Actual_Amt1 as C_Actual_Amt11",
	"EXP_LOAD_FLAG___C_Earn_Amt1 as C_Earn_Amt11",
	"EXP_LOAD_FLAG___C_Amt_Efficiency1 as C_Amt_Efficiency11",
	"EXP_LOAD_FLAG___C_Actual_OT_Amt1 as C_Actual_OT_Amt11",
	"EXP_LOAD_FLAG___C_Earn_OT_Amt1 as C_Earn_OT_Amt11",
	"EXP_LOAD_FLAG___C_OT_Amt_Efficiency1 as C_OT_Amt_Efficiency11",
	"EXP_LOAD_FLAG___C_Actual_Prcnt1 as C_Actual_Prcnt11",
	"EXP_LOAD_FLAG___C_Earn_Prcnt1 as C_Earn_Prcnt11",
	"EXP_LOAD_FLAG___S_Actual_FSG_Hrs as S_Actual_FSG_Hrs1",
	"EXP_LOAD_FLAG___S_Earn_FSG_Hrs as S_Earn_FSG_Hrs1",
	"EXP_LOAD_FLAG___S_FSG_Hrs_Efficiency1 as S_FSG_Hrs_Efficiency11",
	"EXP_LOAD_FLAG___S_Actual_FSG_Amt1 as S_Actual_FSG_Amt11",
	"EXP_LOAD_FLAG___S_Earn_FSG_Amt1 as S_Earn_FSG_Amt11",
	"EXP_LOAD_FLAG___S_FSG_Amt_Efficiency1 as S_FSG_Amt_Efficiency11",
	"EXP_LOAD_FLAG___S_Actual_FSG_Prcnt1 as S_Actual_FSG_Prcnt11",
	"EXP_LOAD_FLAG___S_Earn_FSG_Prcnt1 as S_Earn_FSG_Prcnt11",
	"EXP_LOAD_FLAG___S_Actual_BB_Hrs as S_Actual_BB_Hrs1",
	"EXP_LOAD_FLAG___S_Earn_BB_Hrs as S_Earn_BB_Hrs1",
	"EXP_LOAD_FLAG___S_BB_Hrs_Efficiency1 as S_BB_Hrs_Efficiency11",
	"EXP_LOAD_FLAG___S_Actual_BB_Amt1 as S_Actual_BB_Amt11",
	"EXP_LOAD_FLAG___S_Earn_BB_Amt1 as S_Earn_BB_Amt11",
	"EXP_LOAD_FLAG___S_BB_Amt_Efficiency1 as S_BB_Amt_Efficiency11",
	"EXP_LOAD_FLAG___S_Actual_BB_Prcnt1 as S_Actual_BB_Prcnt11",
	"EXP_LOAD_FLAG___S_Earn_BB_Prcnt1 as S_Earn_BB_Prcnt11",
	"EXP_LOAD_FLAG___S_Actual_Ttl_Hrs as S_Actual_Ttl_Hrs1",
	"EXP_LOAD_FLAG___S_Earn_Ttl_Hrs as S_Earn_Ttl_Hrs1",
	"EXP_LOAD_FLAG___S_Ttl_Hrs_Efficiency1 as S_Ttl_Hrs_Efficiency11",
	"EXP_LOAD_FLAG___S_Actual_Ttl_Amt1 as S_Actual_Ttl_Amt11",
	"EXP_LOAD_FLAG___S_Earn_Ttl_Amt1 as S_Earn_Ttl_Amt11",
	"EXP_LOAD_FLAG___S_Ttl_Amt_Efficiency1 as S_Ttl_Amt_Efficiency11",
	"EXP_LOAD_FLAG___S_Actual_Ttl_Prcnt1 as S_Actual_Ttl_Prcnt11",
	"EXP_LOAD_FLAG___S_Earn_Ttl_Prcnt1 as S_Earn_Ttl_Prcnt11",
	"EXP_LOAD_FLAG___T_Actual_Hrs as T_Actual_Hrs1",
	"EXP_LOAD_FLAG___T_Earn_Hrs as T_Earn_Hrs1",
	"EXP_LOAD_FLAG___T_Hrs_Efficiency1 as T_Hrs_Efficiency11",
	"EXP_LOAD_FLAG___T_Actual_Amt1 as T_Actual_Amt11",
	"EXP_LOAD_FLAG___T_Earn_Amt1 as T_Earn_Amt11",
	"EXP_LOAD_FLAG___T_Amt_Efficiency1 as T_Amt_Efficiency11",
	"EXP_LOAD_FLAG___T_Actual_Prcnt1 as T_Actual_Prcnt11",
	"EXP_LOAD_FLAG___T_Earn_Prcnt1 as T_Earn_Prcnt11",
	"EXP_LOAD_FLAG___ADD_DT as ADD_DT1",
	"EXP_LOAD_FLAG___UPDATE_DT as UPDATE_DT1",
	"EXP_LOAD_FLAG___o_C_Plan_Amt as o_C_Plan_Amt1",
	"EXP_LOAD_FLAG___S_Plan_Amt as S_Plan_Amt1",
	"EXP_LOAD_FLAG___T_Plan_Amt as T_Plan_Amt1",
	"EXP_LOAD_FLAG___CSM_Act_Salary_AMT_Out as CSM_Act_Salary_AMT1",
	"EXP_LOAD_FLAG___CSM_Act_Salary_Hrs as CSM_Act_Salary_Hrs1",
	"EXP_LOAD_FLAG___CSM_Pln_Salary_AMT_Out as CSM_Pln_Salary_AMT1",
	"EXP_LOAD_FLAG___CSM_Pln_Salary_Hrs as CSM_Pln_Salary_Hrs1",
	"EXP_LOAD_FLAG___CSM_Act_Hrly_Mgr_Pay as CSM_Act_Hrly_Mgr_Pay1",
	"EXP_LOAD_FLAG___CSM_Act_Hrly_Mgr_Hrs as CSM_Act_Hrly_Mgr_Hrs1",
	"EXP_LOAD_FLAG___CSM_Pln_Hrly_Mgr_Pay as CSM_Pln_Hrly_Mgr_Pay1",
	"EXP_LOAD_FLAG___CSM_Pln_Hrly_Mgr_Hrs as CSM_Pln_Hrly_Mgr_Hrs1",
	"EXP_LOAD_FLAG___CSM_Act_Hrly_Pay_Out as CSM_Act_Hrly_Pay1",
	"EXP_LOAD_FLAG___CSM_Act_Hrly_Hrs as CSM_Act_Hrly_Hrs1",
	"EXP_LOAD_FLAG___CSM_Pln_Hrly_Pay as CSM_Pln_Hrly_Pay1",
	"EXP_LOAD_FLAG___CSM_Pln_Hrly_Hrs as CSM_Pln_Hrly_Hrs1",
	"EXP_LOAD_FLAG___CSM_Ttl_Pln_Pay_Out as CSM_Pln_Ttl_Pay1",
	"EXP_LOAD_FLAG___CSM_Pln_Ttl_Hrs as CSM_Pln_Ttl_Hrs1",
	"EXP_LOAD_FLAG___o_COUNTRY_CD as COUNTRY_CD1",
	"EXP_LOAD_FLAG___C_ACTUAL_OT_HRS as C_ACTUAL_OT_HRS1",
	"EXP_LOAD_FLAG___C_INCREMENTAL_HRS as C_INCREMENTAL_HRS1",
	"EXP_LOAD_FLAG___H_HOURLY_ACTUAL_HRS as H_HOURLY_ACTUAL_HRS1",
	"EXP_LOAD_FLAG___H_HOURLY_ACTUAL_OT_HRS as H_HOURLY_ACTUAL_OT_HRS1",
	"EXP_LOAD_FLAG___H_HOURLY_ACTUAL_AMT_out as H_HOURLY_ACTUAL_AMT_out1",
	"EXP_LOAD_FLAG___H_HOURLY_ACTUAL_OT_AMT_out as H_HOURLY_ACTUAL_OT_AMT_out1",
	"EXP_LOAD_FLAG___H_SALARY_ACTUAL_HRS as H_SALARY_ACTUAL_HRS1",
	"EXP_LOAD_FLAG___H_SALARY_ACTUAL_AMT_out as H_SALARY_ACTUAL_AMT_out1",
	"EXP_LOAD_FLAG___H_HOURLY_EARN_AMT_out as H_HOURLY_EARN_AMT_out1",
	"EXP_LOAD_FLAG___H_SALARY_EARN_AMT_out as H_SALARY_EARN_AMT_out1",
	"EXP_LOAD_FLAG___H_HOURLY_EARN_HRS as H_HOURLY_EARN_HRS1",
	"EXP_LOAD_FLAG___H_SALARY_EARN_HRS as H_SALARY_EARN_HRS1",
	"EXP_LOAD_FLAG___H_HOURLY_OT_EARN_AMT_out as H_HOURLY_OT_EARN_AMT_out1",
	"EXP_LOAD_FLAG___H_HOURLY_OT_EARN_HRS as H_HOURLY_OT_EARN_HRS1",
	"EXP_LOAD_FLAG___S_Fcst_BB_Hrs as S_Fcst_BB_Hrs",
	"EXP_LOAD_FLAG___S_Fcst_FSG_Hrs as S_Fcst_FSG_Hrs",
	"EXP_LOAD_FLAG___T_Fcst_Hrs as T_Fcst_Hrs",
	"EXP_LOAD_FLAG___LOAD_FLAG as LOAD_FLAG",
	"EXP_LOAD_FLAG___LOAD_DT as LOAD_DT") \
	.withColumn('pyspark_data_action', when((col("LOAD_FLAG") == 'I'),(lit(0))).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_To_SERVICE_SMART_ins_upd, type TARGET 
# COLUMN COUNT: 86


Shortcut_To_SERVICE_SMART_ins_upd = UPD_SERVICE_SMART.selectExpr(
	"CAST(WEEK_DT1 AS TIMESTAMP) as WEEK_DT",
	"CAST(Location_ID1 AS INT) as LOCATION_ID",
	"CAST(COUNTRY_CD1 AS STRING) as COUNTRY_CD",
	"CAST(C_Actual_Hrs1 AS DECIMAL(8,2)) as C_ACTUAL_HRS",
	"CAST(C_Earned_Hrs1 AS DECIMAL(8,2)) as C_EARN_HRS",
	"CAST(C_Hours_Efficiency11 AS DECIMAL(8,2)) as C_HRS_EFFICIENCY",
	"CAST(C_Actual_Spec_Hrs1 AS DECIMAL(8,2)) as C_ACTUAL_SPEC_HRS",
	"CAST(C_Earned_Spec_Hrs1 AS DECIMAL(8,2)) as C_EARN_SPEC_HRS",
	"CAST(C_Spec_Hours_Efficiency11 AS DECIMAL(8,2)) as C_SPEC_HRS_EFFICIENCY",
	"CAST(C_Actual_Amt11 AS DECIMAL(8,2)) as C_ACTUAL_AMT",
	"CAST(C_Earn_Amt11 AS DECIMAL(8,2)) as C_EARN_AMT",
	"CAST(C_Amt_Efficiency11 AS DECIMAL(8,2)) as C_AMT_EFFICIENCY",
	"CAST(C_ACTUAL_OT_HRS1 AS DECIMAL(8,2)) as C_ACTUAL_OT_HRS",
	"CAST(C_Actual_OT_Amt11 AS DECIMAL(8,2)) as C_ACTUAL_OT_AMT",
	"CAST(C_Earn_OT_Amt11 AS DECIMAL(8,2)) as C_EARN_OT_AMT",
	"CAST(C_OT_Amt_Efficiency11 AS DECIMAL(8,2)) as C_OT_AMT_EFFICIENCY",
	"CAST(C_Actual_Prcnt11 AS DECIMAL(8,2)) as C_ACTUAL_PRCNT",
	"CAST(C_Earn_Prcnt11 AS DECIMAL(8,2)) as C_EARN_PRCNT",
	"CAST(C_INCREMENTAL_HRS1 AS DECIMAL(8,2)) as C_INCREMENTAL_HRS",
	"CAST(S_Actual_FSG_Hrs1 AS DECIMAL(8,2)) as S_ACTUAL_FSG_HRS",
	"CAST(S_Earn_FSG_Hrs1 AS DECIMAL(8,2)) as S_EARN_FSG_HRS",
	"CAST(S_FSG_Hrs_Efficiency11 AS DECIMAL(8,2)) as S_FSG_HRS_EFFICIENCY",
	"CAST(S_Actual_FSG_Amt11 AS DECIMAL(8,2)) as S_ACTUAL_FSG_AMT",
	"CAST(S_Earn_FSG_Amt11 AS DECIMAL(8,2)) as S_EARN_FSG_AMT",
	"CAST(S_FSG_Amt_Efficiency11 AS DECIMAL(8,2)) as S_FSG_AMT_EFFICIENCY",
	"CAST(S_Actual_FSG_Prcnt11 AS DECIMAL(8,2)) as S_ACTUAL_FSG_PRCNT",
	"CAST(S_Earn_FSG_Prcnt11 AS DECIMAL(8,2)) as S_EARN_FSG_PRCNT",
	"CAST(S_Actual_BB_Hrs1 AS DECIMAL(8,2)) as S_ACTUAL_BB_HRS",
	"CAST(S_Earn_BB_Hrs1 AS DECIMAL(8,2)) as S_EARN_BB_HRS",
	"CAST(S_BB_Hrs_Efficiency11 AS DECIMAL(8,2)) as S_BB_HRS_EFFICIENCY",
	"CAST(S_Actual_BB_Amt11 AS DECIMAL(8,2)) as S_ACTUAL_BB_AMT",
	"CAST(S_Earn_BB_Amt11 AS DECIMAL(8,2)) as S_EARN_BB_AMT",
	"CAST(S_BB_Amt_Efficiency11 AS DECIMAL(8,2)) as S_BB_AMT_EFFICIENCY",
	"CAST(S_Actual_BB_Prcnt11 AS DECIMAL(8,2)) as S_ACTUAL_BB_PRCNT",
	"CAST(S_Earn_BB_Prcnt11 AS DECIMAL(8,2)) as S_EARN_BB_PRCNT",
	"CAST(S_Actual_Ttl_Hrs1 AS DECIMAL(8,2)) as S_ACTUAL_TTL_HRS",
	"CAST(S_Earn_Ttl_Hrs1 AS DECIMAL(8,2)) as S_EARN_TTL_HRS",
	"CAST(S_Ttl_Hrs_Efficiency11 AS DECIMAL(8,2)) as S_TTL_HRS_EFFICIENCY",
	"CAST(S_Actual_Ttl_Amt11 AS DECIMAL(8,2)) as S_ACTUAL_TTL_AMT",
	"CAST(S_Earn_Ttl_Amt11 AS DECIMAL(8,2)) as S_EARN_TTL_AMT",
	"CAST(S_Ttl_Amt_Efficiency11 AS DECIMAL(8,2)) as S_TTL_AMT_EFFICIENCY",
	"CAST(S_Actual_Ttl_Prcnt11 AS DECIMAL(8,2)) as S_ACTUAL_TTL_PRCNT",
	"CAST(S_Earn_Ttl_Prcnt11 AS DECIMAL(8,2)) as S_EARN_TTL_PRCNT",
	"CAST(T_Actual_Hrs1 AS DECIMAL(8,2)) as T_ACTUAL_HRS",
	"CAST(T_Earn_Hrs1 AS DECIMAL(8,2)) as T_EARN_HRS",
	"CAST(T_Hrs_Efficiency11 AS DECIMAL(8,2)) as T_HRS_EFFICIENCY",
	"CAST(T_Actual_Amt11 AS DECIMAL(8,2)) as T_ACTUAL_AMT",
	"CAST(T_Earn_Amt11 AS DECIMAL(8,2)) as T_EARN_AMT",
	"CAST(T_Amt_Efficiency11 AS DECIMAL(8,2)) as T_AMT_EFFICIENCY",
	"CAST(T_Actual_Prcnt11 AS DECIMAL(8,2)) as T_ACTUAL_PRCNT",
	"CAST(T_Earn_Prcnt11 AS DECIMAL(8,2)) as T_EARN_PRCNT",
	"CAST(o_C_Plan_Amt1 AS DECIMAL(8,2)) as C_PLAN_AMT",
	"CAST(S_Plan_Amt1 AS DECIMAL(8,2)) as S_PLAN_AMT",
	"CAST(T_Plan_Amt1 AS DECIMAL(8,2)) as T_PLAN_AMT",
	"CAST(CSM_Act_Salary_AMT1 AS DECIMAL(8,2)) as C_CSM_ACTUAL_AMT",
	"CAST(CSM_Act_Salary_Hrs1 AS DECIMAL(8,2)) as C_CSM_ACTUAL_HRS",
	"CAST(CSM_Pln_Salary_AMT1 AS DECIMAL(8,2)) as C_CSM_PLAN_AMT",
	"CAST(CSM_Pln_Salary_Hrs1 AS DECIMAL(8,2)) as C_CSM_PLAN_HRS",
	"CAST(CSM_Act_Hrly_Mgr_Pay1 AS DECIMAL(8,2)) as C_CHM_ACTUAL_AMT",
	"CAST(CSM_Act_Hrly_Mgr_Hrs1 AS DECIMAL(8,2)) as C_CHM_ACTUAL_HRS",
	"CAST(CSM_Pln_Hrly_Mgr_Pay1 AS DECIMAL(8,2)) as C_CHM_PLAN_AMT",
	"CAST(CSM_Pln_Hrly_Mgr_Hrs1 AS DECIMAL(8,2)) as C_CHM_PLAN_HRS",
	"CAST(CSM_Act_Hrly_Pay1 AS DECIMAL(8,2)) as C_CHA_ACTUAL_AMT",
	"CAST(CSM_Act_Hrly_Hrs1 AS DECIMAL(8,2)) as C_CHA_ACTUAL_HRS",
	"CAST(CSM_Pln_Hrly_Pay1 AS DECIMAL(8,2)) as C_CHA_PLAN_AMT",
	"CAST(CSM_Pln_Hrly_Hrs1 AS DECIMAL(8,2)) as C_CHA_PLAN_HRS",
	"CAST(CSM_Pln_Ttl_Pay1 AS DECIMAL(8,2)) as CSM_PLAN_TTL_PAY",
	"CAST(CSM_Pln_Ttl_Hrs1 AS DECIMAL(8,2)) as CSM_PLAN_TTL_HRS",
	"CAST(H_HOURLY_ACTUAL_HRS1 AS DECIMAL(8,2)) as H_HOURLY_ACTUAL_HRS",
	"CAST(H_HOURLY_ACTUAL_OT_HRS1 AS DECIMAL(8,2)) as H_HOURLY_ACTUAL_OT_HRS",
	"CAST(H_HOURLY_ACTUAL_AMT_out1 AS DECIMAL(8,2)) as H_HOURLY_ACTUAL_AMT",
	"CAST(H_HOURLY_ACTUAL_OT_AMT_out1 AS DECIMAL(8,2)) as H_HOURLY_ACTUAL_OT_AMT",
	"CAST(H_SALARY_ACTUAL_HRS1 AS DECIMAL(8,2)) as H_SALARY_ACTUAL_HRS",
	"CAST(H_SALARY_ACTUAL_AMT_out1 AS DECIMAL(8,2)) as H_SALARY_ACTUAL_AMT",
	"CAST(H_HOURLY_EARN_AMT_out1 AS DECIMAL(8,2)) as H_HOURLY_EARN_AMT",
	"CAST(H_SALARY_EARN_AMT_out1 AS DECIMAL(8,2)) as H_SALARY_EARN_AMT",
	"CAST(H_HOURLY_EARN_HRS1 AS DECIMAL(8,2)) as H_HOURLY_EARN_HRS",
	"CAST(H_SALARY_EARN_HRS1 AS DECIMAL(8,2)) as H_SALARY_EARN_HRS",
	"CAST(H_HOURLY_OT_EARN_AMT_out1 AS DECIMAL(8,2)) as H_HOURLY_OT_EARN_AMT",
	"CAST(H_HOURLY_OT_EARN_HRS1 AS DECIMAL(8,2)) as H_HOURLY_OT_EARN_HRS",
	"CAST(S_Fcst_BB_Hrs AS DECIMAL(8,2)) as S_FCST_BB_HRS",
	"CAST(S_Fcst_FSG_Hrs AS DECIMAL(8,2)) as S_FCST_FSG_HRS",
	"CAST(T_Fcst_Hrs AS DECIMAL(8,2)) as T_FCST_HRS",
	"CAST(1 AS DECIMAL(9,6)) as EXCH_RATE_PCT",
	"CAST(UPDATE_DT1 AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.WEEK_DT = target.WEEK_DT AND source.LOCATION_ID = target.LOCATION_ID"""
	refined_perf_table = f"{legacy}.SERVICE_SMART"
	executeMerge(Shortcut_To_SERVICE_SMART_ins_upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("SERVICE_SMART", "SERVICE_SMART", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("SERVICE_SMART", "SERVICE_SMART","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		

# COMMAND ----------


