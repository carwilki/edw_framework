#Code converted on 2023-07-24 08:13:16
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

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------
# Processing node LKP_DAY_LIGHT_SAVING_DATE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 7

LKP_DAY_LIGHT_SAVING_DATE_SRC = jdbcOracleConnection(f"""SELECT
DAY_LIGHT_SAVING_START_DT,
DAY_LIGHT_SAVING_END_DT
FROM DAY_LIGHT_SAVING_DATE""",username,password,connection_string)
# Conforming fields names to the component layout
LKP_DAY_LIGHT_SAVING_DATE_SRC = LKP_DAY_LIGHT_SAVING_DATE_SRC\
	.withColumnRenamed(LKP_DAY_LIGHT_SAVING_DATE_SRC.columns[0],'DAY_LIGHT_SAVING_START_DT')\
	.withColumnRenamed(LKP_DAY_LIGHT_SAVING_DATE_SRC.columns[1],'DAY_LIGHT_SAVING_END_DT')\
	.withColumnRenamed(LKP_DAY_LIGHT_SAVING_DATE_SRC.columns[2],'NEW_VALUE')\
	.withColumnRenamed(LKP_DAY_LIGHT_SAVING_DATE_SRC.columns[3],'SDS_CREATED_TSTMP')\
	.withColumnRenamed(LKP_DAY_LIGHT_SAVING_DATE_SRC.columns[4],'SDS_WORK_ORDER_NBR')\
	.withColumnRenamed(LKP_DAY_LIGHT_SAVING_DATE_SRC.columns[5],'EMPLOYEE_ID')\
	.withColumnRenamed(LKP_DAY_LIGHT_SAVING_DATE_SRC.columns[6],'TIME_ZONE_ID')

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST = spark.sql(f"""SELECT
SDS_APPT_ID,
CHANGED_TSTMP,
CHANGED_EMPLOYEE_ID,
TOTAL_BLOCKED_CNT,
LOAD_TSTMP
FROM {legacy}.SDS_APPT_BLOCKED_EXCEPTION_HIST""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_SERVICE_TERRITORY, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SDS_SERVICE_TERRITORY = spark.sql(f"""SELECT
SDS_SERVICE_TERRITORY_ID,
STORE_NBR
FROM {legacy}.SDS_SERVICE_TERRITORY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_WORK_ORDER, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SDS_WORK_ORDER = spark.sql(f"""SELECT
SDS_WORK_ORDER_ID,
SDS_WORK_ORDER_NBR,
SDS_SERVICE_TERRITORY_ID
FROM {legacy}.SDS_WORK_ORDER""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_USER, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SDS_USER = spark.sql(f"""SELECT
SDS_USER_ID,
EMPLOYEE_ID
FROM {legacy}.legacy_SDS_USER""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_To_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.STORE_NBR,
SITE_PROFILE.TIME_ZONE_ID
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TIMEZONE_CONVERSION, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TIMEZONE_CONVERSION = spark.sql(f"""SELECT
TO_TIME_ZONE_ID,
DAY_LIGHT_SAVING_FLAG,
CONVERSION_HOUR
FROM {legacy}.TIMEZONE_CONVERSION
WHERE FROM_TIME_ZONE_ID ='UTC'""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SDS_WORK_ORDER, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_WORK_ORDER_temp = SQ_Shortcut_to_SDS_WORK_ORDER.toDF(*["SQ_Shortcut_to_SDS_WORK_ORDER___" + col for col in SQ_Shortcut_to_SDS_WORK_ORDER.columns])

EXP_SDS_WORK_ORDER = SQ_Shortcut_to_SDS_WORK_ORDER_temp.selectExpr(
	"SQ_Shortcut_to_SDS_WORK_ORDER___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SDS_WORK_ORDER___SDS_WORK_ORDER_ID as SDS_WORK_ORDER_ID",
	"BIGINT(SQ_Shortcut_to_SDS_WORK_ORDER___SDS_WORK_ORDER_NBR) as o_SDS_WORK_ORDER_NBR",
	"SQ_Shortcut_to_SDS_WORK_ORDER___SDS_SERVICE_TERRITORY_ID as SDS_SERVICE_TERRITORY_ID"
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_WORK_ORDER_HIST, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SDS_WORK_ORDER_HIST = spark.sql(f"""SELECT
SDS_WORK_ORDER_ID,
OLD_VALUE,
NEW_VALUE,
SDS_CREATED_TSTMP,
SDS_CREATED_BY_ID
FROM {legacy}.SDS_WORK_ORDER_HIST
WHERE SDS_FIELD_NAME = 'PSVC_Max_Per_Block_Count__c'

AND (NVL(OLD_VALUE, '0.0') NOT IN ('1.0','0.0') OR NVL(NEW_VALUE, '0.0') NOT IN ('1.0','0.0') )""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SDS_WORK_ORDER, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_WORK_ORDER_HIST_temp = SQ_Shortcut_to_SDS_WORK_ORDER_HIST.toDF(*["SQ_Shortcut_to_SDS_WORK_ORDER_HIST___" + col for col in SQ_Shortcut_to_SDS_WORK_ORDER_HIST.columns])
EXP_SDS_WORK_ORDER_temp = EXP_SDS_WORK_ORDER.toDF(*["EXP_SDS_WORK_ORDER___" + col for col in EXP_SDS_WORK_ORDER.columns])

JNR_SDS_WORK_ORDER = EXP_SDS_WORK_ORDER_temp.join(SQ_Shortcut_to_SDS_WORK_ORDER_HIST_temp,[EXP_SDS_WORK_ORDER_temp.EXP_SDS_WORK_ORDER___SDS_WORK_ORDER_ID == SQ_Shortcut_to_SDS_WORK_ORDER_HIST_temp.SQ_Shortcut_to_SDS_WORK_ORDER_HIST___SDS_WORK_ORDER_ID],'inner').selectExpr(
	"SQ_Shortcut_to_SDS_WORK_ORDER_HIST___SDS_WORK_ORDER_ID as SDS_WORK_ORDER_ID",
	"SQ_Shortcut_to_SDS_WORK_ORDER_HIST___OLD_VALUE as OLD_VALUE",
	"SQ_Shortcut_to_SDS_WORK_ORDER_HIST___NEW_VALUE as NEW_VALUE",
	"SQ_Shortcut_to_SDS_WORK_ORDER_HIST___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"SQ_Shortcut_to_SDS_WORK_ORDER_HIST___SDS_CREATED_BY_ID as SDS_CREATED_BY_ID",
	"EXP_SDS_WORK_ORDER___SDS_WORK_ORDER_ID as i_SDS_WORK_ORDER_ID",
	"EXP_SDS_WORK_ORDER___o_SDS_WORK_ORDER_NBR as i_SDS_WORK_ORDER_NBR",
	"EXP_SDS_WORK_ORDER___SDS_SERVICE_TERRITORY_ID as SDS_SERVICE_TERRITORY_ID")

# COMMAND ----------
# Processing node JNR_SDS_USER, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_WORK_ORDER_temp = JNR_SDS_WORK_ORDER.toDF(*["JNR_SDS_WORK_ORDER___" + col for col in JNR_SDS_WORK_ORDER.columns])
SQ_Shortcut_to_SDS_USER_temp = SQ_Shortcut_to_SDS_USER.toDF(*["SQ_Shortcut_to_SDS_USER___" + col for col in SQ_Shortcut_to_SDS_USER.columns])

JNR_SDS_USER = SQ_Shortcut_to_SDS_USER_temp.join(JNR_SDS_WORK_ORDER_temp,[SQ_Shortcut_to_SDS_USER_temp.SQ_Shortcut_to_SDS_USER___SDS_USER_ID == JNR_SDS_WORK_ORDER_temp.JNR_SDS_WORK_ORDER___SDS_CREATED_BY_ID],'inner').selectExpr(
	"JNR_SDS_WORK_ORDER___NEW_VALUE as NEW_VALUE",
	"JNR_SDS_WORK_ORDER___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"JNR_SDS_WORK_ORDER___SDS_CREATED_BY_ID as SDS_CREATED_BY_ID",
	"JNR_SDS_WORK_ORDER___i_SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"JNR_SDS_WORK_ORDER___SDS_SERVICE_TERRITORY_ID as SDS_SERVICE_TERRITORY_ID",
	"SQ_Shortcut_to_SDS_USER___SDS_USER_ID as i_SDS_USER_ID",
	"SQ_Shortcut_to_SDS_USER___EMPLOYEE_ID as i_EMPLOYEE_ID")

# COMMAND ----------
# Processing node JNR_SDS_SERVICE_TERRITORY, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_SERVICE_TERRITORY_temp = SQ_Shortcut_to_SDS_SERVICE_TERRITORY.toDF(*["SQ_Shortcut_to_SDS_SERVICE_TERRITORY___" + col for col in SQ_Shortcut_to_SDS_SERVICE_TERRITORY.columns])
JNR_SDS_USER_temp = JNR_SDS_USER.toDF(*["JNR_SDS_USER___" + col for col in JNR_SDS_USER.columns])

JNR_SDS_SERVICE_TERRITORY = SQ_Shortcut_to_SDS_SERVICE_TERRITORY_temp.join(JNR_SDS_USER_temp,[SQ_Shortcut_to_SDS_SERVICE_TERRITORY_temp.SQ_Shortcut_to_SDS_SERVICE_TERRITORY___SDS_SERVICE_TERRITORY_ID == JNR_SDS_USER_temp.JNR_SDS_USER___SDS_SERVICE_TERRITORY_ID],'right_outer').selectExpr(
	"JNR_SDS_USER___NEW_VALUE as NEW_VALUE",
	"JNR_SDS_USER___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"JNR_SDS_USER___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"JNR_SDS_USER___SDS_SERVICE_TERRITORY_ID as SDS_SERVICE_TERRITORY_ID",
	"JNR_SDS_USER___i_EMPLOYEE_ID as EMPLOYEE_ID",
	"SQ_Shortcut_to_SDS_SERVICE_TERRITORY___SDS_SERVICE_TERRITORY_ID as i_SDS_SERVICE_TERRITORY_ID",
	"SQ_Shortcut_to_SDS_SERVICE_TERRITORY___STORE_NBR as i_STORE_NBR")

# COMMAND ----------
# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_SERVICE_TERRITORY_temp = JNR_SDS_SERVICE_TERRITORY.toDF(*["JNR_SDS_SERVICE_TERRITORY___" + col for col in JNR_SDS_SERVICE_TERRITORY.columns])
SQ_Shortcut_To_SITE_PROFILE_temp = SQ_Shortcut_To_SITE_PROFILE.toDF(*["SQ_Shortcut_To_SITE_PROFILE___" + col for col in SQ_Shortcut_To_SITE_PROFILE.columns])

JNR_SITE_PROFILE = SQ_Shortcut_To_SITE_PROFILE_temp.join(JNR_SDS_SERVICE_TERRITORY_temp,[SQ_Shortcut_To_SITE_PROFILE_temp.SQ_Shortcut_To_SITE_PROFILE___STORE_NBR == JNR_SDS_SERVICE_TERRITORY_temp.JNR_SDS_SERVICE_TERRITORY___i_STORE_NBR],'inner').selectExpr(
	"JNR_SDS_SERVICE_TERRITORY___NEW_VALUE as NEW_VALUE",
	"JNR_SDS_SERVICE_TERRITORY___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"JNR_SDS_SERVICE_TERRITORY___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"JNR_SDS_SERVICE_TERRITORY___EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_SDS_SERVICE_TERRITORY___i_STORE_NBR as STORE_NBR",
	"SQ_Shortcut_To_SITE_PROFILE___STORE_NBR as i_STORE_NBR",
	"SQ_Shortcut_To_SITE_PROFILE___TIME_ZONE_ID as i_TIME_ZONE_ID")

# COMMAND ----------
# Processing node LKP_DAY_LIGHT_SAVING_DATE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7


LKP_DAY_LIGHT_SAVING_DATE_lookup_result = JNR_SITE_PROFILE.selectExpr(
	"JNR_SITE_PROFILE.NEW_VALUE as NEW_VALUE",
	"JNR_SITE_PROFILE.SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"JNR_SITE_PROFILE.SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"JNR_SITE_PROFILE.EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_SITE_PROFILE.i_TIME_ZONE_ID as TIME_ZONE_ID").join(LKP_DAY_LIGHT_SAVING_DATE_SRC, (col('DAY_LIGHT_SAVING_START_DT') <= col('SDS_CREATED_TSTMP')) & (col('DAY_LIGHT_SAVING_END_DT') >= col('SDS_CREATED_TSTMP')), 'left')
LKP_DAY_LIGHT_SAVING_DATE = LKP_DAY_LIGHT_SAVING_DATE_lookup_result.select(
	LKP_DAY_LIGHT_SAVING_DATE_lookup_result.sys_row_id,
	col('DAY_LIGHT_SAVING_START_DT'),
	col('DAY_LIGHT_SAVING_END_DT'),
	col('NEW_VALUE'),
	col('SDS_CREATED_TSTMP'),
	col('SDS_WORK_ORDER_NBR'),
	col('EMPLOYEE_ID'),
	col('TIME_ZONE_ID')
)

# COMMAND ----------
# Processing node EXP_DAY_LIGHT_SAVING_DATE, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
LKP_DAY_LIGHT_SAVING_DATE_temp = LKP_DAY_LIGHT_SAVING_DATE.toDF(*["LKP_DAY_LIGHT_SAVING_DATE___" + col for col in LKP_DAY_LIGHT_SAVING_DATE.columns])

EXP_DAY_LIGHT_SAVING_DATE = LKP_DAY_LIGHT_SAVING_DATE_temp.selectExpr(
	"LKP_DAY_LIGHT_SAVING_DATE___sys_row_id as sys_row_id",
	"LKP_DAY_LIGHT_SAVING_DATE___DAY_LIGHT_SAVING_START_DT as DAY_LIGHT_SAVING_START_DT",
	"LKP_DAY_LIGHT_SAVING_DATE___DAY_LIGHT_SAVING_END_DT as DAY_LIGHT_SAVING_END_DT",
	"LKP_DAY_LIGHT_SAVING_DATE___NEW_VALUE as NEW_VALUE",
	"LKP_DAY_LIGHT_SAVING_DATE___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"LKP_DAY_LIGHT_SAVING_DATE___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"LKP_DAY_LIGHT_SAVING_DATE___EMPLOYEE_ID as EMPLOYEE_ID",
	"LKP_DAY_LIGHT_SAVING_DATE___TIME_ZONE_ID as TIME_ZONE_ID",
	"IF (LKP_DAY_LIGHT_SAVING_DATE___DAY_LIGHT_SAVING_START_DT IS NULL, 0, 1) as o_DAY_LIGHT_SAVING_FLAG"
)

# COMMAND ----------
# Processing node JNR_TIMEZONE_CONVERSION, type JOINER 
# COLUMN COUNT: 9

JNR_TIMEZONE_CONVERSION = SQ_Shortcut_to_TIMEZONE_CONVERSION.join(EXP_DAY_LIGHT_SAVING_DATE,[SQ_Shortcut_to_TIMEZONE_CONVERSION.TO_TIME_ZONE_ID == EXP_DAY_LIGHT_SAVING_DATE.TIME_ZONE_ID, SQ_Shortcut_to_TIMEZONE_CONVERSION.DAY_LIGHT_SAVING_FLAG == EXP_DAY_LIGHT_SAVING_DATE.o_DAY_LIGHT_SAVING_FLAG],'right_outer')

# COMMAND ----------
# Processing node EXP_TIMEZONE_CONVERSION, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
JNR_TIMEZONE_CONVERSION_temp = JNR_TIMEZONE_CONVERSION.toDF(*["JNR_TIMEZONE_CONVERSION___" + col for col in JNR_TIMEZONE_CONVERSION.columns])

EXP_TIMEZONE_CONVERSION = JNR_TIMEZONE_CONVERSION_temp.selectExpr(
	"JNR_TIMEZONE_CONVERSION___NEW_VALUE as NEW_VALUE",
	"JNR_TIMEZONE_CONVERSION___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP_UTC",
	"JNR_TIMEZONE_CONVERSION___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"JNR_TIMEZONE_CONVERSION___EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_TIMEZONE_CONVERSION___CONVERSION_HOUR as CONVERSION_HOUR").selectExpr(
	"JNR_TIMEZONE_CONVERSION___sys_row_id as sys_row_id",
	"JNR_TIMEZONE_CONVERSION___NEW_VALUE as NEW_VALUE",
	"JNR_TIMEZONE_CONVERSION___SDS_CREATED_TSTMP_UTC as SDS_CREATED_TSTMP_UTC",
	"JNR_TIMEZONE_CONVERSION___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"JNR_TIMEZONE_CONVERSION___EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_TIMEZONE_CONVERSION___CONVERSION_HOUR as CONVERSION_HOUR",
	"DATE_ADD(JNR_TIMEZONE_CONVERSION___CONVERSION_HOUR, JNR_TIMEZONE_CONVERSION___SDS_CREATED_TSTMP_UTC) as SDS_CREATED_TSTMP"
)

# COMMAND ----------
# Processing node JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
EXP_TIMEZONE_CONVERSION_temp = EXP_TIMEZONE_CONVERSION.toDF(*["EXP_TIMEZONE_CONVERSION___" + col for col in EXP_TIMEZONE_CONVERSION.columns])
SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp = SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST.toDF(*["SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST___" + col for col in SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST.columns])

JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST = EXP_TIMEZONE_CONVERSION_temp.join(SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp,[EXP_TIMEZONE_CONVERSION_temp.EXP_TIMEZONE_CONVERSION___SDS_WORK_ORDER_NBR == SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp.SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST___SDS_APPT_ID, EXP_TIMEZONE_CONVERSION_temp.EXP_TIMEZONE_CONVERSION___SDS_CREATED_TSTMP == SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp.SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST___CHANGED_TSTMP],'left_outer').selectExpr(
	"EXP_TIMEZONE_CONVERSION___NEW_VALUE as NEW_VALUE",
	"EXP_TIMEZONE_CONVERSION___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"EXP_TIMEZONE_CONVERSION___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"EXP_TIMEZONE_CONVERSION___EMPLOYEE_ID as EMPLOYEE_ID",
	"SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST___SDS_APPT_ID as i_SDS_APPT_ID",
	"SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST___CHANGED_TSTMP as i_CHANGED_TSTMP",
	"SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST___CHANGED_EMPLOYEE_ID as i_CHANGED_EMPLOYEE_ID",
	"SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST___TOTAL_BLOCKED_CNT as i_TOTAL_BLOCKED_CNT",
	"SQ_Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp = JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST.toDF(*["JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___" + col for col in JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST.columns])

FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST = JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp.selectExpr(
	"JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___NEW_VALUE as NEW_VALUE",
	"JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___i_SDS_APPT_ID as SDS_APPT_ID",
	"JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___i_CHANGED_TSTMP as i_CHANGED_TSTMP",
	"JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___i_CHANGED_EMPLOYEE_ID as i_CHANGED_EMPLOYEE_ID",
	"JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___i_TOTAL_BLOCKED_CNT as i_TOTAL_BLOCKED_CNT",
	"JNR_SDS_APPT_BLOCKED_EXCEPTION_HIST___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("SDS_APPT_ID IS NULL OR ( SDS_APPT_ID IS NOT NULL AND ( DECIMAL(IF (NEW_VALUE IS NULL, '0', NEW_VALUE)) != IF (i_TOTAL_BLOCKED_CNT IS NULL, 0, i_TOTAL_BLOCKED_CNT) OR cast(IF (EMPLOYEE_ID IS NULL, '0', EMPLOYEE_ID) as int) != IF (i_CHANGED_EMPLOYEE_ID IS NULL, 0, i_CHANGED_EMPLOYEE_ID) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp = FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST.toDF(*["FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST___" + col for col in FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST.columns])

EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST = FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp.selectExpr(
	"FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST___sys_row_id as sys_row_id",
	"FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"cast(FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST___EMPLOYEE_ID as int) as o_EMPLOYEE_ID",
	"DECIMAL(FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST___NEW_VALUE) as o_NEW_VALUE",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (FIL_SDS_APPT_BLOCKED_EXCEPTION_HIST___SDS_APPT_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR"
)

# COMMAND ----------
# Processing node UPD_SDS_APPT_BLOCKED_EXCEPTION_HIST, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp = EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST.toDF(*["EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST___" + col for col in EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST.columns])

UPD_SDS_APPT_BLOCKED_EXCEPTION_HIST = EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST_temp.selectExpr(
	"EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR",
	"EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST___SDS_CREATED_TSTMP as SDS_CREATED_TSTMP",
	"EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST___o_EMPLOYEE_ID as EMPLOYEE_ID",
	"EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST___o_NEW_VALUE as NEW_VALUE",
	"EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_SDS_APPT_BLOCKED_EXCEPTION_HIST___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR")\
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)) , lit(0)) .when(col('o_UPDATE_VALIDATOR') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST_1, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST_1 = UPD_SDS_APPT_BLOCKED_EXCEPTION_HIST.selectExpr(
	"SDS_APPT_ID as SDS_APPT_ID",
	"CAST(SDS_CREATED_TSTMP AS TIMESTAMP) as CHANGED_TSTMP",
	"CAST(EMPLOYEE_ID AS BIGINT) as CHANGED_EMPLOYEE_ID",
	"CAST(NEW_VALUE AS INT) as TOTAL_BLOCKED_CNT",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"UPD_SDS_APPT_BLOCKED_EXCEPTION_HIST.pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.SDS_APPT_ID = target.SDS_APPT_ID AND source.CHANGED_TSTMP = target.CHANGED_TSTMP"""
  refined_perf_table = f"{legacy}.SDS_APPT_BLOCKED_EXCEPTION_HIST"
  executeMerge(Shortcut_to_SDS_APPT_BLOCKED_EXCEPTION_HIST_1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("SDS_APPT_BLOCKED_EXCEPTION_HIST", "SDS_APPT_BLOCKED_EXCEPTION_HIST", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("SDS_APPT_BLOCKED_EXCEPTION_HIST", "SDS_APPT_BLOCKED_EXCEPTION_HIST","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	