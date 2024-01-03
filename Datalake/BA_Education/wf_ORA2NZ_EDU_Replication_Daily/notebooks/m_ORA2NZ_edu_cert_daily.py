# Databricks notebook source
#Code converted on 2023-10-17 09:36:50
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script
# Read in relation source variables
(username, password, connection_string) = or_kro_read_edhp1(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_EDU_CERT_DAILY, type SOURCE 
# COLUMN COUNT: 16

SQ_Shortcut_to_EDU_CERT_DAILY = jdbcOracleConnection(f"""SELECT
DAY_DT,
EMPLOYEE_ID,
ASSESSMENT_MID,
ASSESSMENT_LID,
TEST_TAKEN_DT,
ASSESSMENT_NAME,
JOB_CD,
LOCATION_ID,
LAST_TEST_SCORE_NBR,
LAST_TEST_PASSED_FLAG,
COMPLIANT_START_DT,
COMPLIANT_EXPIRATION_DT,
CURR_COMPLIANCE_FLAG,
CURR_MISSING_FLAG,
CURR_PERIOD_ATTEMPTS_NBR,
LOAD_DT
FROM BIWPADM.EDU_CERT_DAILY
WHERE LOAD_DT > SYSDATE -1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EDU_CERT_DAILY = SQ_Shortcut_to_EDU_CERT_DAILY \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[1],'EMPLOYEE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[2],'ASSESSMENT_MID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[3],'ASSESSMENT_LID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[4],'TEST_TAKEN_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[5],'ASSESSMENT_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[6],'JOB_CD') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[7],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[8],'LAST_TEST_SCORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[9],'LAST_TEST_PASSED_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[10],'COMPLIANT_START_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[11],'COMPLIANT_EXPIRATION_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[12],'CURR_COMPLIANCE_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[13],'CURR_MISSING_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[14],'CURR_PERIOD_ATTEMPTS_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_DAILY.columns[15],'LOAD_DT')

# COMMAND ----------

SQ_Shortcut_to_EDU_CERT_DAILY.show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS = spark.sql(f"""SELECT
ASSESSMENT_ID,
NO_FORKLIFT_FLAG
FROM {legacy}.USR_ASSESSMENT_EXCEPTIONS""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS = SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS \
	.withColumnRenamed(SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS.columns[0],'ASSESSMENT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS.columns[1],'NO_FORKLIFT_FLAG')

# COMMAND ----------

# Processing node SQ_Shortcut_to_USR_STORE_ATTRIBUTES, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_USR_STORE_ATTRIBUTES = spark.sql(f"""SELECT
LOCATION_ID,
NO_FORKLIFT_FLAG
FROM {legacy}.USR_STORE_ATTRIBUTES""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_USR_STORE_ATTRIBUTES = SQ_Shortcut_to_USR_STORE_ATTRIBUTES \
	.withColumnRenamed(SQ_Shortcut_to_USR_STORE_ATTRIBUTES.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_USR_STORE_ATTRIBUTES.columns[1],'NO_FORKLIFT_FLAG')

# COMMAND ----------

# Processing node EXP_PRE_JOIN, type EXPRESSION 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_EDU_CERT_DAILY_temp = SQ_Shortcut_to_EDU_CERT_DAILY.toDF(*["SQ_Shortcut_to_EDU_CERT_DAILY___" + col for col in SQ_Shortcut_to_EDU_CERT_DAILY.columns])

EXP_PRE_JOIN = SQ_Shortcut_to_EDU_CERT_DAILY_temp.selectExpr(
	"SQ_Shortcut_to_EDU_CERT_DAILY___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_EDU_CERT_DAILY___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_EDU_CERT_DAILY___EMPLOYEE_ID as EMPLOYEE_ID",
	"SQ_Shortcut_to_EDU_CERT_DAILY___ASSESSMENT_MID as ASSESSMENT_MID",
	"SQ_Shortcut_to_EDU_CERT_DAILY___ASSESSMENT_LID as ASSESSMENT_LID",
	"SQ_Shortcut_to_EDU_CERT_DAILY___TEST_TAKEN_DT as TEST_TAKEN_DT",
	"SQ_Shortcut_to_EDU_CERT_DAILY___ASSESSMENT_NAME as ASSESSMENT_NAME",
	"SQ_Shortcut_to_EDU_CERT_DAILY___JOB_CD as JOB_CD",
	"cast(SQ_Shortcut_to_EDU_CERT_DAILY___LOCATION_ID as int) as o_LOCATION_ID",
	"SQ_Shortcut_to_EDU_CERT_DAILY___LAST_TEST_SCORE_NBR as LAST_TEST_SCORE_NBR",
	"SQ_Shortcut_to_EDU_CERT_DAILY___LAST_TEST_PASSED_FLAG as LAST_TEST_PASSED_FLAG",
	"SQ_Shortcut_to_EDU_CERT_DAILY___COMPLIANT_START_DT as COMPLIANT_START_DT",
	"SQ_Shortcut_to_EDU_CERT_DAILY___COMPLIANT_EXPIRATION_DT as COMPLIANT_EXPIRATION_DT",
	"SQ_Shortcut_to_EDU_CERT_DAILY___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"SQ_Shortcut_to_EDU_CERT_DAILY___CURR_MISSING_FLAG as CURR_MISSING_FLAG",
	"SQ_Shortcut_to_EDU_CERT_DAILY___CURR_PERIOD_ATTEMPTS_NBR as CURR_PERIOD_ATTEMPTS_NBR",
	"SQ_Shortcut_to_EDU_CERT_DAILY___LOAD_DT as LOAD_DT",
	"BIGINT(concat( SQ_Shortcut_to_EDU_CERT_DAILY___ASSESSMENT_MID , SQ_Shortcut_to_EDU_CERT_DAILY___ASSESSMENT_LID )) as ASSESSMENT_ID"
)

# COMMAND ----------

# EXP_PRE_JOIN.show()

# COMMAND ----------

# Processing node JNR_STORE_ATTRIBUTES, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 19

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_USR_STORE_ATTRIBUTES_temp = SQ_Shortcut_to_USR_STORE_ATTRIBUTES.toDF(*["SQ_Shortcut_to_USR_STORE_ATTRIBUTES___" + col for col in SQ_Shortcut_to_USR_STORE_ATTRIBUTES.columns])
EXP_PRE_JOIN_temp = EXP_PRE_JOIN.toDF(*["EXP_PRE_JOIN___" + col for col in EXP_PRE_JOIN.columns])

JNR_STORE_ATTRIBUTES = SQ_Shortcut_to_USR_STORE_ATTRIBUTES_temp.join(EXP_PRE_JOIN_temp,[SQ_Shortcut_to_USR_STORE_ATTRIBUTES_temp.SQ_Shortcut_to_USR_STORE_ATTRIBUTES___LOCATION_ID == EXP_PRE_JOIN_temp.EXP_PRE_JOIN___o_LOCATION_ID],'right_outer').selectExpr(
	"EXP_PRE_JOIN___DAY_DT as DAY_DT",
	"EXP_PRE_JOIN___EMPLOYEE_ID as EMPLOYEE_ID",
	"EXP_PRE_JOIN___ASSESSMENT_MID as ASSESSMENT_MID",
	"EXP_PRE_JOIN___ASSESSMENT_LID as ASSESSMENT_LID",
	"EXP_PRE_JOIN___TEST_TAKEN_DT as TEST_TAKEN_DT",
	"EXP_PRE_JOIN___ASSESSMENT_NAME as ASSESSMENT_NAME",
	"EXP_PRE_JOIN___JOB_CD as JOB_CD",
	"EXP_PRE_JOIN___o_LOCATION_ID as LOCATION_ID",
	"EXP_PRE_JOIN___LAST_TEST_SCORE_NBR as LAST_TEST_SCORE_NBR",
	"EXP_PRE_JOIN___LAST_TEST_PASSED_FLAG as LAST_TEST_PASSED_FLAG",
	"EXP_PRE_JOIN___COMPLIANT_START_DT as COMPLIANT_START_DT",
	"EXP_PRE_JOIN___COMPLIANT_EXPIRATION_DT as COMPLIANT_EXPIRATION_DT",
	"EXP_PRE_JOIN___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"EXP_PRE_JOIN___CURR_MISSING_FLAG as CURR_MISSING_FLAG",
	"EXP_PRE_JOIN___CURR_PERIOD_ATTEMPTS_NBR as CURR_PERIOD_ATTEMPTS_NBR",
	"EXP_PRE_JOIN___LOAD_DT as LOAD_DT",
	"EXP_PRE_JOIN___ASSESSMENT_ID as ASSESSMENT_ID",
	"SQ_Shortcut_to_USR_STORE_ATTRIBUTES___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_to_USR_STORE_ATTRIBUTES___NO_FORKLIFT_FLAG as NO_FORKLIFT_FLAG")

# COMMAND ----------

# JNR_STORE_ATTRIBUTES.show()

# COMMAND ----------

# Processing node JNR_EXCEPTIONS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS_temp = SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS.toDF(*["SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___" + col for col in SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS.columns])
JNR_STORE_ATTRIBUTES_temp = JNR_STORE_ATTRIBUTES.toDF(*["JNR_STORE_ATTRIBUTES___" + col for col in JNR_STORE_ATTRIBUTES.columns])

JNR_EXCEPTIONS = SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS_temp.join(JNR_STORE_ATTRIBUTES_temp,[SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS_temp.SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___ASSESSMENT_ID == JNR_STORE_ATTRIBUTES_temp.JNR_STORE_ATTRIBUTES___ASSESSMENT_ID, SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS_temp.SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___NO_FORKLIFT_FLAG == JNR_STORE_ATTRIBUTES_temp.JNR_STORE_ATTRIBUTES___NO_FORKLIFT_FLAG],'right_outer').selectExpr(
	"JNR_STORE_ATTRIBUTES___DAY_DT as DAY_DT",
	"JNR_STORE_ATTRIBUTES___EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_STORE_ATTRIBUTES___ASSESSMENT_MID as ASSESSMENT_MID",
	"JNR_STORE_ATTRIBUTES___ASSESSMENT_LID as ASSESSMENT_LID",
	"JNR_STORE_ATTRIBUTES___TEST_TAKEN_DT as TEST_TAKEN_DT",
	"JNR_STORE_ATTRIBUTES___ASSESSMENT_NAME as ASSESSMENT_NAME",
	"JNR_STORE_ATTRIBUTES___JOB_CD as JOB_CD",
	"JNR_STORE_ATTRIBUTES___LOCATION_ID as LOCATION_ID",
	"JNR_STORE_ATTRIBUTES___LAST_TEST_SCORE_NBR as LAST_TEST_SCORE_NBR",
	"JNR_STORE_ATTRIBUTES___LAST_TEST_PASSED_FLAG as LAST_TEST_PASSED_FLAG",
	"JNR_STORE_ATTRIBUTES___COMPLIANT_START_DT as COMPLIANT_START_DT",
	"JNR_STORE_ATTRIBUTES___COMPLIANT_EXPIRATION_DT as COMPLIANT_EXPIRATION_DT",
	"JNR_STORE_ATTRIBUTES___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"JNR_STORE_ATTRIBUTES___CURR_MISSING_FLAG as CURR_MISSING_FLAG",
	"JNR_STORE_ATTRIBUTES___CURR_PERIOD_ATTEMPTS_NBR as CURR_PERIOD_ATTEMPTS_NBR",
	"JNR_STORE_ATTRIBUTES___LOAD_DT as LOAD_DT",
	"JNR_STORE_ATTRIBUTES___ASSESSMENT_ID as ASSESSMENT_ID",
	"JNR_STORE_ATTRIBUTES___NO_FORKLIFT_FLAG as NO_FORKLIFT_FLAG",
	"SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___ASSESSMENT_ID as ASSESSMENT_ID1",
	"SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___NO_FORKLIFT_FLAG as NO_FORKLIFT_FLAG1")

# COMMAND ----------

# JNR_EXCEPTIONS.show()

# COMMAND ----------

# Processing node EXP_EXCEPTION_ASSESSMENT, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
JNR_EXCEPTIONS_temp = JNR_EXCEPTIONS.toDF(*["JNR_EXCEPTIONS___" + col for col in JNR_EXCEPTIONS.columns])

EXP_EXCEPTION_ASSESSMENT = JNR_EXCEPTIONS_temp.selectExpr(
	"JNR_EXCEPTIONS___DAY_DT as DAY_DT",
	"JNR_EXCEPTIONS___EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_EXCEPTIONS___ASSESSMENT_MID as ASSESSMENT_MID",
	"JNR_EXCEPTIONS___ASSESSMENT_LID as ASSESSMENT_LID",
	"JNR_EXCEPTIONS___TEST_TAKEN_DT as TEST_TAKEN_DT",
	"JNR_EXCEPTIONS___ASSESSMENT_NAME as ASSESSMENT_NAME",
	"JNR_EXCEPTIONS___JOB_CD as JOB_CD",
	"JNR_EXCEPTIONS___LOCATION_ID as LOCATION_ID",
	"JNR_EXCEPTIONS___LAST_TEST_SCORE_NBR as LAST_TEST_SCORE_NBR",
	"JNR_EXCEPTIONS___LAST_TEST_PASSED_FLAG as LAST_TEST_PASSED_FLAG",
	"JNR_EXCEPTIONS___COMPLIANT_START_DT as COMPLIANT_START_DT",
	"JNR_EXCEPTIONS___COMPLIANT_EXPIRATION_DT as COMPLIANT_EXPIRATION_DT",
	"JNR_EXCEPTIONS___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"JNR_EXCEPTIONS___CURR_MISSING_FLAG as CURR_MISSING_FLAG",
	"JNR_EXCEPTIONS___CURR_PERIOD_ATTEMPTS_NBR as CURR_PERIOD_ATTEMPTS_NBR",
	"JNR_EXCEPTIONS___LOAD_DT as LOAD_DT",
	"JNR_EXCEPTIONS___ASSESSMENT_ID1 as ASSESSMENT_ID").selectExpr(
	# "JNR_EXCEPTIONS___sys_row_id as sys_row_id",
	"DAY_DT as DAY_DT",
	"EMPLOYEE_ID as EMPLOYEE_ID",
	"ASSESSMENT_MID as ASSESSMENT_MID",
	"ASSESSMENT_LID as ASSESSMENT_LID",
	"TEST_TAKEN_DT as TEST_TAKEN_DT",
	"ASSESSMENT_NAME as ASSESSMENT_NAME",
	"JOB_CD as JOB_CD",
	"LOCATION_ID as LOCATION_ID",
	"LAST_TEST_SCORE_NBR as LAST_TEST_SCORE_NBR",
	"LAST_TEST_PASSED_FLAG as LAST_TEST_PASSED_FLAG",
	"COMPLIANT_START_DT as COMPLIANT_START_DT",
	"COMPLIANT_EXPIRATION_DT as COMPLIANT_EXPIRATION_DT",
	"IF (ASSESSMENT_ID IS NULL, CURR_COMPLIANCE_FLAG, 1) as o_CURR_COMPLIANCE_FLAG",
	"CURR_MISSING_FLAG as CURR_MISSING_FLAG",
	"CURR_PERIOD_ATTEMPTS_NBR as CURR_PERIOD_ATTEMPTS_NBR",
	"LOAD_DT as LOAD_DT",
	"ASSESSMENT_ID as ASSESSMENT_ID"
)

# COMMAND ----------

# Processing node Shortcut_to_EDU_CERT_DAILY1, type TARGET 
# COLUMN COUNT: 16


Shortcut_to_EDU_CERT_DAILY1 = EXP_EXCEPTION_ASSESSMENT.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(EMPLOYEE_ID as bigint) as EMPLOYEE_ID",
	"CAST(ASSESSMENT_MID as bigint) as ASSESSMENT_MID",
	"CAST(ASSESSMENT_LID as bigint) as ASSESSMENT_LID",
	"CAST(TEST_TAKEN_DT AS TIMESTAMP) as TEST_TAKEN_DT",
	"CAST(ASSESSMENT_NAME AS STRING) as ASSESSMENT_NAME",
	"CAST(JOB_CD AS INT) as JOB_CD",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(LAST_TEST_SCORE_NBR as bigint) as LAST_TEST_SCORE_NBR",
	"CAST(LAST_TEST_PASSED_FLAG as tinyint) as LAST_TEST_PASSED_FLAG",
	"CAST(COMPLIANT_START_DT AS TIMESTAMP) as COMPLIANT_START_DT",
	"CAST(COMPLIANT_EXPIRATION_DT AS TIMESTAMP) as COMPLIANT_EXPIRATION_DT",
	"CAST(o_CURR_COMPLIANCE_FLAG as tinyint) as CURR_COMPLIANCE_FLAG",
	"CAST(CURR_MISSING_FLAG as tinyint) as CURR_MISSING_FLAG",
	"CAST(CURR_PERIOD_ATTEMPTS_NBR as smallint) as CURR_PERIOD_ATTEMPTS_NBR",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)

try:
    Shortcut_to_EDU_CERT_DAILY1.write.saveAsTable(f'{legacy}.EDU_CERT_DAILY', mode = 'append')
    logPrevRunDt("EDU_CERT_DAILY", "EDU_CERT_DAILY", "Completed", "N/A", f"{raw}.log_run_details")

except Exception as e:
    logPrevRunDt("EDU_CERT_DAILY", "EDU_CERT_DAILY","Failed",str(e), f"{raw}.log_run_details")
    raise e

# COMMAND ----------


