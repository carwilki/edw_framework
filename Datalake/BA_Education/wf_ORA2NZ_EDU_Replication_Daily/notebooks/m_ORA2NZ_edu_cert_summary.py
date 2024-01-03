# Databricks notebook source
#Code converted on 2023-10-17 09:36:54
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

# Processing node SQ_Shortcut_to_EDU_CERT_SUMMARY, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_EDU_CERT_SUMMARY = jdbcOracleConnection(f"""SELECT
DAY_DT,
EMPLOYEE_ID,
MISSED_ASSESS_MID,
MISSED_ASSESS_LID,
MISSED_ASSESS_NAME,
JOB_CD,
LOCATION_ID,
CURR_COMPLIANCE_FLAG,
LOAD_DT
FROM BIWPADM.EDU_CERT_SUMMARY
WHERE LOAD_DT > SYSDATE -1 """,username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EDU_CERT_SUMMARY = SQ_Shortcut_to_EDU_CERT_SUMMARY \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[1],'EMPLOYEE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[2],'MISSED_ASSESS_MID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[3],'MISSED_ASSESS_LID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[4],'MISSED_ASSESS_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[5],'JOB_CD') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[6],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[7],'CURR_COMPLIANCE_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SUMMARY.columns[8],'LOAD_DT')

# COMMAND ----------

# Processing node EXP_PRE_JOIN, type EXPRESSION 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_EDU_CERT_SUMMARY_temp = SQ_Shortcut_to_EDU_CERT_SUMMARY.toDF(*["SQ_Shortcut_to_EDU_CERT_SUMMARY___" + col for col in SQ_Shortcut_to_EDU_CERT_SUMMARY.columns])

EXP_PRE_JOIN = SQ_Shortcut_to_EDU_CERT_SUMMARY_temp.selectExpr(
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___EMPLOYEE_ID as EMPLOYEE_ID",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___JOB_CD as JOB_CD",
	"cast(SQ_Shortcut_to_EDU_CERT_SUMMARY___LOCATION_ID as int) as o_LOCATION_ID",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"SQ_Shortcut_to_EDU_CERT_SUMMARY___LOAD_DT as LOAD_DT",
	"BIGINT(concat( SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_MID , SQ_Shortcut_to_EDU_CERT_SUMMARY___MISSED_ASSESS_LID )) as ASSESSMENT_ID"
)

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

# Processing node JNR_STORE_ATTRIBUTES, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_USR_STORE_ATTRIBUTES_temp = SQ_Shortcut_to_USR_STORE_ATTRIBUTES.toDF(*["SQ_Shortcut_to_USR_STORE_ATTRIBUTES___" + col for col in SQ_Shortcut_to_USR_STORE_ATTRIBUTES.columns])
EXP_PRE_JOIN_temp = EXP_PRE_JOIN.toDF(*["EXP_PRE_JOIN___" + col for col in EXP_PRE_JOIN.columns])

JNR_STORE_ATTRIBUTES = SQ_Shortcut_to_USR_STORE_ATTRIBUTES_temp.join(EXP_PRE_JOIN_temp,[SQ_Shortcut_to_USR_STORE_ATTRIBUTES_temp.SQ_Shortcut_to_USR_STORE_ATTRIBUTES___LOCATION_ID == EXP_PRE_JOIN_temp.EXP_PRE_JOIN___o_LOCATION_ID],'right_outer').selectExpr(
	"EXP_PRE_JOIN___DAY_DT as DAY_DT",
	"EXP_PRE_JOIN___EMPLOYEE_ID as EMPLOYEE_ID",
	"EXP_PRE_JOIN___MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"EXP_PRE_JOIN___MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"EXP_PRE_JOIN___MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"EXP_PRE_JOIN___JOB_CD as JOB_CD",
	"EXP_PRE_JOIN___o_LOCATION_ID as LOCATION_ID",
	"EXP_PRE_JOIN___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"EXP_PRE_JOIN___LOAD_DT as LOAD_DT",
	"EXP_PRE_JOIN___ASSESSMENT_ID as ASSESSMENT_ID",
	"SQ_Shortcut_to_USR_STORE_ATTRIBUTES___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_to_USR_STORE_ATTRIBUTES___NO_FORKLIFT_FLAG as NO_FORKLIFT_FLAG")

# COMMAND ----------

# Processing node JNR_EXCEPTIONS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS_temp = SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS.toDF(*["SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___" + col for col in SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS.columns])
JNR_STORE_ATTRIBUTES_temp = JNR_STORE_ATTRIBUTES.toDF(*["JNR_STORE_ATTRIBUTES___" + col for col in JNR_STORE_ATTRIBUTES.columns])

JNR_EXCEPTIONS = SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS_temp.join(JNR_STORE_ATTRIBUTES_temp,[SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS_temp.SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___ASSESSMENT_ID == JNR_STORE_ATTRIBUTES_temp.JNR_STORE_ATTRIBUTES___ASSESSMENT_ID, SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS_temp.SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___NO_FORKLIFT_FLAG == JNR_STORE_ATTRIBUTES_temp.JNR_STORE_ATTRIBUTES___NO_FORKLIFT_FLAG],'right_outer').selectExpr(
	"JNR_STORE_ATTRIBUTES___DAY_DT as DAY_DT",
	"JNR_STORE_ATTRIBUTES___EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_STORE_ATTRIBUTES___MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"JNR_STORE_ATTRIBUTES___MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"JNR_STORE_ATTRIBUTES___MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"JNR_STORE_ATTRIBUTES___JOB_CD as JOB_CD",
	"JNR_STORE_ATTRIBUTES___LOCATION_ID as LOCATION_ID",
	"JNR_STORE_ATTRIBUTES___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"JNR_STORE_ATTRIBUTES___LOAD_DT as LOAD_DT",
	"JNR_STORE_ATTRIBUTES___ASSESSMENT_ID as ASSESSMENT_ID",
	"JNR_STORE_ATTRIBUTES___NO_FORKLIFT_FLAG as NO_FORKLIFT_FLAG",
	"SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___ASSESSMENT_ID as ASSESSMENT_ID1",
	"SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___NO_FORKLIFT_FLAG as NO_FORKLIFT_FLAG1")

# COMMAND ----------

# Processing node EXP_EXCEPTION_ASSESSMENT, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_EXCEPTIONS_temp = JNR_EXCEPTIONS.toDF(*["JNR_EXCEPTIONS___" + col for col in JNR_EXCEPTIONS.columns])

EXP_EXCEPTION_ASSESSMENT = JNR_EXCEPTIONS_temp.selectExpr(
	"JNR_EXCEPTIONS___DAY_DT as DAY_DT",
	"JNR_EXCEPTIONS___EMPLOYEE_ID as EMPLOYEE_ID",
	"JNR_EXCEPTIONS___MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"JNR_EXCEPTIONS___MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"JNR_EXCEPTIONS___MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"JNR_EXCEPTIONS___JOB_CD as JOB_CD",
	"JNR_EXCEPTIONS___LOCATION_ID as LOCATION_ID",
	"JNR_EXCEPTIONS___CURR_COMPLIANCE_FLAG as CURR_COMPLIANCE_FLAG",
	"JNR_EXCEPTIONS___LOAD_DT as LOAD_DT",
	"JNR_EXCEPTIONS___ASSESSMENT_ID1 as ASSESSMENT_ID").selectExpr(
	# "JNR_EXCEPTIONS___sys_row_id as sys_row_id",
	"DAY_DT as DAY_DT",
	"EMPLOYEE_ID as EMPLOYEE_ID",
	"MISSED_ASSESS_MID as MISSED_ASSESS_MID",
	"MISSED_ASSESS_LID as MISSED_ASSESS_LID",
	"MISSED_ASSESS_NAME as MISSED_ASSESS_NAME",
	"JOB_CD as JOB_CD",
	"LOCATION_ID as LOCATION_ID",
	"IF (ASSESSMENT_ID IS NULL, CURR_COMPLIANCE_FLAG, 1) as o_CURR_COMPLIANCE_FLAG",
	"LOAD_DT as LOAD_DT"
)

# COMMAND ----------

# Processing node Shortcut_to_EDU_CERT_SUMMARY1, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_EDU_CERT_SUMMARY1 = EXP_EXCEPTION_ASSESSMENT.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(EMPLOYEE_ID as bigint) as EMPLOYEE_ID",
	"CAST(MISSED_ASSESS_MID as bigint) as MISSED_ASSESS_MID",
	"CAST(MISSED_ASSESS_LID as bigint) as MISSED_ASSESS_LID",
	"CAST(MISSED_ASSESS_NAME AS STRING) as MISSED_ASSESS_NAME",
	"CAST(JOB_CD AS INT) as JOB_CD",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(o_CURR_COMPLIANCE_FLAG as tinyint) as CURR_COMPLIANCE_FLAG",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
try:
    chk=DuplicateChecker()
    chk.check_for_duplicate_primary_keys(Shortcut_to_EDU_CERT_SUMMARY1,['DAY_DT','EMPLOYEE_ID','MISSED_ASSESS_MID','MISSED_ASSESS_LID'])
    Shortcut_to_EDU_CERT_SUMMARY1.write.saveAsTable(f'{legacy}.EDU_CERT_SUMMARY', mode = 'append')
except Exception as e:
    raise e

# COMMAND ----------


