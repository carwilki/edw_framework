# Databricks notebook source
#Code converted on 2023-10-17 09:36:52
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

# Processing node SQ_Shortcut_to_EDU_CERT_SITE_DAILY, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_EDU_CERT_SITE_DAILY = jdbcOracleConnection(f"""SELECT
DAY_DT,
LOCATION_ID,
ASSESSMENT_MID,
ASSESSMENT_LID,
EMPL_CNT,
COMPLIANT_EMPL_CNT,
EMPL_ATTEMPTS_CNT,
COMPLIANT_EMPL_ATTEMPS_CNT,
LOAD_DT
FROM BIWPADM.EDU_CERT_SITE_DAILY
WHERE LOAD_DT > SYSDATE -1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EDU_CERT_SITE_DAILY = SQ_Shortcut_to_EDU_CERT_SITE_DAILY \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns[2],'ASSESSMENT_MID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns[3],'ASSESSMENT_LID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns[4],'EMPL_CNT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns[5],'COMPLIANT_EMPL_CNT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns[6],'EMPL_ATTEMPTS_CNT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns[7],'COMPLIANT_EMPL_ATTEMPS_CNT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns[8],'LOAD_DT')

# COMMAND ----------

# SQ_Shortcut_to_EDU_CERT_SITE_DAILY.show()

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
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_EDU_CERT_SITE_DAILY_temp = SQ_Shortcut_to_EDU_CERT_SITE_DAILY.toDF(*["SQ_Shortcut_to_EDU_CERT_SITE_DAILY___" + col for col in SQ_Shortcut_to_EDU_CERT_SITE_DAILY.columns])

EXP_PRE_JOIN = SQ_Shortcut_to_EDU_CERT_SITE_DAILY_temp.selectExpr(
	"SQ_Shortcut_to_EDU_CERT_SITE_DAILY___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_EDU_CERT_SITE_DAILY___DAY_DT as DAY_DT",
	"cast(SQ_Shortcut_to_EDU_CERT_SITE_DAILY___LOCATION_ID as int) as o_LOCATION_ID",
	"SQ_Shortcut_to_EDU_CERT_SITE_DAILY___ASSESSMENT_MID as ASSESSMENT_MID",
	"SQ_Shortcut_to_EDU_CERT_SITE_DAILY___ASSESSMENT_LID as ASSESSMENT_LID",
	"SQ_Shortcut_to_EDU_CERT_SITE_DAILY___EMPL_CNT as EMPL_CNT",
	"SQ_Shortcut_to_EDU_CERT_SITE_DAILY___COMPLIANT_EMPL_CNT as COMPLIANT_EMPL_CNT",
	"SQ_Shortcut_to_EDU_CERT_SITE_DAILY___EMPL_ATTEMPTS_CNT as EMPL_ATTEMPTS_CNT",
	"SQ_Shortcut_to_EDU_CERT_SITE_DAILY___COMPLIANT_EMPL_ATTEMPS_CNT as COMPLIANT_EMPL_ATTEMPS_CNT",
	"SQ_Shortcut_to_EDU_CERT_SITE_DAILY___LOAD_DT as LOAD_DT",
	"BIGINT(concat( SQ_Shortcut_to_EDU_CERT_SITE_DAILY___ASSESSMENT_MID , SQ_Shortcut_to_EDU_CERT_SITE_DAILY___ASSESSMENT_LID )) as ASSESSMENT_ID"
)

# COMMAND ----------

# Processing node JNR_STORE_ATTRIBUTES, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_USR_STORE_ATTRIBUTES_temp = SQ_Shortcut_to_USR_STORE_ATTRIBUTES.toDF(*["SQ_Shortcut_to_USR_STORE_ATTRIBUTES___" + col for col in SQ_Shortcut_to_USR_STORE_ATTRIBUTES.columns])
EXP_PRE_JOIN_temp = EXP_PRE_JOIN.toDF(*["EXP_PRE_JOIN___" + col for col in EXP_PRE_JOIN.columns])

JNR_STORE_ATTRIBUTES = SQ_Shortcut_to_USR_STORE_ATTRIBUTES_temp.join(EXP_PRE_JOIN_temp,[SQ_Shortcut_to_USR_STORE_ATTRIBUTES_temp.SQ_Shortcut_to_USR_STORE_ATTRIBUTES___LOCATION_ID == EXP_PRE_JOIN_temp.EXP_PRE_JOIN___o_LOCATION_ID],'right_outer').selectExpr(
	"EXP_PRE_JOIN___DAY_DT as DAY_DT",
	"EXP_PRE_JOIN___o_LOCATION_ID as LOCATION_ID",
	"EXP_PRE_JOIN___ASSESSMENT_MID as ASSESSMENT_MID",
	"EXP_PRE_JOIN___ASSESSMENT_LID as ASSESSMENT_LID",
	"EXP_PRE_JOIN___EMPL_CNT as EMPL_CNT",
	"EXP_PRE_JOIN___COMPLIANT_EMPL_CNT as COMPLIANT_EMPL_CNT",
	"EXP_PRE_JOIN___EMPL_ATTEMPTS_CNT as EMPL_ATTEMPTS_CNT",
	"EXP_PRE_JOIN___COMPLIANT_EMPL_ATTEMPS_CNT as COMPLIANT_EMPL_ATTEMPS_CNT",
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
	"JNR_STORE_ATTRIBUTES___LOCATION_ID as LOCATION_ID",
	"JNR_STORE_ATTRIBUTES___ASSESSMENT_MID as ASSESSMENT_MID",
	"JNR_STORE_ATTRIBUTES___ASSESSMENT_LID as ASSESSMENT_LID",
	"JNR_STORE_ATTRIBUTES___EMPL_CNT as EMPL_CNT",
	"JNR_STORE_ATTRIBUTES___COMPLIANT_EMPL_CNT as COMPLIANT_EMPL_CNT",
	"JNR_STORE_ATTRIBUTES___EMPL_ATTEMPTS_CNT as EMPL_ATTEMPTS_CNT",
	"JNR_STORE_ATTRIBUTES___COMPLIANT_EMPL_ATTEMPS_CNT as COMPLIANT_EMPL_ATTEMPS_CNT",
	"JNR_STORE_ATTRIBUTES___LOAD_DT as LOAD_DT",
	"JNR_STORE_ATTRIBUTES___ASSESSMENT_ID as ASSESSMENT_ID",
	"JNR_STORE_ATTRIBUTES___NO_FORKLIFT_FLAG as NO_FORKLIFT_FLAG",
	"SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___ASSESSMENT_ID as ASSESSMENT_ID1",
	"SQ_Shortcut_to_USR_ASSESSMENT_EXCEPTIONS___NO_FORKLIFT_FLAG as NO_FORKLIFT_FLAG1")

# COMMAND ----------

# Processing node EXP_EXCEPTION_ASSESSMENTS, type EXPRESSION 
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_EXCEPTIONS_temp = JNR_EXCEPTIONS.toDF(*["JNR_EXCEPTIONS___" + col for col in JNR_EXCEPTIONS.columns])

EXP_EXCEPTION_ASSESSMENTS = JNR_EXCEPTIONS_temp.selectExpr(
	# "JNR_EXCEPTIONS___sys_row_id as sys_row_id",
	"JNR_EXCEPTIONS___DAY_DT as DAY_DT",
	"JNR_EXCEPTIONS___LOCATION_ID as LOCATION_ID",
	"JNR_EXCEPTIONS___ASSESSMENT_MID as ASSESSMENT_MID",
	"JNR_EXCEPTIONS___ASSESSMENT_LID as ASSESSMENT_LID",
	"JNR_EXCEPTIONS___EMPL_CNT as EMPL_CNT",
	"IF (JNR_EXCEPTIONS___ASSESSMENT_ID1 IS NULL, JNR_EXCEPTIONS___COMPLIANT_EMPL_CNT, JNR_EXCEPTIONS___EMPL_CNT) as o_COMPLIANT_EMPL_CNT",
	"JNR_EXCEPTIONS___EMPL_ATTEMPTS_CNT as EMPL_ATTEMPTS_CNT",
	"JNR_EXCEPTIONS___COMPLIANT_EMPL_ATTEMPS_CNT as COMPLIANT_EMPL_ATTEMPS_CNT",
	"JNR_EXCEPTIONS___LOAD_DT as LOAD_DT"
)

# COMMAND ----------

# Processing node Shortcut_to_EDU_CERT_SITE_DAILY, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_EDU_CERT_SITE_DAILY = EXP_EXCEPTION_ASSESSMENTS.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(ASSESSMENT_MID as bigint) as ASSESSMENT_MID",
	"CAST(ASSESSMENT_LID as bigint) as ASSESSMENT_LID",
	"CAST(EMPL_CNT as bigint) as EMPL_CNT",
	"CAST(o_COMPLIANT_EMPL_CNT as bigint) as COMPLIANT_EMPL_CNT",
	"CAST(EMPL_ATTEMPTS_CNT as bigint) as EMPL_ATTEMPTS_CNT",
	"CAST(COMPLIANT_EMPL_ATTEMPS_CNT as bigint) as COMPLIANT_EMPL_ATTEMPS_CNT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
try:
    # no key
	# chk=DuplicateChecker()
    # chk.check_for_duplicate_primary_keys(Shortcut_to_EDU_CERT_SITE_DAILY,[key])
    Shortcut_to_EDU_CERT_SITE_DAILY.write.saveAsTable(f'{legacy}.EDU_CERT_SITE_DAILY', mode = 'append')
    logPrevRunDt("EDU_CERT_SITE_DAILY", "EDU_CERT_SITE_DAILY", "Completed", "N/A", f"{raw}.log_run_details")

except Exception as e:
    logPrevRunDt("EDU_CERT_SITE_DAILY", "EDU_CERT_SITE_DAILY","Failed",str(e), f"{raw}.log_run_details")
    raise e


# COMMAND ----------


