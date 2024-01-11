# Databricks notebook source
#Code converted on 2023-10-17 09:36:48
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

# print(username, password, connection_string)

# COMMAND ----------

# Processing node SQ_Shortcut_to_EDU_ASSESSMENTS, type SOURCE 
# COLUMN COUNT: 18

SQ_Shortcut_to_EDU_ASSESSMENTS = jdbcOracleConnection(f"""SELECT
ASSESSMENT_MID,
ASSESSMENT_LID,
REVISION_NBR,
ASSESSMENT_NAME,
ASSESSMENT_AUTHOR,
MODIFY_TSTMP,
TIME_LIMIT_FLAG,
TIME_LIMIT_NBR,
SECTIONS_CNT,
LAST_UPDATE_TSTMP,
ASSESSMENT_TYPE_ID,
COURSE_NAME,
ASSESSMENT_DESC,
PETSHOTEL_ASSESSMENT_FLAG,
SALON_ASSESSMENT_FLAG,
LAST_UPDT_USER,
LAST_UPDT_TSTMP,
LOAD_DT
FROM BIWPADM.EDU_ASSESSMENTS""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EDU_ASSESSMENTS = SQ_Shortcut_to_EDU_ASSESSMENTS \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[0],'ASSESSMENT_MID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[1],'ASSESSMENT_LID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[2],'REVISION_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[3],'ASSESSMENT_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[4],'ASSESSMENT_AUTHOR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[5],'MODIFY_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[6],'TIME_LIMIT_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[7],'TIME_LIMIT_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[8],'SECTIONS_CNT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[9],'LAST_UPDATE_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[10],'ASSESSMENT_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[11],'COURSE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[12],'ASSESSMENT_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[13],'PETSHOTEL_ASSESSMENT_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[14],'SALON_ASSESSMENT_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[15],'LAST_UPDT_USER') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[16],'LAST_UPDT_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_ASSESSMENTS.columns[17],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_EDU_ASSESSMENTS1, type TARGET 
# COLUMN COUNT: 18


Shortcut_to_EDU_ASSESSMENTS1 = SQ_Shortcut_to_EDU_ASSESSMENTS.selectExpr(
	"CAST(ASSESSMENT_MID as bigint) as ASSESSMENT_MID",
	"CAST(ASSESSMENT_LID as bigint) as ASSESSMENT_LID",
	"CAST(REVISION_NBR AS INT) as REVISION_NBR",
	"CAST(ASSESSMENT_NAME AS STRING) as ASSESSMENT_NAME",
	"CAST(ASSESSMENT_AUTHOR AS STRING) as ASSESSMENT_AUTHOR",
	"CAST(MODIFY_TSTMP AS TIMESTAMP) as MODIFY_TSTMP",
	"CAST(TIME_LIMIT_FLAG as tinyint) as TIME_LIMIT_FLAG",
	"CAST(TIME_LIMIT_NBR AS INT) as TIME_LIMIT_NBR",
	"CAST(SECTIONS_CNT AS INT) as SECTIONS_CNT",
	"CAST(LAST_UPDATE_TSTMP AS TIMESTAMP) as LAST_UPDATE_TSTMP",
	"CAST(ASSESSMENT_TYPE_ID AS INT) as ASSESSMENT_TYPE_ID",
	"CAST(COURSE_NAME AS STRING) as COURSE_NAME",
	"CAST(ASSESSMENT_DESC AS STRING) as ASSESSMENT_DESC",
	"CAST(PETSHOTEL_ASSESSMENT_FLAG as tinyint) as PETSHOTEL_ASSESSMENT_FLAG",
	"CAST(SALON_ASSESSMENT_FLAG as tinyint) as SALON_ASSESSMENT_FLAG",
	"CAST(LAST_UPDT_USER AS STRING) as LAST_UPDT_USER",
	"CAST(LAST_UPDT_TSTMP AS TIMESTAMP) as LAST_UPDT_TSTMP",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
try:
    chk=DuplicateChecker()
    chk.check_for_duplicate_primary_keys(Shortcut_to_EDU_ASSESSMENTS1,['ASSESSMENT_MID','ASSESSMENT_LID'])
    Shortcut_to_EDU_ASSESSMENTS1.write.saveAsTable(f'{legacy}.EDU_ASSESSMENTS', mode = 'overwrite')
    logPrevRunDt("EDU_ASSESSMENTS", "EDU_ASSESSMENTS", "Completed", "N/A", f"{raw}.log_run_details")

except Exception as e:
    logPrevRunDt("EDU_ASSESSMENTS", "EDU_ASSESSMENTS","Failed",str(e), f"{raw}.log_run_details", )
    raise e

# COMMAND ----------


