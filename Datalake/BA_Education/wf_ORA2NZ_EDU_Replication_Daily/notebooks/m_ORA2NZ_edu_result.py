# Databricks notebook source
#Code converted on 2023-10-17 09:36:56
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

# Processing node SQ_Shortcut_to_EDU_RESULT, type SOURCE 
# COLUMN COUNT: 61

SQ_Shortcut_to_EDU_RESULT = jdbcOracleConnection(f"""SELECT
RESULT_ID,
TEST_TAKEN_DT,
TEST_TAKEN_START_TSTMP,
ASSESSMENT_MID,
ASSESSMENT_LID,
LAST_MODIFIED_TSTMP,
WRITE_ANSWER_FLAG,
EMPLOYEE_ID,
MEMBER_GROUP,
PARTICIPANT_DETAILS,
HOSTNAME,
IP_ADDRESS,
SIGNATURE,
STILL_GOING_FLAG,
STATUS_ID,
SECTIONS_CNT,
MAX_SCORE_NBR,
TOTAL_SCORE_NBR,
SPECIAL_1,
SPECIAL_2,
SPECIAL_3,
SPECIAL_4,
SPECIAL_5,
SPECIAL_6,
SPECIAL_7,
SPECIAL_8,
SPECIAL_9,
SPECIAL_10,
TIME_TAKEN_NBR,
SCORE_RESULT,
SCORE_RESULT_NBR,
PASSED_FLAG,
PERCENTAGE_SCORE_NBR,
SCHEDULE_NAME,
MONITORED_FLAG,
MONITOR_NAME,
TIME_LIMIT_DISABLED_FLAG,
DISABLED_BY,
IMAGE_REF,
SCOREBAND_ID,
FIRST_NAME,
LAST_NAME,
PRIMARY_EMAIL,
RESTRICT_PART_FLAG,
RESTRICT_ADMIN_FLAG,
R_PART_FROM_DT,
R_PART_TO_DT,
R_ADMIN_FROM_DT,
R_ADMIN_TO_DT,
COURSE_NAME,
MEMBER_SUB_GROUP_1,
MEMBER_SUB_GROUP_2,
MEMBER_SUB_GROUP_3,
MEMBER_SUB_GROUP_4,
MEMBER_SUB_GROUP_5,
MEMBER_SUB_GROUP_6,
MEMBER_SUB_GROUP_7,
MEMBER_SUB_GROUP_8,
MEMBER_SUB_GROUP_9,
TEST_CENTER,
LOAD_DT
FROM BIWPADM.EDU_RESULT""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_EDU_RESULT = SQ_Shortcut_to_EDU_RESULT \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[0],'RESULT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[1],'TEST_TAKEN_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[2],'TEST_TAKEN_START_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[3],'ASSESSMENT_MID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[4],'ASSESSMENT_LID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[5],'LAST_MODIFIED_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[6],'WRITE_ANSWER_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[7],'EMPLOYEE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[8],'MEMBER_GROUP') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[9],'PARTICIPANT_DETAILS') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[10],'HOSTNAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[11],'IP_ADDRESS') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[12],'SIGNATURE') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[13],'STILL_GOING_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[14],'STATUS_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[15],'SECTIONS_CNT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[16],'MAX_SCORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[17],'TOTAL_SCORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[18],'SPECIAL_1') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[19],'SPECIAL_2') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[20],'SPECIAL_3') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[21],'SPECIAL_4') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[22],'SPECIAL_5') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[23],'SPECIAL_6') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[24],'SPECIAL_7') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[25],'SPECIAL_8') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[26],'SPECIAL_9') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[27],'SPECIAL_10') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[28],'TIME_TAKEN_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[29],'SCORE_RESULT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[30],'SCORE_RESULT_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[31],'PASSED_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[32],'PERCENTAGE_SCORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[33],'SCHEDULE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[34],'MONITORED_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[35],'MONITOR_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[36],'TIME_LIMIT_DISABLED_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[37],'DISABLED_BY') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[38],'IMAGE_REF') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[39],'SCOREBAND_ID') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[40],'FIRST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[41],'LAST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[42],'PRIMARY_EMAIL') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[43],'RESTRICT_PART_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[44],'RESTRICT_ADMIN_FLAG') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[45],'R_PART_FROM_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[46],'R_PART_TO_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[47],'R_ADMIN_FROM_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[48],'R_ADMIN_TO_DT') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[49],'COURSE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[50],'MEMBER_SUB_GROUP_1') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[51],'MEMBER_SUB_GROUP_2') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[52],'MEMBER_SUB_GROUP_3') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[53],'MEMBER_SUB_GROUP_4') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[54],'MEMBER_SUB_GROUP_5') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[55],'MEMBER_SUB_GROUP_6') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[56],'MEMBER_SUB_GROUP_7') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[57],'MEMBER_SUB_GROUP_8') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[58],'MEMBER_SUB_GROUP_9') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[59],'TEST_CENTER') \
	.withColumnRenamed(SQ_Shortcut_to_EDU_RESULT.columns[60],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_EDU_RESULT, type TARGET 
# COLUMN COUNT: 61


Shortcut_to_EDU_RESULT = SQ_Shortcut_to_EDU_RESULT.selectExpr(
	"CAST(RESULT_ID as bigint) as RESULT_ID",
	"CAST(TEST_TAKEN_DT AS TIMESTAMP) as TEST_TAKEN_DT",
	"CAST(TEST_TAKEN_START_TSTMP AS TIMESTAMP) as TEST_TAKEN_START_TSTMP",
	"CAST(ASSESSMENT_MID as bigint) as ASSESSMENT_MID",
	"CAST(ASSESSMENT_LID as bigint) as ASSESSMENT_LID",
	"CAST(LAST_MODIFIED_TSTMP AS TIMESTAMP) as LAST_MODIFIED_TSTMP",
	"CAST(WRITE_ANSWER_FLAG as tinyint) as WRITE_ANSWER_FLAG",
	"CAST(EMPLOYEE_ID as bigint) as EMPLOYEE_ID",
	"CAST(MEMBER_GROUP AS STRING) as MEMBER_GROUP",
	"CAST(PARTICIPANT_DETAILS AS STRING) as PARTICIPANT_DETAILS",
	"CAST(HOSTNAME AS STRING) as HOSTNAME",
	"CAST(IP_ADDRESS AS STRING) as IP_ADDRESS",
	"CAST(SIGNATURE AS STRING) as SIGNATURE",
	"CAST(STILL_GOING_FLAG as tinyint) as STILL_GOING_FLAG",
	"CAST(STATUS_ID AS INT) as STATUS_ID",
	"CAST(SECTIONS_CNT AS INT) as SECTIONS_CNT",
	"CAST(MAX_SCORE_NBR as bigint) as MAX_SCORE_NBR",
	"CAST(TOTAL_SCORE_NBR as bigint) as TOTAL_SCORE_NBR",
	"CAST(SPECIAL_1 AS STRING) as SPECIAL_1",
	"CAST(SPECIAL_2 AS STRING) as SPECIAL_2",
	"CAST(SPECIAL_3 AS STRING) as SPECIAL_3",
	"CAST(SPECIAL_4 AS STRING) as SPECIAL_4",
	"CAST(SPECIAL_5 AS STRING) as SPECIAL_5",
	"CAST(SPECIAL_6 AS STRING) as SPECIAL_6",
	"CAST(SPECIAL_7 AS STRING) as SPECIAL_7",
	"CAST(SPECIAL_8 AS STRING) as SPECIAL_8",
	"CAST(SPECIAL_9 AS STRING) as SPECIAL_9",
	"CAST(SPECIAL_10 AS STRING) as SPECIAL_10",
	"CAST(TIME_TAKEN_NBR as bigint) as TIME_TAKEN_NBR",
	"CAST(SCORE_RESULT AS STRING) as SCORE_RESULT",
	"CAST(SCORE_RESULT_NBR as bigint) as SCORE_RESULT_NBR",
	"CAST(PASSED_FLAG as tinyint) as PASSED_FLAG",
	"CAST(PERCENTAGE_SCORE_NBR AS INT) as PERCENTAGE_SCORE_NBR",
	"CAST(SCHEDULE_NAME AS STRING) as SCHEDULE_NAME",
	"CAST(MONITORED_FLAG as tinyint) as MONITORED_FLAG",
	"CAST(MONITOR_NAME AS STRING) as MONITOR_NAME",
	"CAST(TIME_LIMIT_DISABLED_FLAG as tinyint) as TIME_LIMIT_DISABLED_FLAG",
	"CAST(DISABLED_BY AS STRING) as DISABLED_BY",
	"CAST(IMAGE_REF AS STRING) as IMAGE_REF",
	"CAST(SCOREBAND_ID as bigint) as SCOREBAND_ID",
	"CAST(FIRST_NAME AS STRING) as FIRST_NAME",
	"CAST(LAST_NAME AS STRING) as LAST_NAME",
	"CAST(PRIMARY_EMAIL AS STRING) as PRIMARY_EMAIL",
	"CAST(RESTRICT_PART_FLAG as tinyint) as RESTRICT_PART_FLAG",
	"CAST(RESTRICT_ADMIN_FLAG as tinyint) as RESTRICT_ADMIN_FLAG",
	"CAST(R_PART_FROM_DT AS TIMESTAMP) as R_PART_FROM_DT",
	"CAST(R_PART_TO_DT AS TIMESTAMP) as R_PART_TO_DT",
	"CAST(R_ADMIN_FROM_DT AS TIMESTAMP) as R_ADMIN_FROM_DT",
	"CAST(R_ADMIN_TO_DT AS TIMESTAMP) as R_ADMIN_TO_DT",
	"CAST(COURSE_NAME AS STRING) as COURSE_NAME",
	"CAST(MEMBER_SUB_GROUP_1 AS STRING) as MEMBER_SUB_GROUP_1",
	"CAST(MEMBER_SUB_GROUP_2 AS STRING) as MEMBER_SUB_GROUP_2",
	"CAST(MEMBER_SUB_GROUP_3 AS STRING) as MEMBER_SUB_GROUP_3",
	"CAST(MEMBER_SUB_GROUP_4 AS STRING) as MEMBER_SUB_GROUP_4",
	"CAST(MEMBER_SUB_GROUP_5 AS STRING) as MEMBER_SUB_GROUP_5",
	"CAST(MEMBER_SUB_GROUP_6 AS STRING) as MEMBER_SUB_GROUP_6",
	"CAST(MEMBER_SUB_GROUP_7 AS STRING) as MEMBER_SUB_GROUP_7",
	"CAST(MEMBER_SUB_GROUP_8 AS STRING) as MEMBER_SUB_GROUP_8",
	"CAST(MEMBER_SUB_GROUP_9 AS STRING) as MEMBER_SUB_GROUP_9",
	"CAST(TEST_CENTER AS STRING) as TEST_CENTER",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
try:
    chk=DuplicateChecker()
    chk.check_for_duplicate_primary_keys(Shortcut_to_EDU_RESULT,['RESULT_ID','TEST_TAKEN_DT'])
    Shortcut_to_EDU_RESULT.write.saveAsTable(f'{legacy}.EDU_RESULT', mode = 'overwrite')
    logPrevRunDt("EDU_RESULT", "EDU_RESULT", "Completed", "N/A", f"{raw}.log_run_details")

except Exception as e:
    logPrevRunDt("EDU_RESULT", "EDU_RESULT","Failed",str(e), f"{raw}.log_run_details")
    raise e


# COMMAND ----------


