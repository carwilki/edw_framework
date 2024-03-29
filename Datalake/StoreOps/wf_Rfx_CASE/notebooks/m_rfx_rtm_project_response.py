# Databricks notebook source
# Code converted on 2023-08-25 11:50:13
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

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_RTM_UNIT_HIERARCHY, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_RFX_RTM_UNIT_HIERARCHY = spark.sql(f"""SELECT DISTINCT
RFX_RTM_UNIT_HIERARCHY.RFX_UNIT_ID,
RFX_RTM_UNIT_HIERARCHY.LOCATION_ID
FROM {legacy}.RFX_RTM_UNIT_HIERARCHY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE, type SOURCE 
# COLUMN COUNT: 18

SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE = spark.sql(f"""SELECT
RFX_RTM_PROJECT_RESPONSE.RFX_PROJECT_ID,
RFX_RTM_PROJECT_RESPONSE.LOCATION_ID,
RFX_RTM_PROJECT_RESPONSE.RFX_TASK_ID,
RFX_RTM_PROJECT_RESPONSE.RFX_QUESTION_ID,
RFX_RTM_PROJECT_RESPONSE.RFX_SUB_QUESTION_NBR,
RFX_RTM_PROJECT_RESPONSE.RFX_SURVEY_TYPE_CD,
RFX_RTM_PROJECT_RESPONSE.RFX_UNIT_ID,
RFX_RTM_PROJECT_RESPONSE.RFX_RESPONSE_TSTMP,
RFX_RTM_PROJECT_RESPONSE.RFX_QUESTION_TEXT,
RFX_RTM_PROJECT_RESPONSE.RFX_SUB_QUESTION_TEXT,
RFX_RTM_PROJECT_RESPONSE.RFX_RESPONSE_TYPE_CD,
RFX_RTM_PROJECT_RESPONSE.OPTION_SELECTED,
RFX_RTM_PROJECT_RESPONSE.OPTION_TEXT,
RFX_RTM_PROJECT_RESPONSE.NUMERIC_VALUE,
RFX_RTM_PROJECT_RESPONSE.TEXT_VALUE,
RFX_RTM_PROJECT_RESPONSE.DATE_VALUE,
RFX_RTM_PROJECT_RESPONSE.ATTACHMENT_CNT,
RFX_RTM_PROJECT_RESPONSE.LOAD_TSTMP
FROM {legacy}.RFX_RTM_PROJECT_RESPONSE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_PRJ_RESPONSE_PRE, type SOURCE 
# COLUMN COUNT: 16

SQ_Shortcut_to_RFX_PRJ_RESPONSE_PRE = spark.sql(f"""SELECT
RFX_PRJ_RESPONSE_PRE.PROJECT_ID,
RFX_PRJ_RESPONSE_PRE.TASK_ID,
RFX_PRJ_RESPONSE_PRE.SURVEY_TYPE,
RFX_PRJ_RESPONSE_PRE.UNIT_ID,
RFX_PRJ_RESPONSE_PRE.QUESTION_ID,
RFX_PRJ_RESPONSE_PRE.QUESTION_TEXT,
RFX_PRJ_RESPONSE_PRE.SUB_QUESTION_NO,
RFX_PRJ_RESPONSE_PRE.SUB_QUESTION_TEXT,
RFX_PRJ_RESPONSE_PRE.RESPONSE_TYPE,
RFX_PRJ_RESPONSE_PRE.OPTION_SELECTED,
RFX_PRJ_RESPONSE_PRE.OPTION_TEXT,
RFX_PRJ_RESPONSE_PRE.RESPONSE_DATE,
RFX_PRJ_RESPONSE_PRE.NUMERIC_VALUE,
RFX_PRJ_RESPONSE_PRE.TEXT_VALUE,
RFX_PRJ_RESPONSE_PRE.DATE_VALUE,
RFX_PRJ_RESPONSE_PRE.ATTACHMENT_COUNT
FROM {raw}.RFX_PRJ_RESPONSE_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_RFX_PROJ_RESPONSE_PRE, type JOINER 
# COLUMN COUNT: 18

JNR_RFX_PROJ_RESPONSE_PRE = SQ_Shortcut_to_RFX_RTM_UNIT_HIERARCHY.join(SQ_Shortcut_to_RFX_PRJ_RESPONSE_PRE,[SQ_Shortcut_to_RFX_RTM_UNIT_HIERARCHY.RFX_UNIT_ID == SQ_Shortcut_to_RFX_PRJ_RESPONSE_PRE.UNIT_ID],'inner')

# COMMAND ----------

# Processing node JNR_RFX_RTM_PROJECT_RESPONSE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 35

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE_temp = SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE.toDF(*["SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___" + col for col in SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE.columns])
JNR_RFX_PROJ_RESPONSE_PRE_temp = JNR_RFX_PROJ_RESPONSE_PRE.toDF(*["JNR_RFX_PROJ_RESPONSE_PRE___" + col for col in JNR_RFX_PROJ_RESPONSE_PRE.columns])

JNR_RFX_RTM_PROJECT_RESPONSE = JNR_RFX_PROJ_RESPONSE_PRE_temp.join(SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE_temp,[JNR_RFX_PROJ_RESPONSE_PRE_temp.JNR_RFX_PROJ_RESPONSE_PRE___PROJECT_ID == SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE_temp.SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_PROJECT_ID, JNR_RFX_PROJ_RESPONSE_PRE_temp.JNR_RFX_PROJ_RESPONSE_PRE___LOCATION_ID == SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE_temp.SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___LOCATION_ID, JNR_RFX_PROJ_RESPONSE_PRE_temp.JNR_RFX_PROJ_RESPONSE_PRE___TASK_ID == SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE_temp.SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_TASK_ID, JNR_RFX_PROJ_RESPONSE_PRE_temp.JNR_RFX_PROJ_RESPONSE_PRE___QUESTION_ID == SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE_temp.SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_QUESTION_ID, JNR_RFX_PROJ_RESPONSE_PRE_temp.JNR_RFX_PROJ_RESPONSE_PRE___SUB_QUESTION_NO == SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE_temp.SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_SUB_QUESTION_NBR, JNR_RFX_PROJ_RESPONSE_PRE_temp.JNR_RFX_PROJ_RESPONSE_PRE___OPTION_SELECTED == SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE_temp.SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___OPTION_SELECTED],'left_outer').selectExpr(
	"JNR_RFX_PROJ_RESPONSE_PRE___PROJECT_ID as PROJECT_ID",
	"JNR_RFX_PROJ_RESPONSE_PRE___LOCATION_ID as LOCATION_ID",
	"JNR_RFX_PROJ_RESPONSE_PRE___TASK_ID as TASK_ID",
	"JNR_RFX_PROJ_RESPONSE_PRE___SURVEY_TYPE as SURVEY_TYPE",
	"JNR_RFX_PROJ_RESPONSE_PRE___UNIT_ID as UNIT_ID",
	"JNR_RFX_PROJ_RESPONSE_PRE___QUESTION_ID as QUESTION_ID",
	"JNR_RFX_PROJ_RESPONSE_PRE___QUESTION_TEXT as QUESTION_TEXT",
	"JNR_RFX_PROJ_RESPONSE_PRE___SUB_QUESTION_NO as SUB_QUESTION_NO",
	"JNR_RFX_PROJ_RESPONSE_PRE___SUB_QUESTION_TEXT as SUB_QUESTION_TEXT",
	"JNR_RFX_PROJ_RESPONSE_PRE___RESPONSE_TYPE as RESPONSE_TYPE",
	"JNR_RFX_PROJ_RESPONSE_PRE___OPTION_SELECTED as OPTION_SELECTED",
	"JNR_RFX_PROJ_RESPONSE_PRE___OPTION_TEXT as OPTION_TEXT",
	"JNR_RFX_PROJ_RESPONSE_PRE___RESPONSE_DATE as RESPONSE_DATE",
	"JNR_RFX_PROJ_RESPONSE_PRE___NUMERIC_VALUE as NUMERIC_VALUE",
	"JNR_RFX_PROJ_RESPONSE_PRE___TEXT_VALUE as TEXT_VALUE",
	"JNR_RFX_PROJ_RESPONSE_PRE___DATE_VALUE as DATE_VALUE",
	"JNR_RFX_PROJ_RESPONSE_PRE___ATTACHMENT_COUNT as ATTACHMENT_COUNT",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_PROJECT_ID as i_RFX_PROJECT_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___LOCATION_ID as i_LOCATION_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_TASK_ID as i_RFX_TASK_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_QUESTION_ID as i_RFX_QUESTION_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_SUB_QUESTION_NBR as i_RFX_SUB_QUESTION_NBR",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_SURVEY_TYPE_CD as i_RFX_SURVEY_TYPE_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_UNIT_ID as i_RFX_UNIT_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_RESPONSE_TSTMP as i_RFX_RESPONSE_TSTMP",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_QUESTION_TEXT as i_RFX_QUESTION_TEXT",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_SUB_QUESTION_TEXT as i_RFX_SUB_QUESTION_TEXT",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___RFX_RESPONSE_TYPE_CD as i_RFX_RESPONSE_TYPE_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___OPTION_SELECTED as i_OPTION_SELECTED",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___OPTION_TEXT as i_OPTION_TEXT",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___NUMERIC_VALUE as i_NUMERIC_VALUE",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___TEXT_VALUE as i_TEXT_VALUE",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___DATE_VALUE as i_DATE_VALUE",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___ATTACHMENT_CNT as i_ATTACHMENT_CNT",
	"SQ_Shortcut_to_RFX_RTM_PROJECT_RESPONSE___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------

# Processing node FIL_RFX_RTM_PROJECT_RESPONSE, type FILTER 
# COLUMN COUNT: 35

# for each involved DataFrame, append the dataframe name to each column
JNR_RFX_RTM_PROJECT_RESPONSE_temp = JNR_RFX_RTM_PROJECT_RESPONSE.toDF(*["JNR_RFX_RTM_PROJECT_RESPONSE___" + col for col in JNR_RFX_RTM_PROJECT_RESPONSE.columns])

FIL_RFX_RTM_PROJECT_RESPONSE = JNR_RFX_RTM_PROJECT_RESPONSE_temp.selectExpr(
	"JNR_RFX_RTM_PROJECT_RESPONSE___PROJECT_ID as PROJECT_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___LOCATION_ID as LOCATION_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___TASK_ID as TASK_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___SURVEY_TYPE as SURVEY_TYPE",
	"JNR_RFX_RTM_PROJECT_RESPONSE___UNIT_ID as UNIT_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___QUESTION_ID as QUESTION_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___QUESTION_TEXT as QUESTION_TEXT",
	"JNR_RFX_RTM_PROJECT_RESPONSE___SUB_QUESTION_NO as SUB_QUESTION_NO",
	"JNR_RFX_RTM_PROJECT_RESPONSE___SUB_QUESTION_TEXT as SUB_QUESTION_TEXT",
	"JNR_RFX_RTM_PROJECT_RESPONSE___RESPONSE_TYPE as RESPONSE_TYPE",
	"JNR_RFX_RTM_PROJECT_RESPONSE___OPTION_SELECTED as OPTION_SELECTED",
	"JNR_RFX_RTM_PROJECT_RESPONSE___OPTION_TEXT as OPTION_TEXT",
	"JNR_RFX_RTM_PROJECT_RESPONSE___RESPONSE_DATE as RESPONSE_DATE",
	"JNR_RFX_RTM_PROJECT_RESPONSE___NUMERIC_VALUE as NUMERIC_VALUE",
	"JNR_RFX_RTM_PROJECT_RESPONSE___TEXT_VALUE as TEXT_VALUE",
	"JNR_RFX_RTM_PROJECT_RESPONSE___DATE_VALUE as DATE_VALUE",
	"JNR_RFX_RTM_PROJECT_RESPONSE___ATTACHMENT_COUNT as ATTACHMENT_COUNT",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_PROJECT_ID as i_RFX_PROJECT_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_LOCATION_ID as i_LOCATION_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_TASK_ID as i_RFX_TASK_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_QUESTION_ID as i_RFX_QUESTION_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_SUB_QUESTION_NBR as i_RFX_SUB_QUESTION_NBR",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_SURVEY_TYPE_CD as i_RFX_SURVEY_TYPE_CD",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_UNIT_ID as i_RFX_UNIT_ID",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_RESPONSE_TSTMP as i_RFX_RESPONSE_TSTMP",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_QUESTION_TEXT as i_RFX_QUESTION_TEXT",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_SUB_QUESTION_TEXT as i_RFX_SUB_QUESTION_TEXT",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_RFX_RESPONSE_TYPE_CD as i_RFX_RESPONSE_TYPE_CD",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_OPTION_SELECTED as i_OPTION_SELECTED",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_OPTION_TEXT as i_OPTION_TEXT",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_NUMERIC_VALUE as i_NUMERIC_VALUE",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_TEXT_VALUE as i_TEXT_VALUE",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_DATE_VALUE as i_DATE_VALUE",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_ATTACHMENT_CNT as i_ATTACHMENT_CNT",
	"JNR_RFX_RTM_PROJECT_RESPONSE___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_RFX_QUESTION_ID IS NULL OR ( i_RFX_QUESTION_ID IS NOT NULL AND ( IF (SURVEY_TYPE IS NULL, '', SURVEY_TYPE) != IF (i_RFX_SURVEY_TYPE_CD IS NULL, '', i_RFX_SURVEY_TYPE_CD) OR IF (UNIT_ID IS NULL, '', UNIT_ID) != IF (i_RFX_UNIT_ID IS NULL, '', i_RFX_UNIT_ID) OR IF (RESPONSE_DATE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), RESPONSE_DATE) != IF (i_RFX_RESPONSE_TSTMP IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_RFX_RESPONSE_TSTMP) OR IF (QUESTION_TEXT IS NULL, '', QUESTION_TEXT) != IF (i_RFX_QUESTION_TEXT IS NULL, '', i_RFX_QUESTION_TEXT) OR IF (SUB_QUESTION_TEXT IS NULL, '', SUB_QUESTION_TEXT) != IF (i_RFX_SUB_QUESTION_TEXT IS NULL, '', i_RFX_SUB_QUESTION_TEXT) OR IF (RESPONSE_TYPE IS NULL, '', RESPONSE_TYPE) != IF (i_RFX_RESPONSE_TYPE_CD IS NULL, '', i_RFX_RESPONSE_TYPE_CD) OR IF (OPTION_TEXT IS NULL, '', OPTION_TEXT) != IF (i_OPTION_TEXT IS NULL, '', i_OPTION_TEXT) OR IF (NUMERIC_VALUE IS NULL, 0, NUMERIC_VALUE) != IF (i_NUMERIC_VALUE IS NULL, 0, i_NUMERIC_VALUE) OR IF (TEXT_VALUE IS NULL, '', TEXT_VALUE) != IF (i_TEXT_VALUE IS NULL, '', i_TEXT_VALUE) OR IF (DATE_VALUE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), DATE_VALUE) != IF (i_DATE_VALUE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_DATE_VALUE) OR IF (ATTACHMENT_COUNT IS NULL, - 9999, ATTACHMENT_COUNT) != IF (i_ATTACHMENT_CNT IS NULL, - 9999, i_ATTACHMENT_CNT) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_RFX_RTM_PROJECT_RESPONSE, type EXPRESSION 
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
FIL_RFX_RTM_PROJECT_RESPONSE_temp = FIL_RFX_RTM_PROJECT_RESPONSE.toDF(*["FIL_RFX_RTM_PROJECT_RESPONSE___" + col for col in FIL_RFX_RTM_PROJECT_RESPONSE.columns])

EXP_RFX_RTM_PROJECT_RESPONSE = FIL_RFX_RTM_PROJECT_RESPONSE_temp.selectExpr(
	"FIL_RFX_RTM_PROJECT_RESPONSE___sys_row_id as sys_row_id",
	"FIL_RFX_RTM_PROJECT_RESPONSE___PROJECT_ID as PROJECT_ID",
	"FIL_RFX_RTM_PROJECT_RESPONSE___LOCATION_ID as LOCATION_ID",
	"FIL_RFX_RTM_PROJECT_RESPONSE___TASK_ID as TASK_ID",
	"FIL_RFX_RTM_PROJECT_RESPONSE___SURVEY_TYPE as SURVEY_TYPE",
	"FIL_RFX_RTM_PROJECT_RESPONSE___UNIT_ID as UNIT_ID",
	"FIL_RFX_RTM_PROJECT_RESPONSE___QUESTION_ID as QUESTION_ID",
	"FIL_RFX_RTM_PROJECT_RESPONSE___QUESTION_TEXT as QUESTION_TEXT",
	"FIL_RFX_RTM_PROJECT_RESPONSE___SUB_QUESTION_NO as SUB_QUESTION_NO",
	"FIL_RFX_RTM_PROJECT_RESPONSE___SUB_QUESTION_TEXT as SUB_QUESTION_TEXT",
	"FIL_RFX_RTM_PROJECT_RESPONSE___RESPONSE_TYPE as RESPONSE_TYPE",
	"FIL_RFX_RTM_PROJECT_RESPONSE___OPTION_SELECTED as OPTION_SELECTED",
	"FIL_RFX_RTM_PROJECT_RESPONSE___OPTION_TEXT as OPTION_TEXT",
	"FIL_RFX_RTM_PROJECT_RESPONSE___RESPONSE_DATE as RESPONSE_DATE",
	"FIL_RFX_RTM_PROJECT_RESPONSE___NUMERIC_VALUE as NUMERIC_VALUE",
	"FIL_RFX_RTM_PROJECT_RESPONSE___TEXT_VALUE as TEXT_VALUE",
	"FIL_RFX_RTM_PROJECT_RESPONSE___DATE_VALUE as DATE_VALUE",
	"FIL_RFX_RTM_PROJECT_RESPONSE___ATTACHMENT_COUNT as ATTACHMENT_COUNT",
	"FIL_RFX_RTM_PROJECT_RESPONSE___i_RFX_QUESTION_ID as i_RFX_QUESTION_ID",
	"FIL_RFX_RTM_PROJECT_RESPONSE___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"IF (FIL_RFX_RTM_PROJECT_RESPONSE___i_RFX_QUESTION_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_RFX_RTM_PROJECT_RESPONSE___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_RFX_RTM_PROJECT_RESPONSE___i_LOAD_TSTMP) as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node UPD_RFX_RTM_PROJECT_RESPONSE, type UPDATE_STRATEGY 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
EXP_RFX_RTM_PROJECT_RESPONSE_temp = EXP_RFX_RTM_PROJECT_RESPONSE.toDF(*["EXP_RFX_RTM_PROJECT_RESPONSE___" + col for col in EXP_RFX_RTM_PROJECT_RESPONSE.columns])

UPD_RFX_RTM_PROJECT_RESPONSE = EXP_RFX_RTM_PROJECT_RESPONSE_temp.selectExpr(
	"EXP_RFX_RTM_PROJECT_RESPONSE___PROJECT_ID as PROJECT_ID",
	"EXP_RFX_RTM_PROJECT_RESPONSE___LOCATION_ID as LOCATION_ID",
	"EXP_RFX_RTM_PROJECT_RESPONSE___TASK_ID as TASK_ID",
	"EXP_RFX_RTM_PROJECT_RESPONSE___SURVEY_TYPE as SURVEY_TYPE",
	"EXP_RFX_RTM_PROJECT_RESPONSE___UNIT_ID as UNIT_ID",
	"EXP_RFX_RTM_PROJECT_RESPONSE___QUESTION_ID as QUESTION_ID",
	"EXP_RFX_RTM_PROJECT_RESPONSE___QUESTION_TEXT as QUESTION_TEXT",
	"EXP_RFX_RTM_PROJECT_RESPONSE___SUB_QUESTION_NO as SUB_QUESTION_NO",
	"EXP_RFX_RTM_PROJECT_RESPONSE___SUB_QUESTION_TEXT as SUB_QUESTION_TEXT",
	"EXP_RFX_RTM_PROJECT_RESPONSE___RESPONSE_TYPE as RESPONSE_TYPE",
	"EXP_RFX_RTM_PROJECT_RESPONSE___OPTION_SELECTED as OPTION_SELECTED",
	"EXP_RFX_RTM_PROJECT_RESPONSE___OPTION_TEXT as OPTION_TEXT",
	"EXP_RFX_RTM_PROJECT_RESPONSE___RESPONSE_DATE as RESPONSE_DATE",
	"EXP_RFX_RTM_PROJECT_RESPONSE___NUMERIC_VALUE as NUMERIC_VALUE",
	"EXP_RFX_RTM_PROJECT_RESPONSE___TEXT_VALUE as TEXT_VALUE",
	"EXP_RFX_RTM_PROJECT_RESPONSE___DATE_VALUE as DATE_VALUE",
	"EXP_RFX_RTM_PROJECT_RESPONSE___ATTACHMENT_COUNT as ATTACHMENT_COUNT",
	"EXP_RFX_RTM_PROJECT_RESPONSE___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR",
	"EXP_RFX_RTM_PROJECT_RESPONSE___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_RFX_RTM_PROJECT_RESPONSE___LOAD_TSTMP as LOAD_TSTMP",
	"if(EXP_RFX_RTM_PROJECT_RESPONSE___o_UPDATE_VALIDATOR==1,0,1) as pyspark_data_action")

# COMMAND ----------

# Processing node Shortcut_to_RFX_RTM_PROJECT_RESPONSE1, type TARGET 
# COLUMN COUNT: 19


Shortcut_to_RFX_RTM_PROJECT_RESPONSE1 = UPD_RFX_RTM_PROJECT_RESPONSE.selectExpr(
	"CAST(PROJECT_ID AS STRING) as RFX_PROJECT_ID",
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(TASK_ID AS STRING) as RFX_TASK_ID",
	"CAST(QUESTION_ID AS STRING) as RFX_QUESTION_ID",
	"CAST(SUB_QUESTION_NO AS BIGINT) as RFX_SUB_QUESTION_NBR",
	"CAST(SURVEY_TYPE AS STRING) as RFX_SURVEY_TYPE_CD",
	"CAST(UNIT_ID AS STRING) as RFX_UNIT_ID",
	"CAST(RESPONSE_DATE AS TIMESTAMP) as RFX_RESPONSE_TSTMP",
	"CAST(QUESTION_TEXT AS STRING) as RFX_QUESTION_TEXT",
	"CAST(SUB_QUESTION_TEXT AS STRING) as RFX_SUB_QUESTION_TEXT",
	"CAST(RESPONSE_TYPE AS STRING) as RFX_RESPONSE_TYPE_CD",
	"OPTION_SELECTED as OPTION_SELECTED",
	"CAST(OPTION_TEXT AS STRING) as OPTION_TEXT",
	"CAST(NUMERIC_VALUE AS DECIMAL(22,4)) as NUMERIC_VALUE",
	"CAST(TEXT_VALUE AS STRING) as TEXT_VALUE",
	"CAST(DATE_VALUE AS TIMESTAMP) as DATE_VALUE",
	"CAST(ATTACHMENT_COUNT AS BIGINT) as ATTACHMENT_CNT",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.RFX_PROJECT_ID = target.RFX_PROJECT_ID AND source.LOCATION_ID = target.LOCATION_ID AND source.RFX_TASK_ID = target.RFX_TASK_ID AND source.RFX_QUESTION_ID = target.RFX_QUESTION_ID AND source.RFX_SUB_QUESTION_NBR = target.RFX_SUB_QUESTION_NBR AND source.OPTION_SELECTED = target.OPTION_SELECTED"""
	refined_perf_table = f"{legacy}.RFX_RTM_PROJECT_RESPONSE"
	executeMerge(Shortcut_to_RFX_RTM_PROJECT_RESPONSE1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("RFX_RTM_PROJECT_RESPONSE", "RFX_RTM_PROJECT_RESPONSE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("RFX_RTM_PROJECT_RESPONSE", "RFX_RTM_PROJECT_RESPONSE","Failed",str(e), f"{raw}.log_run_details")
	raise e
		
