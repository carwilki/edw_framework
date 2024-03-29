# Databricks notebook source
# Code converted on 2023-08-25 11:50:54
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

# Processing node SQ_Shortcut_to_RFX_PROJECT_PRE, type SOURCE 
# COLUMN COUNT: 35

SQ_Shortcut_to_RFX_PROJECT_PRE = spark.sql(f"""SELECT
RFX_PROJECT_PRE.PROJECT_ID,
RFX_PROJECT_PRE.UNIT_ID,
RFX_PROJECT_PRE.PROJECT_TYPE,
RFX_PROJECT_PRE.PROJECT_TITLE,
RFX_PROJECT_PRE.PROJECT_PRIORITY_CODE,
RFX_PROJECT_PRE.CREATOR_USER_ID,
RFX_PROJECT_PRE.CREATOR,
RFX_PROJECT_PRE.EXECUTION_ASSIGNED_ROLE,
RFX_PROJECT_PRE.EXECUTION_ASSIGNED_DEPT,
RFX_PROJECT_PRE.EXECUTION_ASSIGNED_USER_ID,
RFX_PROJECT_PRE.EXECUTION_ASSIGNED_USER,
RFX_PROJECT_PRE.PROJECT_EXECUTION_START_DATE,
RFX_PROJECT_PRE.PROJECT_EXECUTION_END_DATE,
RFX_PROJECT_PRE.STORE_PROJECT_STATUS,
RFX_PROJECT_PRE.ESTIMATED_EFFORT_IN_HOURS,
RFX_PROJECT_PRE.PROJECT_EXECUTION_COMPLETION_DATE,
RFX_PROJECT_PRE.ON_TIME_COMPLETION_FLAG,
RFX_PROJECT_PRE.PROJECT_STATUS,
RFX_PROJECT_PRE.PROJECT_LAUNCH_DATE,
RFX_PROJECT_PRE.PROJECT_START_DATE,
RFX_PROJECT_PRE.PROJECT_END_DATE,
RFX_PROJECT_PRE.PROJECT_CREATION_DATE,
RFX_PROJECT_PRE.CREATOR_UNIT,
RFX_PROJECT_PRE.CREATOR_DEPT,
RFX_PROJECT_PRE.UNIT_ORG_LEVEL,
RFX_PROJECT_PRE.CONFIDENTIAL_FLAG,
RFX_PROJECT_PRE.TASK_COUNT,
RFX_PROJECT_PRE.PROJECT_COMPLETION_DATE,
RFX_PROJECT_PRE.RELEASE_USER_ID,
RFX_PROJECT_PRE.RELEASE_USER,
RFX_PROJECT_PRE.WORKLOAD_FLAG,
RFX_PROJECT_PRE.PROJECT_ASSIGNED_ROLE,
RFX_PROJECT_PRE.PROJECT_ASSIGNED_DEPT,
RFX_PROJECT_PRE.LAST_UPDATED_USER_ID,
RFX_PROJECT_PRE.LAST_UPDATED_USER
FROM {raw}.RFX_PROJECT_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_RTM_UNIT_HIERARCHY, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_RFX_RTM_UNIT_HIERARCHY = spark.sql(f"""SELECT DISTINCT
RFX_RTM_UNIT_HIERARCHY.RFX_UNIT_ID,
RFX_RTM_UNIT_HIERARCHY.LOCATION_ID
FROM {legacy}.RFX_RTM_UNIT_HIERARCHY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_RFX_PROJECT_PRE, type JOINER 
# COLUMN COUNT: 37

JNR_RFX_PROJECT_PRE = SQ_Shortcut_to_RFX_RTM_UNIT_HIERARCHY.join(SQ_Shortcut_to_RFX_PROJECT_PRE,[SQ_Shortcut_to_RFX_RTM_UNIT_HIERARCHY.RFX_UNIT_ID == SQ_Shortcut_to_RFX_PROJECT_PRE.UNIT_ID],'inner')

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_RTM_PROJECT, type SOURCE 
# COLUMN COUNT: 37

SQ_Shortcut_to_RFX_RTM_PROJECT = spark.sql(f"""SELECT
RFX_RTM_PROJECT.RFX_PROJECT_ID,
RFX_RTM_PROJECT.LOCATION_ID,
RFX_RTM_PROJECT.RFX_UNIT_ID,
RFX_RTM_PROJECT.RFX_PROJECT_TYPE_CD,
RFX_RTM_PROJECT.RFX_PROJECT_TITLE,
RFX_RTM_PROJECT.RFX_PROJECT_PRIORITY_CD,
RFX_RTM_PROJECT.RFX_EXECUTION_ASSIGNED_ROLE_CD,
RFX_RTM_PROJECT.RFX_EXECUTION_ASSIGNED_DEPT_CD,
RFX_RTM_PROJECT.RFX_EXECUTION_ASSIGNED_USER_ID,
RFX_RTM_PROJECT.RFX_EXECUTION_ASSIGNED_USER_NAME,
RFX_RTM_PROJECT.RFX_PROJECT_EXECUTION_START_TSTMP,
RFX_RTM_PROJECT.RFX_PROJECT_EXECUTION_END_TSTMP,
RFX_RTM_PROJECT.RFX_PROJECT_EXECUTION_COMPLETION_TSTMP,
RFX_RTM_PROJECT.EST_EFFORT_IN_HOURS,
RFX_RTM_PROJECT.RFX_PROJECT_EXECUTION_STATUS_CD,
RFX_RTM_PROJECT.ON_TIME_COMPLETION_FLAG,
RFX_RTM_PROJECT.RFX_CREATOR_USER_ID,
RFX_RTM_PROJECT.RFX_CREATOR_NAME,
RFX_RTM_PROJECT.RFX_CREATOR_UNIT,
RFX_RTM_PROJECT.RFX_CREATOR_DEPT_CD,
RFX_RTM_PROJECT.RFX_RELEASE_USER_ID,
RFX_RTM_PROJECT.RFX_RELEASE_USER_NAME,
RFX_RTM_PROJECT.RFX_PROJECT_ASSIGNED_ROLE_CD,
RFX_RTM_PROJECT.RFX_PROJECT_ASSIGNED_DEPT_CD,
RFX_RTM_PROJECT.RFX_UNIT_ORG_LEVEL,
RFX_RTM_PROJECT.RFX_TASK_CNT,
RFX_RTM_PROJECT.WORKLOAD_FLAG,
RFX_RTM_PROJECT.CONFIDENTIAL_FLAG,
RFX_RTM_PROJECT.RFX_PROJECT_CREATION_TSTMP,
RFX_RTM_PROJECT.RFX_PROJECT_LAUNCH_TSTMP,
RFX_RTM_PROJECT.RFX_PROJECT_START_TSTMP,
RFX_RTM_PROJECT.RFX_PROJECT_END_TSTMP,
RFX_RTM_PROJECT.RFX_PROJECT_COMPLETION_TSTMP,
RFX_RTM_PROJECT.RFX_PROJECT_STATUS_CD,
RFX_RTM_PROJECT.RFX_LAST_UPDATED_USER_ID,
RFX_RTM_PROJECT.RFX_LAST_UPDATED_USER_NAME,
RFX_RTM_PROJECT.LOAD_TSTMP
FROM {legacy}.RFX_RTM_PROJECT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_RFX_RTM_PROJECT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 73

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_RTM_PROJECT_temp = SQ_Shortcut_to_RFX_RTM_PROJECT.toDF(*["SQ_Shortcut_to_RFX_RTM_PROJECT___" + col for col in SQ_Shortcut_to_RFX_RTM_PROJECT.columns])
JNR_RFX_PROJECT_PRE_temp = JNR_RFX_PROJECT_PRE.toDF(*["JNR_RFX_PROJECT_PRE___" + col for col in JNR_RFX_PROJECT_PRE.columns])

JNR_RFX_RTM_PROJECT = JNR_RFX_PROJECT_PRE_temp.join(SQ_Shortcut_to_RFX_RTM_PROJECT_temp,[JNR_RFX_PROJECT_PRE_temp.JNR_RFX_PROJECT_PRE___PROJECT_ID == SQ_Shortcut_to_RFX_RTM_PROJECT_temp.SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_ID, JNR_RFX_PROJECT_PRE_temp.JNR_RFX_PROJECT_PRE___LOCATION_ID == SQ_Shortcut_to_RFX_RTM_PROJECT_temp.SQ_Shortcut_to_RFX_RTM_PROJECT___LOCATION_ID],'left_outer').selectExpr(
	"JNR_RFX_PROJECT_PRE___PROJECT_ID as PROJECT_ID",
	"JNR_RFX_PROJECT_PRE___LOCATION_ID as LOCATION_ID",
	"JNR_RFX_PROJECT_PRE___UNIT_ID as UNIT_ID",
	"JNR_RFX_PROJECT_PRE___PROJECT_TYPE as PROJECT_TYPE",
	"JNR_RFX_PROJECT_PRE___PROJECT_TITLE as PROJECT_TITLE",
	"JNR_RFX_PROJECT_PRE___PROJECT_PRIORITY_CODE as PROJECT_PRIORITY_CODE",
	"JNR_RFX_PROJECT_PRE___CREATOR_USER_ID as CREATOR_USER_ID",
	"JNR_RFX_PROJECT_PRE___CREATOR as CREATOR",
	"JNR_RFX_PROJECT_PRE___EXECUTION_ASSIGNED_ROLE as EXECUTION_ASSIGNED_ROLE",
	"JNR_RFX_PROJECT_PRE___EXECUTION_ASSIGNED_DEPT as EXECUTION_ASSIGNED_DEPT",
	"JNR_RFX_PROJECT_PRE___EXECUTION_ASSIGNED_USER_ID as EXECUTION_ASSIGNED_USER_ID",
	"JNR_RFX_PROJECT_PRE___EXECUTION_ASSIGNED_USER as EXECUTION_ASSIGNED_USER",
	"JNR_RFX_PROJECT_PRE___PROJECT_EXECUTION_START_DATE as PROJECT_EXECUTION_START_DATE",
	"JNR_RFX_PROJECT_PRE___PROJECT_EXECUTION_END_DATE as PROJECT_EXECUTION_END_DATE",
	"JNR_RFX_PROJECT_PRE___STORE_PROJECT_STATUS as STORE_PROJECT_STATUS",
	"JNR_RFX_PROJECT_PRE___ESTIMATED_EFFORT_IN_HOURS as ESTIMATED_EFFORT_IN_HOURS",
	"JNR_RFX_PROJECT_PRE___PROJECT_EXECUTION_COMPLETION_DATE as PROJECT_EXECUTION_COMPLETION_DATE",
	"JNR_RFX_PROJECT_PRE___ON_TIME_COMPLETION_FLAG as ON_TIME_COMPLETION_FLAG",
	"JNR_RFX_PROJECT_PRE___PROJECT_STATUS as PROJECT_STATUS",
	"JNR_RFX_PROJECT_PRE___PROJECT_LAUNCH_DATE as PROJECT_LAUNCH_DATE",
	"JNR_RFX_PROJECT_PRE___PROJECT_START_DATE as PROJECT_START_DATE",
	"JNR_RFX_PROJECT_PRE___PROJECT_END_DATE as PROJECT_END_DATE",
	"JNR_RFX_PROJECT_PRE___PROJECT_CREATION_DATE as PROJECT_CREATION_DATE",
	"JNR_RFX_PROJECT_PRE___CREATOR_UNIT as CREATOR_UNIT",
	"JNR_RFX_PROJECT_PRE___CREATOR_DEPT as CREATOR_DEPT",
	"JNR_RFX_PROJECT_PRE___UNIT_ORG_LEVEL as UNIT_ORG_LEVEL",
	"JNR_RFX_PROJECT_PRE___CONFIDENTIAL_FLAG as CONFIDENTIAL_FLAG",
	"JNR_RFX_PROJECT_PRE___TASK_COUNT as TASK_COUNT",
	"JNR_RFX_PROJECT_PRE___PROJECT_COMPLETION_DATE as PROJECT_COMPLETION_DATE",
	"JNR_RFX_PROJECT_PRE___RELEASE_USER_ID as RELEASE_USER_ID",
	"JNR_RFX_PROJECT_PRE___RELEASE_USER as RELEASE_USER",
	"JNR_RFX_PROJECT_PRE___WORKLOAD_FLAG as WORKLOAD_FLAG",
	"JNR_RFX_PROJECT_PRE___PROJECT_ASSIGNED_ROLE as PROJECT_ASSIGNED_ROLE",
	"JNR_RFX_PROJECT_PRE___PROJECT_ASSIGNED_DEPT as PROJECT_ASSIGNED_DEPT",
	"JNR_RFX_PROJECT_PRE___LAST_UPDATED_USER_ID as LAST_UPDATED_USER_ID",
	"JNR_RFX_PROJECT_PRE___LAST_UPDATED_USER as LAST_UPDATED_USER",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_ID as i_RFX_PROJECT_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___LOCATION_ID as i_LOCATION_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_UNIT_ID as i_RFX_UNIT_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_TYPE_CD as i_RFX_PROJECT_TYPE_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_TITLE as i_RFX_PROJECT_TITLE",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_PRIORITY_CD as i_RFX_PROJECT_PRIORITY_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_EXECUTION_ASSIGNED_ROLE_CD as i_RFX_EXECUTION_ASSIGNED_ROLE_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_EXECUTION_ASSIGNED_DEPT_CD as i_RFX_EXECUTION_ASSIGNED_DEPT_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_EXECUTION_ASSIGNED_USER_ID as i_RFX_EXECUTION_ASSIGNED_USER_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_EXECUTION_ASSIGNED_USER_NAME as i_RFX_EXECUTION_ASSIGNED_USER_NAME",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_EXECUTION_START_TSTMP as i_RFX_PROJECT_EXECUTION_START_TSTMP",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_EXECUTION_END_TSTMP as i_RFX_PROJECT_EXECUTION_END_TSTMP",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_EXECUTION_COMPLETION_TSTMP as i_RFX_PROJECT_EXECUTION_COMPLETION_TSTMP",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___EST_EFFORT_IN_HOURS as i_EST_EFFORT_IN_HOURS",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_EXECUTION_STATUS_CD as i_RFX_PROJECT_EXECUTION_STATUS_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___ON_TIME_COMPLETION_FLAG as i_ON_TIME_COMPLETION_FLAG1",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_CREATOR_USER_ID as i_RFX_CREATOR_USER_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_CREATOR_NAME as i_RFX_CREATOR_NAME",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_CREATOR_UNIT as i_RFX_CREATOR_UNIT",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_CREATOR_DEPT_CD as i_RFX_CREATOR_DEPT_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_RELEASE_USER_ID as i_RFX_RELEASE_USER_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_RELEASE_USER_NAME as i_RFX_RELEASE_USER_NAME",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_ASSIGNED_ROLE_CD as i_RFX_PROJECT_ASSIGNED_ROLE_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_ASSIGNED_DEPT_CD as i_RFX_PROJECT_ASSIGNED_DEPT_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_UNIT_ORG_LEVEL as i_RFX_UNIT_ORG_LEVEL",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_TASK_CNT as i_RFX_TASK_CNT",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___WORKLOAD_FLAG as i_WORKLOAD_FLAG1",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___CONFIDENTIAL_FLAG as i_CONFIDENTIAL_FLAG1",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_CREATION_TSTMP as i_RFX_PROJECT_CREATION_TSTMP",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_LAUNCH_TSTMP as i_RFX_PROJECT_LAUNCH_TSTMP",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_START_TSTMP as i_RFX_PROJECT_START_TSTMP",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_END_TSTMP as i_RFX_PROJECT_END_TSTMP",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_COMPLETION_TSTMP as i_RFX_PROJECT_COMPLETION_TSTMP",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_PROJECT_STATUS_CD as i_RFX_PROJECT_STATUS_CD",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_LAST_UPDATED_USER_ID as i_RFX_LAST_UPDATED_USER_ID",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___RFX_LAST_UPDATED_USER_NAME as i_RFX_LAST_UPDATED_USER_NAME",
	"SQ_Shortcut_to_RFX_RTM_PROJECT___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------

# Processing node FIL_RFX_RTM_PROJECT, type FILTER 
# COLUMN COUNT: 73

# for each involved DataFrame, append the dataframe name to each column
JNR_RFX_RTM_PROJECT_temp = JNR_RFX_RTM_PROJECT.toDF(*["JNR_RFX_RTM_PROJECT___" + col for col in JNR_RFX_RTM_PROJECT.columns])

FIL_RFX_RTM_PROJECT = JNR_RFX_RTM_PROJECT_temp.selectExpr(
	"JNR_RFX_RTM_PROJECT___PROJECT_ID as PROJECT_ID",
	"JNR_RFX_RTM_PROJECT___LOCATION_ID as LOCATION_ID",
	"JNR_RFX_RTM_PROJECT___UNIT_ID as UNIT_ID",
	"JNR_RFX_RTM_PROJECT___PROJECT_TYPE as PROJECT_TYPE",
	"JNR_RFX_RTM_PROJECT___PROJECT_TITLE as PROJECT_TITLE",
	"JNR_RFX_RTM_PROJECT___PROJECT_PRIORITY_CODE as PROJECT_PRIORITY_CODE",
	"JNR_RFX_RTM_PROJECT___CREATOR_USER_ID as CREATOR_USER_ID",
	"JNR_RFX_RTM_PROJECT___CREATOR as CREATOR",
	"JNR_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_ROLE as EXECUTION_ASSIGNED_ROLE",
	"JNR_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_DEPT as EXECUTION_ASSIGNED_DEPT",
	"JNR_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_USER_ID as EXECUTION_ASSIGNED_USER_ID",
	"JNR_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_USER as EXECUTION_ASSIGNED_USER",
	"JNR_RFX_RTM_PROJECT___PROJECT_EXECUTION_START_DATE as PROJECT_EXECUTION_START_DATE",
	"JNR_RFX_RTM_PROJECT___PROJECT_EXECUTION_END_DATE as PROJECT_EXECUTION_END_DATE",
	"JNR_RFX_RTM_PROJECT___STORE_PROJECT_STATUS as STORE_PROJECT_STATUS",
	"JNR_RFX_RTM_PROJECT___ESTIMATED_EFFORT_IN_HOURS as ESTIMATED_EFFORT_IN_HOURS",
	"JNR_RFX_RTM_PROJECT___PROJECT_EXECUTION_COMPLETION_DATE as PROJECT_EXECUTION_COMPLETION_DATE",
	"JNR_RFX_RTM_PROJECT___ON_TIME_COMPLETION_FLAG as ON_TIME_COMPLETION_FLAG",
	"JNR_RFX_RTM_PROJECT___PROJECT_STATUS as PROJECT_STATUS",
	"JNR_RFX_RTM_PROJECT___PROJECT_LAUNCH_DATE as PROJECT_LAUNCH_DATE",
	"JNR_RFX_RTM_PROJECT___PROJECT_START_DATE as PROJECT_START_DATE",
	"JNR_RFX_RTM_PROJECT___PROJECT_END_DATE as PROJECT_END_DATE",
	"JNR_RFX_RTM_PROJECT___PROJECT_CREATION_DATE as PROJECT_CREATION_DATE",
	"JNR_RFX_RTM_PROJECT___CREATOR_UNIT as CREATOR_UNIT",
	"JNR_RFX_RTM_PROJECT___CREATOR_DEPT as CREATOR_DEPT",
	"JNR_RFX_RTM_PROJECT___UNIT_ORG_LEVEL as UNIT_ORG_LEVEL",
	"JNR_RFX_RTM_PROJECT___CONFIDENTIAL_FLAG as CONFIDENTIAL_FLAG",
	"JNR_RFX_RTM_PROJECT___TASK_COUNT as TASK_COUNT",
	"JNR_RFX_RTM_PROJECT___PROJECT_COMPLETION_DATE as PROJECT_COMPLETION_DATE",
	"JNR_RFX_RTM_PROJECT___RELEASE_USER_ID as RELEASE_USER_ID",
	"JNR_RFX_RTM_PROJECT___RELEASE_USER as RELEASE_USER",
	"JNR_RFX_RTM_PROJECT___WORKLOAD_FLAG as WORKLOAD_FLAG",
	"JNR_RFX_RTM_PROJECT___PROJECT_ASSIGNED_ROLE as PROJECT_ASSIGNED_ROLE",
	"JNR_RFX_RTM_PROJECT___PROJECT_ASSIGNED_DEPT as PROJECT_ASSIGNED_DEPT",
	"JNR_RFX_RTM_PROJECT___LAST_UPDATED_USER_ID as LAST_UPDATED_USER_ID",
	"JNR_RFX_RTM_PROJECT___LAST_UPDATED_USER as LAST_UPDATED_USER",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_ID as i_RFX_PROJECT_ID",
	"JNR_RFX_RTM_PROJECT___i_LOCATION_ID as i_LOCATION_ID",
	"JNR_RFX_RTM_PROJECT___i_RFX_UNIT_ID as i_RFX_UNIT_ID",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_TYPE_CD as i_RFX_PROJECT_TYPE_CD",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_TITLE as i_RFX_PROJECT_TITLE",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_PRIORITY_CD as i_RFX_PROJECT_PRIORITY_CD",
	"JNR_RFX_RTM_PROJECT___i_RFX_EXECUTION_ASSIGNED_ROLE_CD as i_RFX_EXECUTION_ASSIGNED_ROLE_CD",
	"JNR_RFX_RTM_PROJECT___i_RFX_EXECUTION_ASSIGNED_DEPT_CD as i_RFX_EXECUTION_ASSIGNED_DEPT_CD",
	"JNR_RFX_RTM_PROJECT___i_RFX_EXECUTION_ASSIGNED_USER_ID as i_RFX_EXECUTION_ASSIGNED_USER_ID",
	"JNR_RFX_RTM_PROJECT___i_RFX_EXECUTION_ASSIGNED_USER_NAME as i_RFX_EXECUTION_ASSIGNED_USER_NAME",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_EXECUTION_START_TSTMP as i_RFX_PROJECT_EXECUTION_START_TSTMP",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_EXECUTION_END_TSTMP as i_RFX_PROJECT_EXECUTION_END_TSTMP",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_EXECUTION_COMPLETION_TSTMP as i_RFX_PROJECT_EXECUTION_COMPLETION_TSTMP",
	"JNR_RFX_RTM_PROJECT___i_EST_EFFORT_IN_HOURS as i_EST_EFFORT_IN_HOURS",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_EXECUTION_STATUS_CD as i_RFX_PROJECT_EXECUTION_STATUS_CD",
	"JNR_RFX_RTM_PROJECT___i_ON_TIME_COMPLETION_FLAG1 as i_ON_TIME_COMPLETION_FLAG1",
	"JNR_RFX_RTM_PROJECT___i_RFX_CREATOR_USER_ID as i_RFX_CREATOR_USER_ID",
	"JNR_RFX_RTM_PROJECT___i_RFX_CREATOR_NAME as i_RFX_CREATOR_NAME",
	"JNR_RFX_RTM_PROJECT___i_RFX_CREATOR_UNIT as i_RFX_CREATOR_UNIT",
	"JNR_RFX_RTM_PROJECT___i_RFX_CREATOR_DEPT_CD as i_RFX_CREATOR_DEPT_CD",
	"JNR_RFX_RTM_PROJECT___i_RFX_RELEASE_USER_ID as i_RFX_RELEASE_USER_ID",
	"JNR_RFX_RTM_PROJECT___i_RFX_RELEASE_USER_NAME as i_RFX_RELEASE_USER_NAME",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_ASSIGNED_ROLE_CD as i_RFX_PROJECT_ASSIGNED_ROLE_CD",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_ASSIGNED_DEPT_CD as i_RFX_PROJECT_ASSIGNED_DEPT_CD",
	"JNR_RFX_RTM_PROJECT___i_RFX_UNIT_ORG_LEVEL as i_RFX_UNIT_ORG_LEVEL",
	"JNR_RFX_RTM_PROJECT___i_RFX_TASK_CNT as i_RFX_TASK_CNT",
	"JNR_RFX_RTM_PROJECT___i_WORKLOAD_FLAG1 as i_WORKLOAD_FLAG1",
	"JNR_RFX_RTM_PROJECT___i_CONFIDENTIAL_FLAG1 as i_CONFIDENTIAL_FLAG1",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_CREATION_TSTMP as i_RFX_PROJECT_CREATION_TSTMP",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_LAUNCH_TSTMP as i_RFX_PROJECT_LAUNCH_TSTMP",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_START_TSTMP as i_RFX_PROJECT_START_TSTMP",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_END_TSTMP as i_RFX_PROJECT_END_TSTMP",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_COMPLETION_TSTMP as i_RFX_PROJECT_COMPLETION_TSTMP",
	"JNR_RFX_RTM_PROJECT___i_RFX_PROJECT_STATUS_CD as i_RFX_PROJECT_STATUS_CD",
	"JNR_RFX_RTM_PROJECT___i_RFX_LAST_UPDATED_USER_ID as i_RFX_LAST_UPDATED_USER_ID",
	"JNR_RFX_RTM_PROJECT___i_RFX_LAST_UPDATED_USER_NAME as i_RFX_LAST_UPDATED_USER_NAME",
	"JNR_RFX_RTM_PROJECT___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_RFX_PROJECT_ID IS NULL OR ( i_RFX_PROJECT_ID IS NOT NULL AND ( IF (UNIT_ID IS NULL, '', UNIT_ID) != IF (i_RFX_UNIT_ID IS NULL, '', i_RFX_UNIT_ID) OR IF (PROJECT_TYPE IS NULL, '', PROJECT_TYPE) != IF (i_RFX_PROJECT_TYPE_CD IS NULL, '', i_RFX_PROJECT_TYPE_CD) OR IF (PROJECT_TITLE IS NULL, '', PROJECT_TITLE) != IF (i_RFX_PROJECT_TITLE IS NULL, '', i_RFX_PROJECT_TITLE) OR IF (PROJECT_PRIORITY_CODE IS NULL, - 99999, PROJECT_PRIORITY_CODE) != IF (i_RFX_PROJECT_PRIORITY_CD IS NULL, - 99999, i_RFX_PROJECT_PRIORITY_CD) OR IF (EXECUTION_ASSIGNED_ROLE IS NULL, '', EXECUTION_ASSIGNED_ROLE) != IF (i_RFX_EXECUTION_ASSIGNED_ROLE_CD IS NULL, '', i_RFX_EXECUTION_ASSIGNED_ROLE_CD) OR IF (EXECUTION_ASSIGNED_DEPT IS NULL, '', EXECUTION_ASSIGNED_DEPT) != IF (i_RFX_EXECUTION_ASSIGNED_DEPT_CD IS NULL, '', i_RFX_EXECUTION_ASSIGNED_DEPT_CD) OR IF (EXECUTION_ASSIGNED_USER_ID IS NULL, '', EXECUTION_ASSIGNED_USER_ID) != IF (i_RFX_EXECUTION_ASSIGNED_USER_ID IS NULL, '', i_RFX_EXECUTION_ASSIGNED_USER_ID) OR IF (EXECUTION_ASSIGNED_USER IS NULL, '', EXECUTION_ASSIGNED_USER) != IF (i_RFX_EXECUTION_ASSIGNED_USER_NAME IS NULL, '', i_RFX_EXECUTION_ASSIGNED_USER_NAME) OR IF (PROJECT_EXECUTION_START_DATE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), PROJECT_EXECUTION_START_DATE) != IF (i_RFX_PROJECT_EXECUTION_START_TSTMP IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_RFX_PROJECT_EXECUTION_START_TSTMP) OR IF (PROJECT_EXECUTION_END_DATE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), PROJECT_EXECUTION_END_DATE) != IF (i_RFX_PROJECT_EXECUTION_END_TSTMP IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_RFX_PROJECT_EXECUTION_END_TSTMP) OR IF (PROJECT_EXECUTION_COMPLETION_DATE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), PROJECT_EXECUTION_COMPLETION_DATE) != IF (i_RFX_PROJECT_EXECUTION_COMPLETION_TSTMP IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_RFX_PROJECT_EXECUTION_COMPLETION_TSTMP) OR IF (ESTIMATED_EFFORT_IN_HOURS IS NULL, 0, ESTIMATED_EFFORT_IN_HOURS) != IF (i_EST_EFFORT_IN_HOURS IS NULL, 0, i_EST_EFFORT_IN_HOURS) OR IF (STORE_PROJECT_STATUS IS NULL, '', STORE_PROJECT_STATUS) != IF (i_RFX_PROJECT_EXECUTION_STATUS_CD IS NULL, '', i_RFX_PROJECT_EXECUTION_STATUS_CD) OR IF (ON_TIME_COMPLETION_FLAG IS NULL, '', IF (ON_TIME_COMPLETION_FLAG = 'Y', '1', '0')) != IF (i_ON_TIME_COMPLETION_FLAG1 IS NULL, '', i_ON_TIME_COMPLETION_FLAG1) OR IF (CREATOR_USER_ID IS NULL, '', CREATOR_USER_ID) != IF (i_RFX_CREATOR_USER_ID IS NULL, '', i_RFX_CREATOR_USER_ID) OR IF (CREATOR IS NULL, '', CREATOR) != IF (i_RFX_CREATOR_NAME IS NULL, '', i_RFX_CREATOR_NAME) OR IF (CREATOR_UNIT IS NULL, '', CREATOR_UNIT) != IF (i_RFX_CREATOR_UNIT IS NULL, '', i_RFX_CREATOR_UNIT) OR IF (CREATOR_DEPT IS NULL, '', CREATOR_DEPT) != IF (i_RFX_CREATOR_DEPT_CD IS NULL, '', i_RFX_CREATOR_DEPT_CD) OR IF (RELEASE_USER_ID IS NULL, '', RELEASE_USER_ID) != IF (i_RFX_RELEASE_USER_ID IS NULL, '', i_RFX_RELEASE_USER_ID) OR IF (RELEASE_USER IS NULL, '', RELEASE_USER) != IF (i_RFX_RELEASE_USER_NAME IS NULL, '', i_RFX_RELEASE_USER_NAME) OR IF (PROJECT_ASSIGNED_ROLE IS NULL, '', PROJECT_ASSIGNED_ROLE) != IF (i_RFX_PROJECT_ASSIGNED_ROLE_CD IS NULL, '', i_RFX_PROJECT_ASSIGNED_ROLE_CD) OR IF (PROJECT_ASSIGNED_DEPT IS NULL, '', PROJECT_ASSIGNED_DEPT) != IF (i_RFX_PROJECT_ASSIGNED_DEPT_CD IS NULL, '', i_RFX_PROJECT_ASSIGNED_DEPT_CD) OR IF (UNIT_ORG_LEVEL IS NULL, '', UNIT_ORG_LEVEL) != IF (i_RFX_UNIT_ORG_LEVEL IS NULL, '', i_RFX_UNIT_ORG_LEVEL) OR IF (TASK_COUNT IS NULL, - 99999, TASK_COUNT) != IF (i_RFX_TASK_CNT IS NULL, - 99999, i_RFX_TASK_CNT) OR IF (WORKLOAD_FLAG IS NULL, - 99999, WORKLOAD_FLAG) != IF (i_WORKLOAD_FLAG1 IS NULL, - 99999, i_WORKLOAD_FLAG1) OR IF (CONFIDENTIAL_FLAG IS NULL, - 99999, CONFIDENTIAL_FLAG) != IF (i_CONFIDENTIAL_FLAG1 IS NULL, - 99999, i_CONFIDENTIAL_FLAG1) OR IF (PROJECT_CREATION_DATE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), PROJECT_CREATION_DATE) != IF (i_RFX_PROJECT_CREATION_TSTMP IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_RFX_PROJECT_CREATION_TSTMP) OR IF (PROJECT_LAUNCH_DATE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), PROJECT_LAUNCH_DATE) != IF (i_RFX_PROJECT_LAUNCH_TSTMP IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_RFX_PROJECT_LAUNCH_TSTMP) OR IF (PROJECT_START_DATE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), PROJECT_START_DATE) != IF (i_RFX_PROJECT_START_TSTMP IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_RFX_PROJECT_START_TSTMP) OR IF (PROJECT_END_DATE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), PROJECT_END_DATE) != IF (i_RFX_PROJECT_END_TSTMP IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_RFX_PROJECT_END_TSTMP) OR IF (PROJECT_COMPLETION_DATE IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), PROJECT_COMPLETION_DATE) != IF (i_RFX_PROJECT_COMPLETION_TSTMP IS NULL, TO_DATE ( '1901-01-01' , 'y-M-d' ), i_RFX_PROJECT_COMPLETION_TSTMP) OR IF (PROJECT_STATUS IS NULL, '', PROJECT_STATUS) != IF (i_RFX_PROJECT_STATUS_CD IS NULL, '', i_RFX_PROJECT_STATUS_CD) OR IF (LAST_UPDATED_USER_ID IS NULL, '', LAST_UPDATED_USER_ID) != IF (i_RFX_LAST_UPDATED_USER_ID IS NULL, '', i_RFX_LAST_UPDATED_USER_ID) OR IF (LAST_UPDATED_USER IS NULL, '', LAST_UPDATED_USER) != IF (i_RFX_LAST_UPDATED_USER_NAME IS NULL, '', i_RFX_LAST_UPDATED_USER_NAME) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_RFX_RTM_PROJECT, type EXPRESSION 
# COLUMN COUNT: 42

# for each involved DataFrame, append the dataframe name to each column
FIL_RFX_RTM_PROJECT_temp = FIL_RFX_RTM_PROJECT.toDF(*["FIL_RFX_RTM_PROJECT___" + col for col in FIL_RFX_RTM_PROJECT.columns])

EXP_RFX_RTM_PROJECT = FIL_RFX_RTM_PROJECT_temp.selectExpr(
	"FIL_RFX_RTM_PROJECT___sys_row_id as sys_row_id",
	"FIL_RFX_RTM_PROJECT___PROJECT_ID as PROJECT_ID",
	"FIL_RFX_RTM_PROJECT___LOCATION_ID as LOCATION_ID",
	"FIL_RFX_RTM_PROJECT___UNIT_ID as UNIT_ID",
	"FIL_RFX_RTM_PROJECT___PROJECT_TYPE as PROJECT_TYPE",
	"FIL_RFX_RTM_PROJECT___PROJECT_TITLE as PROJECT_TITLE",
	"FIL_RFX_RTM_PROJECT___PROJECT_PRIORITY_CODE as PROJECT_PRIORITY_CODE",
	"FIL_RFX_RTM_PROJECT___CREATOR_USER_ID as CREATOR_USER_ID",
	"FIL_RFX_RTM_PROJECT___CREATOR as CREATOR",
	"FIL_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_ROLE as EXECUTION_ASSIGNED_ROLE",
	"FIL_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_DEPT as EXECUTION_ASSIGNED_DEPT",
	"FIL_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_USER_ID as EXECUTION_ASSIGNED_USER_ID",
	"FIL_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_USER as EXECUTION_ASSIGNED_USER",
	"FIL_RFX_RTM_PROJECT___PROJECT_EXECUTION_START_DATE as PROJECT_EXECUTION_START_DATE",
	"FIL_RFX_RTM_PROJECT___PROJECT_EXECUTION_END_DATE as PROJECT_EXECUTION_END_DATE",
	"FIL_RFX_RTM_PROJECT___STORE_PROJECT_STATUS as STORE_PROJECT_STATUS",
	"FIL_RFX_RTM_PROJECT___ESTIMATED_EFFORT_IN_HOURS as ESTIMATED_EFFORT_IN_HOURS",
	"FIL_RFX_RTM_PROJECT___PROJECT_EXECUTION_COMPLETION_DATE as PROJECT_EXECUTION_COMPLETION_DATE",
	"FIL_RFX_RTM_PROJECT___ON_TIME_COMPLETION_FLAG as ON_TIME_COMPLETION_FLAG",
	"decode ( FIL_RFX_RTM_PROJECT___ON_TIME_COMPLETION_FLAG , 'Y','1' , 'N','0' , NULL ) as o_ON_TIME_COMPLETION_FLAG",
	"FIL_RFX_RTM_PROJECT___PROJECT_STATUS as PROJECT_STATUS",
	"FIL_RFX_RTM_PROJECT___PROJECT_LAUNCH_DATE as PROJECT_LAUNCH_DATE",
	"FIL_RFX_RTM_PROJECT___PROJECT_START_DATE as PROJECT_START_DATE",
	"FIL_RFX_RTM_PROJECT___PROJECT_END_DATE as PROJECT_END_DATE",
	"FIL_RFX_RTM_PROJECT___PROJECT_CREATION_DATE as PROJECT_CREATION_DATE",
	"FIL_RFX_RTM_PROJECT___CREATOR_UNIT as CREATOR_UNIT",
	"FIL_RFX_RTM_PROJECT___CREATOR_DEPT as CREATOR_DEPT",
	"FIL_RFX_RTM_PROJECT___UNIT_ORG_LEVEL as UNIT_ORG_LEVEL",
	"FIL_RFX_RTM_PROJECT___CONFIDENTIAL_FLAG as CONFIDENTIAL_FLAG",
	"FIL_RFX_RTM_PROJECT___TASK_COUNT as TASK_COUNT",
	"FIL_RFX_RTM_PROJECT___PROJECT_COMPLETION_DATE as PROJECT_COMPLETION_DATE",
	"FIL_RFX_RTM_PROJECT___RELEASE_USER_ID as RELEASE_USER_ID",
	"FIL_RFX_RTM_PROJECT___RELEASE_USER as RELEASE_USER",
	"FIL_RFX_RTM_PROJECT___WORKLOAD_FLAG as WORKLOAD_FLAG",
	"FIL_RFX_RTM_PROJECT___PROJECT_ASSIGNED_ROLE as PROJECT_ASSIGNED_ROLE",
	"FIL_RFX_RTM_PROJECT___PROJECT_ASSIGNED_DEPT as PROJECT_ASSIGNED_DEPT",
	"FIL_RFX_RTM_PROJECT___LAST_UPDATED_USER_ID as LAST_UPDATED_USER_ID",
	"FIL_RFX_RTM_PROJECT___LAST_UPDATED_USER as LAST_UPDATED_USER",
	"FIL_RFX_RTM_PROJECT___i_RFX_PROJECT_ID as i_RFX_PROJECT_ID",
	"FIL_RFX_RTM_PROJECT___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"IF (FIL_RFX_RTM_PROJECT___i_RFX_PROJECT_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_RFX_RTM_PROJECT___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_RFX_RTM_PROJECT___i_LOAD_TSTMP) as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node UPD_RFX_RTM_PROJECT, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 39

# for each involved DataFrame, append the dataframe name to each column
EXP_RFX_RTM_PROJECT_temp = EXP_RFX_RTM_PROJECT.toDF(*["EXP_RFX_RTM_PROJECT___" + col for col in EXP_RFX_RTM_PROJECT.columns])

UPD_RFX_RTM_PROJECT = EXP_RFX_RTM_PROJECT_temp.selectExpr(
	"EXP_RFX_RTM_PROJECT___PROJECT_ID as PROJECT_ID",
	"EXP_RFX_RTM_PROJECT___LOCATION_ID as LOCATION_ID",
	"EXP_RFX_RTM_PROJECT___UNIT_ID as UNIT_ID",
	"EXP_RFX_RTM_PROJECT___PROJECT_TYPE as PROJECT_TYPE",
	"EXP_RFX_RTM_PROJECT___PROJECT_TITLE as PROJECT_TITLE",
	"EXP_RFX_RTM_PROJECT___PROJECT_PRIORITY_CODE as PROJECT_PRIORITY_CODE",
	"EXP_RFX_RTM_PROJECT___CREATOR_USER_ID as CREATOR_USER_ID",
	"EXP_RFX_RTM_PROJECT___CREATOR as CREATOR",
	"EXP_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_ROLE as EXECUTION_ASSIGNED_ROLE",
	"EXP_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_DEPT as EXECUTION_ASSIGNED_DEPT",
	"EXP_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_USER_ID as EXECUTION_ASSIGNED_USER_ID",
	"EXP_RFX_RTM_PROJECT___EXECUTION_ASSIGNED_USER as EXECUTION_ASSIGNED_USER",
	"EXP_RFX_RTM_PROJECT___PROJECT_EXECUTION_START_DATE as PROJECT_EXECUTION_START_DATE",
	"EXP_RFX_RTM_PROJECT___PROJECT_EXECUTION_END_DATE as PROJECT_EXECUTION_END_DATE",
	"EXP_RFX_RTM_PROJECT___STORE_PROJECT_STATUS as STORE_PROJECT_STATUS",
	"EXP_RFX_RTM_PROJECT___ESTIMATED_EFFORT_IN_HOURS as ESTIMATED_EFFORT_IN_HOURS",
	"EXP_RFX_RTM_PROJECT___PROJECT_EXECUTION_COMPLETION_DATE as PROJECT_EXECUTION_COMPLETION_DATE",
	"EXP_RFX_RTM_PROJECT___o_ON_TIME_COMPLETION_FLAG as ON_TIME_COMPLETION_FLAG",
	"EXP_RFX_RTM_PROJECT___PROJECT_STATUS as PROJECT_STATUS",
	"EXP_RFX_RTM_PROJECT___PROJECT_LAUNCH_DATE as PROJECT_LAUNCH_DATE",
	"EXP_RFX_RTM_PROJECT___PROJECT_START_DATE as PROJECT_START_DATE",
	"EXP_RFX_RTM_PROJECT___PROJECT_END_DATE as PROJECT_END_DATE",
	"EXP_RFX_RTM_PROJECT___PROJECT_CREATION_DATE as PROJECT_CREATION_DATE",
	"EXP_RFX_RTM_PROJECT___CREATOR_UNIT as CREATOR_UNIT",
	"EXP_RFX_RTM_PROJECT___CREATOR_DEPT as CREATOR_DEPT",
	"EXP_RFX_RTM_PROJECT___UNIT_ORG_LEVEL as UNIT_ORG_LEVEL",
	"EXP_RFX_RTM_PROJECT___CONFIDENTIAL_FLAG as CONFIDENTIAL_FLAG",
	"EXP_RFX_RTM_PROJECT___TASK_COUNT as TASK_COUNT",
	"EXP_RFX_RTM_PROJECT___PROJECT_COMPLETION_DATE as PROJECT_COMPLETION_DATE",
	"EXP_RFX_RTM_PROJECT___RELEASE_USER_ID as RELEASE_USER_ID",
	"EXP_RFX_RTM_PROJECT___RELEASE_USER as RELEASE_USER",
	"EXP_RFX_RTM_PROJECT___WORKLOAD_FLAG as WORKLOAD_FLAG",
	"EXP_RFX_RTM_PROJECT___PROJECT_ASSIGNED_ROLE as PROJECT_ASSIGNED_ROLE",
	"EXP_RFX_RTM_PROJECT___PROJECT_ASSIGNED_DEPT as PROJECT_ASSIGNED_DEPT",
	"EXP_RFX_RTM_PROJECT___LAST_UPDATED_USER_ID as LAST_UPDATED_USER_ID",
	"EXP_RFX_RTM_PROJECT___LAST_UPDATED_USER as LAST_UPDATED_USER",
	"EXP_RFX_RTM_PROJECT___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR",
	"EXP_RFX_RTM_PROJECT___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_RFX_RTM_PROJECT___LOAD_TSTMP as LOAD_TSTMP",
	"if(EXP_RFX_RTM_PROJECT___o_UPDATE_VALIDATOR==1,0,1) as pyspark_data_action")


# COMMAND ----------

# Processing node Shortcut_to_RFX_RTM_PROJECT1, type TARGET 
# COLUMN COUNT: 38


Shortcut_to_RFX_RTM_PROJECT1 = UPD_RFX_RTM_PROJECT.selectExpr(
	"CAST(PROJECT_ID AS STRING) as RFX_PROJECT_ID",
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(UNIT_ID AS STRING) as RFX_UNIT_ID",
	"CAST(PROJECT_TYPE AS STRING) as RFX_PROJECT_TYPE_CD",
	"CAST(PROJECT_TITLE AS STRING) as RFX_PROJECT_TITLE",
	"PROJECT_PRIORITY_CODE as RFX_PROJECT_PRIORITY_CD",
	"CAST(EXECUTION_ASSIGNED_ROLE AS STRING) as RFX_EXECUTION_ASSIGNED_ROLE_CD",
	"CAST(EXECUTION_ASSIGNED_DEPT AS STRING) as RFX_EXECUTION_ASSIGNED_DEPT_CD",
	"CAST(EXECUTION_ASSIGNED_USER_ID AS STRING) as RFX_EXECUTION_ASSIGNED_USER_ID",
	"CAST(EXECUTION_ASSIGNED_USER AS STRING) as RFX_EXECUTION_ASSIGNED_USER_NAME",
	"CAST(PROJECT_EXECUTION_START_DATE AS TIMESTAMP) as RFX_PROJECT_EXECUTION_START_TSTMP",
	"CAST(PROJECT_EXECUTION_END_DATE AS TIMESTAMP) as RFX_PROJECT_EXECUTION_END_TSTMP",
	"CAST(PROJECT_EXECUTION_COMPLETION_DATE AS TIMESTAMP) as RFX_PROJECT_EXECUTION_COMPLETION_TSTMP",
	"CAST(ESTIMATED_EFFORT_IN_HOURS AS DECIMAL(7,2)) as EST_EFFORT_IN_HOURS",
	"CAST(STORE_PROJECT_STATUS AS STRING) as RFX_PROJECT_EXECUTION_STATUS_CD",
	"CAST(ON_TIME_COMPLETION_FLAG AS TINYINT) as ON_TIME_COMPLETION_FLAG",
	"CAST(CREATOR_USER_ID AS STRING) as RFX_CREATOR_USER_ID",
	"CAST(CREATOR AS STRING) as RFX_CREATOR_NAME",
	"CAST(CREATOR_UNIT AS STRING) as RFX_CREATOR_UNIT",
	"CAST(CREATOR_DEPT AS STRING) as RFX_CREATOR_DEPT_CD",
	"CAST(RELEASE_USER_ID AS STRING) as RFX_RELEASE_USER_ID",
	"CAST(RELEASE_USER AS STRING) as RFX_RELEASE_USER_NAME",
	"CAST(PROJECT_ASSIGNED_ROLE AS STRING) as RFX_PROJECT_ASSIGNED_ROLE_CD",
	"CAST(PROJECT_ASSIGNED_DEPT AS STRING) as RFX_PROJECT_ASSIGNED_DEPT_CD",
	"CAST(UNIT_ORG_LEVEL AS STRING) as RFX_UNIT_ORG_LEVEL",
	"TASK_COUNT as RFX_TASK_CNT",
	"WORKLOAD_FLAG as WORKLOAD_FLAG",
	"CONFIDENTIAL_FLAG as CONFIDENTIAL_FLAG",
	"CAST(PROJECT_CREATION_DATE AS TIMESTAMP) as RFX_PROJECT_CREATION_TSTMP",
	"CAST(PROJECT_LAUNCH_DATE AS TIMESTAMP) as RFX_PROJECT_LAUNCH_TSTMP",
	"CAST(PROJECT_START_DATE AS TIMESTAMP) as RFX_PROJECT_START_TSTMP",
	"CAST(PROJECT_END_DATE AS TIMESTAMP) as RFX_PROJECT_END_TSTMP",
	"CAST(PROJECT_COMPLETION_DATE AS TIMESTAMP) as RFX_PROJECT_COMPLETION_TSTMP",
	"CAST(PROJECT_STATUS AS STRING) as RFX_PROJECT_STATUS_CD",
	"CAST(LAST_UPDATED_USER_ID AS STRING) as RFX_LAST_UPDATED_USER_ID",
	"CAST(LAST_UPDATED_USER AS STRING) as RFX_LAST_UPDATED_USER_NAME",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.RFX_PROJECT_ID = target.RFX_PROJECT_ID AND source.LOCATION_ID = target.LOCATION_ID"""
	refined_perf_table = f"{legacy}.RFX_RTM_PROJECT"
	executeMerge(Shortcut_to_RFX_RTM_PROJECT1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("RFX_RTM_PROJECT", "RFX_RTM_PROJECT", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("RFX_RTM_PROJECT", "RFX_RTM_PROJECT","Failed",str(e), f"{raw}.log_run_details")
	raise e
		

# COMMAND ----------


