# Databricks notebook source
# Code converted on 2023-08-25 11:51:11
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

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_task/')
# source_bucket = dbutils.widgets.get('source_bucket')

_bucket = getParameterValue(
    raw, "BA_StoreOps_Parameter.prm", "BA_StoreOps.WF:wf_rfx_case", "source_bucket"
)
source_bucket = _bucket + "rfx_task/"


# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_TASK, type SOURCE
# COLUMN COUNT: 19

source_file = get_src_file("RFX_TASK", source_bucket)
# source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_task/20230907/'

csv_options = {
    "sep": ",",
    "header": True,  # The first row contains column names
    # "inferSchema": True,      # Infer column data types
    "quote": '"',  # Specify the character used for quoting values
    "escape": '"',  # Specify the character used for escaping special characters within quoted values
    "multiLine": True,
}

# Read the CSV file into a DataFrame

SQ_Shortcut_to_RFX_TASK = spark.read.csv(source_file, **csv_options)
if SQ_Shortcut_to_RFX_TASK.head() is None:
    df = spark.sql(f"TRUNCATE TABLE {raw}.RFX_TASK_PRE")
    dbutils.notebook.exit('file not available or empty')
# COMMAND ----------

# Rename columns
import re


# Function to transform column names
def transform_column_name(name):
    # Replace spaces with underscores
    name = name.replace(" ", "_")
    # Remove #_ and _#
    name = re.sub(r"#_", "", name)
    name = name.replace("(%)", "pct")
    return name


def rename_columns(df):
    # Rename columns using transform_column_name function
    for old_col_name in df.columns:
        new_col_name = transform_column_name(old_col_name)
        df = df.withColumnRenamed(old_col_name, new_col_name)
    return df


SQ_Shortcut_to_RFX_TASK = rename_columns(SQ_Shortcut_to_RFX_TASK)

# COMMAND ----------

# Processing node EXP_RFX_TASK_PRE, type EXPRESSION
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_TASK_temp = SQ_Shortcut_to_RFX_TASK.toDF(
    *["SQ_Shortcut_to_RFX_TASK___" + col for col in SQ_Shortcut_to_RFX_TASK.columns]
)

EXP_RFX_TASK_PRE = SQ_Shortcut_to_RFX_TASK_temp.selectExpr(
    "SQ_Shortcut_to_RFX_TASK___PROJECT_ID as PROJECT_ID",
    "SQ_Shortcut_to_RFX_TASK___UNIT_ID as UNIT_ID",
    "SQ_Shortcut_to_RFX_TASK___TASK_ID as TASK_ID",
    "SQ_Shortcut_to_RFX_TASK___SEQUENCE_NO as SEQUENCE_NO",
    "SQ_Shortcut_to_RFX_TASK___TASK_TITLE as TASK_TITLE",
    "SQ_Shortcut_to_RFX_TASK___EXECUTION_ASSIGNED_DEPT as EXECUTION_ASSIGNED_DEPT",
    "SQ_Shortcut_to_RFX_TASK___EXECUTION_ASSIGNED_ROLE as EXECUTION_ASSIGNED_ROLE",
    "SQ_Shortcut_to_RFX_TASK___ASSIGNED_USER_ID as ASSIGNED_USER_ID",
    "SQ_Shortcut_to_RFX_TASK___ASSIGNED_USER as ASSIGNED_USER",
    "SQ_Shortcut_to_RFX_TASK___TASK_EXECUTION_START_DATE as TASK_EXECUTION_START_DATE",
    "SQ_Shortcut_to_RFX_TASK___TASK_EXECUTION_END_DATE as TASK_EXECUTION_END_DATE",
    "SQ_Shortcut_to_RFX_TASK___STATUS as STATUS",
    "SQ_Shortcut_to_RFX_TASK___ESTIMATED_EFFORT_HOURS as ESTIMATED_EFFORT_HOURS",
    "SQ_Shortcut_to_RFX_TASK___TASK_EXECUTION_COMPLETION_DATE as TASK_EXECUTION_COMPLETION_DATE",
    "SQ_Shortcut_to_RFX_TASK___ON_TIME_COMPLETION_INDICATOR as ON_TIME_COMPLETION_INDICATOR",
    "SQ_Shortcut_to_RFX_TASK___TASK_ASSIGNED_DEPT as TASK_ASSIGNED_DEPT",
    "SQ_Shortcut_to_RFX_TASK___TASK_ASSIGNED_ROLE as TASK_ASSIGNED_ROLE",
    "SQ_Shortcut_to_RFX_TASK___LAST_USER_ID as LAST_USER_ID",
    "SQ_Shortcut_to_RFX_TASK___LAST_UPDATED_BY_USER as LAST_UPDATED_BY_USER",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)

# COMMAND ----------

# Processing node Shortcut_to_RFX_TASK_PRE, type TARGET
# COLUMN COUNT: 20


Shortcut_to_RFX_TASK_PRE = EXP_RFX_TASK_PRE.selectExpr(
    "CAST(PROJECT_ID AS STRING) as PROJECT_ID",
    "CAST(UNIT_ID AS STRING) as UNIT_ID",
    "CAST(TASK_ID AS STRING) as TASK_ID",
    "CAST(SEQUENCE_NO AS SMALLINT) as SEQUENCE_NO",
    "CAST(TASK_TITLE AS STRING) as TASK_TITLE",
    "CAST(EXECUTION_ASSIGNED_DEPT AS STRING) as EXECUTION_ASSIGNED_DEPT",
    "CAST(EXECUTION_ASSIGNED_ROLE AS STRING) as EXECUTION_ASSIGNED_ROLE",
    "CAST(ASSIGNED_USER_ID AS STRING) as ASSIGNED_USER_ID",
    "CAST(ASSIGNED_USER AS STRING) as ASSIGNED_USER",
    "CAST(TASK_EXECUTION_START_DATE AS TIMESTAMP) as TASK_EXECUTION_START_DATE",
    "CAST(TASK_EXECUTION_END_DATE AS TIMESTAMP) as TASK_EXECUTION_END_DATE",
    "CAST(STATUS AS STRING) as STATUS",
    "CAST(ESTIMATED_EFFORT_HOURS AS DECIMAL(7,2)) as ESTIMATED_EFFORT_HOURS",
    "CAST(TASK_EXECUTION_COMPLETION_DATE AS TIMESTAMP) as TASK_EXECUTION_COMPLETION_DATE",
    "CAST(ON_TIME_COMPLETION_INDICATOR AS STRING) as ON_TIME_COMPLETION_INDICATOR",
    "CAST(TASK_ASSIGNED_DEPT AS STRING) as TASK_ASSIGNED_DEPT",
    "CAST(TASK_ASSIGNED_ROLE AS STRING) as TASK_ASSIGNED_ROLE",
    "CAST(LAST_USER_ID AS STRING) as LAST_USER_ID",
    "CAST(LAST_UPDATED_BY_USER AS STRING) as LAST_UPDATED_BY_USER",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)
# overwriteDeltaPartition(Shortcut_to_RFX_TASK_PRE,'DC_NBR',dcnbr,f'{raw}.RFX_TASK_PRE')
Shortcut_to_RFX_TASK_PRE.write.mode("overwrite").saveAsTable(f"{raw}.RFX_TASK_PRE")


# COMMAND ----------
