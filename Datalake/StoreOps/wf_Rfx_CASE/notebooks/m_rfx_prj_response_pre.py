# Databricks notebook source
# Code converted on 2023-08-25 11:50:58
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

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_prj_response/')
# source_bucket = dbutils.widgets.get('source_bucket')

_bucket = getParameterValue(
    raw, "BA_StoreOps_Parameter.prm", "BA_StoreOps.WF:wf_rfx_case", "source_bucket"
)
source_bucket = _bucket + "rfx_prj_response/"

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_LOOK_UP, type SOURCE
# COLUMN COUNT: 3

source_file = get_src_file("RFX_PRJ_RESPONSE", source_bucket)
# source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_prj_response/20230907/'

csv_options = {
    "sep": ",",
    "header": True,  # The first row contains column names
    # "inferSchema": True,      # Infer column data types
    "quote": '"',  # Specify the character used for quoting values
    "escape": '"',  # Specify the character used for escaping special characters within quoted values
    "multiLine": True,
}

# Read the CSV file into a DataFrame

SQ_Shortcut_to_RFX_PRJ_RESPONSE = spark.read.csv(source_file, **csv_options)
if SQ_Shortcut_to_RFX_PRJ_RESPONSE.head() is None:
    spark.sql(f"TRUNCATE TABLE {raw}.RFX_PRJ_RESPONSE_PRE").collect()
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


SQ_Shortcut_to_RFX_PRJ_RESPONSE = rename_columns(SQ_Shortcut_to_RFX_PRJ_RESPONSE)

# COMMAND ----------

# Processing node EXP_RFX_PRJ_RESPONSE_PRE, type EXPRESSION
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_PRJ_RESPONSE_temp = SQ_Shortcut_to_RFX_PRJ_RESPONSE.toDF(
    *[
        "SQ_Shortcut_to_RFX_PRJ_RESPONSE___" + col
        for col in SQ_Shortcut_to_RFX_PRJ_RESPONSE.columns
    ]
)

EXP_RFX_PRJ_RESPONSE_PRE = SQ_Shortcut_to_RFX_PRJ_RESPONSE_temp.selectExpr(
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___PROJECT_ID as PROJECT_ID",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___TASK_ID as TASK_ID",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___SURVEY_TYPE as SURVEY_TYPE",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___UNIT_ID as UNIT_ID",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___QUESTION_ID as QUESTION_ID",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___QUESTION_TEXT as QUESTION_TEXT",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___SUB_QUESTION_NO as SUB_QUESTION_NO",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___SUB_QUESTION_TEXT as SUB_QUESTION_TEXT",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___RESPONSE_TYPE as RESPONSE_TYPE",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___OPTION_SELECTED as OPTION_SELECTED",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___OPTION_TEXT as OPTION_TEXT",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___RESPONSE_DATE as RESPONSE_DATE",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___NUMERIC_VALUE as NUMERIC_VALUE",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___TEXT_VALUE as TEXT_VALUE",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___DATE_VALUE as DATE_VALUE",
    "SQ_Shortcut_to_RFX_PRJ_RESPONSE___ATTACHMENT_COUNT as ATTACHMENT_COUNT",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)

# COMMAND ----------

# Processing node Shortcut_to_RFX_PRJ_RESPONSE_PRE, type TARGET
# COLUMN COUNT: 17


Shortcut_to_RFX_PRJ_RESPONSE_PRE = EXP_RFX_PRJ_RESPONSE_PRE.selectExpr(
    "CAST(PROJECT_ID AS STRING) as PROJECT_ID",
    "CAST(TASK_ID AS STRING) as TASK_ID",
    "CAST(SURVEY_TYPE AS STRING) as SURVEY_TYPE",
    "CAST(UNIT_ID AS STRING) as UNIT_ID",
    "CAST(QUESTION_ID AS STRING) as QUESTION_ID",
    "CAST(QUESTION_TEXT AS STRING) as QUESTION_TEXT",
    "CAST(SUB_QUESTION_NO AS INT) as SUB_QUESTION_NO",
    "CAST(SUB_QUESTION_TEXT AS STRING) as SUB_QUESTION_TEXT",
    "CAST(RESPONSE_TYPE AS STRING) as RESPONSE_TYPE",
    "CAST(OPTION_SELECTED AS SMALLINT) as OPTION_SELECTED",
    "CAST(OPTION_TEXT AS STRING) as OPTION_TEXT",
    "CAST(RESPONSE_DATE AS TIMESTAMP) as RESPONSE_DATE",
    "CAST(NUMERIC_VALUE AS DECIMAL(22,4)) as NUMERIC_VALUE",
    "CAST(TEXT_VALUE AS STRING) as TEXT_VALUE",
    "CAST(DATE_VALUE AS TIMESTAMP) as DATE_VALUE",
    "CAST(ATTACHMENT_COUNT AS INT) as ATTACHMENT_COUNT",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)
# overwriteDeltaPartition(Shortcut_to_RFX_PRJ_RESPONSE_PRE,'DC_NBR',dcnbr,f'{raw}.RFX_PRJ_RESPONSE_PRE')
Shortcut_to_RFX_PRJ_RESPONSE_PRE.write.mode("overwrite").saveAsTable(
    f"{raw}.RFX_PRJ_RESPONSE_PRE"
)

# COMMAND ----------
