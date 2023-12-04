# Databricks notebook source
# Code converted on 2023-08-25 11:51:01
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

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_look_up/')
# source_bucket = dbutils.widgets.get('source_bucket')

_bucket = getParameterValue(
    raw, "BA_StoreOps_Parameter.prm", "BA_StoreOps.WF:wf_rfx_case", "source_bucket"
)
source_bucket = _bucket + "rfx_look_up/"

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_LOOK_UP, type SOURCE
# COLUMN COUNT: 3

source_file = get_src_file("RFX_LOOK_UP", source_bucket)

# source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_look_up/20230907/'
csv_options = {
    "sep": ",",
    "header": True,  # The first row contains column names
    # "inferSchema": True,      # Infer column data types
    "quote": '"',  # Specify the character used for quoting values
    "escape": '"',  # Specify the character used for escaping special characters within quoted values
    "multiLine": True,
}
# Read the CSV file into a DataFrame

SQ_Shortcut_to_RFX_LOOK_UP = spark.read.csv(source_file, **csv_options)
if SQ_Shortcut_to_RFX_LOOK_UP.head() is None:
    df = spark.sql(f"TRUNCATE TABLE {raw}.RFX_LOOK_UP_PRE")
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


SQ_Shortcut_to_RFX_LOOK_UP = rename_columns(SQ_Shortcut_to_RFX_LOOK_UP)

# COMMAND ----------

# Processing node EXP_RFX_LOOK_UP_PRE, type EXPRESSION
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_LOOK_UP_temp = SQ_Shortcut_to_RFX_LOOK_UP.toDF(
    *[
        "SQ_Shortcut_to_RFX_LOOK_UP___" + col
        for col in SQ_Shortcut_to_RFX_LOOK_UP.columns
    ]
)

EXP_RFX_LOOK_UP_PRE = SQ_Shortcut_to_RFX_LOOK_UP_temp.selectExpr(
    "SQ_Shortcut_to_RFX_LOOK_UP___Look_Up_Type as LOOKUP_TYPE",
    "SQ_Shortcut_to_RFX_LOOK_UP___Key_Value as RFX_KEY",
    "SQ_Shortcut_to_RFX_LOOK_UP___Key_Description as RFX_DESCRIPTION",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)

# COMMAND ----------

# Processing node Shortcut_to_RFX_LOOK_UP_PRE, type TARGET
# COLUMN COUNT: 4


Shortcut_to_RFX_LOOK_UP_PRE = EXP_RFX_LOOK_UP_PRE.selectExpr(
    "CAST(LOOKUP_TYPE AS STRING) as LOOKUP_TYPE",
    "CAST(RFX_KEY AS STRING) as RFX_KEY",
    "CAST(RFX_DESCRIPTION AS STRING) as RFX_DESCRIPTION",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)
# overwriteDeltaPartition(Shortcut_to_RFX_LOOK_UP_PRE,'DC_NBR',dcnbr,f'{raw}.RFX_LOOK_UP_PRE')
Shortcut_to_RFX_LOOK_UP_PRE.write.mode("overwrite").saveAsTable(
    f"{raw}.RFX_LOOK_UP_PRE"
)

# COMMAND ----------
