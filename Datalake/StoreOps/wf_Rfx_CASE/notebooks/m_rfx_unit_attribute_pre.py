# Databricks notebook source
# Code converted on 2023-08-25 11:51:03
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

#dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_unit_attribute/')
#source_bucket = dbutils.widgets.get('source_bucket')

_bucket=getParameterValue(raw,'BA_StoreOps_Parameter.prm','BA_StoreOps.WF:wf_rfx_case','source_bucket')
source_bucket=_bucket+"rfx_unit_attribute/"

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE, type SOURCE 
# COLUMN COUNT: 7

source_file = get_source_file_rfx('RFX_UNIT_ATTRIBUTE', source_bucket)
#source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_unit_attribute/20230907/'

csv_options = {
    "sep": ",",
    "header": True,           # The first row contains column names
   #"inferSchema": True,      # Infer column data types
    "quote": '\"',             # Specify the character used for quoting values
    "escape": '\"',             # Specify the character used for escaping special characters within quoted values
    "multiLine": True 
}

# Read the CSV file into a DataFrame

SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE = spark.read.csv(source_file, **csv_options)

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

SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE = rename_columns(SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE) 

# COMMAND ----------

# Processing node EXP_RFX_UNIT_ATTRIBUTE_PRE, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE_temp = SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE.toDF(*["SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE___" + col for col in SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE.columns])

EXP_RFX_UNIT_ATTRIBUTE_PRE = SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE_temp.selectExpr(
	"SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE___UNIT_ID as UNIT_ID",
	"SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE___UNIT_ORG_LEVEL as UNIT_ORG_LEVEL",
	"SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE___ATTRIBUTE_ID as ATTRIBUTE_ID",
	"SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE___ATTRIBUTE_DESCRIPTION as ATTRIBUTE_DESC",
	"SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE___ATTRIBUTE_TEXT_VALUE as ATTRIBUTE_TEXT_VALUE",
	"SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE___ATTRIBUTE_NUMERIC_VALUE as ATTRIBUTE_NUMERIC_VALUE",
	"SQ_Shortcut_to_RFX_UNIT_ATTRIBUTE___ATTRIBUTE_DATE_VALUE as ATTRIBUTE_DATE_VALUE",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RFX_UNIT_ATTRIBUTE_PRE, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_RFX_UNIT_ATTRIBUTE_PRE = EXP_RFX_UNIT_ATTRIBUTE_PRE.selectExpr(
	"CAST(UNIT_ID AS STRING) as UNIT_ID",
	"CAST(UNIT_ORG_LEVEL AS SMALLINT) as UNIT_ORG_LEVEL",
	"CAST(ATTRIBUTE_ID AS STRING) as ATTRIBUTE_ID",
	"CAST(ATTRIBUTE_DESC AS STRING) as ATTRIBUTE_DESCRIPTION",
	"CAST(ATTRIBUTE_TEXT_VALUE AS STRING) as ATTRIBUTE_TEXT_VALUE",
	"CAST(ATTRIBUTE_NUMERIC_VALUE AS DECIMAL(22,4)) as ATTRIBUTE_NUMERIC_VALUE",
	"CAST(ATTRIBUTE_DATE_VALUE AS TIMESTAMP) as ATTRIBUTE_DATE_VALUE",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
# overwriteDeltaPartition(Shortcut_to_RFX_UNIT_ATTRIBUTE_PRE,'DC_NBR',dcnbr,f'{raw}.RFX_UNIT_ATTRIBUTE_PRE')
Shortcut_to_RFX_UNIT_ATTRIBUTE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.RFX_UNIT_ATTRIBUTE_PRE')

# COMMAND ----------


