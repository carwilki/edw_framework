# Databricks notebook source
# Code converted on 2023-08-25 11:51:06
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

#dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_unit_hierarchy/')
#source_bucket = dbutils.widgets.get('source_bucket')

_bucket=getParameterValue(raw,'BA_StoreOps_Parameter.prm','BA_StoreOps.WF:wf_rfx_case','source_bucket')
source_bucket=_bucket+"rfx_unit_hierarchy/"

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_UNIT_HRCY, type SOURCE 
# COLUMN COUNT: 24

source_file = get_source_file_rfx('RFX_UNIT_HIERARCHY', source_bucket)
#source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_unit_hierarchy/20230907/'

csv_options = {
    "sep": ",",
    "header": True,           # The first row contains column names
   # "inferSchema": True,      # Infer column data types
    "quote": '\"',             # Specify the character used for quoting values
    "escape": '\"',             # Specify the character used for escaping special characters within quoted values
    "multiLine": True 
}

# Read the CSV file into a DataFrame

SQ_Shortcut_to_RFX_UNIT_HRCY = spark.read.csv(source_file, **csv_options)

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

SQ_Shortcut_to_RFX_UNIT_HRCY = rename_columns(SQ_Shortcut_to_RFX_UNIT_HRCY) 

# COMMAND ----------

# Processing node EXP_RFX_UNIT_HRCY_PRE, type EXPRESSION 
# COLUMN COUNT: 25

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_UNIT_HRCY_temp = SQ_Shortcut_to_RFX_UNIT_HRCY.toDF(*["SQ_Shortcut_to_RFX_UNIT_HRCY___" + col for col in SQ_Shortcut_to_RFX_UNIT_HRCY.columns])

EXP_RFX_UNIT_HRCY_PRE = SQ_Shortcut_to_RFX_UNIT_HRCY_temp.selectExpr(
	"SQ_Shortcut_to_RFX_UNIT_HRCY___UNIT_ID as UNIT_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___UNIT_NAME as UNIT_NAME",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___STORE_ID as STORE_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___STORE_NAME as STORE_NAME",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___UNIT_ORG_LEVEL as UNIT_ORG_LEVEL",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___UNIT_STATUS as UNIT_STATUS",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___STATE_CODE as STATE_CODE",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___ZIP_CODE as ZIP_CODE",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___CORPORATE_ID as CORPORATE_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___CORPORATE_NAME as CORPORATE_NAME",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_2_ID as LEVEL_2_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_2_NAME as LEVEL_2_NAME",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_3_ID as LEVEL_3_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_3_NAME as LEVEL_3_NAME",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_4_ID as LEVEL_4_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_4_NAME as LEVEL_4_NAME",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_5_ID as LEVEL_5_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_5_NAME as LEVEL_5_NAME",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_6_ID as LEVEL_6_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_6_NAME as LEVEL_6_NAME",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_7_ID as LEVEL_7_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___LEVEL_7_NAME as LEVEL_7_NAME",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___DISTRICT_ID as DISTRICT_ID",
	"SQ_Shortcut_to_RFX_UNIT_HRCY___DISTRICT_NAME as DISTRICT_NAME",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RFX_UNIT_HRCY_PRE, type TARGET 
# COLUMN COUNT: 25


Shortcut_to_RFX_UNIT_HRCY_PRE = EXP_RFX_UNIT_HRCY_PRE.selectExpr(
	"CAST(UNIT_ID AS STRING) as UNIT_ID",
	"CAST(UNIT_NAME AS STRING) as UNIT_NAME",
	"CAST(STORE_ID AS STRING) as STORE_ID",
	"CAST(STORE_NAME AS STRING) as STORE_NAME",
	"CAST(UNIT_ORG_LEVEL AS SMALLINT) as UNIT_ORG_LEVEL",
	"CAST(UNIT_STATUS AS STRING) as UNIT_STATUS",
	"CAST(STATE_CODE AS STRING) as STATE_CODE",
	"CAST(ZIP_CODE AS STRING) as ZIP_CODE",
	"CAST(CORPORATE_ID AS STRING) as CORPORATE_ID",
	"CAST(CORPORATE_NAME AS STRING) as CORPORATE_NAME",
	"CAST(LEVEL_2_ID AS STRING) as LEVEL_2_ID",
	"CAST(LEVEL_2_NAME AS STRING) as LEVEL_2_NAME",
	"CAST(LEVEL_3_ID AS STRING) as LEVEL_3_ID",
	"CAST(LEVEL_3_NAME AS STRING) as LEVEL_3_NAME",
	"CAST(LEVEL_4_ID AS STRING) as LEVEL_4_ID",
	"CAST(LEVEL_4_NAME AS STRING) as LEVEL_4_NAME",
	"CAST(LEVEL_5_ID AS STRING) as LEVEL_5_ID",
	"CAST(LEVEL_5_NAME AS STRING) as LEVEL_5_NAME",
	"CAST(LEVEL_6_ID AS STRING) as LEVEL_6_ID",
	"CAST(LEVEL_6_NAME AS STRING) as LEVEL_6_NAME",
	"CAST(LEVEL_7_ID AS STRING) as LEVEL_7_ID",
	"CAST(LEVEL_7_NAME AS STRING) as LEVEL_7_NAME",
	"CAST(DISTRICT_ID AS STRING) as DISTRICT_ID",
	"CAST(DISTRICT_NAME AS STRING) as DISTRICT_NAME",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
# overwriteDeltaPartition(Shortcut_to_RFX_UNIT_HRCY_PRE,'DC_NBR',dcnbr,f'{raw}.RFX_UNIT_HRCY_PRE')
Shortcut_to_RFX_UNIT_HRCY_PRE.write.mode("overwrite").saveAsTable(f'{raw}.RFX_UNIT_HRCY_PRE')

# COMMAND ----------


