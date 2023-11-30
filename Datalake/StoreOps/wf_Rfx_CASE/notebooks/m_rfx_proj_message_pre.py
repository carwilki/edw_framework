# Databricks notebook source
# Code converted on 2023-08-25 11:51:08
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

#dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_proj_message/')
#source_bucket = dbutils.widgets.get('source_bucket')


_bucket=getParameterValue(raw,'BA_StoreOps_Parameter.prm','BA_StoreOps.WF:wf_rfx_case','source_bucket')
source_bucket=_bucket+"rfx_proj_message/"


# COMMAND ----------


# Processing node SQ_Shortcut_to_RFX_LOOK_UP, type SOURCE 
# COLUMN COUNT: 3

source_file = get_src_file('RFX_PROJ_MESSAGE', source_bucket)
#source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_proj_message/20230907/'


csv_options = {
    "sep": ",",
    "header": True,           # The first row contains column names
    #"inferSchema": True,      # Infer column data types
    "quote": '\"',             # Specify the character used for quoting values
    "escape": '\"',             # Specify the character used for escaping special characters within quoted values
    "multiLine": True 
}


# Read the CSV file into a DataFrame

SQ_Shortcut_to_RFX_PROJ_MESSAGE = spark.read.csv(source_file, **csv_options)

if SQ_Shortcut_to_RFX_PROJ_MESSAGE.head() is None:
    df = spark.sql(f'TRUNCATE TABLE {raw}.RFX_PROJ_MESSAGE_PRE')
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

SQ_Shortcut_to_RFX_PROJ_MESSAGE = rename_columns(SQ_Shortcut_to_RFX_PROJ_MESSAGE) 

# COMMAND ----------

# Processing node EXP_RFX_PROJ_MESSAGE_PRE, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_PROJ_MESSAGE_temp = SQ_Shortcut_to_RFX_PROJ_MESSAGE.toDF(*["SQ_Shortcut_to_RFX_PROJ_MESSAGE___" + col for col in SQ_Shortcut_to_RFX_PROJ_MESSAGE.columns])

EXP_RFX_PROJ_MESSAGE_PRE = SQ_Shortcut_to_RFX_PROJ_MESSAGE_temp.selectExpr(
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___PROJECT_ID as PROJECT_ID",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___UNIT_ID as UNIT_ID",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___UNIT_ORG_LEVEL as UNIT_ORG_LEVEL",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___MESSAGE_DATE as MESSAGE_DATE",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___USER_ID as USER_ID",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___USER_NAME as USER_NAME",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___MESSAGE_TEXT as MESSAGE_TEXT",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___MESSAGE_TYPE as MESSAGE_TYPE",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___MESSAGE_TYPE_DESC as MESSAGE_TYPE_DESC",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___MESSAGE_STATUS as MESSAGE_STATUS",
	"SQ_Shortcut_to_RFX_PROJ_MESSAGE___RECORD_TYPE as RECORD_TYPE",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RFX_PROJ_MESSAGE_PRE, type TARGET 
# COLUMN COUNT: 12


Shortcut_to_RFX_PROJ_MESSAGE_PRE = EXP_RFX_PROJ_MESSAGE_PRE.selectExpr(
	"CAST(PROJECT_ID AS STRING) as PROJECT_ID",
	"CAST(UNIT_ID AS STRING) as UNIT_ID",
	"CAST(UNIT_ORG_LEVEL AS smallint) as UNIT_ORG_LEVEL",
	"CAST(MESSAGE_DATE AS TIMESTAMP) as MESSAGE_DATE",
	"CAST(USER_ID AS STRING) as USER_ID",
	"CAST(USER_NAME AS STRING) as USER_NAME",
	"CAST(MESSAGE_TEXT AS STRING) as MESSAGE_TEXT",
	"CAST(MESSAGE_TYPE AS STRING) as MESSAGE_TYPE",
	"CAST(MESSAGE_TYPE_DESC AS STRING) as MESSAGE_TYPE_DESC",
	"CAST(MESSAGE_STATUS AS STRING) as MESSAGE_STATUS",
	"CAST(RECORD_TYPE AS STRING) as RECORD_TYPE",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
# overwriteDeltaPartition(Shortcut_to_RFX_PROJ_MESSAGE_PRE,'DC_NBR',dcnbr,f'{raw}.RFX_PROJ_MESSAGE_PRE')
Shortcut_to_RFX_PROJ_MESSAGE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.RFX_PROJ_MESSAGE_PRE')

# COMMAND ----------


