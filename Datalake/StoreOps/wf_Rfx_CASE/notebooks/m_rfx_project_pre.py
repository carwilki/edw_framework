# Databricks notebook source
# Code converted on 2023-08-25 11:51:13
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

#dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_project/')
#source_bucket = dbutils.widgets.get('source_bucket')

_bucket=getParameterValue(raw,'BA_StoreOps_Parameter.prm','BA_StoreOps.WF:wf_rfx_case','source_bucket')
source_bucket=_bucket+"rfx_project/"


# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_PROJECT, type SOURCE 
# COLUMN COUNT: 35

source_file = get_src_file('RFX_PROJECT', source_bucket)
#source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_project/20230907/'

csv_options = {
    "sep": ",",
    "header": True,           # The first row contains column names
   # "inferSchema": True,      # Infer column data types
    "quote": '\"',             # Specify the character used for quoting values
    "escape": '\"',             # Specify the character used for escaping special characters within quoted values
    "multiLine": True 
}

# Read the CSV file into a DataFrame

SQ_Shortcut_to_RFX_PROJECT = spark.read.csv(source_file, **csv_options)
if SQ_Shortcut_to_RFX_PROJECT.head() is None:
    df = spark.sql(f'TRUNCATE TABLE {raw}.RFX_PROJECT_PRE')
    dbutils.notebook.exit('file not available or empty')

# COMMAND ----------

# Rename columns
import re
# Function to transform column names
def transform_column_name(name):
    # Replace spaces with underscores
    name = name.replace(" ", "_")
    name = name.replace("(", "")
    name = name.replace(")", "")
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

SQ_Shortcut_to_RFX_PROJECT = rename_columns(SQ_Shortcut_to_RFX_PROJECT) 

# COMMAND ----------

# Processing node EXP_RFX_PROJECT_PRE, type EXPRESSION 
# COLUMN COUNT: 36

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_RFX_PROJECT_temp = SQ_Shortcut_to_RFX_PROJECT.toDF(*["SQ_Shortcut_to_RFX_PROJECT___" + col for col in SQ_Shortcut_to_RFX_PROJECT.columns])

EXP_RFX_PROJECT_PRE = SQ_Shortcut_to_RFX_PROJECT_temp.selectExpr(
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_ID as PROJECT_ID",
	"SQ_Shortcut_to_RFX_PROJECT___UNIT_ID as UNIT_ID",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_TYPE as PROJECT_TYPE",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_TITLE as PROJECT_TITLE",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_PRIORITY_CODE as PROJECT_PRIORITY_CODE",
	"SQ_Shortcut_to_RFX_PROJECT___CREATOR_USER_ID as CREATOR_USER_ID",
	"SQ_Shortcut_to_RFX_PROJECT___CREATOR as CREATOR",
	"SQ_Shortcut_to_RFX_PROJECT___EXECUTION_ASSIGNED_ROLE as EXECUTION_ASSIGNED_ROLE",
	"SQ_Shortcut_to_RFX_PROJECT___EXECUTION_ASSIGNED_DEPT as EXECUTION_ASSIGNED_DEPT",
	"SQ_Shortcut_to_RFX_PROJECT___EXECUTION_ASSIGNED_USER_ID as EXECUTION_ASSIGNED_USER_ID",
	"SQ_Shortcut_to_RFX_PROJECT___EXECUTION_ASSIGNED_USER as EXECUTION_ASSIGNED_USER",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_EXECUTION_START_DATE as PROJECT_EXECUTION_START_DATE",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_EXECUTION_END_DATE as PROJECT_EXECUTION_END_DATE",
	"SQ_Shortcut_to_RFX_PROJECT___STORE_PROJECT_STATUS as STORE_PROJECT_STATUS",
	"SQ_Shortcut_to_RFX_PROJECT___ESTIMATED_EFFORT_HOURS as ESTIMATED_EFFORT_HOURS",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_EXECUTION_COMPLETION_DATE as PROJECT_EXECUTION_COMPLETION_DATE",
	"SQ_Shortcut_to_RFX_PROJECT___ON_TIME_COMPLETION_FLAG as ON_TIME_COMPLETION_FLAG",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_STATUS as PROJECT_STATUS",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_LAUNCH_DATE as PROJECT_LAUNCH_DATE",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_START_DATE as PROJECT_START_DATE",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_END_DATE as PROJECT_END_DATE",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_CREATION_DATE as PROJECT_CREATION_DATE",
	"SQ_Shortcut_to_RFX_PROJECT___CREATOR_UNIT as CREATOR_UNIT",
	"SQ_Shortcut_to_RFX_PROJECT___CREATOR_DEPT as CREATOR_DEPT",
	"SQ_Shortcut_to_RFX_PROJECT___UNIT_ORG_LEVEL as UNIT_ORG_LEVEL",
	"SQ_Shortcut_to_RFX_PROJECT___CONFIDENTIAL as CONFIDENTIAL",
	"SQ_Shortcut_to_RFX_PROJECT___TASK_COUNT as TASK_COUNT",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_COMPLETION_DATE as PROJECT_COMPLETION_DATE",
	"SQ_Shortcut_to_RFX_PROJECT___RELEASE_USER_ID as RELEASE_USER_ID",
	"SQ_Shortcut_to_RFX_PROJECT___RELEASE_USER as RELEASE_USER",
	"SQ_Shortcut_to_RFX_PROJECT___WORKLOAD as WORKLOAD",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_ASSIGNED_ROLE as PROJECT_ASSIGNED_ROLE",
	"SQ_Shortcut_to_RFX_PROJECT___PROJECT_ASSIGNED_DEPT as PROJECT_ASSIGNED_DEPT",
	"SQ_Shortcut_to_RFX_PROJECT___LAST_UPDATED_USER_ID as LAST_UPDATED_USER_ID",
	"SQ_Shortcut_to_RFX_PROJECT___LAST_UPDATED_USER as LAST_UPDATED_USER",
	"CURRENT_TIMESTAMP as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node Shortcut_to_RFX_PROJECT_PRE, type TARGET 
# COLUMN COUNT: 36


Shortcut_to_RFX_PROJECT_PRE = EXP_RFX_PROJECT_PRE.selectExpr(
	"CAST(PROJECT_ID AS STRING) as PROJECT_ID",
	"CAST(UNIT_ID AS STRING) as UNIT_ID",
	"CAST(PROJECT_TYPE AS STRING) as PROJECT_TYPE",
	"CAST(PROJECT_TITLE AS STRING) as PROJECT_TITLE",
	"CAST(PROJECT_PRIORITY_CODE AS SMALLINT) as PROJECT_PRIORITY_CODE",
	"CAST(CREATOR_USER_ID AS STRING) as CREATOR_USER_ID",
	"CAST(CREATOR AS STRING) as CREATOR",
	"CAST(EXECUTION_ASSIGNED_ROLE AS STRING) as EXECUTION_ASSIGNED_ROLE",
	"CAST(EXECUTION_ASSIGNED_DEPT AS STRING) as EXECUTION_ASSIGNED_DEPT",
	"CAST(EXECUTION_ASSIGNED_USER_ID AS STRING) as EXECUTION_ASSIGNED_USER_ID",
	"CAST(EXECUTION_ASSIGNED_USER AS STRING) as EXECUTION_ASSIGNED_USER",
	"CAST(PROJECT_EXECUTION_START_DATE AS TIMESTAMP) as PROJECT_EXECUTION_START_DATE",
	"CAST(PROJECT_EXECUTION_END_DATE AS TIMESTAMP) as PROJECT_EXECUTION_END_DATE",
	"CAST(STORE_PROJECT_STATUS AS STRING) as STORE_PROJECT_STATUS",
	"CAST(ESTIMATED_EFFORT_HOURS AS DECIMAL(7,2)) as ESTIMATED_EFFORT_IN_HOURS",
	"CAST(PROJECT_EXECUTION_COMPLETION_DATE AS TIMESTAMP) as PROJECT_EXECUTION_COMPLETION_DATE",
	"CAST(ON_TIME_COMPLETION_FLAG AS STRING) as ON_TIME_COMPLETION_FLAG",
	"CAST(PROJECT_STATUS AS STRING) as PROJECT_STATUS",
	"CAST(PROJECT_LAUNCH_DATE AS TIMESTAMP) as PROJECT_LAUNCH_DATE",
	"CAST(PROJECT_START_DATE AS TIMESTAMP) as PROJECT_START_DATE",
	"CAST(PROJECT_END_DATE AS TIMESTAMP) as PROJECT_END_DATE",
	"CAST(PROJECT_CREATION_DATE AS TIMESTAMP) as PROJECT_CREATION_DATE",
	"CAST(CREATOR_UNIT AS STRING) as CREATOR_UNIT",
	"CAST(CREATOR_DEPT AS STRING) as CREATOR_DEPT",
	"CAST(UNIT_ORG_LEVEL AS STRING) as UNIT_ORG_LEVEL",
	"CAST(CONFIDENTIAL AS SMALLINT) as CONFIDENTIAL_FLAG",
	"CAST(TASK_COUNT AS SMALLINT) as TASK_COUNT",
	"CAST(PROJECT_COMPLETION_DATE AS TIMESTAMP) as PROJECT_COMPLETION_DATE",
	"CAST(RELEASE_USER_ID AS STRING) as RELEASE_USER_ID",
	"CAST(RELEASE_USER AS STRING) as RELEASE_USER",
	"CAST(WORKLOAD AS SMALLINT) as WORKLOAD_FLAG",
	"CAST(PROJECT_ASSIGNED_ROLE AS STRING) as PROJECT_ASSIGNED_ROLE",
	"CAST(PROJECT_ASSIGNED_DEPT AS STRING) as PROJECT_ASSIGNED_DEPT",
	"CAST(LAST_UPDATED_USER_ID AS STRING) as LAST_UPDATED_USER_ID",
	"CAST(LAST_UPDATED_USER AS STRING) as LAST_UPDATED_USER",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
# overwriteDeltaPartition(Shortcut_to_RFX_PROJECT_PRE,'DC_NBR',dcnbr,f'{raw}.RFX_PROJECT_PRE')
Shortcut_to_RFX_PROJECT_PRE.write.mode("overwrite").saveAsTable(f'{raw}.RFX_PROJECT_PRE')

# COMMAND ----------


