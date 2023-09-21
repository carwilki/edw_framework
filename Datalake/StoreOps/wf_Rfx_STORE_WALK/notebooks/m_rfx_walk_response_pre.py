# Databricks notebook source
#Code converted on 2023-08-24 12:25:56
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

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

#dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_walk_response/')
#source_bucket = dbutils.widgets.get('source_bucket')

source_bucket = getParameterValue(raw,'BA_StoreOPS_Parameter.prm','BA_StoreOPS.WF:wf_Rfx_STORE_WALK.M:m_rfx_walk_response_pre','source_bucket')



def get_source_file(key, _bucket):
  import builtins

  lst = dbutils.fs.ls(_bucket)
  fldr = builtins.max(lst, key=lambda x: x.name).name
  lst = dbutils.fs.ls(_bucket + fldr)
  files = [x.path for x in lst if x.name.startswith(key)]
  return files[0] if files else None


# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_WALK_RESPONSE, type SOURCE 
# COLUMN COUNT: 20

source_file = get_source_file('RFX_WALK_RESPONSE', source_bucket)
#source_file = 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_walk_response/20230831/RFX_WALK_RESPONSE-en-us-120710099-20230831.csv'

csv_options = {
    "sep": ",",
    "header": True,           # The first row contains column names
    "inferSchema": True,      # Infer column data types
    "quote": '"',             # Specify the character used for quoting values
    "escape": '"'             # Specify the character used for escaping special characters within quoted values
}
# Read the CSV file into a DataFrame

SQ_Shortcut_to_RFX_WALK_RESPONSE = spark.read.csv(source_file, **csv_options)

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

SQ_Shortcut_to_RFX_WALK_RESPONSE = rename_columns(SQ_Shortcut_to_RFX_WALK_RESPONSE).filter("OPTION_SELECTED IS NOT NULL")

# COMMAND ----------

# Processing node Shortcut_to_RFX_WALK_RESPONSE_PRE, type TARGET 
# COLUMN COUNT: 21


Shortcut_to_RFX_WALK_RESPONSE_PRE = SQ_Shortcut_to_RFX_WALK_RESPONSE.selectExpr(
	"CAST(WALK_ID AS BIGINT) as WALK_ID",
	"CAST(Walk_Type_Id AS INT) as WALK_TYPE_ID",
	"CAST(Store_Id AS STRING) as STORE_ID",
	"CAST(Group_Handle AS STRING) as GROUP_HANDLE",
	"CAST(Question_Handle AS STRING) as QUESTION_HANDLE",
	"CAST(SUB_QUESTION_NO AS SMALLINT) as SUB_QUESTION_NO",
	"CAST(Response_Date AS TIMESTAMP) as RESPONSE_DATE",
	"CAST(Question_Text AS STRING) as QUESTION_TEXT",
	"CAST(Sub_Question_Text AS STRING) as SUB_QUESTION_TEXT",
	"CAST(Response_Type AS STRING) as RESPONSE_TYPE",
	"CAST(OPTION_SELECTED AS SMALLINT) as OPTION_SELECTED",
	"CAST(Option_Text AS STRING) as OPTION_TEXT",
	"CAST(Numeric_Value AS DECIMAL(22,4)) as NUMERIC_VALUE",
	"CAST(Text_Value AS STRING) as TEXT_VALUE",
	"CAST(Date_Value AS TIMESTAMP) as DATE_VALUE",
	"CAST(ATTACHMENT_COUNT AS SMALLINT) as ATTACHMENT_COUNT",
	"CAST(Comments AS STRING) as COMMENTS",
	"CAST(Points_Scored AS DECIMAL(7,2)) as POINTS_SCORED",
	"CAST(Total_Points AS DECIMAL(7,2)) as TOTAL_POINTS",
	"CAST(Target_Points AS DECIMAL(7,2)) as TARGET_POINTS",
	"CURRENT_TIMESTAMP() as LOAD_TSTMP"
)

Shortcut_to_RFX_WALK_RESPONSE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.RFX_WALK_RESPONSE_PRE')
