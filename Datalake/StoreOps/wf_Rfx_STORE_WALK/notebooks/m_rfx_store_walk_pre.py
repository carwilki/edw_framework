# Databricks notebook source
#Code converted on 2023-08-24 12:25:57
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

#dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_store_walk/')
#source_bucket = dbutils.widgets.get('source_bucket')
source_bucket = getParameterValue(raw,'BA_StoreOPS_Parameter.prm','BA_StoreOPS.WF:wf_Rfx_STORE_WALK.M:m_rfx_store_walk_pre','source_bucket')


def get_source_file(key, _bucket):
  import builtins

  lst = dbutils.fs.ls(_bucket)
  fldr = builtins.max(lst, key=lambda x: x.name).name
  lst = dbutils.fs.ls(_bucket + fldr)
  files = [x.path for x in lst if x.name.startswith(key)]
  return files[0] if files else None


# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_STORE_WALK, type SOURCE 
# COLUMN COUNT: 17

source_file = get_source_file('RFX_STORE_WALK', source_bucket)

#source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/nas/storeops/rfx_store_walk/20230831/RFX_STORE_WALK-en-us-120710099-20230831.csv'

csv_options = {
    "sep": ",",
    "header": True,           # The first row contains column names
    "inferSchema": True,      # Infer column data types
    "quote": '"',             # Specify the character used for quoting values
    "escape": '"'             # Specify the character used for escaping special characters within quoted values
}
# Read the CSV file into a DataFrame

SQ_Shortcut_to_RFX_STORE_WALK = spark.read.csv(source_file, **csv_options)



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

SQ_Shortcut_to_RFX_STORE_WALK = rename_columns(SQ_Shortcut_to_RFX_STORE_WALK) 

# COMMAND ----------

# Processing node Shortcut_to_RFX_STORE_WALK_PRE, type TARGET 
# COLUMN COUNT: 18


Shortcut_to_RFX_STORE_WALK_PRE = SQ_Shortcut_to_RFX_STORE_WALK.selectExpr(
	"CAST(WALK_ID AS BIGINT) as WALK_ID",
	"CAST(Store_Id AS STRING) as STORE_ID",
	"CAST(Store_Name AS STRING) as STORE_NAME",
	"CAST(Walk_Date_Time AS TIMESTAMP) as WALK_DATE_TIME",
	"CAST(Submit_Date_Time AS TIMESTAMP) as SUBMISSION_DATE_TIME",
	"CAST(Walk_Type_Id AS INT) as WALK_TYPE_ID",
	"CAST(Walk_Type AS STRING) as WALK_TYPE",
	"CAST(Walk_Participants AS STRING) as WALK_PARTICIPANTS",
	"CAST(Pass_Status AS STRING) as PASS_STATUS",
	"CAST(Points_Scored AS DECIMAL(7,2)) as POINTS_SCORED",
	"CAST(Total_Points AS DECIMAL(7,2)) as TOTAL_POINTS",
	"CAST(Target_Points AS DECIMAL(7,2)) as TARGET_POINTS",
	"CAST(Score_pct AS DECIMAL(5,2)) as SCORE_PCT",
	"CAST(TASK_COUNT AS SMALLINT) as TASK_COUNT",
	"CAST(Walk_Owner_Id AS STRING) as WALK_OWNER_ID",
	"CAST(Walk_Owner AS STRING) as WALK_OWNER",
	"CAST(Walk_Project_Id AS STRING) as WALK_PROJECT_ID",
	"CURRENT_TIMESTAMP() as LOAD_TSTMP"
)
Shortcut_to_RFX_STORE_WALK_PRE.write.mode("overwrite").saveAsTable(f'{raw}.RFX_STORE_WALK_PRE')

# COMMAND ----------


