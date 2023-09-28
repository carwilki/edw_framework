# Databricks notebook source
# Code converted on 2023-09-05 14:08:54
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

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/masterdata/employee/ztb_event_hold/')
# source_bucket = dbutils.widgets.get('source_bucket')

_bucket=getParameterValue(raw,'INT_Labor_Mgmt_Parameter.prm','INT_Labor_Mgmt.WF:wf_LMS_Holiday_Freight','source_bucket')
source_bucket=_bucket+"ztb_event_hold/"

def get_source_file(key, _bucket):
  import builtins

  lst = dbutils.fs.ls(_bucket)
  fldr = builtins.max(lst, key=lambda x: x.name).name
  _path = os.path.join(_bucket, fldr)
  lst = dbutils.fs.ls(_path)
  files = [x.path for x in lst if x.name.startswith(key)]
  return files[0] if files else None

file_path = get_source_file('ztb_event_hold',source_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file")

Shortcut_to_ZTB_EVENT_HOLD_PRE = spark.read.csv(file_path, header=True, sep="|")


# COMMAND ----------

# Processing node Shortcut_to_ZTB_EVENT_HOLD_PRE, type TARGET 
# COLUMN COUNT: 5

Shortcut_to_ZTB_EVENT_HOLD_PRE = Shortcut_to_ZTB_EVENT_HOLD_PRE.selectExpr(
	"CAST(MANDT AS STRING) as MANDT",
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(STO_TYPE AS STRING) as STO_TYPE",
	"CAST(SKU_NBR AS INT) as SKU_NBR",
	"CAST(QUANTITY AS DECIMAL(13,3)) as QUANTITY"
)
Shortcut_to_ZTB_EVENT_HOLD_PRE.write.mode("overwrite").saveAsTable(f'{raw}.ZTB_EVENT_HOLD_PRE')

# COMMAND ----------


