# Databricks notebook source
# Code converted on 2023-09-05 14:08:55
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

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/masterdata/employee/ztb_holiday_sto/')
# source_bucket = dbutils.widgets.get('source_bucket')

source_bucket=getParameterValue(raw,'INT_Labor_Mgmt_Parameter.prm','INT_Labor_Mgmt.WF:wf_LMS_Holiday_Freight.M:m_ZTB_Holiday_STO_pre','source_bucket')

def get_source_file(key, _bucket):
  import builtins

  lst = dbutils.fs.ls(_bucket)
  fldr = builtins.max(lst, key=lambda x: x.name).name
  _path = os.path.join(_bucket, fldr)
  lst = dbutils.fs.ls(_path)
  files = [x.path for x in lst if x.name.startswith(key)]
  return files[0] if files else None

file_path = get_source_file('ztb_holiday_sto',source_bucket)

#file_path = "gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/masterdata/employee/ztb_holiday_sto/20230907/ztb_holiday_sto_20230907.dat"

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file")

SQ_Shortcut_to_ZTB_HOLIDAY_STO = spark.read.csv(file_path, header=True, sep="|")
# .selectExp(
#   "VENDOR_ID AS VENDOR_ID",
# )



# COMMAND ----------

# Processing node Shortcut_to_ZTB_HOLIDAY_STO_PRE, type TARGET 
# COLUMN COUNT: 10

Shortcut_to_ZTB_HOLIDAY_STO_PRE = SQ_Shortcut_to_ZTB_HOLIDAY_STO.selectExpr(
	"CAST(CLIENT AS STRING) as CLIENT",
	"CAST(PO_NBR AS BIGINT) as PO_NBR",
	"CAST(VENDOR_ID AS BIGINT) as VENDOR_ID",
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(PURCH_GROUP_ID AS INT) as PURCH_GROUP_ID",
	"CAST(STO_TYPE AS STRING) as STO_TYPE",
	"TO_TIMESTAMP(DELIVERY_DATE, 'MM/dd/yyyy HH:mm:ss') as DELIVERY_DATE",
	"TO_TIMESTAMP(EXE_SENT_DATE, 'MM/dd/yyyy HH:mm:ss') as EXE_SENT_DATE",
	"TO_TIMESTAMP(TMS_SENT_DATE, 'MM/dd/yyyy HH:mm:ss') as TMS_SENT_DATE",
	"CAST(TMS_PROCESSED AS TINYINT) as TMS_PROCESSED"
)
Shortcut_to_ZTB_HOLIDAY_STO_PRE.write.mode("overwrite").saveAsTable(f'{raw}.ZTB_HOLIDAY_STO_PRE')

# COMMAND ----------


