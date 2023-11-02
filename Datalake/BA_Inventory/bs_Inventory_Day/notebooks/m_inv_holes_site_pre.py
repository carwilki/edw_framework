# Databricks notebook source
#Code converted on 2023-09-15 14:55:22
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


# COMMAND ----------

# Processing node SQ_ztb_rf_holes, type SOURCE 
# COLUMN COUNT: 6

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/inventory/ztb_rf_holes/')
# source_bucket = dbutils.widgets.get('source_bucket')

# _bucket=getParameterValue(raw,'BA_Inventory_Parameter.prm','BA_Inventory.WF:bs_Inventory_Day','source_bucket')
# source_bucket=_bucket+"ztb_rf_holes/"

# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_LOOK_UP, type SOURCE 
# COLUMN COUNT: 3

_bucket=getParameterValue(raw,'BA_Inventory_Parameter.prm','BA_Inventory.WF:bs_Inventory_Day','source_bucket')
source_bucket=_bucket+"ztb_rf_holes/"

def get_src_file(key, _bucket):
    import builtins
    lst = dbutils.fs.ls(_bucket)
    dirs = [item for item in lst if item.isDir()]
    fldr = builtins.max(dirs, key=lambda x: x.name).name
    lst = dbutils.fs.ls(_bucket + fldr)
    files = [x.path for x in lst if x.name.lower().startswith(key.lower())]
    return files[0] if files else None
source_file = get_src_file('ZTB_RF_HOLES', source_bucket)
# source_file = get_source_file_rfx('ZTB_RF_HOLES', source_bucket)
# def get_source_file(key, _bucket):
#   import builtins

#   lst = dbutils.fs.ls(_bucket)
#   fldr = builtins.max(lst, key=lambda x: x.name).name
#   lst = dbutils.fs.ls(_bucket + fldr)
#   print(lst)
#   files = [x.path for x in lst if x.name.startswith(key)]
#   return files[0] if files else None

# source_file = get_source_file('ZTB_RF_HOLES', source_bucket)

SQ_ztb_rf_holes = spark.read.csv(source_file, sep='|', header=True, quote='"').withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Conforming fields names to the component layout
SQ_ztb_rf_holes = SQ_ztb_rf_holes \
	.withColumnRenamed(SQ_ztb_rf_holes.columns[0],'create_date') \
	.withColumnRenamed(SQ_ztb_rf_holes.columns[1],'create_time') \
	.withColumnRenamed(SQ_ztb_rf_holes.columns[2],'SKU_NBR') \
	.withColumnRenamed(SQ_ztb_rf_holes.columns[3],'STORE_NBR') \
	.withColumnRenamed(SQ_ztb_rf_holes.columns[4],'on_hand_qty') \
	.withColumnRenamed(SQ_ztb_rf_holes.columns[5],'user_name')

# COMMAND ----------

# Processing node EXP_conv_dat_type, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6

EXP_conv_dat_type = SQ_ztb_rf_holes.selectExpr(
	"sys_row_id",
	"create_time",
	"on_hand_qty",
	"user_name",
	"to_utc_timestamp(from_unixtime(unix_timestamp(create_date, 'MM/dd/yyyy HH:mm:ss')), 'UTC') as DAY_DT",
	"cast(SKU_NBR as int) as SKU_NBR",
	"cast(STORE_NBR as int) as STORE_NBR"
)

# COMMAND ----------

# Processing node Shortcut_to_INV_HOLES_SITE_PRE, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_INV_HOLES_SITE_PRE = EXP_conv_dat_type.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(create_time AS STRING) as CREATE_TIME",
	"CAST(SKU_NBR AS INT) as SKU_NBR",
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(on_hand_qty AS INT) as ON_HAND_QTY",
	"CAST(user_name AS STRING) as USER_NAME"
)
Shortcut_to_INV_HOLES_SITE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.INV_HOLES_SITE_PRE')
