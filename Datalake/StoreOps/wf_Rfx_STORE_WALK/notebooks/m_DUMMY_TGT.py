# Databricks notebook source
#Code converted on 2023-08-24 12:25:55
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

dbutils.widgets.text(name='target_bucket', defaultValue='/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/')
target_bucket = dbutils.widgets.get('target_bucket')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQ_Shortcut_to_RFX_RTM_LOOK_UP, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_RFX_RTM_LOOK_UP = spark.sql(f"""SELECT RFX_LOOKUP_TYPE_CD FROM {refine}.RFX_RTM_LOOK_UP WHERE 1=2""")


# COMMAND ----------

# Processing node Shortcut_to_DUMMY_TGT, type TARGET 
# COLUMN COUNT: 1


Shortcut_to_DUMMY_TGT = SQ_Shortcut_to_RFX_RTM_LOOK_UP.selectExpr(
	"CAST(RFX_LOOKUP_TYPE_CD AS STRING) as DUMMY_COL"
)

_target_path = os.path.join(target_bucket, 'shortcut_to_dummy_tgt.out')
Shortcut_to_DUMMY_TGT.write.mode('overwrite').text(_target_path)

# COMMAND ----------


