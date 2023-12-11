# Databricks notebook source
#Code converted on 2023-09-08 09:28:27
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

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/financials/bpge/')
# source_bucket = dbutils.widgets.get('source_bucket')
source_bucket_bpge=getParameterValue(raw,'BA_Financials_Parameter.prm','BA_BA_Financials.WF:wf_GL_Project_Control','source_bucket_bpge')

  
source_file = get_src_file('BPGE', source_bucket_bpge)

SQ_Shortcut_to_BPGE = spark.read.csv(source_file, sep='|', header=True).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_BPGE, type SOURCE 
# COLUMN COUNT: 5

# _sql = f"""
# SELECT objnr
#       ,wrttp
#       ,vorga
#       ,twaer
#       ,wtges
#   FROM SAPPR3.bpge
#  WHERE mandt = '100'
#    AND lednr = '0001'
# """


# COMMAND ----------

# Processing node Shortcut_to_GL_BPGE_PRE, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_GL_BPGE_PRE = SQ_Shortcut_to_BPGE.selectExpr(
	"CAST(OBJNR AS STRING) as OBJNR",
	"CAST(WRTTP AS STRING) as WRTTP",
	"CAST(VORGA AS STRING) as VORGA",
	"CAST(TWAER AS STRING) as TWAER",
	"CAST(WTGES AS DECIMAL(15,2)) as WTGES"
)
Shortcut_to_GL_BPGE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_BPGE_PRE')

# COMMAND ----------


