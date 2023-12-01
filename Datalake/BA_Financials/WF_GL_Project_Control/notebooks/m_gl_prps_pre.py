# Databricks notebook source
#Code converted on 2023-09-08 09:28:28
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

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/financials/prps/')
# source_bucket = dbutils.widgets.get('source_bucket')
source_bucket_prps =getParameterValue(raw,'BA_Financials_Parameter.prm','BA_BA_Financials.WF:wf_GL_Project_Control','source_bucket_prps')



  
source_file = get_src_file('PRPS', source_bucket_prps)

SQ_Shortcut_to_PRPS = spark.read.csv(source_file, sep='|', header=True).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_PRPS, type SOURCE 
# COLUMN COUNT: 17

# _sql = f"""
# SELECT pspnr
#       ,posid
#       ,post1
#       ,objnr
#       ,erdat
#       ,aedat
#       ,vernr
#       ,verna
#       ,astnr
#       ,astna
#       ,pbukr
#       ,pkokr
#       ,prctr
#       ,fkstl
#       ,pwpos
#       ,werks
#       ,zzmatkl
#   FROM {legacy}.sappr3.prps
#  WHERE mandt = '100'
#    AND (   TO_DATE (erdat, 'YYYYMMDD') > TRUNC (SYSDATE) - 7
#         OR    DECODE (TRIM(AEDAT), '00000000', TO_DATE(17990101, 'YYYYMMDD'), NULL,TO_DATE(17990101, 'YYYYMMDD'),TO_DATE (ERDAT, 'YYYYMMDD')) > TRUNC (SYSDATE) - 7
#        )
# """


# COMMAND ----------

# Processing node Shortcut_to_GL_PRPS_PRE, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_GL_PRPS_PRE = SQ_Shortcut_to_PRPS.selectExpr(
	"CAST(PSPNR AS STRING) as PSPNR",
	"CAST(OBJNR AS STRING) as OBJNR",
	"CAST(POSID AS STRING) as POSID",
	"CAST(POST1 AS STRING) as POST1",
	"CAST(ERDAT AS STRING) as ERDAT",
	"CAST(AEDAT AS STRING) as AEDAT",
	"CAST(PWPOS AS STRING) as PWPOS",
	"CAST(VERNR AS STRING) as VERNR",
	"CAST(VERNA AS STRING) as VERNA",
	"CAST(PBUKR AS STRING) as PBUKR",
	"CAST(PKOKR AS STRING) as PKOKR",
	"CAST(WERKS AS STRING) as WERKS",
	"CAST(PRCTR AS STRING) as PRCTR",
	"CAST(FKSTL AS STRING) as FKSTL",
	"CAST(ASTNR AS STRING) as ASTNR",
	"CAST(ASTNA AS STRING) as ASTNA",
	"CAST(ZZMATKL AS STRING) as ZZMATKL"
)
Shortcut_to_GL_PRPS_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_PRPS_PRE')

# COMMAND ----------


