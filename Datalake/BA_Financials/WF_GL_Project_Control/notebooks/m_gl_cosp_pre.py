# Databricks notebook source
#Code converted on 2023-09-08 09:28:26
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

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/financials/cosp/')
# source_bucket = dbutils.widgets.get('source_bucket')
source_bucket_cosp=getParameterValue(raw,'BA_Financials_Parameter.prm','BA_BA_Financials.WF:wf_GL_Project_Control','source_bucket_cosp')


  
source_file = get_src_file('COSP', source_bucket_cosp)

SQ_Shortcut_to_COSP = spark.read.csv(source_file, sep='|', header=True).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_COSP, type SOURCE 
# COLUMN COUNT: 17

# _sql = f"""
# SELECT objnr
#       ,gjahr
#       ,wrttp
#       ,kstar
#       ,twaer
#       ,wtg001
#       ,wtg002
#       ,wtg003
#       ,wtg004
#       ,wtg005
#       ,wtg006
#       ,wtg007
#       ,wtg008
#       ,wtg009
#       ,wtg010
#       ,wtg011
#       ,wtg012
#   FROM SAPPR3.cosp
#  WHERE mandt = '100'
#    AND gjahr > CAST(EXTRACT(YEAR FROM SYSDATE) - 2 AS STRING)
# """


# COMMAND ----------

# Processing node Shortcut_to_GL_COSP_PRE, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_GL_COSP_PRE = SQ_Shortcut_to_COSP.selectExpr(
	"CAST(OBJNR AS STRING) as OBJNR",
	"CAST(GJAHR AS STRING) as GJAHR",
	"CAST(WRTTP AS STRING) as WRTTP",
	"CAST(KSTAR AS STRING) as KSTAR",
	"CAST(TWAER AS STRING) as TWAER",
	"CAST(WTG001 AS DECIMAL(15,2)) as WTG001",
	"CAST(WTG002 AS DECIMAL(15,2)) as WTG002",
	"CAST(WTG003 AS DECIMAL(15,2)) as WTG003",
	"CAST(WTG004 AS DECIMAL(15,2)) as WTG004",
	"CAST(WTG005 AS DECIMAL(15,2)) as WTG005",
	"CAST(WTG006 AS DECIMAL(15,2)) as WTG006",
	"CAST(WTG007 AS DECIMAL(15,2)) as WTG007",
	"CAST(WTG008 AS DECIMAL(15,2)) as WTG008",
	"CAST(WTG009 AS DECIMAL(15,2)) as WTG009",
	"CAST(WTG010 AS DECIMAL(15,2)) as WTG010",
	"CAST(WTG011 AS DECIMAL(15,2)) as WTG011",
	"CAST(WTG012 AS DECIMAL(15,2)) as WTG012"
)
Shortcut_to_GL_COSP_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_COSP_PRE')

# COMMAND ----------


