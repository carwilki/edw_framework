# Databricks notebook source
#Code converted on 2023-09-26 09:20:10
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

# Processing node SQ_Shortcut_to_ZTB_RF_PHYINV, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 11

source_bucket=getParameterValue(raw,'wf_inv_physical.prm','BA_Inventory.WF:wf_inv_physical.M:m_ZTB_RF_PHYINV_Pre','source_bucket')
source_file = get_src_file('ZTB_RF_PHYINV', source_bucket)

print(source_file)

# COMMAND ----------

SQ_Shortcut_to_ZTB_RF_PHYINV = spark.read.csv(source_file, sep='|', header=True)

# COMMAND ----------

# Processing node Shortcut_to_ZTB_RF_PHYINV_PRE, type TARGET 
# COLUMN COUNT: 11


Shortcut_to_ZTB_RF_PHYINV_PRE = SQ_Shortcut_to_ZTB_RF_PHYINV.selectExpr(
	"CAST(MANDT AS STRING) as MANDT",
	"CAST(SITE AS STRING) as SITE",
	"CAST(LOCATION AS INT) as LOCATION",
	"CAST(UPC AS STRING) as UPC",
	"CAST(ARTICLE AS STRING) as ARTICLE",
	"CAST(DESCRIPTION AS STRING) as DESCRIPTION",
	"CAST(COUNT_QTY AS INT) as COUNT_QTY",
	"CAST(STATUS AS STRING) as STATUS",
	"CAST(USER_NAME AS STRING) as USER_NAME",
  	"TO_TIMESTAMP(CREATE_DATE, 'MM/dd/yyyy HH:mm:ss') as CREATE_DATE",
	"TO_TIMESTAMP(POST_DATE, 'MM/dd/yyyy HH:mm:ss') as POST_DATE"
)

Shortcut_to_ZTB_RF_PHYINV_PRE.write.mode("overwrite").saveAsTable(f'{raw}.ZTB_RF_PHYINV_PRE')
