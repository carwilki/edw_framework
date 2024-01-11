# Databricks notebook source
#Code converted on 2023-09-12 13:31:07
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

# Processing node SQ_Shortcut_to_ZTB_POG_GRP_RLS, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 7

# Read from SAP file

# COMMAND ----------

parameter_file_name='INT_Merchandising_Parameter.prm'
parameter_section='INT_Merchandising.WF:wf_IKB_ODS_Daily'
parameter_key='source_bucket'

source_bucket=getParameterValue(raw,parameter_file_name,parameter_section,parameter_key)
key=getParameterValue(raw,parameter_file_name,parameter_section,'key')
source_file = get_src_file(key, source_bucket)

SQ_Shortcut_to_ZTB_POG_GRP_RLS = spark.read.csv(source_file, sep='|', header=True)

# COMMAND ----------

# Processing node Shortcut_to_ZTB_POG_GRP_RLS_PRE, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_ZTB_POG_GRP_RLS_PRE = SQ_Shortcut_to_ZTB_POG_GRP_RLS.selectExpr(
	"CAST(MANDT AS STRING) as MANDT",
	"CAST(POG_GROUP AS STRING) as POG_GROUP",
	"CAST(SALES_ORG AS STRING) as SALES_ORG",
	"CAST(FLRPLN_EXCLUSION AS STRING) as FLRPLN_EXCLUSION",
	"CAST(CREATED_ON AS TIMESTAMP) as CREATED_ON",
	"CAST(CREATED_TIME AS TIMESTAMP) as CREATED_TIME",
	"CAST(CREATED_BY AS STRING) as CREATED_BY"
)
Shortcut_to_ZTB_POG_GRP_RLS_PRE.write.mode("overwrite").saveAsTable(f'{raw}.ZTB_POG_GRP_RLS_PRE')

# COMMAND ----------


