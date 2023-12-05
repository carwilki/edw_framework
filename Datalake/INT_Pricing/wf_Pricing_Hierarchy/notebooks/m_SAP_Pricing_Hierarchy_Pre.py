# Databricks notebook source
#Code converted on 2023-09-19 11:15:48
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

# Processing node EXP_BUS_LOGIC, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 18

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/pricing/sap_pricing_hierarchy_pre/')
# source_bucket = dbutils.widgets.get('source_bucket')

source_bucket=getParameterValue(raw,'wf_Pricing_Hierarchy.prm','INT_Pricing.WF:wf_Pricing_Hierarchy.M:m_SAP_Pricing_Hierarchy_Pre','source_bucket')
source_file = get_src_file('SAP_PRICING_HIERARCHY_PRE', source_bucket)
print(source_file)


# COMMAND ----------

EXP_BUS_LOGIC = spark.read.csv(source_file, sep='|', header=True)

# COMMAND ----------

Shortcut_to_SAP_PRICING_HIERARCHY_PRE = EXP_BUS_LOGIC.selectExpr(
	"CAST(HIER_NODE AS STRING) as HIER_NODE",
	"TO_DATE(START_DT, 'MM/dd/yyyy HH:mm:ss') as START_DT",
	"TO_DATE(END_DT, 'MM/dd/yyyy HH:mm:ss') as END_DT",
	"CAST(HIER_LEVEL AS TINYINT) as HIER_LEVEL",
	"CAST(PARENT_NODE AS STRING) as PARENT_NODE",
	"CAST(CATFLG AS STRING) as CATFLG",
	"CAST(ROLE AS STRING) as ROLE",
	"CAST(STRATEGY AS STRING) as STRATEGY",
	"CAST(PRODUCTCLF AS STRING) as PRODUCTCLF",
	"CAST(PRICE_GROUP AS STRING) as PRICE_GROUP",
	"CAST(REFERENCE_NODE AS STRING) as REFERENCE_NODE",
	"CAST(RESPONSABILITY AS STRING) as RESPONSABILITY",
	"CAST(NODE_DESC AS STRING) as NODE_DESC",
	"CAST(UPPER_DESC AS STRING) as UPPER_DESC",
	"CAST(LONG_DESC AS STRING) as LONG_DESC",
	"TO_TIMESTAMP(LOAD_TSTMP, 'MM/dd/yyyy HH:mm:ss') as LOAD_TSTMP"
)
Shortcut_to_SAP_PRICING_HIERARCHY_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SAP_PRICING_HIERARCHY_PRE')

# COMMAND ----------


