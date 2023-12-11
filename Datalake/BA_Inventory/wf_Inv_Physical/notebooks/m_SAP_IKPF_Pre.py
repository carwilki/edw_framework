# Databricks notebook source
#Code converted on 2023-09-26 09:20:13
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

# Processing node SQ_Shortcut_to_IKPF, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 25

source_bucket=getParameterValue(raw,'wf_inv_physical.prm','BA_Inventory.WF:wf_inv_physical.M:m_SAP_IKPF_Pre','source_bucket')
source_file = get_src_file('IKPF', source_bucket)

print(source_file)

# COMMAND ----------

SQ_Shortcut_to_IKPF = spark.read.csv(source_file, sep='|', header=True)

# COMMAND ----------

# Processing node Shortcut_to_SAP_IKPF_PRE, type TARGET 
# COLUMN COUNT: 25


Shortcut_to_SAP_IKPF_PRE = SQ_Shortcut_to_IKPF.selectExpr(
	"CAST(MANDT AS STRING) as MANDT",
	"CAST(DOC_NBR AS STRING) as DOC_NBR",
	"CAST(FISCAL_YR AS INT) as FISCAL_YR",
	"CAST(VGART AS STRING) as VGART",
	"CAST(SITE_NBR AS STRING) as SITE_NBR",
	"CAST(LGORT AS STRING) as LGORT",
	"CAST(SOBKZ AS STRING) as SOBKZ",
	"TO_DATE(BLDAT, 'MM/dd/yyyy HH:mm:ss') as BLDAT",
  "TO_DATE(GIDAT, 'MM/dd/yyyy HH:mm:ss') as GIDAT",
  "TO_DATE(ZLDAT, 'MM/dd/yyyy HH:mm:ss') as ZLDAT",
  "TO_DATE(POSTING_DT, 'MM/dd/yyyy HH:mm:ss') as POSTING_DT",
	"CAST(MONAT AS INT) as MONAT",
	"CAST(USNAM AS STRING) as USNAM",
	"CAST(SPERR AS STRING) as SPERR",
	"CAST(ZSTAT AS STRING) as ZSTAT",
	"CAST(DSTAT AS STRING) as DSTAT",
	"CAST(XBLNI AS STRING) as XBLNI",
	"CAST(LSTAT AS STRING) as LSTAT",
	"CAST(XBUFI AS STRING) as XBUFI",
	"CAST(KEORD AS STRING) as KEORD",
	"CAST(ORDNG AS STRING) as ORDNG",
	"CAST(INVNU AS STRING) as INVNU",
	"CAST(IBLTXT AS STRING) as IBLTXT",
	"CAST(INVART AS STRING) as INVART",
	"CAST(WSTI_BSTAT AS STRING) as WSTI_BSTAT"
)

Shortcut_to_SAP_IKPF_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SAP_IKPF_PRE')
