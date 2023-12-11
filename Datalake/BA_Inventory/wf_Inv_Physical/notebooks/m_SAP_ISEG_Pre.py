# Databricks notebook source
#Code converted on 2023-09-26 09:20:09
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

# Processing node SQ_Shortcut_to_ISEG, type APPLICATION_SOURCE_QUALIFIER 
# COLUMN COUNT: 65

source_bucket=getParameterValue(raw,'wf_inv_physical.prm','BA_Inventory.WF:wf_inv_physical.M:m_SAP_ISEG_Pre','source_bucket')
source_file = get_src_file('ISEG', source_bucket)

print(source_file)

# COMMAND ----------

SQ_Shortcut_to_ISEG = spark.read.csv(source_file, sep='|', header=True)

# COMMAND ----------

# Processing node Shortcut_to_SAP_ISEG_PRE, type TARGET 
# COLUMN COUNT: 65


Shortcut_to_SAP_ISEG_PRE = SQ_Shortcut_to_ISEG.selectExpr(
	"CAST(MANDT AS STRING) as MANDT",
	"CAST(DOC_NBR AS STRING) as DOC_NBR",
	"CAST(FISCAL_YR AS INT) as FISCAL_YR",
	"CAST(ZEILI AS INT) as ZEILI",
	"CAST(SKU_NBR AS STRING) as SKU_NBR",
	"CAST(SITE_NBR AS STRING) as SITE_NBR",
	"CAST(LGORT AS STRING) as LGORT",
	"CAST(CHARG AS STRING) as CHARG",
	"CAST(SOBKZ AS STRING) as SOBKZ",
	"CAST(BSTAR AS STRING) as BSTAR",
	"CAST(KDAUF AS STRING) as KDAUF",
	"CAST(KDPOS AS INT) as KDPOS",
	"CAST(KDEIN AS INT) as KDEIN",
	"CAST(LIFNR AS STRING) as LIFNR",
	"CAST(KUNNR AS STRING) as KUNNR",
	"CAST(PLPLA AS STRING) as PLPLA",
	"CAST(USNAM AS STRING) as USNAM",
	"TO_DATE(AEDAT, 'MM/dd/yyyy HH:mm:ss') as AEDAT",
	"CAST(USNAZ AS STRING) as USNAZ",
	"TO_DATE(ZLDAT, 'MM/dd/yyyy HH:mm:ss') as ZLDAT",
	"CAST(USNAD AS STRING) as USNAD",
	"TO_DATE(BUDAT, 'MM/dd/yyyy HH:mm:ss') as BUDAT",
	"CAST(XBLNI AS STRING) as XBLNI",
	"CAST(XZAEL AS STRING) as XZAEL",
	"CAST(XDIFF AS STRING) as XDIFF",
	"CAST(XNZAE AS STRING) as XNZAE",
	"CAST(XLOEK AS STRING) as XLOEK",
	"CAST(XAMEI AS STRING) as XAMEI",
	"CAST(BOOK_QTY AS DECIMAL(13,3)) as BOOK_QTY",
	"CAST(XNULL AS STRING) as XNULL",
	"CAST(PHYSICAL_QTY AS DECIMAL(13,3)) as PHYSICAL_QTY",
	"CAST(MEINS AS STRING) as MEINS",
	"CAST(ERFMG AS DECIMAL(13,3)) as ERFMG",
	"CAST(ERFME AS STRING) as ERFME",
	"CAST(MBLNR AS STRING) as MBLNR",
	"CAST(MJAHR AS INT) as MJAHR",
	"CAST(ZEILE AS INT) as ZEILE",
	"CAST(NBLNR AS STRING) as NBLNR",
	"CAST(DIFF_AMT AS DECIMAL(13,2)) as DIFF_AMT",
	"CAST(WAERS AS STRING) as WAERS",
	"CAST(ABCIN AS STRING) as ABCIN",
	"CAST(PS_PSP_PNR AS INT) as PS_PSP_PNR",
	"CAST(VKWRT AS DECIMAL(13,2)) as VKWRT",
	"CAST(EXVKW AS DECIMAL(13,2)) as EXVKW",
	"CAST(BUCHW AS DECIMAL(13,2)) as BUCHW",
	"CAST(KWART AS STRING) as KWART",
	"CAST(VKWRA AS DECIMAL(13,2)) as VKWRA",
	"CAST(VKMZL AS DECIMAL(13,2)) as VKMZL",
	"CAST(VKNZL AS DECIMAL(13,2)) as VKNZL",
	"CAST(WRTZL AS DECIMAL(13,2)) as WRTZL",
	"CAST(WRTBM AS DECIMAL(13,2)) as WRTBM",
	"CAST(DIWZL AS DECIMAL(13,2)) as DIWZL",
	"CAST(ATTYP AS STRING) as ATTYP",
	"CAST(GRUND AS SMALLINT) as GRUND",
	"CAST(SAMAT AS STRING) as SAMAT",
	"CAST(XDISPATCH AS STRING) as XDISPATCH", 
 	"TO_DATE(WSTI_COUNTDATE, 'MM/dd/yyyy HH:mm:ss') as WSTI_COUNTDATE",
	"TO_DATE(WSTI_COUNTTIME, 'MM/dd/yyyy HH:mm:ss') as WSTI_COUNTTIME",
	"TO_DATE(WSTI_FREEZEDATE, 'MM/dd/yyyy HH:mm:ss') as WSTI_FREEZEDATE",
	"TO_DATE(WSTI_FREEZETIME, 'MM/dd/yyyy HH:mm:ss') as WSTI_FREEZETIME",
	"CAST(WSTI_POSM AS DECIMAL(13,3)) as WSTI_POSM",
	"CAST(WSTI_POSW AS DECIMAL(13,2)) as WSTI_POSW",
	"CAST(WSTI_XCALC AS STRING) as WSTI_XCALC",
	"TO_DATE(WSTI_ENTERDATE, 'MM/dd/yyyy HH:mm:ss') as WSTI_ENTERDATE", 
	"TO_DATE(WSTI_ENTERTIME, 'MM/dd/yyyy HH:mm:ss') as WSTI_ENTERTIME"
)
Shortcut_to_SAP_ISEG_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SAP_ISEG_PRE')
