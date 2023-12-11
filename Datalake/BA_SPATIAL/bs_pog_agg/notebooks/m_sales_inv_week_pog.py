# Databricks notebook source
#Code converted on 2023-10-11 11:42:44
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
from Datalake.utils.pk.pk import DuplicateChecker

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

# Processing node ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG, type SOURCE 
# COLUMN COUNT: 10

_sql = f"""
SELECT P.WEEK_DT AS WEEK_DT,
       P.POG_ID AS POG_ID,
       TRIM(S.COUNTRY_CD) AS COUNTRY_CD,
       SUM(P.NET_SALES_AMT) AS NET_SALES_AMT,
       SUM(P.NET_SALES_COST) AS NET_SALES_COST,
       SUM(P.NET_SALES_QTY) AS NET_SALES_QTY,
       SUM(P.MERCH_SALES_AMT) AS MERCH_SALES_AMT,
       SUM(P.ON_HAND_QTY) AS ON_HAND_QTY,
       SUM(P.ON_HAND_COST) AS ON_HAND_COST,
       MAX(P.EXCH_RATE_PCT) AS EXCH_RATE_PCT
FROM {legacy}.SALES_INV_WEEK_STORE_POG P LEFT JOIN {legacy}.SITE_PROFILE S ON P.LOCATION_ID = S.LOCATION_ID
WHERE P.WEEK_DT = DATE_ADD(CURRENT_DATE, - (DATE_PART('dow', CURRENT_DATE) - 1))
GROUP BY P.WEEK_DT, P.POG_ID, S.COUNTRY_CD
"""

ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())


# Conforming fields names to the component layout
ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG = ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[0],'WEEK_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[1],'POG_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[2],'COUNTRY_CD') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[3],'NET_SALES_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[4],'NET_SALES_COST') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[5],'NET_SALES_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[6],'MERCH_SALES_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[7],'ON_HAND_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[8],'ON_HAND_COST') \
	.withColumnRenamed(ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.columns[9],'EXCH_RATE_PCT')

# COMMAND ----------

# Processing node Shortcut_To_SALES_INV_WEEK_POG, type TARGET 
# COLUMN COUNT: 10


Shortcut_To_SALES_INV_WEEK_POG = ASQ_Shortcut_To_SALES_INV_WEEK_STORE_POG.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(POG_ID AS INT) as POG_ID",
	"CAST(COUNTRY_CD AS STRING) as COUNTRY_CD",
	"CAST(NET_SALES_AMT AS DECIMAL(10,2)) as NET_SALES_AMT",
	"CAST(NET_SALES_COST AS DECIMAL(10,2)) as NET_SALES_COST",
	"CAST(NET_SALES_QTY AS DECIMAL(12,3)) as NET_SALES_QTY",
	"CAST(MERCH_SALES_AMT AS DECIMAL(10,2)) as MERCH_SALES_AMT",
	"CAST(ON_HAND_QTY AS DECIMAL(12,3)) as ON_HAND_QTY",
	"CAST(ON_HAND_COST AS DECIMAL(10,2)) as ON_HAND_COST",
	"CAST(EXCH_RATE_PCT AS DECIMAL(9,6)) as EXCH_RATE_PCT"
)

DuplicateChecker.check_for_duplicate_primary_keys(Shortcut_To_SALES_INV_WEEK_POG, ["WEEK_DT", "POG_ID", "COUNTRY_CD"])

Shortcut_To_SALES_INV_WEEK_POG.write.mode("append").saveAsTable(f'{legacy}.SALES_INV_WEEK_POG')
