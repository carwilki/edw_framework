# Databricks notebook source
#Code converted on 2023-10-11 11:42:43
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
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# Processing node ASQ_Shortcut_To_MERCH_DISCOUNT_DAY, type SOURCE 
# COLUMN COUNT: 7

_sql = f"""
SELECT
    DAYS.WEEK_DT,
    MERCH_DISCOUNT_DAY.PRODUCT_ID AS PRODUCT_ID,
    MERCH_DISCOUNT_DAY.LOCATION_ID AS LOCATION_ID,
    SUM(MERCH_DISCOUNT_DAY.MERCH_DISCOUNT_AMT) AS MERCH_DISCOUNT_AMT,
    SUM(MERCH_DISCOUNT_DAY.MERCH_DISCOUNT_QTY) AS MERCH_DISCOUNT_QTY,
    SUM(MERCH_DISCOUNT_DAY.MERCH_DISCOUNT_RETURN_AMT) AS MERCH_DISCOUNT_RETURN_AMT,
    SUM(MERCH_DISCOUNT_DAY.MERCH_DISCOUNT_RETURN_QTY) AS MERCH_DISCOUNT_RETURN_QTY
FROM {legacy}.MERCH_DISCOUNT_DAY JOIN {enterprise}.DAYS
ON
    DAYS.WEEK_DT = DATE_ADD(CURRENT_DATE, -(date_part('dow', CURRENT_DATE) - 1))
    AND MERCH_DISCOUNT_DAY.DAY_DT = DAYS.DAY_DT
GROUP BY
    DAYS.WEEK_DT,
    MERCH_DISCOUNT_DAY.PRODUCT_ID,
    MERCH_DISCOUNT_DAY.LOCATION_ID
"""

ASQ_Shortcut_To_MERCH_DISCOUNT_DAY = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_MERCH_DISCOUNT_DAY = ASQ_Shortcut_To_MERCH_DISCOUNT_DAY \
	.withColumnRenamed(ASQ_Shortcut_To_MERCH_DISCOUNT_DAY.columns[0],'WEEK_DT') \
	.withColumnRenamed(ASQ_Shortcut_To_MERCH_DISCOUNT_DAY.columns[1],'PRODUCT_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_MERCH_DISCOUNT_DAY.columns[2],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_MERCH_DISCOUNT_DAY.columns[3],'MERCH_DISCOUNT_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_MERCH_DISCOUNT_DAY.columns[4],'MERCH_DISCOUNT_QTY') \
	.withColumnRenamed(ASQ_Shortcut_To_MERCH_DISCOUNT_DAY.columns[5],'MERCH_DISCOUNT_RETURN_AMT') \
	.withColumnRenamed(ASQ_Shortcut_To_MERCH_DISCOUNT_DAY.columns[6],'MERCH_DISCOUNT_RETURN_QTY')

# COMMAND ----------

# Processing node Shortcut_To_POG_MERCH_DISCOUNT_WEEK_PRE, type TARGET 
# COLUMN COUNT: 7


Shortcut_To_POG_MERCH_DISCOUNT_WEEK_PRE = ASQ_Shortcut_To_MERCH_DISCOUNT_DAY.selectExpr(
	"CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(MERCH_DISCOUNT_AMT AS DECIMAL(8,2)) as MERCH_DISCOUNT_AMT",
	"CAST(MERCH_DISCOUNT_QTY AS INT) as MERCH_DISCOUNT_QTY",
	"CAST(MERCH_DISCOUNT_RETURN_AMT AS DECIMAL(8,2)) as MERCH_DISCOUNT_RETURN_AMT",
	"CAST(MERCH_DISCOUNT_RETURN_QTY AS INT) as MERCH_DISCOUNT_RETURN_QTY"
)

# Checking for the duplicates is not needed due to GROUP BY In the source query

Shortcut_To_POG_MERCH_DISCOUNT_WEEK_PRE.write.mode("overwrite").saveAsTable(f'{raw}.POG_MERCH_DISCOUNT_WEEK_PRE')
