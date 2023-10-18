# Databricks notebook source
#Code converted on 2023-09-25 13:30:29
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

# Processing node SQ_MARD_MARC_MBEW, type SOURCE 
# COLUMN COUNT: 13

SQ_MARD_MARC_MBEW = spark.sql(f"""
SELECT CURRENT_DATE - INTERVAL 1 DAY  AS DAY_DT,
MARD_PRE.MATNR AS MARD_MATNR,
MARD_PRE.WERKS AS MARD_WERKS,
MARD_PRE.LABST,
MARD_PRE.INSME,
MARD_PRE.SPEME,
MARC_PRE.TRAME,
MBEW_PRE.VERPR,
MBEW_PRE.LAEPR,
MBEW_PRE.LBKUM,
MBEW_PRE.SALK3,
MBEW_PRE.STPRV,
MARC_PRE.BWESB
FROM {raw}.MARC_PRE, {raw}.MARD_PRE, {raw}.MBEW_PRE 
WHERE mard_pre.mandt = marc_pre.mandt 
  and mard_pre.matnr = marc_pre.matnr 
  and mard_pre.werks = marc_pre.werks 
  and mard_pre.mandt = mbew_pre.mandt 
  and mard_pre.matnr = mbew_pre.matnr 
  and mard_pre.werks = mbew_pre.bwkey""")  # .withColumn("sys_row_id", monotonically_increasing_id())
  
# Conforming fields names to the component layout
SQ_MARD_MARC_MBEW = SQ_MARD_MARC_MBEW \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[1],'MARD_MATNR') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[2],'MARD_WERKS') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[3],'LABST') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[4],'INSME') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[5],'SPEME') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[6],'TRAME') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[7],'VERPR') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[8],'LAEPR') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[9],'LBKUM') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[10],'SALK3') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[11],'STPRV') \
	.withColumnRenamed(SQ_MARD_MARC_MBEW.columns[12],'BWESB')

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_MARD_MARC_MBEW_temp = SQ_MARD_MARC_MBEW.toDF(*["SQ_MARD_MARC_MBEW___" + col for col in SQ_MARD_MARC_MBEW.columns])

EXPTRANS = SQ_MARD_MARC_MBEW_temp.selectExpr(
	# "SQ_MARD_MARC_MBEW___sys_row_id as sys_row_id",
	"SQ_MARD_MARC_MBEW___DAY_DT as DAY_DT",
	"SQ_MARD_MARC_MBEW___MARD_MATNR as MARD_MATNR",
	"SQ_MARD_MARC_MBEW___MARD_WERKS as MARD_WERKS",
	"SQ_MARD_MARC_MBEW___VERPR as VERPR",
	"SQ_MARD_MARC_MBEW___LAEPR as LAEPR",
	"SQ_MARD_MARC_MBEW___LBKUM as LBKUM",
	"SQ_MARD_MARC_MBEW___SALK3 as SALK3",
	"SQ_MARD_MARC_MBEW___STPRV as STPRV",
	"CASE WHEN length(cast(SQ_MARD_MARC_MBEW___STPRV as integer)) > 6 THEN CAST(LTRIM(substring(SQ_MARD_MARC_MBEW___STPRV, 1, 1), ' ') AS decimal(9, 3)) ELSE CAST(SQ_MARD_MARC_MBEW___STPRV AS decimal(9, 3)) END AS O_STPRV",
	"SQ_MARD_MARC_MBEW___TRAME + SQ_MARD_MARC_MBEW___BWESB as O_XFR_TRANS_QTY",
	"SQ_MARD_MARC_MBEW___LABST + SQ_MARD_MARC_MBEW___INSME + SQ_MARD_MARC_MBEW___SPEME as O_ON_HAND_QTY"
)

# COMMAND ----------

# Processing node Shortcut_to_INVENTORY_PRE, type TARGET 
# COLUMN COUNT: 10


Shortcut_to_INVENTORY_PRE = EXPTRANS.selectExpr(
	"CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
	"CAST(MARD_MATNR AS INT) as SKU_NBR",
	"CAST(MARD_WERKS AS INT) as STORE_NBR",
	"CAST(O_ON_HAND_QTY AS INT) as ON_HAND_QTY",
	"CAST(O_XFR_TRANS_QTY AS INT) as XFER_IN_TRANS_QTY",
	"CAST(VERPR AS DECIMAL(9,3)) as MAP_AMT",
	"CAST(LAEPR AS TIMESTAMP) as PRICE_CHANGE_DT",
	"CAST(LBKUM AS INT) as VALUATED_STOCK_QTY",
	"CAST(SALK3 AS DECIMAL(11,3)) as VALUATED_STOCK_AMT",
	"CAST(O_STPRV AS DECIMAL(9,3)) as PREV_PRICE_AMT"
)

Shortcut_to_INVENTORY_PRE.write.mode("overwrite").saveAsTable(f'{raw}.INVENTORY_PRE')
