# Databricks notebook source
#Code converted on 2023-09-15 14:55:19
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

# Processing node SQ_Shortcut_To_RETAIL_PRICE_PRE, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""
INSERT INTO {legacy}.INV_INSTOCK_PRICE_DAY
SELECT ID.DAY_DT
	,ID.PRODUCT_ID
	,ID.LOCATION_ID
	,ID.FROM_LOCATION_ID
	,ID.SOURCE_VENDOR_ID
	,SKU.STATUS_ID AS SKU_STATUS_ID
	,SITE_SALES_FLAG AS STORE_OPEN_IND
	,0 OUT_OF_STOCK_IND
	,0 POG_LISTED_IND
	,0 SAP_LISTED_IND
	,0 INLINE_IND
	,0 PLANNER_IND
	,0 SUBS_IND
	,ID.MAP_AMT
	,CASE 
		WHEN TRIM(SITE.COUNTRY_CD) = 'CA'
			THEN C.EXCHANGE_RATE_PCNT
		WHEN TRIM(SITE.COUNTRY_CD) = 'US'
			THEN 1
		END AS EXCHANGE_RATE_PCT
	,ID.ON_HAND_QTY
	,ID.COMMITTED_QTY
	,ID.XFER_IN_TRANS_QTY
	,ID.ON_ORDER_QTY
	,ID.SUM_COST
	,ID.BUM_COST
	,ID.ON_ORDER_CD
	,ID.RETAIL_PRICE_AMT
	,0 SKU_FACINGS_QTY
	,0 SKU_CAPACITY_QTY
	,ID.PETSPERK_AMT
	,ID.PETSPERK_IND
	,ID.RETAIL_PRICE_AMT AS LOCAL_PRICE_AMT
	,ID.PETSPERK_AMT AS LOC_PETPERKS_PRICE_AMT
	,CURRENT_DATE AS LOAD_DT
FROM (
	SELECT DAY_DT
		,INV.PRODUCT_ID
		,INV.LOCATION_ID
		,MAP_AMT
		,ON_HAND_QTY
		,COMMITTED_QTY
		,XFER_IN_TRANS_QTY
		,ON_ORDER_QTY
		,SUM_COST
		,BUM_COST
		,ON_ORDER_CD
		,RETAIL_PRICE_AMT
		,PETSPERK_AMT
		,NVL(FROM_LOCATION_ID, 90000) FROM_LOCATION_ID
		,NVL(SOURCE_VENDOR_ID, 1802) SOURCE_VENDOR_ID
		,CASE 
			WHEN NVL(PETSPERK_AMT, 0) > 0
				THEN 1
			ELSE 0
			END AS PETSPERK_IND
	FROM {raw}.INVENTORY_DAY_PRE INV
	LEFT OUTER JOIN {legacy}.SUPPLY_CHAIN SC ON INV.LOCATION_ID = SC.LOCATION_ID
		AND INV.PRODUCT_ID = SC.PRODUCT_ID
	WHERE DAY_DT = CURRENT_DATE - INTERVAL 1 DAY
		AND (
			(ON_HAND_QTY <> 0)
			OR (COMMITTED_QTY <> 0)
			OR (XFER_IN_TRANS_QTY <> 0)
			OR (ON_ORDER_QTY <> 0)
			)
	) ID
LEFT OUTER JOIN (
	SELECT DAY_DT
		,PRODUCT_ID
		,LOCATION_ID
	FROM {legacy}.INV_INSTOCK_PRICE_DAY IIR
	WHERE IIR.DAY_DT = CURRENT_DATE - INTERVAL 1 DAY) IIR ON ID.DAY_DT = IIR.DAY_DT
	AND ID.PRODUCT_ID = IIR.PRODUCT_ID
	AND ID.LOCATION_ID = IIR.LOCATION_ID
	,(
		SELECT EXCHANGE_RATE_PCNT
		FROM {legacy}.CURRENCY_DAY
		WHERE DAY_DT = CURRENT_DATE - INTERVAL 1 DAY) C
	,{legacy}.SKU_PROFILE SKU
	,{legacy}.SITE_PROFILE SITE
WHERE ID.PRODUCT_ID = SKU.PRODUCT_ID
	AND ID.LOCATION_ID = SITE.LOCATION_ID
	AND IIR.PRODUCT_ID IS NULL
"""

spark.sql(_sql)

# COMMAND ----------

# logPrevRunDt("INV_INSTOCK_PRICE_DAY", "INV_INSTOCK_PRICE_DAY", "Completed", "N/A", f"{raw}.log_run_details")
