# Databricks notebook source
#Code converted on 2023-09-26 09:20:17
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

# Processing node SQ_Shortcut_to_INV_PHYSICAL_PRE, type SOURCE 
# COLUMN COUNT: 80

SQ_Shortcut_to_INV_PHYSICAL_PRE = spark.sql(f"""SELECT
POSTING_DT,
DOC_NBR,
SITE_NBR,
SKU_NBR,
CLIENT,
FISCAL_YR,
BOOK_QTY,
PHYSICAL_QTY,
OVERSTOCK_QTY,
DIFF_AMT,
TRANS_TYPE,
STORAGE_LOC,
SPECIAL_STOCK_IND,
DOC_DT,
PLANNED_DT,
LAST_COUNT_DT,
FISCAL_PERIOD,
CHANGED_BY,
POSTING_BLOCK,
COUNT_STATUS,
ADJ_POSTING_STATUS,
PHYS_INV_REF_NBR,
DELETE_FLAG_STATUS,
BOOK_INV_FREEZE,
GROUP_CRIT_TYPE,
GROUP_CRIT_PI,
PHYS_INV_TYPE_DESC,
PHYS_INV_DOC_DESC,
INV_BALANCE_STATUS,
LN_NBR,
BATCH_NBR,
PHYS_INV_STOCK_TYPE_ID,
SALES_ORDER_NBR,
ITEM_NBR,
DELIVERY_SCHED,
VENDOR_NBR,
ACCT_CUST_NBR,
DISTRIBUTION_DIFF,
CHANGED_DT,
COUNTED_BY,
ADJ_POSTING_BY,
ITEM_COUNTED_FLAG,
DIFFERENCE_POSTED,
ITEM_RECOUNTED_FLAG,
ITEM_DELETED_FLAG,
ALT_UOM_FLAG,
ZERO_COUNT_FLAG,
BASE_UOM,
QTY_UOM,
PHYS_INV_UOM,
NBR_ARTICLE_DOC,
ARTICLE_DOC_YR,
ITEM_ARTICLE_DOC,
NBR_RECOUNT_DOC,
CURRENCY_CD,
PHYS_INV_CYCLE_COUNT_IND,
WBS_ELEMENT,
SALES_PRICE_TAX_INC,
EXT_SALES_VALUE_LOCAL,
BOOK_VALUE_SP,
INV_FLAG,
SALES_PRICE_TAX_EXC,
SALES_INV_DIFF_VAT,
SALES_INV_DIFF_NOVAT,
PHYS_INV_COUNT_VALUE,
BOOK_QTY_VALUE,
INV_DIFF,
ARTCILE_CATEGORY,
INV_DIFF_REASON,
XSITE_CONF_ARTICLE,
PHYS_INV_DIST_DIFF,
DATE_COUNT_OT,
TIME_COUNT_OT,
FREEZE_DT_INV_BALANCE,
FREEZE_TIME_INV_BALANCE,
BOOK_QTY_CHANGED,
RETAIL_VALUE,
BOOK_INV,
PHYS_INV_ENTRY_DATE,
PHYS_INV_ENTRY_TIME
FROM {raw}.INV_PHYSICAL_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_CURRENCY_DAY, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_CURRENCY_DAY = spark.sql(f"""SELECT
DAY_DT,
CURRENCY_ID,
EXCHANGE_RATE_PCNT
FROM {legacy}.CURRENCY_DAY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE = spark.sql(f"""SELECT
  PRODUCT_ID,
  SKU_NBR
FROM {legacy}.SKU_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
  LOCATION_ID,
  LOCATION_NBR
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_Site_Profile, type JOINER 
# COLUMN COUNT: 82

JNR_Site_Profile = SQ_Shortcut_to_INV_PHYSICAL_PRE.join(SQ_Shortcut_to_SITE_PROFILE,[SQ_Shortcut_to_INV_PHYSICAL_PRE.SITE_NBR == SQ_Shortcut_to_SITE_PROFILE.LOCATION_NBR],'inner')

# COMMAND ----------

# Processing node Jnr_Sku_Profile, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 83

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SKU_PROFILE_temp = SQ_Shortcut_to_SKU_PROFILE.toDF(*["SQ_Shortcut_to_SKU_PROFILE___" + col for col in SQ_Shortcut_to_SKU_PROFILE.columns])
JNR_Site_Profile_temp = JNR_Site_Profile.toDF(*["JNR_Site_Profile___" + col for col in JNR_Site_Profile.columns])

Jnr_Sku_Profile = SQ_Shortcut_to_SKU_PROFILE_temp.join(JNR_Site_Profile_temp,[SQ_Shortcut_to_SKU_PROFILE_temp.SQ_Shortcut_to_SKU_PROFILE___SKU_NBR == JNR_Site_Profile_temp.JNR_Site_Profile___SKU_NBR],'inner').selectExpr(
	"JNR_Site_Profile___LOCATION_ID as LOCATION_ID",
	"JNR_Site_Profile___CLIENT as CLIENT",
	"JNR_Site_Profile___DOC_NBR as DOC_NBR",
	"JNR_Site_Profile___FISCAL_YR as FISCAL_YR",
	"JNR_Site_Profile___TRANS_TYPE as TRANS_TYPE",
	"JNR_Site_Profile___SITE_NBR as SITE_NBR",
	"JNR_Site_Profile___STORAGE_LOC as STORAGE_LOC",
	"JNR_Site_Profile___SPECIAL_STOCK_IND as SPECIAL_STOCK_IND",
	"JNR_Site_Profile___DOC_DT as DOC_DT",
	"JNR_Site_Profile___PLANNED_DT as PLANNED_DT",
	"JNR_Site_Profile___LAST_COUNT_DT as LAST_COUNT_DT",
	"JNR_Site_Profile___POSTING_DT as POSTING_DT",
	"JNR_Site_Profile___FISCAL_PERIOD as FISCAL_PERIOD",
	"JNR_Site_Profile___CHANGED_BY as CHANGED_BY",
	"JNR_Site_Profile___POSTING_BLOCK as POSTING_BLOCK",
	"JNR_Site_Profile___COUNT_STATUS as COUNT_STATUS",
	"JNR_Site_Profile___ADJ_POSTING_STATUS as ADJ_POSTING_STATUS",
	"JNR_Site_Profile___PHYS_INV_REF_NBR as PHYS_INV_REF_NBR",
	"JNR_Site_Profile___DELETE_FLAG_STATUS as DELETE_FLAG_STATUS",
	"JNR_Site_Profile___BOOK_INV_FREEZE as BOOK_INV_FREEZE",
	"JNR_Site_Profile___GROUP_CRIT_TYPE as GROUP_CRIT_TYPE",
	"JNR_Site_Profile___GROUP_CRIT_PI as GROUP_CRIT_PI",
	"JNR_Site_Profile___PHYS_INV_TYPE_DESC as PHYS_INV_TYPE_DESC",
	"JNR_Site_Profile___PHYS_INV_DOC_DESC as PHYS_INV_DOC_DESC",
	"JNR_Site_Profile___INV_BALANCE_STATUS as INV_BALANCE_STATUS",
	"JNR_Site_Profile___LN_NBR as LN_NBR",
	"JNR_Site_Profile___SKU_NBR as SKU_NBR",
	"JNR_Site_Profile___BATCH_NBR as BATCH_NBR",
	"JNR_Site_Profile___PHYS_INV_STOCK_TYPE_ID as PHYS_INV_STOCK_TYPE_ID",
	"JNR_Site_Profile___SALES_ORDER_NBR as SALES_ORDER_NBR",
	"JNR_Site_Profile___ITEM_NBR as ITEM_NBR",
	"JNR_Site_Profile___DELIVERY_SCHED as DELIVERY_SCHED",
	"JNR_Site_Profile___VENDOR_NBR as VENDOR_NBR",
	"JNR_Site_Profile___ACCT_CUST_NBR as ACCT_CUST_NBR",
	"JNR_Site_Profile___DISTRIBUTION_DIFF as DISTRIBUTION_DIFF",
	"JNR_Site_Profile___CHANGED_DT as CHANGED_DT",
	"JNR_Site_Profile___COUNTED_BY as COUNTED_BY",
	"JNR_Site_Profile___ADJ_POSTING_BY as ADJ_POSTING_BY",
	"JNR_Site_Profile___ITEM_COUNTED_FLAG as ITEM_COUNTED_FLAG",
	"JNR_Site_Profile___DIFFERENCE_POSTED as DIFFERENCE_POSTED",
	"JNR_Site_Profile___ITEM_RECOUNTED_FLAG as ITEM_RECOUNTED_FLAG",
	"JNR_Site_Profile___ITEM_DELETED_FLAG as ITEM_DELETED_FLAG",
	"JNR_Site_Profile___ALT_UOM_FLAG as ALT_UOM_FLAG",
	"JNR_Site_Profile___BOOK_QTY as BOOK_QTY",
	"JNR_Site_Profile___ZERO_COUNT_FLAG as ZERO_COUNT_FLAG",
	"JNR_Site_Profile___PHYSICAL_QTY as PHYSICAL_QTY",
	"JNR_Site_Profile___BASE_UOM as BASE_UOM",
	"JNR_Site_Profile___QTY_UOM as QTY_UOM",
	"JNR_Site_Profile___PHYS_INV_UOM as PHYS_INV_UOM",
	"JNR_Site_Profile___NBR_ARTICLE_DOC as NBR_ARTICLE_DOC",
	"JNR_Site_Profile___ARTICLE_DOC_YR as ARTICLE_DOC_YR",
	"JNR_Site_Profile___ITEM_ARTICLE_DOC as ITEM_ARTICLE_DOC",
	"JNR_Site_Profile___NBR_RECOUNT_DOC as NBR_RECOUNT_DOC",
	"JNR_Site_Profile___DIFF_AMT as DIFF_AMT",
	"JNR_Site_Profile___CURRENCY_CD as CURRENCY_CD",
	"JNR_Site_Profile___PHYS_INV_CYCLE_COUNT_IND as PHYS_INV_CYCLE_COUNT_IND",
	"JNR_Site_Profile___WBS_ELEMENT as WBS_ELEMENT",
	"JNR_Site_Profile___SALES_PRICE_TAX_INC as SALES_PRICE_TAX_INC",
	"JNR_Site_Profile___EXT_SALES_VALUE_LOCAL as EXT_SALES_VALUE_LOCAL",
	"JNR_Site_Profile___BOOK_VALUE_SP as BOOK_VALUE_SP",
	"JNR_Site_Profile___INV_FLAG as INV_FLAG",
	"JNR_Site_Profile___SALES_PRICE_TAX_EXC as SALES_PRICE_TAX_EXC",
	"JNR_Site_Profile___SALES_INV_DIFF_VAT as SALES_INV_DIFF_VAT",
	"JNR_Site_Profile___SALES_INV_DIFF_NOVAT as SALES_INV_DIFF_NOVAT",
	"JNR_Site_Profile___PHYS_INV_COUNT_VALUE as PHYS_INV_COUNT_VALUE",
	"JNR_Site_Profile___BOOK_QTY_VALUE as BOOK_QTY_VALUE",
	"JNR_Site_Profile___INV_DIFF as INV_DIFF",
	"JNR_Site_Profile___ARTCILE_CATEGORY as ARTCILE_CATEGORY",
	"JNR_Site_Profile___INV_DIFF_REASON as INV_DIFF_REASON",
	"JNR_Site_Profile___XSITE_CONF_ARTICLE as XSITE_CONF_ARTICLE",
	"JNR_Site_Profile___PHYS_INV_DIST_DIFF as PHYS_INV_DIST_DIFF",
	"JNR_Site_Profile___DATE_COUNT_OT as DATE_COUNT_OT",
	"JNR_Site_Profile___TIME_COUNT_OT as TIME_COUNT_OT",
	"JNR_Site_Profile___FREEZE_DT_INV_BALANCE as FREEZE_DT_INV_BALANCE",
	"JNR_Site_Profile___FREEZE_TIME_INV_BALANCE as FREEZE_TIME_INV_BALANCE",
	"JNR_Site_Profile___BOOK_QTY_CHANGED as BOOK_QTY_CHANGED",
	"JNR_Site_Profile___RETAIL_VALUE as RETAIL_VALUE",
	"JNR_Site_Profile___BOOK_INV as BOOK_INV",
	"JNR_Site_Profile___PHYS_INV_ENTRY_DATE as PHYS_INV_ENTRY_DATE",
	"JNR_Site_Profile___PHYS_INV_ENTRY_TIME as PHYS_INV_ENTRY_TIME",
	"SQ_Shortcut_to_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE___SKU_NBR as SKU_NBR1",
	"JNR_Site_Profile___OVERSTOCK_QTY as OVERSTOCK_QTY")

# COMMAND ----------

# Processing node Jnr_Currency_Day, type JOINER 
# COLUMN COUNT: 85

Jnr_Currency_Day = Jnr_Sku_Profile.join(SQ_Shortcut_to_CURRENCY_DAY,[Jnr_Sku_Profile.POSTING_DT == SQ_Shortcut_to_CURRENCY_DAY.DAY_DT, Jnr_Sku_Profile.CURRENCY_CD == SQ_Shortcut_to_CURRENCY_DAY.CURRENCY_ID],'left_outer')

# COMMAND ----------

# Processing node Exp_Fields, type EXPRESSION 
# COLUMN COUNT: 88

# for each involved DataFrame, append the dataframe name to each column
Jnr_Currency_Day_temp = Jnr_Currency_Day.toDF(*["Jnr_Currency_Day___" + col for col in Jnr_Currency_Day.columns])
# INV_temp = INV.toDF(*["INV___" + col for col in INV.columns])

Exp_Fields = Jnr_Currency_Day_temp.selectExpr(
	"Jnr_Currency_Day___sys_row_id as sys_row_id",
	"Jnr_Currency_Day___POSTING_DT as POSTING_DT",
	"Jnr_Currency_Day___DOC_NBR as DOC_NBR",
	"Jnr_Currency_Day___LOCATION_ID as LOCATION_ID",
	"Jnr_Currency_Day___PRODUCT_ID as PRODUCT_ID",
	"Jnr_Currency_Day___CLIENT as CLIENT",
	"Jnr_Currency_Day___FISCAL_YR as FISCAL_YR",
	"Jnr_Currency_Day___SITE_NBR as SITE_NBR",
	"Jnr_Currency_Day___SKU_NBR as SKU_NBR",
	"Jnr_Currency_Day___BOOK_QTY as BOOK_QTY",
	"Jnr_Currency_Day___PHYSICAL_QTY as PHYSICAL_QTY",
	"Jnr_Currency_Day___PHYSICAL_QTY - Jnr_Currency_Day___BOOK_QTY as DIFF_QTY",
	"Jnr_Currency_Day___DIFF_AMT as DIFF_AMT",
	"Jnr_Currency_Day___OVERSTOCK_QTY as OVERSTOCK_QTY",
	"Jnr_Currency_Day___TRANS_TYPE as TRANS_TYPE",
	"Jnr_Currency_Day___STORAGE_LOC as STORAGE_LOC",
	"Jnr_Currency_Day___SPECIAL_STOCK_IND as SPECIAL_STOCK_IND",
	"Jnr_Currency_Day___DOC_DT as DOC_DT",
	"Jnr_Currency_Day___PLANNED_DT as PLANNED_DT",
	"Jnr_Currency_Day___LAST_COUNT_DT as LAST_COUNT_DT",
	"Jnr_Currency_Day___FISCAL_PERIOD as FISCAL_PERIOD",
	"Jnr_Currency_Day___CHANGED_BY as CHANGED_BY",
	"Jnr_Currency_Day___POSTING_BLOCK as POSTING_BLOCK",
	"Jnr_Currency_Day___COUNT_STATUS as COUNT_STATUS",
	"Jnr_Currency_Day___ADJ_POSTING_STATUS as ADJ_POSTING_STATUS",
	"Jnr_Currency_Day___PHYS_INV_REF_NBR as PHYS_INV_REF_NBR",
	"Jnr_Currency_Day___DELETE_FLAG_STATUS as DELETE_FLAG_STATUS",
	"Jnr_Currency_Day___BOOK_INV_FREEZE as BOOK_INV_FREEZE",
	"Jnr_Currency_Day___GROUP_CRIT_TYPE as GROUP_CRIT_TYPE",
	"Jnr_Currency_Day___GROUP_CRIT_PI as GROUP_CRIT_PI",
	"Jnr_Currency_Day___PHYS_INV_TYPE_DESC as PHYS_INV_TYPE_DESC",
	"Jnr_Currency_Day___PHYS_INV_DOC_DESC as PHYS_INV_DOC_DESC",
	"IF(TRIM(Jnr_Currency_Day___PHYS_INV_TYPE_DESC) = 'LIVESTOCK CYCT', '1', IF(TRIM(Jnr_Currency_Day___PHYS_INV_TYPE_DESC) = 'PHYSICAL INV.', '2', '0')) as PHYS_INV_TYPE_ID",
	"Jnr_Currency_Day___INV_BALANCE_STATUS as INV_BALANCE_STATUS",
	"Jnr_Currency_Day___LN_NBR as LN_NBR",
	"Jnr_Currency_Day___BATCH_NBR as BATCH_NBR",
	"Jnr_Currency_Day___PHYS_INV_STOCK_TYPE_ID as PHYS_INV_STOCK_TYPE_ID",
	"Jnr_Currency_Day___SALES_ORDER_NBR as SALES_ORDER_NBR",
	"Jnr_Currency_Day___ITEM_NBR as ITEM_NBR",
	"Jnr_Currency_Day___DELIVERY_SCHED as DELIVERY_SCHED",
	"Jnr_Currency_Day___VENDOR_NBR as VENDOR_NBR",
	"Jnr_Currency_Day___ACCT_CUST_NBR as ACCT_CUST_NBR",
	"Jnr_Currency_Day___DISTRIBUTION_DIFF as DISTRIBUTION_DIFF",
	"Jnr_Currency_Day___CHANGED_DT as CHANGED_DT",
	"Jnr_Currency_Day___COUNTED_BY as COUNTED_BY",
	"Jnr_Currency_Day___ADJ_POSTING_BY as ADJ_POSTING_BY",
	"Jnr_Currency_Day___ITEM_COUNTED_FLAG as ITEM_COUNTED_FLAG",
	"Jnr_Currency_Day___DIFFERENCE_POSTED as DIFFERENCE_POSTED",
	"Jnr_Currency_Day___ITEM_RECOUNTED_FLAG as ITEM_RECOUNTED_FLAG",
	"Jnr_Currency_Day___ITEM_DELETED_FLAG as ITEM_DELETED_FLAG",
	"Jnr_Currency_Day___ALT_UOM_FLAG as ALT_UOM_FLAG",
	"Jnr_Currency_Day___ZERO_COUNT_FLAG as ZERO_COUNT_FLAG",
	"Jnr_Currency_Day___BASE_UOM as BASE_UOM",
	"Jnr_Currency_Day___QTY_UOM as QTY_UOM",
	"Jnr_Currency_Day___PHYS_INV_UOM as PHYS_INV_UOM",
	"Jnr_Currency_Day___NBR_ARTICLE_DOC as NBR_ARTICLE_DOC",
	"Jnr_Currency_Day___ARTICLE_DOC_YR as ARTICLE_DOC_YR",
	"Jnr_Currency_Day___ITEM_ARTICLE_DOC as ITEM_ARTICLE_DOC",
	"Jnr_Currency_Day___NBR_RECOUNT_DOC as NBR_RECOUNT_DOC",
	"Jnr_Currency_Day___CURRENCY_CD as CURRENCY_CD",
	"IF(Jnr_Currency_Day___CURRENCY_ID IS NOT NULL, Jnr_Currency_Day___EXCHANGE_RATE_PCNT, 1) as EXCH_RATE_PCT",
	"Jnr_Currency_Day___PHYS_INV_CYCLE_COUNT_IND as PHYS_INV_CYCLE_COUNT_IND",
	"Jnr_Currency_Day___WBS_ELEMENT as WBS_ELEMENT",
	"Jnr_Currency_Day___SALES_PRICE_TAX_INC as SALES_PRICE_TAX_INC",
	"Jnr_Currency_Day___EXT_SALES_VALUE_LOCAL as EXT_SALES_VALUE_LOCAL",
	"Jnr_Currency_Day___BOOK_VALUE_SP as BOOK_VALUE_SP",
	"Jnr_Currency_Day___INV_FLAG as INV_FLAG",
	"Jnr_Currency_Day___SALES_PRICE_TAX_EXC as SALES_PRICE_TAX_EXC",
	"Jnr_Currency_Day___SALES_INV_DIFF_VAT as SALES_INV_DIFF_VAT",
	"Jnr_Currency_Day___SALES_INV_DIFF_NOVAT as SALES_INV_DIFF_NOVAT",
	"Jnr_Currency_Day___PHYS_INV_COUNT_VALUE as PHYS_INV_COUNT_VALUE",
	"Jnr_Currency_Day___BOOK_QTY_VALUE as BOOK_QTY_VALUE",
	"Jnr_Currency_Day___INV_DIFF as INV_DIFF",
	"Jnr_Currency_Day___ARTCILE_CATEGORY as ARTCILE_CATEGORY",
	"Jnr_Currency_Day___INV_DIFF_REASON as INV_DIFF_REASON",
	"Jnr_Currency_Day___XSITE_CONF_ARTICLE as XSITE_CONF_ARTICLE",
	"Jnr_Currency_Day___PHYS_INV_DIST_DIFF as PHYS_INV_DIST_DIFF",
	"Jnr_Currency_Day___DATE_COUNT_OT as DATE_COUNT_OT",
	"Jnr_Currency_Day___TIME_COUNT_OT as TIME_COUNT_OT",
	"Jnr_Currency_Day___FREEZE_DT_INV_BALANCE as FREEZE_DT_INV_BALANCE",
	"Jnr_Currency_Day___FREEZE_TIME_INV_BALANCE as FREEZE_TIME_INV_BALANCE",
	"Jnr_Currency_Day___BOOK_QTY_CHANGED as BOOK_QTY_CHANGED",
	"Jnr_Currency_Day___RETAIL_VALUE as RETAIL_VALUE",
	"Jnr_Currency_Day___BOOK_INV as BOOK_INV",
	"Jnr_Currency_Day___PHYS_INV_ENTRY_DATE as PHYS_INV_ENTRY_DATE",
	"Jnr_Currency_Day___PHYS_INV_ENTRY_TIME as PHYS_INV_ENTRY_TIME",
	"IF(Jnr_Currency_Day___BOOK_QTY = Jnr_Currency_Day___PHYSICAL_QTY, 0, 1) as PHYS_INV_DIFF_ADJ_TYPE_ID",
	"CURRENT_TIMESTAMP as UPDATE_DT",
	"CURRENT_TIMESTAMP as LOAD_DT"
)

# COMMAND ----------

# Processing node Ups_Inv_Physical, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 90

# for each involved DataFrame, append the dataframe name to each column
Exp_Fields_temp = Exp_Fields.toDF(*["Exp_Fields___" + col for col in Exp_Fields.columns])

Ups_Inv_Physical = Exp_Fields_temp.selectExpr(
	"Exp_Fields___POSTING_DT as POSTING_DT",
	"Exp_Fields___DOC_NBR as DOC_NBR",
	"Exp_Fields___LOCATION_ID as LOCATION_ID",
	"Exp_Fields___PRODUCT_ID as PRODUCT_ID",
	"Exp_Fields___CLIENT as CLIENT",
	"Exp_Fields___FISCAL_YR as FISCAL_YR",
	"Exp_Fields___SITE_NBR as SITE_NBR",
	"Exp_Fields___SKU_NBR as SKU_NBR",
	"Exp_Fields___BOOK_QTY as BOOK_QTY",
	"Exp_Fields___PHYSICAL_QTY as PHYSICAL_QTY",
	"Exp_Fields___DIFF_QTY as DIFF_QTY",
	"Exp_Fields___DIFF_AMT as DIFF_AMT",
	"Exp_Fields___TRANS_TYPE as TRANS_TYPE",
	"Exp_Fields___STORAGE_LOC as STORAGE_LOC",
	"Exp_Fields___SPECIAL_STOCK_IND as SPECIAL_STOCK_IND",
	"Exp_Fields___DOC_DT as DOC_DT",
	"Exp_Fields___PLANNED_DT as PLANNED_DT",
	"Exp_Fields___LAST_COUNT_DT as LAST_COUNT_DT",
	"Exp_Fields___FISCAL_PERIOD as FISCAL_PERIOD",
	"Exp_Fields___CHANGED_BY as CHANGED_BY",
	"Exp_Fields___POSTING_BLOCK as POSTING_BLOCK",
	"Exp_Fields___COUNT_STATUS as COUNT_STATUS",
	"Exp_Fields___ADJ_POSTING_STATUS as ADJ_POSTING_STATUS",
	"Exp_Fields___PHYS_INV_REF_NBR as PHYS_INV_REF_NBR",
	"Exp_Fields___DELETE_FLAG_STATUS as DELETE_FLAG_STATUS",
	"Exp_Fields___BOOK_INV_FREEZE as BOOK_INV_FREEZE",
	"Exp_Fields___GROUP_CRIT_TYPE as GROUP_CRIT_TYPE",
	"Exp_Fields___GROUP_CRIT_PI as GROUP_CRIT_PI",
	"Exp_Fields___PHYS_INV_TYPE_DESC as PHYS_INV_TYPE_DESC",
	"Exp_Fields___PHYS_INV_DOC_DESC as PHYS_INV_DOC_DESC",
	"Exp_Fields___PHYS_INV_TYPE_ID as PHYS_INV_TYPE_ID",
	"Exp_Fields___INV_BALANCE_STATUS as INV_BALANCE_STATUS",
	"Exp_Fields___LN_NBR as LN_NBR",
	"Exp_Fields___BATCH_NBR as BATCH_NBR",
	"Exp_Fields___PHYS_INV_STOCK_TYPE_ID as PHYS_INV_STOCK_TYPE_ID",
	"Exp_Fields___SALES_ORDER_NBR as SALES_ORDER_NBR",
	"Exp_Fields___ITEM_NBR as ITEM_NBR",
	"Exp_Fields___DELIVERY_SCHED as DELIVERY_SCHED",
	"Exp_Fields___VENDOR_NBR as VENDOR_NBR",
	"Exp_Fields___ACCT_CUST_NBR as ACCT_CUST_NBR",
	"Exp_Fields___DISTRIBUTION_DIFF as DISTRIBUTION_DIFF",
	"Exp_Fields___CHANGED_DT as CHANGED_DT",
	"Exp_Fields___COUNTED_BY as COUNTED_BY",
	"Exp_Fields___ADJ_POSTING_BY as ADJ_POSTING_BY",
	"Exp_Fields___ITEM_COUNTED_FLAG as ITEM_COUNTED_FLAG",
	"Exp_Fields___DIFFERENCE_POSTED as DIFFERENCE_POSTED",
	"Exp_Fields___ITEM_RECOUNTED_FLAG as ITEM_RECOUNTED_FLAG",
	"Exp_Fields___ITEM_DELETED_FLAG as ITEM_DELETED_FLAG",
	"Exp_Fields___ALT_UOM_FLAG as ALT_UOM_FLAG",
	"Exp_Fields___ZERO_COUNT_FLAG as ZERO_COUNT_FLAG",
	"Exp_Fields___BASE_UOM as BASE_UOM",
	"Exp_Fields___QTY_UOM as QTY_UOM",
	"Exp_Fields___PHYS_INV_UOM as PHYS_INV_UOM",
	"Exp_Fields___NBR_ARTICLE_DOC as NBR_ARTICLE_DOC",
	"Exp_Fields___ARTICLE_DOC_YR as ARTICLE_DOC_YR",
	"Exp_Fields___ITEM_ARTICLE_DOC as ITEM_ARTICLE_DOC",
	"Exp_Fields___NBR_RECOUNT_DOC as NBR_RECOUNT_DOC",
	"Exp_Fields___CURRENCY_CD as CURRENCY_CD",
	"Exp_Fields___EXCH_RATE_PCT as EXCH_RATE_PCT",
	"Exp_Fields___PHYS_INV_CYCLE_COUNT_IND as PHYS_INV_CYCLE_COUNT_IND",
	"Exp_Fields___WBS_ELEMENT as WBS_ELEMENT",
	"Exp_Fields___SALES_PRICE_TAX_INC as SALES_PRICE_TAX_INC",
	"Exp_Fields___EXT_SALES_VALUE_LOCAL as EXT_SALES_VALUE_LOCAL",
	"Exp_Fields___BOOK_VALUE_SP as BOOK_VALUE_SP",
	"Exp_Fields___INV_FLAG as INV_FLAG",
	"Exp_Fields___SALES_PRICE_TAX_EXC as SALES_PRICE_TAX_EXC",
	"Exp_Fields___SALES_INV_DIFF_VAT as SALES_INV_DIFF_VAT",
	"Exp_Fields___SALES_INV_DIFF_NOVAT as SALES_INV_DIFF_NOVAT",
	"Exp_Fields___PHYS_INV_COUNT_VALUE as PHYS_INV_COUNT_VALUE",
	"Exp_Fields___BOOK_QTY_VALUE as BOOK_QTY_VALUE",
	"Exp_Fields___INV_DIFF as INV_DIFF",
	"Exp_Fields___ARTCILE_CATEGORY as ARTCILE_CATEGORY",
	"Exp_Fields___INV_DIFF_REASON as INV_DIFF_REASON",
	"Exp_Fields___XSITE_CONF_ARTICLE as XSITE_CONF_ARTICLE",
	"Exp_Fields___PHYS_INV_DIST_DIFF as PHYS_INV_DIST_DIFF",
	"Exp_Fields___DATE_COUNT_OT as DATE_COUNT_OT",
	"Exp_Fields___TIME_COUNT_OT as TIME_COUNT_OT",
	"Exp_Fields___FREEZE_DT_INV_BALANCE as FREEZE_DT_INV_BALANCE",
	"Exp_Fields___FREEZE_TIME_INV_BALANCE as FREEZE_TIME_INV_BALANCE",
	"Exp_Fields___BOOK_QTY_CHANGED as BOOK_QTY_CHANGED",
	"Exp_Fields___RETAIL_VALUE as RETAIL_VALUE",
	"Exp_Fields___BOOK_INV as BOOK_INV",
	"Exp_Fields___PHYS_INV_ENTRY_DATE as PHYS_INV_ENTRY_DATE",
	"Exp_Fields___PHYS_INV_ENTRY_TIME as PHYS_INV_ENTRY_TIME",
	"Exp_Fields___PHYS_INV_DIFF_ADJ_TYPE_ID as PHYS_INV_DIFF_ADJ_TYPE_ID",
	"Exp_Fields___UPDATE_DT as UPDATE_DT",
	"Exp_Fields___LOAD_DT as LOAD_DT",
	"Exp_Fields___OVERSTOCK_QTY as OVERSTOCK_QTY") \
	.withColumn('CURRENCY_ID', lit(None)) \
	.withColumn('EXCHANGE_RATE_PCNT', lit(None)) \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------

# Processing node Shortcut_to_INV_PHYSICAL, type TARGET 
# COLUMN COUNT: 88


Shortcut_to_INV_PHYSICAL = Ups_Inv_Physical.selectExpr(
	"CAST(POSTING_DT AS DATE) as POSTING_DT",
	"CAST(DOC_NBR AS STRING) as DOC_NBR",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(CLIENT AS STRING) as CLIENT",
	"CAST(FISCAL_YR AS INT) as FISCAL_YR",
	"CAST(SITE_NBR AS STRING) as SITE_NBR",
	"CAST(SKU_NBR AS INT) as SKU_NBR",
	"CAST(BOOK_QTY AS DECIMAL(13,3)) as BOOK_QTY",
	"CAST(PHYSICAL_QTY AS DECIMAL(13,3)) as PHYSICAL_QTY",
	"CAST(DIFF_QTY AS DECIMAL(13,3)) as DIFF_QTY",
	"CAST(OVERSTOCK_QTY AS DECIMAL(13,3)) as OVERSTOCK_QTY",
	"CAST(DIFF_AMT AS DECIMAL(13,2)) as DIFF_AMT",
	"CAST(TRANS_TYPE AS STRING) as TRANS_TYPE",
	"CAST(STORAGE_LOC AS STRING) as STORAGE_LOC",
	"CAST(SPECIAL_STOCK_IND AS STRING) as SPECIAL_STOCK_IND",
	"CAST(DOC_DT AS DATE) as DOC_DT",
	"CAST(PLANNED_DT AS DATE) as PLANNED_DT",
	"CAST(LAST_COUNT_DT AS DATE) as LAST_COUNT_DT",
	"CAST(FISCAL_PERIOD AS INT) as FISCAL_PERIOD",
	"CAST(CHANGED_BY AS STRING) as CHANGED_BY",
	"CAST(POSTING_BLOCK AS STRING) as POSTING_BLOCK",
	"CAST(COUNT_STATUS AS STRING) as COUNT_STATUS",
	"CAST(ADJ_POSTING_STATUS AS STRING) as ADJ_POSTING_STATUS",
	"CAST(PHYS_INV_REF_NBR AS STRING) as PHYS_INV_REF_NBR",
	"CAST(DELETE_FLAG_STATUS AS STRING) as DELETE_FLAG_STATUS",
	"CAST(BOOK_INV_FREEZE AS STRING) as BOOK_INV_FREEZE",
	"CAST(GROUP_CRIT_TYPE AS STRING) as GROUP_CRIT_TYPE",
	"CAST(GROUP_CRIT_PI AS STRING) as GROUP_CRIT_PI",
	"CAST(PHYS_INV_TYPE_DESC AS STRING) as PHYS_INV_TYPE_DESC",
	"CAST(PHYS_INV_DOC_DESC AS STRING) as PHYS_INV_DOC_DESC",
	"CAST(PHYS_INV_TYPE_ID AS STRING) as PHYS_INV_TYPE_ID",
	"CAST(INV_BALANCE_STATUS AS STRING) as INV_BALANCE_STATUS",
	"CAST(LN_NBR AS INT) as LN_NBR",
	"CAST(BATCH_NBR AS STRING) as BATCH_NBR",
	"CAST(PHYS_INV_STOCK_TYPE_ID AS STRING) as PHYS_INV_STOCK_TYPE_ID",
	"CAST(SALES_ORDER_NBR AS STRING) as SALES_ORDER_NBR",
	"CAST(ITEM_NBR AS INT) as ITEM_NBR",
	"CAST(DELIVERY_SCHED AS INT) as DELIVERY_SCHED",
	"CAST(VENDOR_NBR AS STRING) as VENDOR_NBR",
	"CAST(ACCT_CUST_NBR AS STRING) as ACCT_CUST_NBR",
	"CAST(DISTRIBUTION_DIFF AS STRING) as DISTRIBUTION_DIFF",
	"CAST(CHANGED_DT AS DATE) as CHANGED_DT",
	"CAST(COUNTED_BY AS STRING) as COUNTED_BY",
	"CAST(ADJ_POSTING_BY AS STRING) as ADJ_POSTING_BY",
	"CAST(ITEM_COUNTED_FLAG AS STRING) as ITEM_COUNTED_FLAG",
	"CAST(DIFFERENCE_POSTED AS STRING) as DIFFERENCE_POSTED",
	"CAST(ITEM_RECOUNTED_FLAG AS STRING) as ITEM_RECOUNTED_FLAG",
	"CAST(ITEM_DELETED_FLAG AS STRING) as ITEM_DELETED_FLAG",
	"CAST(ALT_UOM_FLAG AS STRING) as ALT_UOM_FLAG",
	"CAST(ZERO_COUNT_FLAG AS STRING) as ZERO_COUNT_FLAG",
	"CAST(BASE_UOM AS STRING) as BASE_UOM",
	"CAST(QTY_UOM AS DECIMAL(13,3)) as QTY_UOM",
	"CAST(PHYS_INV_UOM AS STRING) as PHYS_INV_UOM",
	"CAST(NBR_ARTICLE_DOC AS STRING) as NBR_ARTICLE_DOC",
	"CAST(ARTICLE_DOC_YR AS INT) as ARTICLE_DOC_YR",
	"CAST(ITEM_ARTICLE_DOC AS INT) as ITEM_ARTICLE_DOC",
	"CAST(NBR_RECOUNT_DOC AS STRING) as NBR_RECOUNT_DOC",
	"CAST(CURRENCY_CD AS STRING) as CURRENCY_CD",
	"CAST(EXCH_RATE_PCT AS DECIMAL(9,6)) as EXCH_RATE_PCT",
	"CAST(PHYS_INV_CYCLE_COUNT_IND AS STRING) as PHYS_INV_CYCLE_COUNT_IND",
	"CAST(WBS_ELEMENT AS INT) as WBS_ELEMENT",
	"CAST(SALES_PRICE_TAX_INC AS DECIMAL(13,2)) as SALES_PRICE_TAX_INC",
	"CAST(EXT_SALES_VALUE_LOCAL AS DECIMAL(13,2)) as EXT_SALES_VALUE_LOCAL",
	"CAST(BOOK_VALUE_SP AS DECIMAL(13,2)) as BOOK_VALUE_SP",
	"CAST(INV_FLAG AS STRING) as INV_FLAG",
	"CAST(SALES_PRICE_TAX_EXC AS DECIMAL(13,2)) as SALES_PRICE_TAX_EXC",
	"CAST(SALES_INV_DIFF_VAT AS DECIMAL(13,2)) as SALES_INV_DIFF_VAT",
	"CAST(SALES_INV_DIFF_NOVAT AS DECIMAL(13,2)) as SALES_INV_DIFF_NOVAT",
	"CAST(PHYS_INV_COUNT_VALUE AS DECIMAL(13,2)) as PHYS_INV_COUNT_VALUE",
	"CAST(BOOK_QTY_VALUE AS DECIMAL(13,2)) as BOOK_QTY_VALUE",
	"CAST(INV_DIFF AS DECIMAL(13,2)) as INV_DIFF",
	"CAST(ARTCILE_CATEGORY AS STRING) as ARTCILE_CATEGORY",
	"CAST(INV_DIFF_REASON AS SMALLINT) as INV_DIFF_REASON",
	"CAST(XSITE_CONF_ARTICLE AS STRING) as XSITE_CONF_ARTICLE",
	"CAST(PHYS_INV_DIST_DIFF AS STRING) as PHYS_INV_DIST_DIFF",
	"CAST(DATE_COUNT_OT AS DATE) as DATE_COUNT_OT",
	"CAST(TIME_COUNT_OT AS DATE) as TIME_COUNT_OT",
	"CAST(FREEZE_DT_INV_BALANCE AS DATE) as FREEZE_DT_INV_BALANCE",
	"CAST(FREEZE_TIME_INV_BALANCE AS DATE) as FREEZE_TIME_INV_BALANCE",
	"CAST(BOOK_QTY_CHANGED AS DECIMAL(13,3)) as BOOK_QTY_CHANGED",
	"CAST(RETAIL_VALUE AS DECIMAL(13,2)) as RETAIL_VALUE",
	"CAST(BOOK_INV AS STRING) as BOOK_INV",
	"CAST(PHYS_INV_ENTRY_DATE AS DATE) as PHYS_INV_ENTRY_DATE",
	"CAST(PHYS_INV_ENTRY_TIME AS DATE) as PHYS_INV_ENTRY_TIME",
	"CAST(PHYS_INV_DIFF_ADJ_TYPE_ID AS INT) as PHYS_INV_DIFF_ADJ_TYPE_ID",
	"CAST(UPDATE_DT AS DATE) as UPDATE_DT",
	"CAST(LOAD_DT AS DATE) as LOAD_DT"  #,
	# "pyspark_data_action as pyspark_data_action"
)

# Shortcut_to_INV_PHYSICAL.write.mode("append").saveAsTable(f'{legacy}.INV_PHYSICAL')

# COMMAND ----------

try:
  refined_perf_table = f"{legacy}.INV_PHYSICAL"
  Shortcut_to_INV_PHYSICAL.createOrReplaceTempView('temp_INV_PHYSICAL')
  primary_key = 'source.POSTING_DT = target.POSTING_DT and source.DOC_NBR=target.DOC_NBR and source.LOCATION_ID=target.LOCATION_ID and source.PRODUCT_ID=target.PRODUCT_ID'

  merge_sql = f"""MERGE INTO {refined_perf_table} as target
                  USING temp_INV_PHYSICAL as source
                  ON {primary_key}
                  WHEN MATCHED THEN
                    UPDATE SET *
                  WHEN NOT MATCHED THEN
                    INSERT *
                  """
  spark.sql(merge_sql)
  logPrevRunDt("INV_PHYSICAL", "INV_PHYSICAL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("INV_PHYSICAL", "INV_PHYSICAL","Failed",str(e), f"{raw}.log_run_details", )
  raise e
