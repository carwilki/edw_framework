# Databricks notebook source
#Code converted on 2023-12-08 16:32:21
import os
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)
dbutils.widgets.text(name = 'env', defaultValue = 'dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
refine = getEnvPrefix(env) + 'refine'

# Set global variables
starttime = datetime.now() #start timestamp of the script
target_bucket=getParameterValue(raw,'wf_NZ2ORA_Inventory_Replication','m_NZ2ORA_INV_INSTOCK_PRICE_DAY_FLAT','target_bucket')
target_file=getParameterValue(raw,'wf_NZ2ORA_Inventory_Replication','m_NZ2ORA_INV_INSTOCK_PRICE_DAY_FLAT','target_file')

# COMMAND ----------

print(target_bucket, target_file)

# COMMAND ----------

current_date = datetime.today().strftime('%Y%m%d')
target_bucket = target_bucket.strip("/") + "/" + current_date
print(target_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY, type SOURCE 
# COLUMN COUNT: 30

SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY = spark.sql(f"""SELECT

I.DAY_DT, I.PRODUCT_ID, I.LOCATION_ID,

   I.FROM_LOCATION_ID, I.SOURCE_VENDOR_ID, I.SKU_STATUS_ID,

   I.STORE_OPEN_IND, I.OUT_OF_STOCK_CNT, I.POG_LISTED_IND,

   I.SAP_LISTED_IND, I.INLINE_CNT, I.PLANNER_IND,

   I.SUBS_IND, I.MAP_AMT, I.EXCH_RATE_PCT,

   I.ON_HAND_QTY, I.COMMITTED_QTY, I.XFER_IN_TRANS_QTY,

   I.ON_ORDER_QTY, I.SUM_COST_AMT, I.BUM_COST_AMT,

   I.ON_ORDER_CD, I.RETAIL_PRICE_AMT, I.SKU_FACINGS_QTY,

   I.SKU_CAPACITY_QTY, I.PETPERKS_AMT, I.PETPERKS_IND,

   I.LOCAL_PRICE_AMT, I.LOC_PETPERKS_PRICE_AMT, I.LOAD_DT

FROM {legacy}.INV_INSTOCK_PRICE_DAY I

WHERE I.LOAD_DT = CURRENT_DATE
""")

# Conforming fields names to the component layout
SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY = SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[0],'DAY_DT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[1],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[2],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[3],'FROM_LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[4],'SOURCE_VENDOR_ID') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[5],'SKU_STATUS_ID') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[6],'STORE_OPEN_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[7],'OUT_OF_STOCK_CNT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[8],'POG_LISTED_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[9],'SAP_LISTED_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[10],'INLINE_CNT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[11],'PLANNER_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[12],'SUBS_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[13],'MAP_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[14],'EXCH_RATE_PCT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[15],'ON_HAND_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[16],'COMMITTED_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[17],'XFER_IN_TRANS_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[18],'ON_ORDER_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[19],'SUM_COST_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[20],'BUM_COST_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[21],'ON_ORDER_CD') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[22],'RETAIL_PRICE_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[23],'SKU_FACINGS_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[24],'SKU_CAPACITY_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[25],'PETPERKS_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[26],'PETPERKS_IND') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[27],'LOCAL_PRICE_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[28],'LOC_PETPERKS_PRICE_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.columns[29],'LOAD_DT')

# COMMAND ----------

# Processing node Shortcut_to_INV_INSTOCK_PRICE_DAY_PRE, type TARGET 
# COLUMN COUNT: 30

Shortcut_to_INV_INSTOCK_PRICE_DAY_PRE = SQ_Shortcut_to_INV_INSTOCK_PRICE_DAY.selectExpr(	
    "CASE WHEN DAY_DT IS NULL THEN RPAD('*', 19) ELSE  LPAD(date_format(DAY_DT, 'MM/dd/yyyy HH:mm:ss')::string ,19 ) END as DAY_DT",
	"CASE WHEN PRODUCT_ID IS NULL THEN RPAD('*', 7) ELSE LPAD(PRODUCT_ID::string ,7 ) END as PRODUCT_ID",
	"CASE WHEN LOCATION_ID IS NULL THEN RPAD('*', 5) ELSE LPAD(LOCATION_ID::string ,5 ) END as LOCATION_ID",
	"CASE WHEN FROM_LOCATION_ID IS NULL THEN RPAD('*', 5) ELSE LPAD(FROM_LOCATION_ID::string ,5 ) END as FROM_LOCATION_ID",
	"CASE WHEN SOURCE_VENDOR_ID IS NULL THEN RPAD('*', 10) ELSE LPAD(SOURCE_VENDOR_ID::string ,10 ) END as SOURCE_VENDOR_ID",
	"CASE WHEN SKU_STATUS_ID IS NULL THEN RPAD('*', 2) ELSE LPAD(SKU_STATUS_ID::string ,2 )  END as SKU_STATUS_ID",
	"CASE WHEN STORE_OPEN_IND IS NULL THEN RPAD('*', 1) ELSE LPAD(STORE_OPEN_IND::string ,1 ) END as STORE_OPEN_IND",
	"CASE WHEN OUT_OF_STOCK_CNT IS NULL THEN RPAD('*', 1) ELSE LPAD(OUT_OF_STOCK_CNT::string ,1 ) END as OUT_OF_STOCK_CNT",
	"CASE WHEN POG_LISTED_IND IS NULL THEN RPAD('*', 1) ELSE LPAD(POG_LISTED_IND::string ,1 ) END as POG_LISTED_IND",
	"CASE WHEN SAP_LISTED_IND IS NULL THEN RPAD('*', 1) ELSE LPAD(SAP_LISTED_IND::string ,1 ) END as SAP_LISTED_IND",
	"CASE WHEN INLINE_CNT IS NULL THEN RPAD('*', 1) ELSE LPAD(INLINE_CNT::string ,1 ) END as INLINE_CNT",
	"CASE WHEN PLANNER_IND IS NULL THEN RPAD('*', 1) ELSE LPAD(PLANNER_IND::string ,1 ) END as PLANNER_IND",
	"CASE WHEN SUBS_IND IS NULL THEN RPAD('*', 1) ELSE LPAD(SUBS_IND::string ,1 ) END as SUBS_IND",
	"CASE WHEN MAP_AMT IS NULL THEN RPAD('*', 9) ELSE LPAD(MAP_AMT::string ,9 ) END as MAP_AMT",
	"CASE WHEN EXCH_RATE_PCT IS NULL THEN RPAD('*', 9) ELSE LPAD(EXCH_RATE_PCT::string ,9 ) END as EXCH_RATE_PCT",
	"CASE WHEN ON_HAND_QTY IS NULL THEN RPAD('*', 9) ELSE LPAD(ON_HAND_QTY::string ,9 ) END as ON_HAND_QTY",
	"CASE WHEN COMMITTED_QTY IS NULL THEN RPAD('*', 9) ELSE LPAD(COMMITTED_QTY::string ,9 ) END as COMMITTED_QTY",
	"CASE WHEN XFER_IN_TRANS_QTY IS NULL THEN RPAD('*', 9) ELSE LPAD(XFER_IN_TRANS_QTY::string ,9 ) END as XFER_IN_TRANS_QTY",
	"CASE WHEN ON_ORDER_QTY IS NULL THEN RPAD('*', 9) ELSE LPAD(ON_ORDER_QTY::string ,9 ) END as ON_ORDER_QTY",
	"CASE WHEN SUM_COST_AMT IS NULL THEN RPAD('*', 9) ELSE LPAD(SUM_COST_AMT::string ,9 ) END as SUM_COST_AMT",
	"CASE WHEN BUM_COST_AMT IS NULL THEN RPAD('*', 9) ELSE LPAD(BUM_COST_AMT::string ,9 ) END as BUM_COST_AMT",
	"CASE WHEN ON_ORDER_CD IS NULL THEN RPAD('*', 1) ELSE LPAD(ON_ORDER_CD::string , 1) END as ON_ORDER_CD",
	"CASE WHEN RETAIL_PRICE_AMT IS NULL THEN RPAD('*', 8) ELSE LPAD(RETAIL_PRICE_AMT::string ,8 ) END as RETAIL_PRICE_AMT",
	"CASE WHEN SKU_FACINGS_QTY IS NULL THEN RPAD('*', 7) ELSE LPAD(SKU_FACINGS_QTY::string ,7 ) END as SKU_FACINGS_QTY",
	"CASE WHEN SKU_CAPACITY_QTY IS NULL THEN RPAD('*', 9) ELSE LPAD(SKU_CAPACITY_QTY::string ,9 ) END as SKU_CAPACITY_QTY",
	"CASE WHEN PETPERKS_AMT IS NULL THEN RPAD('*', 8) ELSE LPAD(PETPERKS_AMT::string ,8 ) END as PETPERKS_AMT",
	"CASE WHEN PETPERKS_IND IS NULL THEN RPAD('*', 1) ELSE LPAD(PETPERKS_IND::string ,1 ) END as PETPERKS_IND",
	"CASE WHEN LOCAL_PRICE_AMT IS NULL THEN RPAD('*', 8) ELSE LPAD(LOCAL_PRICE_AMT::string ,8 ) END as LOCAL_PRICE_AMT",
	"CASE WHEN LOC_PETPERKS_PRICE_AMT IS NULL THEN RPAD('*', 8) ELSE LPAD(LOC_PETPERKS_PRICE_AMT::string ,8 ) END as LOC_PETPERKS_PRICE_AMT",
    "CASE WHEN LOAD_DT IS NULL THEN RPAD('*', 19) ELSE LPAD(date_format(LOAD_DT, 'MM/dd/yyyy HH:mm:ss')::string ,19 ) END as LOAD_DT"
)
# writeToFlatFile(Shortcut_to_INV_INSTOCK_PRICE_DAY_PRE, target_bucket, target_file, 'overwrite' )

cols = Shortcut_to_INV_INSTOCK_PRICE_DAY_PRE.schema.names
Shortcut_to_INV_INSTOCK_PRICE_DAY_PRE_text = Shortcut_to_INV_INSTOCK_PRICE_DAY_PRE.select(concat(*cols).alias('value'))

Shortcut_to_INV_INSTOCK_PRICE_DAY_PRE_text.repartition(1).write.mode('overwrite').text(target_bucket.strip("/") + "/" + target_file[:-4])
removeTransactionFiles(target_bucket)
newFilePath = target_bucket.strip("/") + "/" + target_file[:-4]
print(newFilePath)
print(target_bucket)
renamePartFileNames(newFilePath, newFilePath,'.dat')
