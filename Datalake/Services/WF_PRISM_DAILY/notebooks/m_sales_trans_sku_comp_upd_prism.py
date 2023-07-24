#Code converted on 2023-07-24 08:12:44
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

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')
args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------
# Processing node SQ_Shortcut_to_SALES_TRANS_UPC, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SALES_TRANS_UPC = spark.sql(f"""SELECT
SALES_TRANS_UPC.DAY_DT,
SALES_TRANS_UPC.LOCATION_ID,
SALES_TRANS_UPC.SALES_INSTANCE_ID,
SALES_TRANS_UPC.TP_INVOICE_NBR,
SALES_TRANS_UPC.PRODUCT_ID
FROM SALES_TRANS_UPC
WHERE DAY_DT   > '11-19-2016'""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_WORK_ORDER_RPT, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_SDS_WORK_ORDER_RPT = spark.sql(f"""SELECT
SDS_WORK_ORDER_RPT.SDS_WORK_ORDER_NBR,
SDS_WORK_ORDER_RPT.PETM_POS_INVOICE_ID,
SDS_WORK_ORDER_RPT.LOCATION_ID,
SDS_WORK_ORDER_RPT.SDS_APPT_CREATION_CHANNEL
FROM SDS_WORK_ORDER_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SALES_TRANS_SKU, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_SALES_TRANS_SKU = spark.sql(f"""SELECT
SALES_TRANS_SKU.DAY_DT,
SALES_TRANS_SKU.SALES_INSTANCE_ID_DIST_KEY,
SALES_TRANS_SKU.PRODUCT_ID,
SALES_TRANS_SKU.SALES_INSTANCE_ID,
SALES_TRANS_SKU.LOAD_TSTMP
FROM SALES_TRANS_SKU
WHERE SALES_TYPE_ID!=4 and LOAD_TSTMP >  CURRENT_DATE -1""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f"""SELECT
SKU_PROFILE_RPT.PRODUCT_ID,
SKU_PROFILE_RPT.SAP_DEPT_ID
FROM SKU_PROFILE_RPT
WHERE SAP_DEPT_ID=81""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_Sales_Trans_Upc, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALES_TRANS_UPC_temp = SQ_Shortcut_to_SALES_TRANS_UPC.toDF(*["SQ_Shortcut_to_SALES_TRANS_UPC___" + col for col in SQ_Shortcut_to_SALES_TRANS_UPC.columns])
SQ_Shortcut_to_SKU_PROFILE_RPT_temp = SQ_Shortcut_to_SKU_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT.columns])

JNR_Sales_Trans_Upc = SQ_Shortcut_to_SKU_PROFILE_RPT_temp.join(SQ_Shortcut_to_SALES_TRANS_UPC_temp,[SQ_Shortcut_to_SKU_PROFILE_RPT_temp.SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID == SQ_Shortcut_to_SALES_TRANS_UPC_temp.SQ_Shortcut_to_SALES_TRANS_UPC___PRODUCT_ID],'inner').selectExpr(
	"SQ_Shortcut_to_SALES_TRANS_UPC___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_SALES_TRANS_UPC___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SALES_TRANS_UPC___SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"SQ_Shortcut_to_SALES_TRANS_UPC___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_SALES_TRANS_UPC___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID as PRODUCT_ID1",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_DEPT_ID as SAP_DEPT_ID")

# COMMAND ----------
# Processing node EXP_Pass_Through_src, type EXPRESSION 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNR_Sales_Trans_Upc_temp = JNR_Sales_Trans_Upc.toDF(*["JNR_Sales_Trans_Upc___" + col for col in JNR_Sales_Trans_Upc.columns])

EXP_Pass_Through_src = JNR_Sales_Trans_Upc_temp.selectExpr(
	"JNR_Sales_Trans_Upc___sys_row_id as sys_row_id",
	"JNR_Sales_Trans_Upc___DAY_DT as DAY_DT",
	"JNR_Sales_Trans_Upc___LOCATION_ID as LOCATION_ID",
	"JNR_Sales_Trans_Upc___SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"JNR_Sales_Trans_Upc___PRODUCT_ID as PRODUCT_ID",
	"JNR_Sales_Trans_Upc___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"cast(JNR_Sales_Trans_Upc___TP_INVOICE_NBR as int) as o_TP_INVOICE_NBR",
	"JNR_Sales_Trans_Upc___PRODUCT_ID1 as PRODUCT_ID1",
	"JNR_Sales_Trans_Upc___SAP_DEPT_ID as SAP_DEPT_ID"
)

# COMMAND ----------
# Processing node JNR_Sales_Trans_Sku, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALES_TRANS_SKU_temp = SQ_Shortcut_to_SALES_TRANS_SKU.toDF(*["SQ_Shortcut_to_SALES_TRANS_SKU___" + col for col in SQ_Shortcut_to_SALES_TRANS_SKU.columns])
EXP_Pass_Through_src_temp = EXP_Pass_Through_src.toDF(*["EXP_Pass_Through_src___" + col for col in EXP_Pass_Through_src.columns])

JNR_Sales_Trans_Sku = SQ_Shortcut_to_SALES_TRANS_SKU_temp.join(EXP_Pass_Through_src_temp,[SQ_Shortcut_to_SALES_TRANS_SKU_temp.SQ_Shortcut_to_SALES_TRANS_SKU___DAY_DT == EXP_Pass_Through_src_temp.EXP_Pass_Through_src___DAY_DT, SQ_Shortcut_to_SALES_TRANS_SKU_temp.SQ_Shortcut_to_SALES_TRANS_SKU___PRODUCT_ID == EXP_Pass_Through_src_temp.EXP_Pass_Through_src___PRODUCT_ID, SQ_Shortcut_to_SALES_TRANS_SKU_temp.SQ_Shortcut_to_SALES_TRANS_SKU___SALES_INSTANCE_ID == EXP_Pass_Through_src_temp.EXP_Pass_Through_src___SALES_INSTANCE_ID],'inner').selectExpr(
	"EXP_Pass_Through_src___DAY_DT as DAY_DT",
	"EXP_Pass_Through_src___LOCATION_ID as LOCATION_ID",
	"EXP_Pass_Through_src___SALES_INSTANCE_ID as SALES_INSTANCE_ID1",
	"EXP_Pass_Through_src___PRODUCT_ID as PRODUCT_ID",
	"EXP_Pass_Through_src___o_TP_INVOICE_NBR as TP_INVOICE_NBR",
	"EXP_Pass_Through_src___PRODUCT_ID1 as PRODUCT_ID1",
	"EXP_Pass_Through_src___SAP_DEPT_ID as SAP_DEPT_ID",
	"SQ_Shortcut_to_SALES_TRANS_SKU___DAY_DT as DAY_DT1",
	"SQ_Shortcut_to_SALES_TRANS_SKU___SALES_INSTANCE_ID_DIST_KEY as SALES_INSTANCE_ID_DIST_KEY",
	"SQ_Shortcut_to_SALES_TRANS_SKU___PRODUCT_ID as PRODUCT_ID2",
	"SQ_Shortcut_to_SALES_TRANS_SKU___SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"SQ_Shortcut_to_SALES_TRANS_SKU___LOAD_TSTMP as LOAD_TSTMP")

# COMMAND ----------
# Processing node JNR_SDS_WORK_ORDER_RPT, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
JNR_Sales_Trans_Sku_temp = JNR_Sales_Trans_Sku.toDF(*["JNR_Sales_Trans_Sku___" + col for col in JNR_Sales_Trans_Sku.columns])
SQ_Shortcut_to_SDS_WORK_ORDER_RPT_temp = SQ_Shortcut_to_SDS_WORK_ORDER_RPT.toDF(*["SQ_Shortcut_to_SDS_WORK_ORDER_RPT___" + col for col in SQ_Shortcut_to_SDS_WORK_ORDER_RPT.columns])

JNR_SDS_WORK_ORDER_RPT = SQ_Shortcut_to_SDS_WORK_ORDER_RPT_temp.join(JNR_Sales_Trans_Sku_temp,[SQ_Shortcut_to_SDS_WORK_ORDER_RPT_temp.SQ_Shortcut_to_SDS_WORK_ORDER_RPT___PETM_POS_INVOICE_ID == JNR_Sales_Trans_Sku_temp.JNR_Sales_Trans_Sku___TP_INVOICE_NBR, SQ_Shortcut_to_SDS_WORK_ORDER_RPT_temp.SQ_Shortcut_to_SDS_WORK_ORDER_RPT___LOCATION_ID == JNR_Sales_Trans_Sku_temp.JNR_Sales_Trans_Sku___LOCATION_ID],'inner').selectExpr(
	"JNR_Sales_Trans_Sku___DAY_DT as DAY_DT",
	"JNR_Sales_Trans_Sku___LOCATION_ID as LOCATION_ID",
	"JNR_Sales_Trans_Sku___SALES_INSTANCE_ID1 as SALES_INSTANCE_ID1",
	"JNR_Sales_Trans_Sku___PRODUCT_ID as PRODUCT_ID",
	"JNR_Sales_Trans_Sku___TP_INVOICE_NBR as TP_INVOICE_NBR",
	"JNR_Sales_Trans_Sku___PRODUCT_ID1 as PRODUCT_ID1",
	"JNR_Sales_Trans_Sku___SAP_DEPT_ID as SAP_DEPT_ID",
	"JNR_Sales_Trans_Sku___DAY_DT1 as DAY_DT1",
	"JNR_Sales_Trans_Sku___SALES_INSTANCE_ID_DIST_KEY as SALES_INSTANCE_ID_DIST_KEY",
	"JNR_Sales_Trans_Sku___PRODUCT_ID2 as PRODUCT_ID2",
	"JNR_Sales_Trans_Sku___SALES_INSTANCE_ID as SALES_INSTANCE_ID",
	"JNR_Sales_Trans_Sku___LOAD_TSTMP as LOAD_TSTMP",
	"SQ_Shortcut_to_SDS_WORK_ORDER_RPT___PETM_POS_INVOICE_ID as PETM_POS_INVOICE_ID",
	"SQ_Shortcut_to_SDS_WORK_ORDER_RPT___LOCATION_ID as LOCATION_ID1",
	"SQ_Shortcut_to_SDS_WORK_ORDER_RPT___SDS_APPT_CREATION_CHANNEL as SDS_APPT_CREATION_CHANNEL",
	"SQ_Shortcut_to_SDS_WORK_ORDER_RPT___SDS_WORK_ORDER_NBR as SDS_WORK_ORDER_NBR")

# COMMAND ----------
# Processing node SRT_TARGET, type SORTER 
# COLUMN COUNT: 6

SRT_TARGET = JNR_SDS_WORK_ORDER_RPT.select(
).sort(col('DAY_DT1').asc(), col('SALES_INSTANCE_ID_DIST_KEY').asc(), col('PRODUCT_ID2').asc(), col('SDS_WORK_ORDER_NBR').desc())

# COMMAND ----------
# Processing node AGG_TARGET, type AGGREGATOR 
# COLUMN COUNT: 6

AGG_TARGET = SRT_TARGET
	.groupBy("DAY_DT1","SALES_INSTANCE_ID_DIST_KEY","PRODUCT_ID2")
	.agg( \
	min(SRT_TARGET.SDS_APPT_CREATION_CHANNEL).alias('SDS_APPT_CREATION_CHANNEL'),
	min(SRT_TARGET.LOAD_TSTMP).alias('LOAD_TSTMP'),
	min(SRT_TARGET.SDS_WORK_ORDER_NBR).alias('SDS_WORK_ORDER_NBR')
	)
	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_TARGET, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
AGG_TARGET_temp = AGG_TARGET.toDF(*["AGG_TARGET___" + col for col in AGG_TARGET.columns])

EXP_TARGET = AGG_TARGET_temp.selectExpr(
	"AGG_TARGET___sys_row_id as sys_row_id",
	"AGG_TARGET___DAY_DT1 as DAY_DT1",
	"AGG_TARGET___SALES_INSTANCE_ID_DIST_KEY as SALES_INSTANCE_ID_DIST_KEY",
	"AGG_TARGET___PRODUCT_ID2 as PRODUCT_ID2",
	"AGG_TARGET___SDS_APPT_CREATION_CHANNEL as SDS_APPT_CREATION_CHANNEL",
	"DECODE ( AGG_TARGET___SDS_APPT_CREATION_CHANNEL , 'In-Store' , 'STR' , 'SRC' , 'CSR' , 'Online' , 'MOB' , 'Online Mobile' , 'MOB' , 'Online Web' , 'MOB' ) as ORDER_CREATION_CHANNEL",
	"CURRENT_TIMESTAMP () as UPDATE_TSTMP",
	"AGG_TARGET___LOAD_TSTMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node UPD_TARGET, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_TARGET_temp = EXP_TARGET.toDF(*["EXP_TARGET___" + col for col in EXP_TARGET.columns])

UPD_TARGET = EXP_TARGET_temp.selectExpr(
	"EXP_TARGET___DAY_DT1 as DAY_DT",
	"EXP_TARGET___SALES_INSTANCE_ID_DIST_KEY as SALES_INSTANCE_ID_DIST_KEY",
	"EXP_TARGET___PRODUCT_ID2 as PRODUCT_ID",
	"EXP_TARGET___ORDER_CREATION_CHANNEL as ORDER_CREATION_CHANNEL",
	"EXP_TARGET___UPDATE_TSTMP as UPDATE_TSTMP")
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node SALES_TRANS_SKU1, type TARGET 
# COLUMN COUNT: 65


SALES_TRANS_SKU1 = UPD_TARGET.selectExpr(
	"CAST(DAY_DT AS DATE) as DAY_DT",
	"col("SALES_INSTANCE_ID_DIST_KEY") as SALES_INSTANCE_ID_DIST_KEY",
	"CAST(PRODUCT_ID AS BIGINT) as PRODUCT_ID",
	"col("SALES_INSTANCE_ID") as SALES_INSTANCE_ID",
	"CAST(lit(None) AS BIGINT) as LOCATION_ID",
	"col("SALES_TYPE_ID") as SALES_TYPE_ID",
	"CAST(lit(None) AS CHAR) as VOID_TYPE_CD",
	"col("TXN_WAS_POST_VOIDED_FLAG") as TXN_WAS_POST_VOIDED_FLAG",
	"col("ORDER_NBR") as ORDER_NBR",
	"col("ORDER_SEQ_NBR") as ORDER_SEQ_NBR",
	"CAST(lit(None) AS STRING) as ORDER_CHANNEL",
	"CAST(lit(None) AS BIGINT) as ORDER_ASSIST_LOCATION_ID",
	"CAST(lit(None) AS STRING) as ORDER_FULFILLMENT_CHANNEL",
	"CAST(ORDER_CREATION_CHANNEL AS STRING) as ORDER_CREATION_CHANNEL",
	"CAST(lit(None) AS STRING) as ORDER_CREATION_DEVICE_TYPE",
	"CAST(lit(None) AS INT) as ORDER_CREATION_DEVICE_WIDTH",
	"CAST(lit(None) AS STRING) as TXN_SEGMENT",
	"CAST(lit(None) AS STRING) as PAYMENT_DEVICE_TYPE",
	"CAST(lit(None) AS TIMESTAMP) as TRANS_TSTMP",
	"col("LOYALTY_NBR") as LOYALTY_NBR",
	"CAST(lit(None) AS STRING) as LOYALTY_REDEMPTION_ID",
	"CAST(lit(None) AS STRING) as LUID",
	"col("CUST_TRANS_ID") as CUST_TRANS_ID",
	"CAST(lit(None) AS BIGINT) as CUSTOMER_EID",
	"CAST(lit(None) AS BIGINT) as CUSTOMER_GID",
	"col("SALES_CUSTOMER_LINK_EXCL_TYPE_ID") as SALES_CUSTOMER_LINK_EXCL_TYPE_ID",
	"col("SPECIAL_SALES_FLAG") as SPECIAL_SALES_FLAG",
	"col("RECEIPTLESS_RETURN_FLAG") as RECEIPTLESS_RETURN_FLAG",
	"CAST(lit(None) AS DATE) as TRAINING_START_DT",
	"CAST(lit(None) AS STRING) as TRAINER_NAME",
	"CAST(lit(None) AS DECIMAL(10,2)) as SALES_AMT",
	"CAST(lit(None) AS DECIMAL(10,2)) as SALES_COST",
	"CAST(lit(None) AS BIGINT) as SALES_QTY",
	"CAST(lit(None) AS DECIMAL(10,2)) as SPECIAL_SALES_AMT",
	"CAST(lit(None) AS BIGINT) as SPECIAL_SALES_QTY",
	"CAST(lit(None) AS DECIMAL(10,2)) as RETURN_AMT",
	"CAST(lit(None) AS DECIMAL(10,2)) as RETURN_COST",
	"CAST(lit(None) AS BIGINT) as RETURN_QTY",
	"CAST(lit(None) AS DECIMAL(10,2)) as CLEARANCE_AMT",
	"CAST(lit(None) AS INT) as CLEARANCE_QTY",
	"CAST(lit(None) AS DECIMAL(10,2)) as CLEARANCE_RETURN_AMT",
	"CAST(lit(None) AS INT) as CLEARANCE_RETURN_QTY",
	"CAST(lit(None) AS DECIMAL(10,2)) as SPECIAL_RETURN_AMT",
	"CAST(lit(None) AS BIGINT) as SPECIAL_RETURN_QTY",
	"CAST(lit(None) AS DECIMAL(10,2)) as SPECIAL_SRVC_AMT",
	"CAST(lit(None) AS DECIMAL(10,4)) as DISCOUNT_AMT",
	"CAST(lit(None) AS BIGINT) as DISCOUNT_QTY",
	"CAST(lit(None) AS DECIMAL(10,4)) as DISCOUNT_RETURN_AMT",
	"CAST(lit(None) AS BIGINT) as DISCOUNT_RETURN_QTY",
	"CAST(lit(None) AS DECIMAL(8,2)) as POS_COUPON_AMT",
	"col("POS_COUPON_QTY") as POS_COUPON_QTY",
	"CAST(lit(None) AS DECIMAL(8,2)) as POS_COUPON_ALLOC_AMT",
	"CAST(lit(None) AS INT) as POS_COUPON_ALLOC_QTY",
	"CAST(lit(None) AS DECIMAL(10,2)) as NET_SALES_AMT",
	"CAST(lit(None) AS DECIMAL(10,2)) as NET_SALES_COST",
	"CAST(lit(None) AS BIGINT) as NET_SALES_QTY",
	"CAST(lit(None) AS DECIMAL(9,3)) as MA_SALES_AMT",
	"col("MA_SALES_QTY") as MA_SALES_QTY",
	"CAST(lit(None) AS DECIMAL(12,3)) as MA_TRANS_AMT",
	"CAST(lit(None) AS DECIMAL(12,3)) as MA_TRANS_COST",
	"col("MA_TRANS_QTY") as MA_TRANS_QTY",
	"CAST(lit(None) AS DECIMAL(12,3)) as NET_MARGIN_AMT",
	"CAST(lit(None) AS DECIMAL(9,6)) as EXCH_RATE_PCT",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(lit(None) AS TIMESTAMP) as LOAD_TSTMP",
	"UPD_TARGET.pyspark_data_action as pyspark_data_action"
)
SALES_TRANS_SKU1.write.saveAsTable(f'{raw}.SALES_TRANS_SKU')

# COMMAND ----------
# Processing node SALES_TRANS_COMPONENT, type TARGET 
# COLUMN COUNT: 99


SALES_TRANS_COMPONENT = UPD_TARGET.selectExpr(
	"CAST(DAY_DT AS DATE) as DAY_DT",
	"CAST(lit(None) AS BIGINT) as LOCATION_ID",
	"col("SALES_INSTANCE_ID_DIST_KEY") as SALES_INSTANCE_ID_DIST_KEY",
	"col("UPC_ID") as UPC_ID",
	"col("TP_INVOICE_NBR") as TP_INVOICE_NBR",
	"col("PARENT_UPC_ID") as PARENT_UPC_ID",
	"CAST(lit(None) AS CHAR) as COMBO_TYPE_CD",
	"col("POS_TXN_SEQ_NBR") as POS_TXN_SEQ_NBR",
	"col("CPN_POS_TXN_SEQ_NBR") as CPN_POS_TXN_SEQ_NBR",
	"col("OFFER_ID") as OFFER_ID",
	"CAST(lit(None) AS BIGINT) as MA_EVENT_ID",
	"col("DISCOUNT_TYPE_ID") as DISCOUNT_TYPE_ID",
	"col("COUPON_TYPE_ID") as COUPON_TYPE_ID",
	"CAST(lit(None) AS STRING) as COUPON_ACCESS_CD",
	"CAST(PRODUCT_ID AS BIGINT) as PRODUCT_ID",
	"CAST(lit(None) AS BIGINT) as MOVEMENT_ID",
	"col("PO_NBR") as PO_NBR",
	"CAST(lit(None) AS BIGINT) as PO_LINE_NBR",
	"col("PAYMENT_TYPE_ID") as PAYMENT_TYPE_ID",
	"CAST(lit(None) AS STRING) as TRANS_ACCT_NBR",
	"CAST(lit(None) AS CHAR) as AUTH_APPROVAL_CODE",
	"CAST(lit(None) AS BIGINT) as SALES_COMPONENT_ID",
	"CAST(lit(None) AS BIGINT) as SALES_COMPONENT_TYPE_ID",
	"col("ISSUANCE_ID") as ISSUANCE_ID",
	"col("TAX_TYPE_ID") as TAX_TYPE_ID",
	"col("SALES_INSTANCE_ID") as SALES_INSTANCE_ID",
	"CAST(lit(None) AS CHAR) as VOID_TYPE_CD",
	"col("TXN_WAS_POST_VOIDED_FLAG") as TXN_WAS_POST_VOIDED_FLAG",
	"CAST(lit(None) AS TIMESTAMP) as TRANS_TSTMP",
	"col("REGISTER_NBR") as REGISTER_NBR",
	"col("TRANSACTION_NBR") as TRANSACTION_NBR",
	"col("SALES_TYPE_ID") as SALES_TYPE_ID",
	"CAST(lit(None) AS CHAR) as SALES_CUST_CAPTURE_CD",
	"col("CUST_TRANS_ID") as CUST_TRANS_ID",
	"CAST(lit(None) AS BIGINT) as CASHIER_NBR",
	"CAST(lit(None) AS BIGINT) as PETPERK_OVERRIDE_NBR",
	"col("PETPERK_EMAIL_IND") as PETPERK_EMAIL_IND",
	"CAST(lit(None) AS CHAR) as KEYED_FLAG",
	"col("NON_TAX_FLAG") as NON_TAX_FLAG",
	"CAST(lit(None) AS BIGINT) as EMPLOYEE_ID",
	"CAST(lit(None) AS STRING) as CUST_FIRST_NAME",
	"CAST(lit(None) AS STRING) as CUST_LAST_NAME",
	"CAST(lit(None) AS STRING) as TENDER_UID",
	"CAST(lit(None) AS STRING) as TAX_EXEMPT_ID",
	"col("ORDER_NBR") as ORDER_NBR",
	"CAST(lit(None) AS STRING) as ORDER_CHANNEL",
	"CAST(lit(None) AS BIGINT) as ORDER_ASSIST_LOCATION_ID",
	"CAST(lit(None) AS STRING) as ORDER_FULFILLMENT_CHANNEL",
	"CAST(ORDER_CREATION_CHANNEL AS STRING) as ORDER_CREATION_CHANNEL",
	"col("CDC_EMAIL_ID") as CDC_EMAIL_ID",
	"col("CDC_FIRST_NAME_ID") as CDC_FIRST_NAME_ID",
	"col("CDC_LAST_NAME_ID") as CDC_LAST_NAME_ID",
	"col("CDC_PHONE_NBR_ID") as CDC_PHONE_NBR_ID",
	"CAST(lit(None) AS STRING) as DIGITAL_RECEIPT_ANSWER_CD",
	"col("OFFLINE_CUST_LKP_IND") as OFFLINE_CUST_LKP_IND",
	"CAST(lit(None) AS BIGINT) as REV_SALES_TYPE_CTRL_ID",
	"col("REV_SALES_TYPE_ID") as REV_SALES_TYPE_ID",
	"col("EP_SALES_TYPE_ID") as EP_SALES_TYPE_ID",
	"CAST(lit(None) AS DECIMAL(11,2)) as NET_SALES_AMT",
	"CAST(lit(None) AS INT) as NET_SALES_QTY",
	"CAST(lit(None) AS DECIMAL(12,3)) as NET_MARGIN_AMT",
	"CAST(lit(None) AS DECIMAL(10,2)) as SALES_AMT",
	"CAST(lit(None) AS DECIMAL(10,2)) as SALES_COST",
	"CAST(lit(None) AS INT) as SALES_QTY",
	"CAST(lit(None) AS DECIMAL(8,2)) as RETURN_AMT",
	"CAST(lit(None) AS DECIMAL(8,2)) as RETURN_COST",
	"CAST(lit(None) AS INT) as RETURN_QTY",
	"CAST(lit(None) AS DECIMAL(10,2)) as CLEARANCE_AMT",
	"CAST(lit(None) AS INT) as CLEARANCE_QTY",
	"CAST(lit(None) AS DECIMAL(10,2)) as CLEARANCE_RETURN_AMT",
	"CAST(lit(None) AS INT) as CLEARANCE_RETURN_QTY",
	"CAST(lit(None) AS DECIMAL(10,4)) as DISCOUNT_AMT",
	"CAST(lit(None) AS INT) as DISCOUNT_QTY",
	"CAST(lit(None) AS DECIMAL(10,4)) as DISCOUNT_RETURN_AMT",
	"CAST(lit(None) AS INT) as DISCOUNT_RETURN_QTY",
	"CAST(lit(None) AS DECIMAL(8,2)) as POS_COUPON_AMT",
	"CAST(lit(None) AS INT) as POS_COUPON_QTY",
	"CAST(lit(None) AS DECIMAL(8,2)) as POS_COUPON_ALLOC_AMT",
	"CAST(lit(None) AS INT) as POS_COUPON_ALLOC_QTY",
	"CAST(lit(None) AS DECIMAL(12,2)) as SPECIAL_SALES_AMT",
	"CAST(lit(None) AS INT) as SPECIAL_SALES_QTY",
	"CAST(lit(None) AS DECIMAL(12,2)) as SPECIAL_RETURN_AMT",
	"CAST(lit(None) AS INT) as SPECIAL_RETURN_QTY",
	"CAST(lit(None) AS DECIMAL(8,2)) as SPECIAL_SRVC_AMT",
	"CAST(lit(None) AS INT) as SPECIAL_SRVC_QTY",
	"CAST(lit(None) AS DECIMAL(9,3)) as MA_SALES_AMT",
	"col("MA_SALES_QTY") as MA_SALES_QTY",
	"CAST(lit(None) AS DECIMAL(12,3)) as MA_TRANS_AMT",
	"CAST(lit(None) AS DECIMAL(12,3)) as MA_TRANS_COST",
	"col("MA_TRANS_QTY") as MA_TRANS_QTY",
	"CAST(lit(None) AS DECIMAL(8,2)) as SALES_TRANS_AMT",
	"CAST(lit(None) AS DECIMAL(8,2)) as RETURN_TRANS_AMT",
	"CAST(lit(None) AS BIGINT) as ISSUANCE_QTY",
	"col("OFFER_QTY") as OFFER_QTY",
	"CAST(lit(None) AS DECIMAL(6,4)) as TAX_PCT",
	"CAST(lit(None) AS DECIMAL(8,2)) as TAX_AMT",
	"CAST(lit(None) AS DECIMAL(9,6)) as EXCH_RATE_PCT",
	"CAST(UPDATE_TSTMP AS DATE) as UPDATE_DT",
	"CAST(lit(None) AS DATE) as LOAD_DT",
	"UPD_TARGET.pyspark_data_action as pyspark_data_action"
)
SALES_TRANS_COMPONENT.write.saveAsTable(f'{raw}.SALES_TRANS_COMPONENT')