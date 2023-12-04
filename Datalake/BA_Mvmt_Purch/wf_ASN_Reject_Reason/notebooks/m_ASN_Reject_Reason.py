# Databricks notebook source
#Code converted on 2023-10-31 09:34:07
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


# COMMAND ----------

# Processing node LKP_SKU_PROFILE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_SKU_PROFILE_SRC = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE""")
# Conforming fields names to the component layout
# LKP_SKU_PROFILE_SRC = LKP_SKU_PROFILE_SRC \
# 	.withColumnRenamed(LKP_SKU_PROFILE_SRC.columns[0],'')

# COMMAND ----------

# Processing node LKP_SITE_PROFILE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_SITE_PROFILE_SRC = spark.sql(f"""SELECT
LOCATION_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE""")
# Conforming fields names to the component layout
# LKP_SITE_PROFILE_SRC = LKP_SITE_PROFILE_SRC \
# 	.withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[0],'')


source_bucket=getParameterValue(raw,'wf_ASN_Reject_Reason','m_ASN_Reject_Reason','source_bucket')
key=getParameterValue(raw,'wf_ASN_Reject_Reason','m_ASN_Reject_Reason','key')

source_file=get_src_file(key,source_bucket)

# COMMAND ----------

# print(source_bucket)

# COMMAND ----------

# Processing node SQ_Shortcut_to_ASN_N_CPLT_RPT, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_ASN_N_CPLT_RPT = spark.read.csv(source_file, sep=',', header=False , inferSchema=True).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_ASN_N_CPLT_RPT = SQ_Shortcut_to_ASN_N_CPLT_RPT \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[0],'STATUS_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[1],'MSG_NO') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[2],'TEXT') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[3],'ASN_CREATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[4],'ASN_CREATE_TM') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[5],'DOC_NO') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[6],'PURCH_DOC') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[7],'VENDOR_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[8],'NAME') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[9],'UPC_ID') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[10],'DESCRIPTION') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[11],'ARTICLE') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[12],'BILL_OF_LOAD') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[13],'DELIVERY_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[14],'SITE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[15],'GR_CREATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_ASN_N_CPLT_RPT.columns[16],'GR_CREATE_TM')

# COMMAND ----------

# SQ_Shortcut_to_ASN_N_CPLT_RPT.show()

# COMMAND ----------

# Processing node EXP_REQ_FIELDS, type EXPRESSION 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column

EXP_REQ_FIELDS = SQ_Shortcut_to_ASN_N_CPLT_RPT.selectExpr(
    "sys_row_id as sys_row_id",
    "IF (STATUS_ID IS NULL, ' ', STATUS_ID) as MESSAGE_ID",
    "IF (MSG_NO IS NULL, ' ', MSG_NO) as MESSAGE_NO",
    "IF (TEXT IS NULL, ' ', TEXT) as MESSAGE_TEXT",
    "IF (TO_TIMESTAMP (ASN_CREATE_DT , 'M/d/yyyy') IS NOT NULL , TO_DATE ( ASN_CREATE_DT , 'M/d/yyyy' ), NULL) as ASN_CREATE_DATE",
    "IF (ASN_CREATE_TM IS NULL,ASN_CREATE_TM ,'1970-01-01 '||date_format( ASN_CREATE_TM , 'HH:mm:ss' )) as ASN_CREATE_TIME",
    "IF (DOC_NO IS NULL, NULL, DOC_NO) as IDOC_NUMBER",
    "IF (PURCH_DOC IS NULL, ' ', PURCH_DOC) as PO_NUMBER",
    "IF (VENDOR_ID IS NULL, ' ', VENDOR_ID) as VENDOR",
    "IF (NAME IS NULL, ' ', NAME) as VENDOR_NAME",
    "IF (UPC_ID IS NULL, ' ', UPC_ID) as GTIN_NR",
    "IF (DESCRIPTION IS NULL, ' ', DESCRIPTION) as MATERIAL_DESC",
    "( IF (ARTICLE IS NULL, ' ', ARTICLE) ) as MATERIAL",
    "IF (BILL_OF_LOAD IS NULL, ' ', BILL_OF_LOAD) as BOLNR",
    "IF (TO_TIMESTAMP (DELIVERY_DATE , 'M/d/yyyy') IS NOT NULL , TO_DATE ( DELIVERY_DATE , 'M/d/yyyy' ), NULL) as PO_DELIVERY_DATE",
    "IF (SITE_NBR IS NULL, ' ', SITE_NBR) as SITE",
    "IF (TO_TIMESTAMP (GR_CREATE_DT , 'M/d/yyyy') IS NOT NULL , TO_DATE ( GR_CREATE_DT , 'M/d/yyyy' ), NULL) as GR_CREATE_DATE",
     "IF (GR_CREATE_TM IS NULL,GR_CREATE_TM ,'1970-01-01 '||date_format( GR_CREATE_TM , 'HH:mm:ss' )) as GR_CREATE_TIME",
    "TO_TIMESTAMP(CURRENT_TIMESTAMP) as LOAD_TS"
    #"TO_TIMESTAMP('2023-11-12 22:00:33') as LOAD_TS"
)

# COMMAND ----------

# Processing node LKP_SKU_PROFILE, type LOOKUP_FROM_PRECACHED_DATASET 
# COLUMN COUNT: 3


LKP_SKU_PROFILE_lookup_result = EXP_REQ_FIELDS.join(LKP_SKU_PROFILE_SRC, (col('SKU_NBR') == col('MATERIAL')), 'left') \
.withColumn('row_num_PRODUCT_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("PRODUCT_ID")))

LKP_SKU_PROFILE = LKP_SKU_PROFILE_lookup_result.filter(col("row_num_PRODUCT_ID") == 1).select(
	LKP_SKU_PROFILE_lookup_result.sys_row_id,
	col('PRODUCT_ID')
)

# COMMAND ----------

# Processing node LKP_SITE_PROFILE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3


LKP_SITE_PROFILE_lookup_result = EXP_REQ_FIELDS.selectExpr(
	"SITE as SITE_NBR","sys_row_id as sys_row_id").join(LKP_SITE_PROFILE_SRC, (col('STORE_NBR') == col('SITE_NBR')), 'left') \
.withColumn('row_num_LOCATION_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("LOCATION_ID")))

LKP_SITE_PROFILE = LKP_SITE_PROFILE_lookup_result.filter(col("row_num_LOCATION_ID") == 1).select(
	LKP_SITE_PROFILE_lookup_result.sys_row_id,
	col('LOCATION_ID')
)

# COMMAND ----------

# Processing node EXP_ALL_FIELDS, type EXPRESSION 
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
LKP_SKU_PROFILE_temp = LKP_SKU_PROFILE.toDF(*["LKP_SKU_PROFILE___" + col for col in LKP_SKU_PROFILE.columns])
LKP_SITE_PROFILE_temp = LKP_SITE_PROFILE.toDF(*["LKP_SITE_PROFILE___" + col for col in LKP_SITE_PROFILE.columns])
EXP_REQ_FIELDS_temp = EXP_REQ_FIELDS.toDF(*["EXP_REQ_FIELDS___" + col for col in EXP_REQ_FIELDS.columns])

# Joining dataframes EXP_REQ_FIELDS, LKP_SKU_PROFILE, LKP_SITE_PROFILE to form EXP_ALL_FIELDS
EXP_ALL_FIELDS_joined = EXP_REQ_FIELDS_temp.join(LKP_SKU_PROFILE_temp, col('EXP_REQ_FIELDS___sys_row_id') == col('LKP_SKU_PROFILE___sys_row_id'), 'inner')\
 .join(LKP_SITE_PROFILE_temp, col('LKP_SKU_PROFILE___sys_row_id') == col('LKP_SITE_PROFILE___sys_row_id'), 'inner')

EXP_ALL_FIELDS = EXP_ALL_FIELDS_joined.selectExpr(
	# "EXP_ALL_FIELDS_joined___sys_row_id as sys_row_id",
	"EXP_REQ_FIELDS___MESSAGE_ID as MESSAGE_ID",
	"EXP_REQ_FIELDS___MESSAGE_NO as MESSAGE_NO",
	"EXP_REQ_FIELDS___MESSAGE_TEXT as MESSAGE_TEXT",
	"EXP_REQ_FIELDS___ASN_CREATE_DATE as ASN_CREATE_DATE",
	"EXP_REQ_FIELDS___ASN_CREATE_TIME as ASN_CREATE_TIME",
	"EXP_REQ_FIELDS___IDOC_NUMBER as IDOC_NUMBER",
	"EXP_REQ_FIELDS___PO_NUMBER as PO_NUMBER",
	"EXP_REQ_FIELDS___VENDOR as VENDOR",
	"EXP_REQ_FIELDS___VENDOR_NAME as VENDOR_NAME",
	"EXP_REQ_FIELDS___GTIN_NR as GTIN_NR",
	"EXP_REQ_FIELDS___MATERIAL_DESC as MATERIAL_DESC",
	"EXP_REQ_FIELDS___MATERIAL as MATERIAL",
	"EXP_REQ_FIELDS___BOLNR as BOLNR",
	"EXP_REQ_FIELDS___PO_DELIVERY_DATE as PO_DELIVERY_DATE",
	"EXP_REQ_FIELDS___SITE as SITE",
	"EXP_REQ_FIELDS___GR_CREATE_DATE as GR_CREATE_DATE",
	"EXP_REQ_FIELDS___GR_CREATE_TIME as GR_CREATE_TIME",
	"LKP_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID",
	"LKP_SITE_PROFILE___LOCATION_ID as LOCATION_ID",
	"EXP_REQ_FIELDS___LOAD_TS as LOAD_TS"
)

# COMMAND ----------

# EXP_ALL_FIELDS.show()

# COMMAND ----------

# Processing node Shortcut_to_ASN_REJECT_RSN, type TARGET 
# COLUMN COUNT: 20
# DB check  looks like all informatica CHAR were converted STRING
Shortcut_to_ASN_REJECT_RSN = EXP_ALL_FIELDS.selectExpr(
    "CAST(MESSAGE_ID AS STRING) as MESSAGE_ID",
    "lpad(CAST(MESSAGE_NO AS STRING), 3, '0') as MESSAGE_NO",
    "CAST(MESSAGE_TEXT AS STRING) as MESSAGE_TEXT",
    "CAST(ASN_CREATE_DATE AS DATE) as ASN_CREATE_DATE",
    "CAST(ASN_CREATE_TIME as timestamp)as ASN_CREATE_TIME",
    "CAST(IDOC_NUMBER AS bigint) as IDOC_NUMBER",
    "lpad(CAST(PO_NUMBER AS STRING), 10, '0') as PO_NBR",
    "CASE WHEN VENDOR==' ' THEN VENDOR ELSE lpad(CAST(VENDOR AS STRING), 10, '0') END as VENDOR_ID",
    "CAST(VENDOR_NAME AS STRING) as VENDOR_NAME",
    "CASE WHEN GTIN_NR==' ' THEN GTIN_NR ELSE lpad(CAST(GTIN_NR AS STRING), 12, '0') END as UPC_ID",
    "CAST(MATERIAL_DESC AS STRING) as MATERIAL_DESC",
    "CASE WHEN MATERIAL==' ' THEN '0' ELSE LTRIM( '0',MATERIAL) END as SKU_NBR",
    "CAST(BOLNR AS STRING) as BOLNR",
    "CAST(PO_DELIVERY_DATE AS DATE) as PO_DELIVERY_DATE",
    "CAST((CASE WHEN SITE==' ' THEN '0' ELSE LTRIM('0',SITE) END) AS INT) as STORE_NBR",
    "CAST(GR_CREATE_DATE AS DATE) as GR_CREATE_DATE",
    "CAST(GR_CREATE_TIME as timestamp) as GR_CREATE_TIME",
    "CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
    "CAST(LOCATION_ID AS INT) as LOCATION_ID",
    "CAST(LOAD_TS AS TIMESTAMP) as LOAD_TS"
).withColumn("MESSAGE_TEXT",regexp_replace(col('MESSAGE_TEXT'), r'[^\x00-\x7F]', ' '))






 
try:
    
    Shortcut_to_ASN_REJECT_RSN.write.saveAsTable(f'{legacy}.ASN_REJECT_RSN', mode = 'append')
    logPrevRunDt("ASN_REJECT_RSN", "ASN_REJECT_RSN", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
    raise e
