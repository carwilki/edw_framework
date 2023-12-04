# Databricks notebook source
#Code converted on 2023-10-30 11:25:32
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

# Processing node LKPTRANS_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKPTRANS_SRC = spark.sql(f"""SELECT
MOVE_REASON_ID,
MOVE_REASON_DESC
FROM {legacy}.MOVE_REASON""")

# COMMAND ----------

# LKPTRANS_SRC.show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_VENDOR_PROFILE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_VENDOR_PROFILE = spark.sql(f"""SELECT
VENDOR_NBR,
VENDOR_ID,
VENDOR_NAME
FROM {legacy}.VENDOR_PROFILE
ORDER BY 1""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_VENDOR_PROFILE = SQ_Shortcut_to_VENDOR_PROFILE \
	.withColumnRenamed(SQ_Shortcut_to_VENDOR_PROFILE.columns[0],'VENDOR_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_VENDOR_PROFILE.columns[1],'VENDOR_ID') \
	.withColumnRenamed(SQ_Shortcut_to_VENDOR_PROFILE.columns[2],'VENDOR_NAME')

# COMMAND ----------

# SQ_Shortcut_to_VENDOR_PROFILE.show()

# COMMAND ----------

# Processing node EXP_vendor_profile, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_VENDOR_PROFILE_temp = SQ_Shortcut_to_VENDOR_PROFILE.toDF(*["SQ_Shortcut_to_VENDOR_PROFILE___" + col for col in SQ_Shortcut_to_VENDOR_PROFILE.columns])

EXP_vendor_profile = SQ_Shortcut_to_VENDOR_PROFILE_temp.selectExpr(
	"SQ_Shortcut_to_VENDOR_PROFILE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_VENDOR_PROFILE___VENDOR_ID as VENDOR_ID",
	"SQ_Shortcut_to_VENDOR_PROFILE___VENDOR_NAME as VENDOR_NAME",
	"SQ_Shortcut_to_VENDOR_PROFILE___VENDOR_NBR as VENDOR_NBR"
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE, type SOURCE 
# COLUMN COUNT: 27
#  note   AND CREATE_DATE = CURRENT_DATE
SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE = spark.sql(f"""SELECT
VENDOR,
CLIENT,
SITE,
ARTICLE,
CREATE_DATE,
CREATE_TIME,
ARTICLE_SLIP,
UPC_CODE,
DESCRIPTION,
ADJUST_QTY,
REASON_CODE,
MOVE_TYPE,
STATUS,
RETAIL_PRICE,
UNIT_COST,
POST_DATE,
USER_NAME,
CHANGE_IND,
ERROR_MSG,
DOC_NUMBER,
RTV_IND,
POST_NAME,
CHANGE_TIME,
LOCATION_ID,
STORE_NBR,
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE, {legacy}.SITE_PROFILE, {raw}.SAP_ZTB_RF_AUDIT_PRE
WHERE CAST(SAP_ZTB_RF_AUDIT_PRE.SITE AS INTEGER)=SITE_PROFILE.STORE_NBR
AND CAST(SAP_ZTB_RF_AUDIT_PRE.ARTICLE AS INTEGER)=SKU_PROFILE.SKU_NBR
AND REASON_CODE in (0501,0502,0511,0512,0521,0522) and STATUS in('A','S') AND CREATE_DATE = CURRENT_DATE
ORDER BY 1""").withColumn("sys_row_id", monotonically_increasing_id())

# CAST(SAP_ZTB_RF_AUDIT_PRE.SITE AS INTEGER)=SITE_PROFILE.STORE_NBR
# AND CAST(SAP_ZTB_RF_AUDIT_PRE.ARTICLE AS INTEGER)=SKU_PROFILE.SKU_NBR
# Conforming fields names to the component layout
SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE = SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[0],'VENDOR') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[1],'CLIENT') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[2],'SITE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[3],'ARTICLE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[4],'CREATE_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[5],'CREATE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[6],'ARTICLE_SLIP') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[7],'UPC_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[8],'DESCRIPTION') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[9],'ADJUST_QTY') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[10],'REASON_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[11],'MOVE_TYPE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[12],'STATUS') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[13],'RETAIL_PRICE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[14],'UNIT_COST') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[15],'POST_DATE') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[16],'USER_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[17],'CHANGE_IND') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[18],'ERROR_MSG') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[19],'DOC_NUMBER') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[20],'RTV_IND') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[21],'POST_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[22],'CHANGE_TIME') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[23],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[24],'STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[25],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns[26],'SKU_NBR')

# COMMAND ----------

# SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.show()

# COMMAND ----------

# Processing node SRT_VENDOR_PROFILE, type SORTER 
# COLUMN COUNT: 3

SRT_VENDOR_PROFILE = EXP_vendor_profile.select('*'
).sort(col('VENDOR_NBR').asc())

# COMMAND ----------

# Processing node EXP_ztb_rf_audit, type EXPRESSION 
# COLUMN COUNT: 31

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE_temp = SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.toDF(*["SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___" + col for col in SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.columns])

EXP_ztb_rf_audit = SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE_temp.selectExpr(
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___CLIENT as CLIENT",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___SITE as SITE",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___ARTICLE as ARTICLE",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___CREATE_DATE as CREATE_DATE",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___CREATE_TIME as CREATE_TIME",
	"TO_DATE (concat( date_format(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___CREATE_DATE, 'yyyy/M/d') , ' ' , date_format(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___CREATE_TIME, 'HH:mm:ss') ), 'yyyy/M/d HH:mm:ss' ) as O_CREATE_DATE_TIME",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___ARTICLE_SLIP as ARTICLE_SLIP",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___UPC_CODE as UPC_CODE",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___DESCRIPTION as DESCRIPTION",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___ADJUST_QTY as ADJUST_QTY",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___REASON_CODE as REASON_CODE",
	"cast(SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___REASON_CODE as int) as O_REASON_CODE",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___MOVE_TYPE as MOVE_TYPE",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___STATUS as STATUS",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___RETAIL_PRICE as RETAIL_PRICE",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___UNIT_COST as UNIT_COST",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___POST_DATE as POST_DATE",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___USER_NAME as USER_NAME",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___CHANGE_IND as CHANGE_IND",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___ERROR_MSG as ERROR_MSG",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___DOC_NUMBER as DOC_NUMBER",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___VENDOR as VENDOR",
	"LTRIM('0', SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___VENDOR) as O_VENDOR",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___RTV_IND as RTV_IND",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___POST_NAME as POST_NAME",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___CHANGE_TIME as CHANGE_TIME",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_SAP_ZTB_RF_AUDIT_PRE___SKU_NBR as SKU_NBR",
	"CURRENT_TIMESTAMP as O_LOADTSTMP"
)

# COMMAND ----------

# EXP_ztb_rf_audit.show()

# COMMAND ----------

# Processing node SRT_ZTB_RF_AUDIT, type SORTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 29

SRT_ZTB_RF_AUDIT = EXP_ztb_rf_audit.selectExpr(
	"O_VENDOR as VENDOR",
	"CLIENT as CLIENT",
	"SITE as SITE",
	"ARTICLE as ARTICLE",
	"CREATE_DATE as CREATE_DATE",
	"O_CREATE_DATE_TIME as CREATE_DATE_TIME",
	"ARTICLE_SLIP as ARTICLE_SLIP",
	"UPC_CODE as UPC_CODE",
	"DESCRIPTION as DESCRIPTION",
	"ADJUST_QTY as ADJUST_QTY",
	"REASON_CODE as REASON_CODE",
	"O_REASON_CODE as O_REASON_CODE",
	"MOVE_TYPE as MOVE_TYPE",
	"STATUS as STATUS",
	"RETAIL_PRICE as RETAIL_PRICE",
	"UNIT_COST as UNIT_COST",
	"POST_DATE as POST_DATE",
	"USER_NAME as USER_NAME",
	"CHANGE_IND as CHANGE_IND",
	"ERROR_MSG as ERROR_MSG",
	"DOC_NUMBER as DOC_NUMBER",
	"RTV_IND as RTV_IND",
	"POST_NAME as POST_NAME",
	"CHANGE_TIME as CHANGE_TIME",
	"LOCATION_ID as LOCATION_ID",
	"STORE_NBR as STORE_NBR",
	"PRODUCT_ID as PRODUCT_ID",
	"SKU_NBR as SKU_NBR",
	"O_LOADTSTMP as LOADTSTMP",
 	"sys_row_id as sys_row_id").sort(col('VENDOR').asc())

# COMMAND ----------

# SRT_VENDOR_PROFILE.show()
# SRT_ZTB_RF_AUDIT.show()

# COMMAND ----------

# Processing node JNRTRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
SRT_VENDOR_PROFILE_temp = SRT_VENDOR_PROFILE.toDF(*["SRT_VENDOR_PROFILE___" + col for col in SRT_VENDOR_PROFILE.columns])
SRT_ZTB_RF_AUDIT_temp = SRT_ZTB_RF_AUDIT.toDF(*["SRT_ZTB_RF_AUDIT___" + col for col in SRT_ZTB_RF_AUDIT.columns])

JNRTRANS = SRT_VENDOR_PROFILE_temp.join(SRT_ZTB_RF_AUDIT_temp,[SRT_VENDOR_PROFILE_temp.SRT_VENDOR_PROFILE___VENDOR_NBR == SRT_ZTB_RF_AUDIT_temp.SRT_ZTB_RF_AUDIT___VENDOR],'inner').selectExpr(
	"SRT_VENDOR_PROFILE___VENDOR_ID as VENDOR_ID",
	"SRT_VENDOR_PROFILE___VENDOR_NAME as VENDOR_NAME",
	"SRT_VENDOR_PROFILE___VENDOR_NBR as VENDOR_NBR",
	"SRT_ZTB_RF_AUDIT___CLIENT as CLIENT",
	"SRT_ZTB_RF_AUDIT___SITE as SITE",
	"SRT_ZTB_RF_AUDIT___ARTICLE as ARTICLE",
	"SRT_ZTB_RF_AUDIT___CREATE_DATE as CREATE_DATE",
	"SRT_ZTB_RF_AUDIT___CREATE_DATE_TIME as CREATE_TIME",
	"SRT_ZTB_RF_AUDIT___ARTICLE_SLIP as ARTICLE_SLIP",
	"SRT_ZTB_RF_AUDIT___UPC_CODE as UPC_CODE",
	"SRT_ZTB_RF_AUDIT___DESCRIPTION as DESCRIPTION",
	"SRT_ZTB_RF_AUDIT___ADJUST_QTY as ADJUST_QTY",
	"SRT_ZTB_RF_AUDIT___REASON_CODE as REASON_CODE",
	"SRT_ZTB_RF_AUDIT___O_REASON_CODE as O_REASON_CODE",
	"SRT_ZTB_RF_AUDIT___MOVE_TYPE as MOVE_TYPE",
	"SRT_ZTB_RF_AUDIT___STATUS as STATUS",
	"SRT_ZTB_RF_AUDIT___RETAIL_PRICE as RETAIL_PRICE",
	"SRT_ZTB_RF_AUDIT___UNIT_COST as UNIT_COST",
	"SRT_ZTB_RF_AUDIT___POST_DATE as POST_DATE",
	"SRT_ZTB_RF_AUDIT___USER_NAME as USER_NAME",
	"SRT_ZTB_RF_AUDIT___CHANGE_IND as CHANGE_IND",
	"SRT_ZTB_RF_AUDIT___ERROR_MSG as ERROR_MSG",
	"SRT_ZTB_RF_AUDIT___DOC_NUMBER as DOC_NUMBER",
	"SRT_ZTB_RF_AUDIT___VENDOR as VENDOR",
	"SRT_ZTB_RF_AUDIT___RTV_IND as RTV_IND",
	"SRT_ZTB_RF_AUDIT___POST_NAME as POST_NAME",
	"SRT_ZTB_RF_AUDIT___CHANGE_TIME as CHANGE_TIME",
	"SRT_ZTB_RF_AUDIT___LOCATION_ID as LOCATION_ID",
	"SRT_ZTB_RF_AUDIT___STORE_NBR as STORE_NBR",
	"SRT_ZTB_RF_AUDIT___PRODUCT_ID as PRODUCT_ID",
	"SRT_ZTB_RF_AUDIT___SKU_NBR as SKU_NBR",
	"SRT_ZTB_RF_AUDIT___LOADTSTMP as O_LOADTSTMP",
 	"SRT_ZTB_RF_AUDIT___sys_row_id as sys_row_id")

# COMMAND ----------

# Processing node LKPTRANS, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3


LKPTRANS_lookup_result = JNRTRANS.selectExpr(
	"O_REASON_CODE as IN_REASON_CODE" , "sys_row_id as sys_row_id_ts").join(LKPTRANS_SRC, (col('MOVE_REASON_ID') == col('IN_REASON_CODE')), 'left') \
.withColumn('row_num_MOVE_REASON_ID', row_number().over(Window.partitionBy("sys_row_id_ts").orderBy("MOVE_REASON_ID")))
LKPTRANS = LKPTRANS_lookup_result.filter(col("row_num_MOVE_REASON_ID") == 1).select(
	LKPTRANS_lookup_result.sys_row_id_ts,
	col('MOVE_REASON_DESC')
)

# COMMAND ----------

# JNRTRANS.show()

# COMMAND ----------

# Processing node MOVEMENT_LIVE_PET, type TARGET 
# COLUMN COUNT: 17

# Joining dataframes JNRTRANS, LKPTRANS to form MOVEMENT_LIVE_PET
MOVEMENT_LIVE_PET_joined = JNRTRANS.join(LKPTRANS, JNRTRANS.sys_row_id == LKPTRANS.sys_row_id_ts, 'inner')
# MOVEMENT_LIVE_PET_joined.show()
MOVEMENT_LIVE_PET = MOVEMENT_LIVE_PET_joined.selectExpr(
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(CREATE_TIME AS TIMESTAMP) as CREATE_TSTMP",
	"CAST(STORE_NBR AS INT) as STORE_NBR",
	"CAST(ARTICLE AS INT) as SKU_NBR",
	"CAST(DESCRIPTION AS STRING) as SKU_DESC",
	"CAST(ADJUST_QTY AS INT) as ADJUST_QTY",
	"CAST(VENDOR_ID as bigint) as VENDOR_ID",
	"CAST(VENDOR_NBR AS STRING) as VENDOR_NBR",
	"CAST(REASON_CODE as smallint) as MOVE_REASON_ID",
	"CAST(MOVE_REASON_DESC AS STRING) as MOVE_REASON_DESC",
	"CAST(MOVE_TYPE AS STRING) as PET_MOVE_TYPE_CD",
	"CAST(STATUS AS CHAR(1)) as PET_MOVE_STATUS_CD",
	"CAST(RETAIL_PRICE AS DECIMAL(11,2)) as RETAIL_PRICE_AMT",
	"CAST(UNIT_COST AS DECIMAL(11,2)) as UNIT_COST",
	"CAST(USER_NAME AS STRING) as USER_NAME",
	"CAST(O_LOADTSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
try:
    chk=DuplicateChecker()
    chk.check_for_duplicate_primary_keys(MOVEMENT_LIVE_PET,["LOCATION_ID","PRODUCT_ID","CREATE_TSTMP"])
    MOVEMENT_LIVE_PET.write.saveAsTable(f'{legacy}.MOVEMENT_LIVE_PET', mode = 'append')
except Exception as e:
    raise e

# COMMAND ----------


