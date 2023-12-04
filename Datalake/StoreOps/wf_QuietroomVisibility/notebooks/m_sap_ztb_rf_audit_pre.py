# Databricks notebook source
#Code converted on 2023-10-30 11:25:33
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

# source_bucket=getParameterValue(raw,'wf_QuietroomVisibility','m_sap_ztb_rf_audit_pre','source_bucket')
# key=getParameterValue(raw,'wf_QuietroomVisibility','m_sap_ztb_rf_audit_pre','key')

# source_file=get_src_file(key,source_bucket)

_bucket = getParameterValue(
    raw,
    "BA_Store_Ops.prm",
    "BA_Store_Ops.WF:wf_QuietRoom_Visibility",
    "source_bucket",
)
source_bucket = _bucket + "ztb_rf_audit/"

source_file = get_src_file("ZTB_RF_AUDIT", source_bucket)


# COMMAND ----------

# SQ_Shortcut_to_ZTB_RF_AUDIT = spark.read.csv(source_file, sep='|', header=True, inferSchema=True).withColumn("sys_row_id", monotonically_increasing_id())
SQ_Shortcut_to_ZTB_RF_AUDIT = spark.read.csv(source_file, sep='|', header=True).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
# SQ_Shortcut_to_ZTB_RF_AUDIT = SQ_Shortcut_to_ZTB_RF_AUDIT \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[0],'CLIENT') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[1],'SITE') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[2],'ARTICLE') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[3],'CREATE_DATE') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[4],'CREATE_TIME') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[5],'ARTICLE_SLIP') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[6],'UPC_CODE') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[7],'DESCRIPTION') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[8],'ADJUST_QTY') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[9],'MOVE_TYPE') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[10],'STATUS') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[11],'RETAIL_PRICE') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[12],'UNIT_COST') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[13],'POST_DATE') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[14],'CHANGE_IND') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[15],'ERROR_MSG') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[16],'DOC_NUMBER') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[17],'VENDOR') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[18],'RTV_IND') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[19],'POST_NAME') \
# 	.withColumnRenamed(SQ_Shortcut_to_ZTB_RF_AUDIT.columns[20],'CHANGE_TIME')
# SQ_Shortcut_to_ZTB_RF_AUDIT.show()

# COMMAND ----------

# SQ_Shortcut_to_ZTB_RF_AUDIT.show()

# COMMAND ----------

# Processing node Shortcut_to_SAP_ZTB_RF_AUDIT_PRE, type TARGET 
# COLUMN COUNT: 23


Shortcut_to_SAP_ZTB_RF_AUDIT_PRE = SQ_Shortcut_to_ZTB_RF_AUDIT.selectExpr(
	"CAST(CLIENT AS STRING) as CLIENT",
	"CAST(SITE AS STRING) as SITE",
	"CAST(ARTICLE AS STRING) as ARTICLE",
	"to_timestamp(CREATE_DATE , 'M/d/yyyy HH:mm:ss') as CREATE_DATE",
	"to_timestamp(CREATE_TIME , 'M/d/yyyy HH:mm:ss') as CREATE_TIME",
	"CAST(ARTICLE_SLIP AS STRING) as ARTICLE_SLIP",
	"CAST(UPC_CODE AS STRING) as UPC_CODE",
	"CAST(DESCRIPTION AS STRING) as DESCRIPTION",
	"CAST(ADJUST_QTY AS INT) as ADJUST_QTY",
	"CAST(REASON_CODE AS SMALLINT) as REASON_CODE",
	"CAST(MOVE_TYPE AS STRING) as MOVE_TYPE",
	"CAST(STATUS AS CHAR(1)) as STATUS",
	"CAST(RETAIL_PRICE AS DECIMAL(11,2)) as RETAIL_PRICE",
	"CAST(UNIT_COST AS DECIMAL(11,2)) as UNIT_COST",
	"to_timestamp(POST_DATE ,'M/d/yyyy HH:mm:ss') as POST_DATE",
	"CAST(USER_NAME AS STRING) as USER_NAME",
	"CAST(CHANGE_IND AS CHAR(1)) as CHANGE_IND",
	"CAST(ERROR_MSG AS STRING) as ERROR_MSG",
	"CAST(DOC_NUMBER AS STRING) as DOC_NUMBER",
	"CAST(VENDOR AS STRING) as VENDOR",
	"CAST(RTV_IND AS CHAR(1)) as RTV_IND",
	"CAST(POST_NAME AS STRING) as POST_NAME",
	"to_timestamp(CHANGE_TIME , 'M/d/yyyy HH:mm:ss') as CHANGE_TIME"
)
try:
    # chk=DuplicateChecker()
    # chk.check_for_duplicate_primary_keys(Shortcut_to_SAP_ZTB_RF_AUDIT_PRE,[key])
    Shortcut_to_SAP_ZTB_RF_AUDIT_PRE.write.saveAsTable(f'{raw}.SAP_ZTB_RF_AUDIT_PRE', mode = 'overwrite')
except Exception as e:
    raise e

# COMMAND ----------


