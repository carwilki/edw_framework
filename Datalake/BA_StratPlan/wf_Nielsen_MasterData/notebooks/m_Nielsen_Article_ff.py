# Databricks notebook source
# Code converted on 2023-12-07 08:16:57
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
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'


# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 15

SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f"""SELECT
SKU_PROFILE_RPT.PRODUCT_ID,
SKU_PROFILE_RPT.SKU_NBR,
SKU_PROFILE_RPT.PRIMARY_UPC_ID,
SKU_PROFILE_RPT.SKU_DESC,
SKU_PROFILE_RPT.SAP_CATEGORY_DESC,
SKU_PROFILE_RPT.VP_DESC,
SKU_PROFILE_RPT.SAP_CLASS_DESC,
SKU_PROFILE_RPT.SAP_DEPT_DESC,
SKU_PROFILE_RPT.SAP_DIVISION_DESC,
SKU_PROFILE_RPT.PRIMARY_VENDOR_NAME,
SKU_PROFILE_RPT.SIZE_DESC,
SKU_PROFILE_RPT.FLAVOR_DESC,
SKU_PROFILE_RPT.BRAND_NAME,
SKU_PROFILE_RPT.OWNBRAND_FLAG,
SKU_PROFILE_RPT.STATUS_NAME
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_UPC, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_UPC = spark.sql(f"""SELECT
UPC.UPC_ID,
UPC.PRODUCT_ID,
UPC.SKU_NBR
FROM {legacy}.UPC""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_SKU_UPC, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_UPC_temp = SQ_Shortcut_to_UPC.toDF(*["SQ_Shortcut_to_UPC___" + col for col in SQ_Shortcut_to_UPC.columns])
SQ_Shortcut_to_SKU_PROFILE_RPT_temp = SQ_Shortcut_to_SKU_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT.columns])

JNR_SKU_UPC = SQ_Shortcut_to_UPC_temp.join(SQ_Shortcut_to_SKU_PROFILE_RPT_temp,[SQ_Shortcut_to_UPC_temp.SQ_Shortcut_to_UPC___PRODUCT_ID == SQ_Shortcut_to_SKU_PROFILE_RPT_temp.SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_SKU_PROFILE_RPT___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_NBR as SKU_NBR",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___PRIMARY_UPC_ID as PRIMARY_UPC_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_DESC as SKU_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_CATEGORY_DESC as SAP_CATEGORY_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___VP_DESC as VP_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_DEPT_DESC as SAP_DEPT_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_DIVISION_DESC as SAP_DIVISION_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___PRIMARY_VENDOR_NAME as PRIMARY_VENDOR_NAME",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SIZE_DESC as SIZE_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___FLAVOR_DESC as FLAVOR_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___BRAND_NAME as BRAND_NAME",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___OWNBRAND_FLAG as OWNBRAND_FLAG",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___STATUS_NAME as STATUS_NAME",
	"SQ_Shortcut_to_UPC___UPC_ID as UPC_ID",
	"SQ_Shortcut_to_UPC___PRODUCT_ID as PRODUCT_ID1",
	"SQ_Shortcut_to_UPC___SKU_NBR as SKU_NBR1")

# COMMAND ----------

JNR_SKU_UPC_temp=JNR_SKU_UPC.toDF(*["JNR_SKU_UPC___" + col for col in JNR_SKU_UPC.columns])

EXP_CALCPRIMARY=JNR_SKU_UPC_temp.selectExpr(
    "JNR_SKU_UPC___sys_row_id as sys_row_id",
    "JNR_SKU_UPC___PRODUCT_ID as PRODUCT_ID",
    "JNR_SKU_UPC___SKU_NBR as SKU_NBR",
    "JNR_SKU_UPC___PRIMARY_UPC_ID as PRIMARY_UPC_ID",
    "JNR_SKU_UPC___SKU_DESC as SKU_DESC",
    "JNR_SKU_UPC___SAP_CATEGORY_DESC as SAP_CATEGORY_DESC",
    "JNR_SKU_UPC___VP_DESC as VP_DESC",
    "JNR_SKU_UPC___SAP_CLASS_DESC as SAP_CLASS_DESC",
    "JNR_SKU_UPC___SAP_DEPT_DESC as SAP_DEPT_DESC",
    "JNR_SKU_UPC___SAP_DIVISION_DESC as SAP_DIVISION_DESC",
    "JNR_SKU_UPC___PRIMARY_VENDOR_NAME as PRIMARY_VENDOR_NAME",
    "JNR_SKU_UPC___SIZE_DESC as SIZE_DESC",
    "JNR_SKU_UPC___FLAVOR_DESC as FLAVOR_DESC",
    "JNR_SKU_UPC___BRAND_NAME as BRAND_NAME",
    "JNR_SKU_UPC___OWNBRAND_FLAG as OWNBRAND_FLAG",
    "JNR_SKU_UPC___STATUS_NAME as STATUS_NAME",
    "JNR_SKU_UPC___UPC_ID as UPC_ID",
    "JNR_SKU_UPC___PRODUCT_ID as PRODUCT_ID1",
    "JNR_SKU_UPC___SKU_NBR as SKU_NBR1",
    "IF (JNR_SKU_UPC___PRIMARY_UPC_ID = JNR_SKU_UPC___UPC_ID, 'Y', 'N') as IS_PRIMARY"
)

# COMMAND ----------

# Processing node SRT_ARTICLES, type SORTER 
# COLUMN COUNT: 15

# SRT_ARTICLES = JNR_SKU_UPC.sort(col('SKU_NBR').asc(), col('UPC_ID').asc()) #Changed
SRT_ARTICLES = EXP_CALCPRIMARY.sort(col('SKU_NBR').asc(), col('UPC_ID').asc())

# COMMAND ----------

# # Processing node EXP_CALCPRIMARY, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# # COLUMN COUNT: 1

# # for each involved DataFrame, append the dataframe name to each column
# SRT_ARTICLES_temp = SRT_ARTICLES.toDF(*["SRT_ARTICLES___" + col for col in SRT_ARTICLES.columns])

# EXP_CALCPRIMARY = SRT_ARTICLES_temp.selectExpr(
# 	"SRT_ARTICLES___sys_row_id as sys_row_id",
# 	"IF (SRT_ARTICLES___PRIMARY_UPC_ID = SRT_ARTICLES___UPC_ID, 'Y', 'N') as IS_PRIMARY"
# )

# COMMAND ----------

target_bucket=getParameterValue(raw,'wf_Nielsen_MasterData','m_Nielsen_Article_ff','target_bucket')
target_file=getParameterValue(raw,'wf_Nielsen_MasterData','m_Nielsen_Article_ff','key')

# COMMAND ----------

# Processing node Shortcut_to_Nielsen_Article_Flat, type TARGET 
# COLUMN COUNT: 15

# Joining dataframes SRT_ARTICLES, EXP_CALCPRIMARY to form Shortcut_to_Nielsen_Article_Flat
# Shortcut_to_Nielsen_Article_Flat_joined = SRT_ARTICLES.join(EXP_CALCPRIMARY, SRT_ARTICLES.sys_row_id == EXP_CALCPRIMARY.sys_row_id, 'inner')

Shortcut_to_Nielsen_Article_Flat = SRT_ARTICLES.selectExpr(
	"UPC_ID as UPC_ID",
	"SKU_NBR as SKU_NBR",
	"CAST(SKU_DESC AS STRING) as SKU_DESC",
	"CAST(SAP_CATEGORY_DESC AS STRING) as CATEGORY",
	"CAST(SAP_CLASS_DESC AS STRING) as CLASS",
	"CAST(SAP_DEPT_DESC AS STRING) as DEPARTMENT",
	"CAST(SAP_DIVISION_DESC AS STRING) as DIVISION",
	"CAST(OWNBRAND_FLAG AS STRING) as PRIVATE_LABEL",
	"CAST(PRIMARY_VENDOR_NAME AS STRING) as SUPPLIER",
	"CAST(SIZE_DESC AS STRING) as SIZE",
	"CAST(FLAVOR_DESC AS STRING) as FLAVOR",
	"CAST(BRAND_NAME AS STRING) as BRAND",
	"CAST(VP_DESC AS STRING) as VP",
	"CAST(STATUS_NAME AS STRING) as STATUS",
	"CAST(IS_PRIMARY AS STRING) as PRIMARY_UPC"
)


target_bucket=target_bucket+datetime.now().strftime("%Y%m%d")+'/'
target_file=target_file.replace('.txt','')+'_'+datetime.now().strftime("%Y%m%d")+'.txt'

# writeToFlatFile(Shortcut_to_Nielsen_Article_Flat, target_bucket, target_file, 'overwrite' )
# writeToFlatFile_withoutquotes(Shortcut_to_Nielsen_Article_Flat, target_bucket, target_file, 'overwrite' )

# COMMAND ----------

print(target_bucket+target_file)
# print(target_file)

# COMMAND ----------

if env == "prod":
    gs_source_path = target_bucket + target_file
    today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/Nielsen/petm_article/" + today + '/'
else:
    gs_source_path = target_bucket + target_file
    #today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/Nielsen/petm_article/"

try:
    writeToFlatFile(Shortcut_to_Nielsen_Article_Flat, target_bucket, target_file, 'overwrite' )
    copy_file_to_nas(gs_source_path, nas_target_path)
 
except Exception as e:
    raise e

# COMMAND ----------


