# Databricks notebook source
# Code converted on 2023-12-07 08:23:24
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
#test
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

# Variable_declaration_comment

target_bucket=getParameterValue(raw,'wf_SMG_Extract','m_smg_site_extract','target_bucket')
target_file=getParameterValue(raw,'wf_SMG_Extract','m_smg_site_extract','key')

p_sku_dept=getParameterValue(raw,'wf_SMG_Extract','m_smg_site_extract','p_sku_dept')

nas_target_base_path = getParameterValue(raw,'wf_SMG_Extract','m_smg_site_extract','nas_target')
today = str(date.today())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT = spark.sql(f"""SELECT DISTINCT
SALES_DAY_SKU_STORE_RPT.PRODUCT_ID,
SALES_DAY_SKU_STORE_RPT.LOCATION_ID
FROM {legacy}.SALES_DAY_SKU_STORE_RPT@v15 as SALES_DAY_SKU_STORE_RPT
WHERE SALES_DAY_SKU_STORE_RPT.WEEK_DT >= to_date('2023-12-24','yyyy-MM-dd') - 7""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE = spark.sql(f"""SELECT
SKU_PROFILE.PRODUCT_ID
FROM {legacy}.SKU_PROFILE
WHERE {p_sku_dept}""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_Normal_Join, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp = SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.toDF(*["SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___" + col for col in SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT.columns])
SQ_Shortcut_to_SKU_PROFILE_temp = SQ_Shortcut_to_SKU_PROFILE.toDF(*["SQ_Shortcut_to_SKU_PROFILE___" + col for col in SQ_Shortcut_to_SKU_PROFILE.columns])

JNR_Normal_Join = SQ_Shortcut_to_SKU_PROFILE_temp.join(SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp,[SQ_Shortcut_to_SKU_PROFILE_temp.SQ_Shortcut_to_SKU_PROFILE___PRODUCT_ID == SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT_temp.SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___PRODUCT_ID],'inner').selectExpr(
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_SALES_DAY_SKU_STORE_RPT___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID1")


# COMMAND ----------
JNR_Normal_Join_dedupe = JNR_Normal_Join.dropDuplicates(["LOCATION_ID"])

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 22

SQ_Shortcut_to_SITE_PROFILE_RPT = spark.sql(f"""SELECT
SITE_PROFILE_RPT.LOCATION_ID,
SITE_PROFILE_RPT.STORE_NBR,
SITE_PROFILE_RPT.STORE_NAME,
SITE_PROFILE_RPT.SITE_EMAIL_ADDRESS,
SITE_PROFILE_RPT.REGION_ID,
SITE_PROFILE_RPT.REGION_DESC,
SITE_PROFILE_RPT.REGION_VP_NAME,
SITE_PROFILE_RPT.RVP_EMAIL_ADDRESS,
SITE_PROFILE_RPT.DISTRICT_ID,
SITE_PROFILE_RPT.DISTRICT_DESC,
SITE_PROFILE_RPT.DIST_MGR_NAME,
SITE_PROFILE_RPT.DM_EMAIL_ADDRESS,
SITE_PROFILE_RPT.SITE_ADDRESS,
SITE_PROFILE_RPT.SITE_CITY,
SITE_PROFILE_RPT.STATE_CD,
SITE_PROFILE_RPT.POSTAL_CD,
SITE_PROFILE_RPT.COUNTRY_CD,
SITE_PROFILE_RPT.SITE_MAIN_TELE_NO,
SITE_PROFILE_RPT.HOTEL_FLAG,
SITE_PROFILE_RPT.DAYCAMP_FLAG,
SITE_PROFILE_RPT.VET_FLAG,
SITE_PROFILE_RPT.OPEN_DT
FROM {legacy}.SITE_PROFILE_RPT
WHERE REGION_ID NOT IN (0,100,3000,3400,4000,5000,8000,8100)

AND SITE_SALES_FLAG = 1

AND STORE_OPEN_CLOSE_FLAG = 'O'

AND OPEN_DT < to_date('2023-12-24','yyyy-MM-dd') - 30""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SRTTRANS, type SORTER 
# COLUMN COUNT: 1

SRTTRANS = JNR_Normal_Join_dedupe.sort(col('LOCATION_ID').asc())

# COMMAND ----------

# Processing node JNR_Master_Outer_Join, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_RPT_temp = SQ_Shortcut_to_SITE_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SITE_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SITE_PROFILE_RPT.columns])
SRTTRANS_temp = SRTTRANS.toDF(*["SRTTRANS___" + col for col in SRTTRANS.columns])

JNR_Master_Outer_Join = SRTTRANS_temp.join(SQ_Shortcut_to_SITE_PROFILE_RPT_temp,[SRTTRANS_temp.SRTTRANS___LOCATION_ID == SQ_Shortcut_to_SITE_PROFILE_RPT_temp.SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___STORE_NBR as STORE_NBR",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___STORE_NAME as STORE_NAME",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___SITE_EMAIL_ADDRESS as SITE_EMAIL_ADDRESS",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___REGION_ID as REGION_ID",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___REGION_DESC as REGION_DESC",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___REGION_VP_NAME as REGION_VP_NAME",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___RVP_EMAIL_ADDRESS as RVP_EMAIL_ADDRESS",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___DISTRICT_ID as DISTRICT_ID",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___DISTRICT_DESC as DISTRICT_DESC",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___DIST_MGR_NAME as DIST_MGR_NAME",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___DM_EMAIL_ADDRESS as DM_EMAIL_ADDRESS",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___SITE_ADDRESS as SITE_ADDRESS",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___SITE_CITY as SITE_CITY",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___STATE_CD as STATE_CD",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___POSTAL_CD as POSTAL_CD",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___COUNTRY_CD as COUNTRY_CD",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___SITE_MAIN_TELE_NO as SITE_MAIN_TELE_NO",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___HOTEL_FLAG as HOTEL_FLAG",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___DAYCAMP_FLAG as DAYCAMP_FLAG",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___VET_FLAG as VET_FLAG",
	"SQ_Shortcut_to_SITE_PROFILE_RPT___OPEN_DT as OPEN_DT",
	"SRTTRANS___LOCATION_ID as SALON_LOCATION_ID")

# COMMAND ----------

# Processing node EXP_Format, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
JNR_Master_Outer_Join_temp = JNR_Master_Outer_Join.toDF(*["JNR_Master_Outer_Join___" + col for col in JNR_Master_Outer_Join.columns])

EXP_Format = JNR_Master_Outer_Join_temp.selectExpr(
	#"IF (LENGTH(JNR_Master_Outer_Join___STORE_NBR) < 4, LPAD ( JNR_Master_Outer_Join___STORE_NBR , 4 , '0' ), String(JNR_Master_Outer_Join___STORE_NBR)) as o_STORE_NBR",
	"String(JNR_Master_Outer_Join___STORE_NBR) as o_STORE_NBR",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___STORE_NAME ) ) as o_STORE_NAME",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___SITE_EMAIL_ADDRESS ) ) as o_SITE_EMAIL_ADDRESS",
	"JNR_Master_Outer_Join___REGION_ID as REGION_ID",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___REGION_DESC ) ) as o_REGION_DESC",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___REGION_VP_NAME ) ) as o_REGION_VP_NAME",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___RVP_EMAIL_ADDRESS ) ) as o_RVP_EMAIL_ADDRESS",
	"JNR_Master_Outer_Join___DISTRICT_ID as DISTRICT_ID",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___DISTRICT_DESC ) ) as o_DISTRICT_DESC",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___DIST_MGR_NAME ) ) as o_DIST_MGR_NAME",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___DM_EMAIL_ADDRESS ) ) as o_DM_EMAIL_ADDRESS",
	"REGEXP_REPLACE(LTRIM ( RTRIM ( JNR_Master_Outer_Join___SITE_ADDRESS ) ), '(' || ',' || ')', ' ' ) as o_SITE_ADDRESS",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___SITE_CITY ) ) as o_SITE_CITY",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___STATE_CD ) ) as o_STATE_CD",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___POSTAL_CD ) ) as o_POSTAL_CD",
	"LTRIM ( RTRIM ( JNR_Master_Outer_Join___COUNTRY_CD ) ) as o_COUNTRY_CD",
	"REGEXP_REPLACE(LTRIM ( RTRIM ( JNR_Master_Outer_Join___SITE_MAIN_TELE_NO ) ), '(' || '-' || '|' ||'/' || ')', '' ) as o_SITE_MAIN_TELE_NO",
	"JNR_Master_Outer_Join___HOTEL_FLAG as HOTEL_FLAG",
	"JNR_Master_Outer_Join___DAYCAMP_FLAG as DAYCAMP_FLAG",
	"JNR_Master_Outer_Join___VET_FLAG as VET_FLAG",
	"IF (JNR_Master_Outer_Join___SALON_LOCATION_ID IS NULL, 0, 1) as SALON_FLAG",
	"JNR_Master_Outer_Join___OPEN_DT as OPEN_DT",
	"DATE_ADD(JNR_Master_Outer_Join___OPEN_DT, 30) as SURVEY_START_DT"
)

# COMMAND ----------

# Processing node Shortcut_to_SMG_Site_Extract_FlatFile, type TARGET 
# COLUMN COUNT: 23


Shortcut_to_SMG_Site_Extract_FlatFile = EXP_Format.selectExpr(
	"o_STORE_NBR as STORE_NBR",
	"CAST(o_STORE_NAME AS STRING) as STORE_NAME",
	"CAST(o_SITE_EMAIL_ADDRESS AS STRING) as SITE_EMAIL_ADDRESS",
	"REGION_ID as REGION_ID",
	"CAST(o_REGION_DESC AS STRING) as REGION_DESC",
	"CAST(o_REGION_VP_NAME AS STRING) as REGION_VP_NAME",
	"CAST(o_RVP_EMAIL_ADDRESS AS STRING) as RVP_EMAIL_ADDRESS",
	"DISTRICT_ID as DISTRICT_ID",
	"CAST(o_DISTRICT_DESC AS STRING) as DISTRICT_DESC",
	"CAST(o_DIST_MGR_NAME AS STRING) as DIST_MGR_NAME",
	"CAST(o_DM_EMAIL_ADDRESS AS STRING) as DM_EMAIL_ADDRESS",
	"CAST(o_SITE_ADDRESS AS STRING) as SITE_ADDRESS",
	"CAST(o_SITE_CITY AS STRING) as SITE_CITY",
	"CAST(o_STATE_CD AS STRING) as STATE_CD",
	"CAST(o_POSTAL_CD AS STRING) as POSTAL_CD",
	"CAST(o_COUNTRY_CD AS STRING) as COUNTRY_CD",
	"CAST(o_SITE_MAIN_TELE_NO AS STRING) as SITE_MAIN_TELE_NO",
	"HOTEL_FLAG as HOTEL_FLAG",
	"DAYCAMP_FLAG as DAY_CAMP_FLAG",
	"VET_FLAG as VET_FLAG",
	"SALON_FLAG as SALON_FLAG",
	"DATE_FORMAT(OPEN_DT,'MM/dd/yyyy HH:mm:ss') as OPEN_DT",
 	"DATE_FORMAT(SURVEY_START_DT,'MM/dd/yyyy HH:mm:ss') as SURVEY_START_DT"
)

# COMMAND ----------
Shortcut_to_SMG_Site_Extract_FlatFile = Shortcut_to_SMG_Site_Extract_FlatFile.withColumnRenamed("STORE_NBR","#STORE_NBR")

# COMMAND ----------

def writeToFlatFile_comma(df, filePath, fileName, mode):
    print(filePath)
    if mode == "overwrite":
        dbutils.fs.rm(filePath.strip("/") + "/", True)
 
    df.repartition(1).write.mode(mode).option("header", "True").option(
        "inferSchema", "true"
    ).option("delimiter", ",").option("ignoreTrailingWhiteSpace", "False").csv(filePath)
    print("File added to GCS Path")
    removeTransactionFiles(filePath)
    newFilePath = filePath.strip("/") + "/" + fileName
 
    renamePartFileName(filePath, newFilePath)


# COMMAND ----------
gs_source_path = target_bucket + target_file
nas_target_path = nas_target_base_path + today+ "\\"

try:
    writeToFlatFile_comma(Shortcut_to_SMG_Site_Extract_FlatFile, target_bucket, target_file, 'overwrite' )
    copy_file_to_nas(gs_source_path, nas_target_path)
 
except Exception as e:
    raise e


#things to revert back i> snapshot date to be removed ii> hardcoded version table SALES_DAY_SKU_STORE_RPT@v15