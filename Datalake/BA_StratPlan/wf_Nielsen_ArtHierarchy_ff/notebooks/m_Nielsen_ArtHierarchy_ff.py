# Databricks notebook source
#Code converted on 2023-12-11 11:03:52
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
target_bucket=getParameterValue(raw,'wf_Nielsen_ArtHierarchy_ff','m_Nielsen_ArtHierarchy_ff','target_bucket')
target_file=getParameterValue(raw,'wf_Nielsen_ArtHierarchy_ff','m_Nielsen_ArtHierarchy_ff','target_file')


# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 12

SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_DESC,
SAP_CATEGORY_ID,
SAP_CATEGORY_DESC,
VP_ID,
VP_DESC,
SAP_CLASS_ID,
SAP_CLASS_DESC,
SAP_DEPT_ID,
SAP_DEPT_DESC,
SAP_DIVISION_ID,
SAP_DIVISION_DESC
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SKU_PROFILE_RPT = SQ_Shortcut_to_SKU_PROFILE_RPT \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[1],'SKU_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[2],'SAP_CATEGORY_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[3],'SAP_CATEGORY_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[4],'VP_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[5],'VP_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[6],'SAP_CLASS_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[7],'SAP_CLASS_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[8],'SAP_DEPT_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[9],'SAP_DEPT_DESC') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[10],'SAP_DIVISION_ID') \
	.withColumnRenamed(SQ_Shortcut_to_SKU_PROFILE_RPT.columns[11],'SAP_DIVISION_DESC')

# COMMAND ----------

# Processing node SQ_Shortcut_to_UPC, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_UPC = spark.sql(f"""SELECT DISTINCT
UPC_ID,
PRODUCT_ID
FROM {legacy}.UPC""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_UPC = SQ_Shortcut_to_UPC \
	.withColumnRenamed(SQ_Shortcut_to_UPC.columns[0],'UPC_ID') \
	.withColumnRenamed(SQ_Shortcut_to_UPC.columns[1],'PRODUCT_ID')

# COMMAND ----------

# Processing node JNR_UPC, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_UPC_temp = SQ_Shortcut_to_UPC.toDF(*["SQ_Shortcut_to_UPC___" + col for col in SQ_Shortcut_to_UPC.columns])
SQ_Shortcut_to_SKU_PROFILE_RPT_temp = SQ_Shortcut_to_SKU_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT.columns])

JNR_UPC = SQ_Shortcut_to_SKU_PROFILE_RPT_temp.join(SQ_Shortcut_to_UPC_temp,[SQ_Shortcut_to_SKU_PROFILE_RPT_temp.SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID == SQ_Shortcut_to_UPC_temp.SQ_Shortcut_to_UPC___PRODUCT_ID],'left_outer').selectExpr(
	"SQ_Shortcut_to_UPC___UPC_ID as UPC_ID",
	"SQ_Shortcut_to_UPC___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID as PRODUCT_ID1",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_DESC as SKU_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___VP_ID as VP_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___VP_DESC as VP_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_DIVISION_ID as SAP_DIVISION_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_DIVISION_DESC as SAP_DIVISION_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_DEPT_ID as SAP_DEPT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_DEPT_DESC as SAP_DEPT_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_CLASS_ID as SAP_CLASS_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_CLASS_DESC as SAP_CLASS_DESC",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_CATEGORY_ID as SAP_CATEGORY_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SAP_CATEGORY_DESC as SAP_CATEGORY_DESC")

# COMMAND ----------

# Processing node SRT_UPC, type SORTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

SRT_UPC = JNR_UPC.selectExpr(
	"UPC_ID as UPC_ID",
	"SKU_DESC as DESCRIPTION",
	"VP_DESC as L1Desc",
	"SAP_DIVISION_DESC as L2Desc",
	"SAP_DEPT_DESC as L3Desc",
	"SAP_CLASS_DESC as L4Desc",
	"SAP_CATEGORY_DESC as L5Desc",
	"VP_ID as L1Key",
	"SAP_DIVISION_ID as L2Key",
	"SAP_DEPT_ID as L3Key",
	"SAP_CLASS_ID as L4Key",
	"SAP_CATEGORY_ID as L5Key").sort(col('UPC_ID').asc())

# COMMAND ----------

def writeToFlatFiles_sorted(df, filePath, fileName, mode , header="True", ext=''):
    print(filePath)
    if mode == "overwrite":
        dbutils.fs.rm(filePath.strip("/") + "/" + fileName, True)

    df.repartition(1).orderBy(col("UPC_ID").asc_nulls_last()).write.mode(mode).option("header", header).option(
        "inferSchema", "true"
    ).option("delimiter", "|").option("ignoreTrailingWhiteSpace", "False").csv(filePath.strip("/") + "/" + fileName)
    print("File added to GCS Path")
    removeTransactionFiles(filePath.strip("/") + "/" + fileName)
    newFilePath = filePath.strip("/") + "/" + fileName
    print(newFilePath)
    print(filePath)
    renamePartFileNames(newFilePath, newFilePath,ext)

# COMMAND ----------

# Processing node Shortcut_to_Nielsen_ArtHierarchy_Flat, type TARGET 
# COLUMN COUNT: 12


Shortcut_to_Nielsen_ArtHierarchy_Flat = SRT_UPC.selectExpr(
	"CAST(SUBSTR(UPC_ID,0,12) as DECIMAL(12)) as `#UPC_ID`",
	"CAST(DESCRIPTION AS STRING) as DESCRIPTION",
	"CAST(L1Desc AS STRING) as L1Desc",
	"CAST(L2Desc AS STRING) as L2Desc",
	"CAST(L3Desc AS STRING) as L3Desc",
	"CAST(L4Desc AS STRING) as L4Desc",
	"CAST(L5Desc AS STRING) as L5Desc",
	"L1Key as L1Key",
	"L2Key as L2Key",
	"L3Key as L3Key",
	"L4Key as L4Key",
	"L5Key as L5Key"
)

target_bucket=target_bucket+datetime.now().strftime("%Y%m%d")+'/'

writeToFlatFiles_sorted(Shortcut_to_Nielsen_ArtHierarchy_Flat, target_bucket, target_file[:-4], 'overwrite' , header="True" , ext=".txt" )

# COMMAND ----------

if env == "prod":
    gs_source_path = target_bucket + target_file
    today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/nas05/edwshare/DataLake/Temp_NZ_Migration/Nielsen/petm_hierarchy/" + today + '/'
else:
    gs_source_path = target_bucket + target_file
    #today=datetime.now().strftime("%Y%m%d")
    nas_target_path = "/mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test/Nielsen/petm_hierarchy/"
