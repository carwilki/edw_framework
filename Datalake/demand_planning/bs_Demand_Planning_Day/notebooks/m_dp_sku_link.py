# Databricks notebook source
#Code converted on 2023-10-06 15:46:20
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

# Variable_declaration_comment
SKU_LINK_TYPE_CD=getParameterValue(raw,'wf_PMSourceFileDir_RH01.prm','BA_Demand_Planning.WF:bs_Demand_Planning_Day.ST:s_dp_sku_link','SKU_LINK_TYPE_CD')
print(SKU_LINK_TYPE_CD)

# COMMAND ----------

# Processing node SQ_Shortcut_To_DP_SKU_LINK, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_To_DP_SKU_LINK = spark.sql(f"""SELECT
PRODUCT_ID,
LOCATION_ID,
SKU_LINK_TYPE_CD,
SKU_LINK_EFF_DT,
LINK_PRODUCT_ID,
LINK_LOCATION_ID,
SKU_LINK_END_DT,
LOAD_DT
FROM {legacy}.DP_SKU_LINK
WHERE SKU_LINK_TYPE_CD IN ('{SKU_LINK_TYPE_CD}')

AND SKU_LINK_EFF_DT >= (SELECT MIN(SKU_LINK_EFF_DT) FROM {raw}.DP_SKU_LINK_PRE)""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_DP_SKU_LINK = SQ_Shortcut_To_DP_SKU_LINK \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK.columns[1],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK.columns[2],'SKU_LINK_TYPE_CD') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK.columns[3],'SKU_LINK_EFF_DT') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK.columns[4],'LINK_PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK.columns[5],'LINK_LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK.columns[6],'SKU_LINK_END_DT') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK.columns[7],'LOAD_DT')

# COMMAND ----------

# Processing node SQ_Shortcut_To_SKU_PROFILE1, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_SKU_PROFILE1 = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_SKU_PROFILE1 = SQ_Shortcut_To_SKU_PROFILE1 \
	.withColumnRenamed(SQ_Shortcut_To_SKU_PROFILE1.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_To_SKU_PROFILE1.columns[1],'SKU_NBR')

# COMMAND ----------

# Processing node SQ_Shortcut_To_SKU_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_SKU_PROFILE = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_SKU_PROFILE = SQ_Shortcut_To_SKU_PROFILE \
	.withColumnRenamed(SQ_Shortcut_To_SKU_PROFILE.columns[0],'PRODUCT_ID') \
	.withColumnRenamed(SQ_Shortcut_To_SKU_PROFILE.columns[1],'SKU_NBR')

# COMMAND ----------

# Processing node SQ_Shortcut_To_DP_SKU_LINK_PRE, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_To_DP_SKU_LINK_PRE = spark.sql(f"""SELECT
SKU_LINK_TYPE_CD,
FROM_STORE_NBR,
FROM_SKU_NBR,
TO_STORE_NBR,
TO_SKU_NBR,
SKU_LINK_EFF_DT,
SKU_LINK_END_DT
FROM {raw}.DP_SKU_LINK_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_DP_SKU_LINK_PRE = SQ_Shortcut_To_DP_SKU_LINK_PRE \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK_PRE.columns[0],'SKU_LINK_TYPE_CD') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK_PRE.columns[1],'FROM_STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK_PRE.columns[2],'FROM_SKU_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK_PRE.columns[3],'TO_STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK_PRE.columns[4],'TO_SKU_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK_PRE.columns[5],'SKU_LINK_EFF_DT') \
	.withColumnRenamed(SQ_Shortcut_To_DP_SKU_LINK_PRE.columns[6],'SKU_LINK_END_DT')

# COMMAND ----------

# Processing node SQ_Shortcut_To_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_SITE_PROFILE = spark.sql(f"""SELECT
LOCATION_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_SITE_PROFILE = SQ_Shortcut_To_SITE_PROFILE \
	.withColumnRenamed(SQ_Shortcut_To_SITE_PROFILE.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_To_SITE_PROFILE.columns[1],'STORE_NBR')

# COMMAND ----------

# Processing node JNR_SITE_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_DP_SKU_LINK_PRE_temp = SQ_Shortcut_To_DP_SKU_LINK_PRE.toDF(*["SQ_Shortcut_To_DP_SKU_LINK_PRE___" + col for col in SQ_Shortcut_To_DP_SKU_LINK_PRE.columns])
SQ_Shortcut_To_SITE_PROFILE_temp = SQ_Shortcut_To_SITE_PROFILE.toDF(*["SQ_Shortcut_To_SITE_PROFILE___" + col for col in SQ_Shortcut_To_SITE_PROFILE.columns])

JNR_SITE_PROFILE = SQ_Shortcut_To_DP_SKU_LINK_PRE_temp.join(SQ_Shortcut_To_SITE_PROFILE_temp,[SQ_Shortcut_To_DP_SKU_LINK_PRE_temp.SQ_Shortcut_To_DP_SKU_LINK_PRE___TO_STORE_NBR == SQ_Shortcut_To_SITE_PROFILE_temp.SQ_Shortcut_To_SITE_PROFILE___STORE_NBR],'inner').selectExpr(
	"SQ_Shortcut_To_DP_SKU_LINK_PRE___SKU_LINK_TYPE_CD as SKU_LINK_TYPE_CD",
	"SQ_Shortcut_To_DP_SKU_LINK_PRE___FROM_STORE_NBR as FROM_STORE_NBR",
	"SQ_Shortcut_To_DP_SKU_LINK_PRE___FROM_SKU_NBR as FROM_SKU_NBR",
	"SQ_Shortcut_To_DP_SKU_LINK_PRE___TO_STORE_NBR as in_TO_STORE_NBR",
	"SQ_Shortcut_To_DP_SKU_LINK_PRE___TO_SKU_NBR as TO_SKU_NBR",
	"SQ_Shortcut_To_DP_SKU_LINK_PRE___SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"SQ_Shortcut_To_DP_SKU_LINK_PRE___SKU_LINK_END_DT as SKU_LINK_END_DT",
	"SQ_Shortcut_To_SITE_PROFILE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_To_SITE_PROFILE___STORE_NBR as in_STORE_NBR")

# COMMAND ----------

# Processing node SQ_Shortcut_To_SITE_PROFILE1, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_To_SITE_PROFILE1 = spark.sql(f"""SELECT
LOCATION_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_SITE_PROFILE1 = SQ_Shortcut_To_SITE_PROFILE1 \
	.withColumnRenamed(SQ_Shortcut_To_SITE_PROFILE1.columns[0],'LOCATION_ID') \
	.withColumnRenamed(SQ_Shortcut_To_SITE_PROFILE1.columns[1],'STORE_NBR')

# COMMAND ----------

# Processing node SQ_Shortcut_to_SAP_SKU_LINK_TYPE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SAP_SKU_LINK_TYPE = spark.sql(f"""SELECT
SAP_SKU_LINK_TYPE_CD,
EDW_SKU_LINK_TYPE_CD
FROM {legacy}.SAP_SKU_LINK_TYPE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SAP_SKU_LINK_TYPE = SQ_Shortcut_to_SAP_SKU_LINK_TYPE \
	.withColumnRenamed(SQ_Shortcut_to_SAP_SKU_LINK_TYPE.columns[0],'SAP_SKU_LINK_TYPE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_SAP_SKU_LINK_TYPE.columns[1],'EDW_SKU_LINK_TYPE_CD')

# COMMAND ----------

# Processing node JNR_SKU_PROFILE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_SKU_PROFILE_temp = SQ_Shortcut_To_SKU_PROFILE.toDF(*["SQ_Shortcut_To_SKU_PROFILE___" + col for col in SQ_Shortcut_To_SKU_PROFILE.columns])
JNR_SITE_PROFILE_temp = JNR_SITE_PROFILE.toDF(*["JNR_SITE_PROFILE___" + col for col in JNR_SITE_PROFILE.columns])

JNR_SKU_PROFILE = JNR_SITE_PROFILE_temp.join(SQ_Shortcut_To_SKU_PROFILE_temp,[JNR_SITE_PROFILE_temp.JNR_SITE_PROFILE___TO_SKU_NBR == SQ_Shortcut_To_SKU_PROFILE_temp.SQ_Shortcut_To_SKU_PROFILE___SKU_NBR],'inner').selectExpr(
	"JNR_SITE_PROFILE___SKU_LINK_TYPE_CD as SKU_LINK_TYPE_CD",
	"JNR_SITE_PROFILE___FROM_STORE_NBR as FROM_STORE_NBR",
	"JNR_SITE_PROFILE___FROM_SKU_NBR as FROM_SKU_NBR",
	"JNR_SITE_PROFILE___TO_SKU_NBR as in_TO_SKU_NBR",
	"JNR_SITE_PROFILE___SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"JNR_SITE_PROFILE___SKU_LINK_END_DT as SKU_LINK_END_DT",
	"JNR_SITE_PROFILE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_To_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_To_SKU_PROFILE___SKU_NBR as in_SKU_NBR")

# COMMAND ----------

# Processing node JNR_LINKSITE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_SKU_PROFILE_temp = JNR_SKU_PROFILE.toDF(*["JNR_SKU_PROFILE___" + col for col in JNR_SKU_PROFILE.columns])
SQ_Shortcut_To_SITE_PROFILE1_temp = SQ_Shortcut_To_SITE_PROFILE1.toDF(*["SQ_Shortcut_To_SITE_PROFILE1___" + col for col in SQ_Shortcut_To_SITE_PROFILE1.columns])

JNR_LINKSITE = JNR_SKU_PROFILE_temp.join(SQ_Shortcut_To_SITE_PROFILE1_temp,[JNR_SKU_PROFILE_temp.JNR_SKU_PROFILE___FROM_STORE_NBR == SQ_Shortcut_To_SITE_PROFILE1_temp.SQ_Shortcut_To_SITE_PROFILE1___STORE_NBR],'inner').selectExpr(
	"JNR_SKU_PROFILE___SKU_LINK_TYPE_CD as SKU_LINK_TYPE_CD",
	"JNR_SKU_PROFILE___FROM_STORE_NBR as in_FROM_STORE_NBR",
	"JNR_SKU_PROFILE___FROM_SKU_NBR as FROM_SKU_NBR",
	"JNR_SKU_PROFILE___SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"JNR_SKU_PROFILE___SKU_LINK_END_DT as SKU_LINK_END_DT",
	"JNR_SKU_PROFILE___LOCATION_ID as LOCATION_ID",
	"JNR_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_To_SITE_PROFILE1___LOCATION_ID as LINK_LOCATION_ID",
	"SQ_Shortcut_To_SITE_PROFILE1___STORE_NBR as in_STORE_NBR")

# COMMAND ----------

# Processing node JNR_LINKSKU, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_SKU_PROFILE1_temp = SQ_Shortcut_To_SKU_PROFILE1.toDF(*["SQ_Shortcut_To_SKU_PROFILE1___" + col for col in SQ_Shortcut_To_SKU_PROFILE1.columns])
JNR_LINKSITE_temp = JNR_LINKSITE.toDF(*["JNR_LINKSITE___" + col for col in JNR_LINKSITE.columns])

JNR_LINKSKU = JNR_LINKSITE_temp.join(SQ_Shortcut_To_SKU_PROFILE1_temp,[JNR_LINKSITE_temp.JNR_LINKSITE___FROM_SKU_NBR == SQ_Shortcut_To_SKU_PROFILE1_temp.SQ_Shortcut_To_SKU_PROFILE1___SKU_NBR],'inner').selectExpr(
	"JNR_LINKSITE___SKU_LINK_TYPE_CD as SKU_LINK_TYPE_CD",
	"JNR_LINKSITE___FROM_SKU_NBR as in_FROM_SKU_NBR",
	"JNR_LINKSITE___SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"JNR_LINKSITE___SKU_LINK_END_DT as SKU_LINK_END_DT",
	"JNR_LINKSITE___LOCATION_ID as LOCATION_ID",
	"JNR_LINKSITE___PRODUCT_ID as PRODUCT_ID",
	"JNR_LINKSITE___LINK_LOCATION_ID as LINK_LOCATION_ID",
	"SQ_Shortcut_To_SKU_PROFILE1___PRODUCT_ID as LINK_PRODUCT_ID",
	"SQ_Shortcut_To_SKU_PROFILE1___SKU_NBR as in_SKU_NBR")

# COMMAND ----------

# Processing node JNR_SAP_SKU_LINK_TYPE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
JNR_LINKSKU_temp = JNR_LINKSKU.toDF(*["JNR_LINKSKU___" + col for col in JNR_LINKSKU.columns])
SQ_Shortcut_to_SAP_SKU_LINK_TYPE_temp = SQ_Shortcut_to_SAP_SKU_LINK_TYPE.toDF(*["SQ_Shortcut_to_SAP_SKU_LINK_TYPE___" + col for col in SQ_Shortcut_to_SAP_SKU_LINK_TYPE.columns])

JNR_SAP_SKU_LINK_TYPE = JNR_LINKSKU_temp.join(SQ_Shortcut_to_SAP_SKU_LINK_TYPE_temp,[JNR_LINKSKU_temp.JNR_LINKSKU___SKU_LINK_TYPE_CD == SQ_Shortcut_to_SAP_SKU_LINK_TYPE_temp.SQ_Shortcut_to_SAP_SKU_LINK_TYPE___SAP_SKU_LINK_TYPE_CD],'inner').selectExpr(
	"JNR_LINKSKU___SKU_LINK_TYPE_CD as in_SKU_LINK_TYPE_CD",
	"JNR_LINKSKU___SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"JNR_LINKSKU___SKU_LINK_END_DT as SKU_LINK_END_DT",
	"JNR_LINKSKU___LOCATION_ID as LOCATION_ID",
	"JNR_LINKSKU___PRODUCT_ID as PRODUCT_ID",
	"JNR_LINKSKU___LINK_LOCATION_ID as LINK_LOCATION_ID",
	"JNR_LINKSKU___LINK_PRODUCT_ID as LINK_PRODUCT_ID",
	"SQ_Shortcut_to_SAP_SKU_LINK_TYPE___SAP_SKU_LINK_TYPE_CD as in_SAP_SKU_LINK_TYPE_CD",
	"SQ_Shortcut_to_SAP_SKU_LINK_TYPE___EDW_SKU_LINK_TYPE_CD as EDW_SKU_LINK_TYPE_CD")

# COMMAND ----------

# Processing node JNR_DP_SKU_LINK, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_DP_SKU_LINK_temp = SQ_Shortcut_To_DP_SKU_LINK.toDF(*["SQ_Shortcut_To_DP_SKU_LINK___" + col for col in SQ_Shortcut_To_DP_SKU_LINK.columns])
JNR_SAP_SKU_LINK_TYPE_temp = JNR_SAP_SKU_LINK_TYPE.toDF(*["JNR_SAP_SKU_LINK_TYPE___" + col for col in JNR_SAP_SKU_LINK_TYPE.columns])

JNR_DP_SKU_LINK = JNR_SAP_SKU_LINK_TYPE_temp.join(SQ_Shortcut_To_DP_SKU_LINK_temp,[JNR_SAP_SKU_LINK_TYPE_temp.JNR_SAP_SKU_LINK_TYPE___PRODUCT_ID == SQ_Shortcut_To_DP_SKU_LINK_temp.SQ_Shortcut_To_DP_SKU_LINK___PRODUCT_ID, JNR_SAP_SKU_LINK_TYPE_temp.JNR_SAP_SKU_LINK_TYPE___LOCATION_ID == SQ_Shortcut_To_DP_SKU_LINK_temp.SQ_Shortcut_To_DP_SKU_LINK___LOCATION_ID, JNR_SAP_SKU_LINK_TYPE_temp.JNR_SAP_SKU_LINK_TYPE___SKU_LINK_EFF_DT == SQ_Shortcut_To_DP_SKU_LINK_temp.SQ_Shortcut_To_DP_SKU_LINK___SKU_LINK_EFF_DT, JNR_SAP_SKU_LINK_TYPE_temp.JNR_SAP_SKU_LINK_TYPE___LINK_LOCATION_ID == SQ_Shortcut_To_DP_SKU_LINK_temp.SQ_Shortcut_To_DP_SKU_LINK___LINK_LOCATION_ID, JNR_SAP_SKU_LINK_TYPE_temp.JNR_SAP_SKU_LINK_TYPE___LINK_PRODUCT_ID == SQ_Shortcut_To_DP_SKU_LINK_temp.SQ_Shortcut_To_DP_SKU_LINK___LINK_PRODUCT_ID, JNR_SAP_SKU_LINK_TYPE_temp.JNR_SAP_SKU_LINK_TYPE___EDW_SKU_LINK_TYPE_CD == SQ_Shortcut_To_DP_SKU_LINK_temp.SQ_Shortcut_To_DP_SKU_LINK___SKU_LINK_TYPE_CD],'fullouter').selectExpr(
	"JNR_SAP_SKU_LINK_TYPE___SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"JNR_SAP_SKU_LINK_TYPE___SKU_LINK_END_DT as SKU_LINK_END_DT",
	"JNR_SAP_SKU_LINK_TYPE___LOCATION_ID as LOCATION_ID",
	"JNR_SAP_SKU_LINK_TYPE___PRODUCT_ID as PRODUCT_ID",
	"JNR_SAP_SKU_LINK_TYPE___LINK_LOCATION_ID as LINK_LOCATION_ID",
	"JNR_SAP_SKU_LINK_TYPE___LINK_PRODUCT_ID as LINK_PRODUCT_ID",
	"JNR_SAP_SKU_LINK_TYPE___EDW_SKU_LINK_TYPE_CD as EDW_SKU_LINK_TYPE_CD",
	"SQ_Shortcut_To_DP_SKU_LINK___PRODUCT_ID as OLD_PRODUCT_ID",
	"SQ_Shortcut_To_DP_SKU_LINK___LOCATION_ID as OLD_LOCATION_ID",
	"SQ_Shortcut_To_DP_SKU_LINK___SKU_LINK_TYPE_CD as OLD_SKU_LINK_TYPE_CD",
	"SQ_Shortcut_To_DP_SKU_LINK___SKU_LINK_EFF_DT as OLD_SKU_LINK_EFF_DT",
	"SQ_Shortcut_To_DP_SKU_LINK___LINK_PRODUCT_ID as OLD_LINK_PRODUCT_ID",
	"SQ_Shortcut_To_DP_SKU_LINK___LINK_LOCATION_ID as OLD_LINK_LOCATION_ID",
	"SQ_Shortcut_To_DP_SKU_LINK___SKU_LINK_END_DT as OLD_SKU_LINK_END_DT",
	"SQ_Shortcut_To_DP_SKU_LINK___LOAD_DT as OLD_LOAD_DT")

# COMMAND ----------

# Processing node EXP_UPD_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
JNR_DP_SKU_LINK_temp = JNR_DP_SKU_LINK.toDF(*["JNR_DP_SKU_LINK___" + col for col in JNR_DP_SKU_LINK.columns])

EXP_UPD_VALIDATOR = JNR_DP_SKU_LINK_temp.selectExpr(
	# "JNR_DP_SKU_LINK___sys_row_id as sys_row_id",
	"JNR_DP_SKU_LINK___SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"JNR_DP_SKU_LINK___SKU_LINK_END_DT as SKU_LINK_END_DT",
	"JNR_DP_SKU_LINK___LOCATION_ID as LOCATION_ID",
	"JNR_DP_SKU_LINK___PRODUCT_ID as PRODUCT_ID",
	"JNR_DP_SKU_LINK___LINK_LOCATION_ID as LINK_LOCATION_ID",
	"JNR_DP_SKU_LINK___LINK_PRODUCT_ID as LINK_PRODUCT_ID",
	"JNR_DP_SKU_LINK___EDW_SKU_LINK_TYPE_CD as EDW_SKU_LINK_TYPE_CD",
	"JNR_DP_SKU_LINK___OLD_PRODUCT_ID as OLD_PRODUCT_ID",
	"JNR_DP_SKU_LINK___OLD_LOCATION_ID as OLD_LOCATION_ID",
	"JNR_DP_SKU_LINK___OLD_SKU_LINK_TYPE_CD as OLD_SKU_LINK_TYPE_CD",
	"JNR_DP_SKU_LINK___OLD_SKU_LINK_EFF_DT as OLD_SKU_LINK_EFF_DT",
	"JNR_DP_SKU_LINK___OLD_LINK_PRODUCT_ID as OLD_LINK_PRODUCT_ID",
	"JNR_DP_SKU_LINK___OLD_LINK_LOCATION_ID as OLD_LINK_LOCATION_ID",
	"JNR_DP_SKU_LINK___OLD_SKU_LINK_END_DT as OLD_SKU_LINK_END_DT",
	"JNR_DP_SKU_LINK___OLD_LOAD_DT as OLD_LOAD_DT",
	"IF (JNR_DP_SKU_LINK___PRODUCT_ID IS NOT NULL AND JNR_DP_SKU_LINK___OLD_PRODUCT_ID IS NULL, 'INSERT', IF (JNR_DP_SKU_LINK___PRODUCT_ID IS NULL AND JNR_DP_SKU_LINK___OLD_PRODUCT_ID IS NOT NULL AND JNR_DP_SKU_LINK___OLD_SKU_LINK_END_DT <> DATE '1900-01-01' , 'DELETE', IF (JNR_DP_SKU_LINK___PRODUCT_ID IS NOT NULL AND JNR_DP_SKU_LINK___OLD_PRODUCT_ID IS NOT NULL AND JNR_DP_SKU_LINK___SKU_LINK_END_DT <> JNR_DP_SKU_LINK___OLD_SKU_LINK_END_DT, 'UPDATE', NULL))) as o_UPD_VALIDATOR"
)

# COMMAND ----------

# Processing node rtr_INS_UPD_DEL, type ROUTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 48


# Creating output dataframe for rtr_INS_UPD_DEL, output group DELETE
rtr_INS_UPD_DEL_DELETE = EXP_UPD_VALIDATOR.selectExpr(
	"SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"SKU_LINK_END_DT as SKU_LINK_END_DT",
	"LOCATION_ID as LOCATION_ID",
	"PRODUCT_ID as PRODUCT_ID",
	"LINK_LOCATION_ID as LINK_LOCATION_ID",
	"LINK_PRODUCT_ID as LINK_PRODUCT_ID",
	"EDW_SKU_LINK_TYPE_CD as EDW_SKU_LINK_TYPE_CD",
	"OLD_PRODUCT_ID as OLD_PRODUCT_ID",
	"OLD_LOCATION_ID as OLD_LOCATION_ID",
	"OLD_SKU_LINK_TYPE_CD as OLD_SKU_LINK_TYPE_CD",
	"OLD_SKU_LINK_EFF_DT as OLD_SKU_LINK_EFF_DT",
	"OLD_LINK_PRODUCT_ID as OLD_LINK_PRODUCT_ID",
	"OLD_LINK_LOCATION_ID as OLD_LINK_LOCATION_ID",
	"OLD_SKU_LINK_END_DT as OLD_SKU_LINK_END_DT",
	"OLD_LOAD_DT as OLD_LOAD_DT",
	"o_UPD_VALIDATOR as UPD_VALIDATOR").select(	col('SKU_LINK_EFF_DT').alias('SKU_LINK_EFF_DT3'),
	col('SKU_LINK_END_DT').alias('SKU_LINK_END_DT3'),
	col('LOCATION_ID').alias('LOCATION_ID3'),
	col('PRODUCT_ID').alias('PRODUCT_ID3'),
	col('LINK_LOCATION_ID').alias('LINK_LOCATION_ID3'),
	col('LINK_PRODUCT_ID').alias('LINK_PRODUCT_ID3'),
	col('EDW_SKU_LINK_TYPE_CD').alias('EDW_SKU_LINK_TYPE_CD3'),
	col('OLD_PRODUCT_ID').alias('OLD_PRODUCT_ID3'),
	col('OLD_LOCATION_ID').alias('OLD_LOCATION_ID3'),
	col('OLD_SKU_LINK_TYPE_CD').alias('OLD_SKU_LINK_TYPE_CD3'),
	col('OLD_SKU_LINK_EFF_DT').alias('OLD_SKU_LINK_EFF_DT3'),
	col('OLD_LINK_PRODUCT_ID').alias('OLD_LINK_PRODUCT_ID3'),
	col('OLD_LINK_LOCATION_ID').alias('OLD_LINK_LOCATION_ID3'),
	col('OLD_SKU_LINK_END_DT').alias('OLD_SKU_LINK_END_DT3'),
	col('OLD_LOAD_DT').alias('OLD_LOAD_DT3'),
	col('UPD_VALIDATOR').alias('UPD_VALIDATOR3')).filter("UPD_VALIDATOR = 'DELETE'")

# col('sys_row_id'),
# Creating output dataframe for rtr_INS_UPD_DEL, output group INSERT_UPDATE
rtr_INS_UPD_DEL_INSERT_UPDATE = EXP_UPD_VALIDATOR.selectExpr(
	"SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"SKU_LINK_END_DT as SKU_LINK_END_DT",
	"LOCATION_ID as LOCATION_ID",
	"PRODUCT_ID as PRODUCT_ID",
	"LINK_LOCATION_ID as LINK_LOCATION_ID",
	"LINK_PRODUCT_ID as LINK_PRODUCT_ID",
	"EDW_SKU_LINK_TYPE_CD as EDW_SKU_LINK_TYPE_CD",
	"OLD_PRODUCT_ID as OLD_PRODUCT_ID",
	"OLD_LOCATION_ID as OLD_LOCATION_ID",
	"OLD_SKU_LINK_TYPE_CD as OLD_SKU_LINK_TYPE_CD",
	"OLD_SKU_LINK_EFF_DT as OLD_SKU_LINK_EFF_DT",
	"OLD_LINK_PRODUCT_ID as OLD_LINK_PRODUCT_ID",
	"OLD_LINK_LOCATION_ID as OLD_LINK_LOCATION_ID",
	"OLD_SKU_LINK_END_DT as OLD_SKU_LINK_END_DT",
	"OLD_LOAD_DT as OLD_LOAD_DT",
	"o_UPD_VALIDATOR as UPD_VALIDATOR").select(
	col('SKU_LINK_EFF_DT').alias('SKU_LINK_EFF_DT1'),
	col('SKU_LINK_END_DT').alias('SKU_LINK_END_DT1'),
	col('LOCATION_ID').alias('LOCATION_ID1'),
	col('PRODUCT_ID').alias('PRODUCT_ID1'),
	col('LINK_LOCATION_ID').alias('LINK_LOCATION_ID1'),
	col('LINK_PRODUCT_ID').alias('LINK_PRODUCT_ID1'),
	col('EDW_SKU_LINK_TYPE_CD').alias('EDW_SKU_LINK_TYPE_CD1'),
	col('OLD_PRODUCT_ID').alias('OLD_PRODUCT_ID1'),
	col('OLD_LOCATION_ID').alias('OLD_LOCATION_ID1'),
	col('OLD_SKU_LINK_TYPE_CD').alias('OLD_SKU_LINK_TYPE_CD1'),
	col('OLD_SKU_LINK_EFF_DT').alias('OLD_SKU_LINK_EFF_DT1'),
	col('OLD_LINK_PRODUCT_ID').alias('OLD_LINK_PRODUCT_ID1'),
	col('OLD_LINK_LOCATION_ID').alias('OLD_LINK_LOCATION_ID1'),
	col('OLD_SKU_LINK_END_DT').alias('OLD_SKU_LINK_END_DT1'),
	col('OLD_LOAD_DT').alias('OLD_LOAD_DT1'),
	col('UPD_VALIDATOR').alias('UPD_VALIDATOR1')).filter("UPD_VALIDATOR = 'INSERT' OR UPD_VALIDATOR = 'UPDATE'")


# COMMAND ----------

# Processing node EXP_DELETE, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
rtr_INS_UPD_DEL_DELETE_temp = rtr_INS_UPD_DEL_DELETE.toDF(*["rtr_INS_UPD_DEL_DELETE___" + col for col in rtr_INS_UPD_DEL_DELETE.columns])

EXP_DELETE = rtr_INS_UPD_DEL_DELETE_temp.selectExpr(
	"rtr_INS_UPD_DEL_DELETE___OLD_PRODUCT_ID3 as OLD_PRODUCT_ID",
	"rtr_INS_UPD_DEL_DELETE___OLD_LOCATION_ID3 as OLD_LOCATION_ID",
	"rtr_INS_UPD_DEL_DELETE___OLD_SKU_LINK_TYPE_CD3 as OLD_SKU_LINK_TYPE_CD",
	"rtr_INS_UPD_DEL_DELETE___OLD_SKU_LINK_EFF_DT3 as OLD_SKU_LINK_EFF_DT",
	"rtr_INS_UPD_DEL_DELETE___OLD_LINK_PRODUCT_ID3 as OLD_LINK_PRODUCT_ID",
	"rtr_INS_UPD_DEL_DELETE___OLD_LINK_LOCATION_ID3 as OLD_LINK_LOCATION_ID",
	"rtr_INS_UPD_DEL_DELETE___OLD_LOAD_DT3 as OLD_LOAD_DT").selectExpr(
	# "rtr_INS_UPD_DEL_DELETE___sys_row_id as sys_row_id",
	"OLD_PRODUCT_ID as OLD_PRODUCT_ID",
	"OLD_LOCATION_ID as OLD_LOCATION_ID",
	"OLD_SKU_LINK_TYPE_CD as OLD_SKU_LINK_TYPE_CD",
	"OLD_SKU_LINK_EFF_DT as OLD_SKU_LINK_EFF_DT",
	"OLD_LINK_PRODUCT_ID as OLD_LINK_PRODUCT_ID",
	"OLD_LINK_LOCATION_ID as OLD_LINK_LOCATION_ID",
	"OLD_LOAD_DT as OLD_LOAD_DT",
	"DATE'1900-01-01'  as o_SKU_LINK_END_DT"
)

# COMMAND ----------

# Processing node UPD_TRANS_DEL, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
EXP_DELETE_temp = EXP_DELETE.toDF(*["EXP_DELETE___" + col for col in EXP_DELETE.columns])

UPD_TRANS_DEL = EXP_DELETE_temp.selectExpr(
	"EXP_DELETE___OLD_PRODUCT_ID as OLD_PRODUCT_ID",
	"EXP_DELETE___OLD_LOCATION_ID as OLD_LOCATION_ID",
	"EXP_DELETE___OLD_SKU_LINK_TYPE_CD as OLD_SKU_LINK_TYPE_CD",
	"EXP_DELETE___OLD_SKU_LINK_EFF_DT as OLD_SKU_LINK_EFF_DT",
	"EXP_DELETE___OLD_LINK_PRODUCT_ID as OLD_LINK_PRODUCT_ID",
	"EXP_DELETE___OLD_LINK_LOCATION_ID as OLD_LINK_LOCATION_ID",
	"EXP_DELETE___OLD_LOAD_DT as OLD_LOAD_DT",
	"EXP_DELETE___o_SKU_LINK_END_DT as SKU_LINK_END_DT") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------

# Processing node EXP_INS_UPD, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
rtr_INS_UPD_DEL_INSERT_UPDATE_temp = rtr_INS_UPD_DEL_INSERT_UPDATE.toDF(*["rtr_INS_UPD_DEL_INSERT_UPDATE___" + col for col in rtr_INS_UPD_DEL_INSERT_UPDATE.columns])

EXP_INS_UPD = rtr_INS_UPD_DEL_INSERT_UPDATE_temp.selectExpr(
	"rtr_INS_UPD_DEL_INSERT_UPDATE___SKU_LINK_EFF_DT1 as SKU_LINK_EFF_DT",
	"rtr_INS_UPD_DEL_INSERT_UPDATE___SKU_LINK_END_DT1 as SKU_LINK_END_DT",
	"rtr_INS_UPD_DEL_INSERT_UPDATE___LOCATION_ID1 as LOCATION_ID",
	"rtr_INS_UPD_DEL_INSERT_UPDATE___PRODUCT_ID1 as PRODUCT_ID",
	"rtr_INS_UPD_DEL_INSERT_UPDATE___LINK_LOCATION_ID1 as LINK_LOCATION_ID",
	"rtr_INS_UPD_DEL_INSERT_UPDATE___LINK_PRODUCT_ID1 as LINK_PRODUCT_ID",
	"rtr_INS_UPD_DEL_INSERT_UPDATE___EDW_SKU_LINK_TYPE_CD1 as EDW_SKU_LINK_TYPE_CD",
	"rtr_INS_UPD_DEL_INSERT_UPDATE___OLD_LOAD_DT1 as in_OLD_LOAD_DT",
	"rtr_INS_UPD_DEL_INSERT_UPDATE___UPD_VALIDATOR1 as UPD_VALIDATOR").selectExpr(
	# "rtr_INS_UPD_DEL_INSERT_UPDATE___sys_row_id as sys_row_id",
	"SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"SKU_LINK_END_DT as SKU_LINK_END_DT",
	"LOCATION_ID as LOCATION_ID",
	"PRODUCT_ID as PRODUCT_ID",
	"LINK_LOCATION_ID as LINK_LOCATION_ID",
	"LINK_PRODUCT_ID as LINK_PRODUCT_ID",
	"EDW_SKU_LINK_TYPE_CD as EDW_SKU_LINK_TYPE_CD",
	"IF (UPD_VALIDATOR = 'INSERT', CURRENT_TIMESTAMP, in_OLD_LOAD_DT) as o_LOAD_DT",
	"UPD_VALIDATOR as UPD_VALIDATOR"
)

# COMMAND ----------

# Processing node UPD_TRANS_INS_UPD, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
EXP_INS_UPD_temp = EXP_INS_UPD.toDF(*["EXP_INS_UPD___" + col for col in EXP_INS_UPD.columns])

UPD_TRANS_INS_UPD = EXP_INS_UPD_temp.selectExpr(
	"EXP_INS_UPD___SKU_LINK_EFF_DT as SKU_LINK_EFF_DT",
	"EXP_INS_UPD___SKU_LINK_END_DT as SKU_LINK_END_DT",
	"EXP_INS_UPD___LOCATION_ID as LOCATION_ID",
	"EXP_INS_UPD___PRODUCT_ID as PRODUCT_ID",
	"EXP_INS_UPD___LINK_LOCATION_ID as LINK_LOCATION_ID",
	"EXP_INS_UPD___LINK_PRODUCT_ID as LINK_PRODUCT_ID",
	"EXP_INS_UPD___EDW_SKU_LINK_TYPE_CD as EDW_SKU_LINK_TYPE_CD",
	"EXP_INS_UPD___o_LOAD_DT as LOAD_DT",
	"EXP_INS_UPD___UPD_VALIDATOR as UPD_VALIDATOR") \
	.withColumn('pyspark_data_action', when((col('UPD_VALIDATOR') == lit('INSERT')) ,(lit(0))) .otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_To_DP_SKU_LINK_DEL, type TARGET 
# COLUMN COUNT: 8


Shortcut_To_DP_SKU_LINK_DEL = UPD_TRANS_DEL.selectExpr(
	"CAST(OLD_PRODUCT_ID AS BIGINT) as PRODUCT_ID",
	"CAST(OLD_LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(OLD_SKU_LINK_TYPE_CD AS STRING) as SKU_LINK_TYPE_CD",
	"CAST(OLD_SKU_LINK_EFF_DT AS TIMESTAMP) as SKU_LINK_EFF_DT",
	"CAST(OLD_LINK_PRODUCT_ID AS BIGINT) as LINK_PRODUCT_ID",
	"CAST(OLD_LINK_LOCATION_ID AS BIGINT) as LINK_LOCATION_ID",
	"CAST(SKU_LINK_END_DT AS TIMESTAMP) as SKU_LINK_END_DT",
	"CAST(OLD_LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.PRODUCT_ID = target.PRODUCT_ID AND source.LOCATION_ID = target.LOCATION_ID AND source.SKU_LINK_TYPE_CD = target.SKU_LINK_TYPE_CD AND source.SKU_LINK_EFF_DT = target.SKU_LINK_EFF_DT AND source.LINK_PRODUCT_ID = target.LINK_PRODUCT_ID AND source.LINK_LOCATION_ID = target.LINK_LOCATION_ID"""
  refined_perf_table = f"{legacy}.DP_SKU_LINK"
  executeMerge(Shortcut_To_DP_SKU_LINK_DEL, refined_perf_table, primary_key)
  
#   Shortcut_To_DP_SKU_LINK_DEL.createOrReplaceTempView("Shortcut_To_DP_SKU_LINK_DEL_view")
#   spark.sql(f"""
# 	MERGE INTO {refined_perf_table} target
# 	USING Shortcut_To_DP_SKU_LINK_DEL_view source
# 	ON {primary_key}
# 	WHEN MATCHED THEN
# 	DELETE """
# 	)
  logger.info(f"Merge with {refined_perf_table} completed]")  
except Exception as e:
  raise e
	

# COMMAND ----------

# Processing node Shortcut_To_DP_SKU_LINK_INS_UPD, type TARGET 
# COLUMN COUNT: 8


Shortcut_To_DP_SKU_LINK_INS_UPD = UPD_TRANS_INS_UPD.selectExpr(
	"CAST(PRODUCT_ID AS BIGINT) as PRODUCT_ID",
	"CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
	"CAST(EDW_SKU_LINK_TYPE_CD AS STRING) as SKU_LINK_TYPE_CD",
	"CAST(SKU_LINK_EFF_DT AS TIMESTAMP) as SKU_LINK_EFF_DT",
	"CAST(LINK_PRODUCT_ID AS BIGINT) as LINK_PRODUCT_ID",
	"CAST(LINK_LOCATION_ID AS BIGINT) as LINK_LOCATION_ID",
	"CAST(SKU_LINK_END_DT AS TIMESTAMP) as SKU_LINK_END_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
  primary_key = """source.PRODUCT_ID = target.PRODUCT_ID AND source.LOCATION_ID = target.LOCATION_ID AND source.SKU_LINK_TYPE_CD = target.SKU_LINK_TYPE_CD AND source.SKU_LINK_EFF_DT = target.SKU_LINK_EFF_DT AND source.LINK_PRODUCT_ID = target.LINK_PRODUCT_ID AND source.LINK_LOCATION_ID = target.LINK_LOCATION_ID"""
  refined_perf_table = f"{legacy}.DP_SKU_LINK"
  executeMerge(Shortcut_To_DP_SKU_LINK_INS_UPD, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("DP_SKU_LINK", "DP_SKU_LINK", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("DP_SKU_LINK", "DP_SKU_LINK","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------


