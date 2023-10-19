# Databricks notebook source
#Code converted on 2023-09-26 10:21:27
import os
import argparse
from pyspark.sql import *
from pyspark.sql.functions import *
from datetime import datetime
from Datalake.utils.genericUtilities import *
from Datalake.utils.configs import *
from Datalake.utils.mergeUtils import *
from Datalake.utils.logger import *

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

(username,password,connection_string) = evdh_prd_sqlServer(env)


# COMMAND ----------

# Processing node LKP_VENDOR_TYPE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_VENDOR_TYPE_SRC = spark.sql(f"""SELECT
VENDOR_TYPE_ID,
VENDOR_ACCOUNT_GROUP_CD
FROM {legacy}.VENDOR_TYPE""")
# Conforming fields names to the component layout
# LKP_VENDOR_TYPE_SRC = LKP_VENDOR_TYPE_SRC \
# 	.withColumnRenamed(LKP_VENDOR_TYPE_SRC.columns[0],'')

# COMMAND ----------

# LKP_VENDOR_TYPE_SRC.filter("VENDOR_ACCOUNT_GROUP_CD =' '").show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_Vendor, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_Vendor = jdbcSqlServerConnection(f"""(SELECT
Vendor.VendorNbr,
Vendor.VendorAccountGroup
FROM EnterpriseVendorDataHub.dbo.Vendor) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_Vendor = SQ_Shortcut_to_Vendor \
	.withColumnRenamed(SQ_Shortcut_to_Vendor.columns[0],'VendorNbr') \
	.withColumnRenamed(SQ_Shortcut_to_Vendor.columns[1],'VendorAccountGroup')

# COMMAND ----------

# SQ_Shortcut_to_Vendor.show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_SubRange, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SubRange = jdbcSqlServerConnection(f"""(SELECT
SubRange.SubRangeCD,
SubRange.VendorNbr,
SubRange.SubRangeDesc
FROM EnterpriseVendorDataHub.dbo.SubRange) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SubRange = SQ_Shortcut_to_SubRange \
	.withColumnRenamed(SQ_Shortcut_to_SubRange.columns[0],'SubRangeCD') \
	.withColumnRenamed(SQ_Shortcut_to_SubRange.columns[1],'VendorNbr') \
	.withColumnRenamed(SQ_Shortcut_to_SubRange.columns[2],'SubRangeDesc')

# COMMAND ----------

# SQ_Shortcut_to_SubRange.show()

# COMMAND ----------

# Processing node JNR_Subrange, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_Vendor_temp = SQ_Shortcut_to_Vendor.toDF(*["SQ_Shortcut_to_Vendor___" + col for col in SQ_Shortcut_to_Vendor.columns])
SQ_Shortcut_to_SubRange_temp = SQ_Shortcut_to_SubRange.toDF(*["SQ_Shortcut_to_SubRange___" + col for col in SQ_Shortcut_to_SubRange.columns])
# 
JNR_Subrange = SQ_Shortcut_to_SubRange_temp.join(SQ_Shortcut_to_Vendor_temp,[SQ_Shortcut_to_SubRange_temp.SQ_Shortcut_to_SubRange___VendorNbr == SQ_Shortcut_to_Vendor_temp.SQ_Shortcut_to_Vendor___VendorNbr],'right_outer').selectExpr(
	"SQ_Shortcut_to_Vendor___VendorNbr as VendorNbr",
	"SQ_Shortcut_to_Vendor___VendorAccountGroup as VendorAccountGroup",
	"SQ_Shortcut_to_SubRange___SubRangeCD as SubRangeCD",
	"SQ_Shortcut_to_SubRange___VendorNbr as VendorNbr1",
	"SQ_Shortcut_to_SubRange___SubRangeDesc as SubRangeDesc",
	"SQ_Shortcut_to_Vendor___sys_row_id as sys_row_id",
  )

# COMMAND ----------

# JNR_Subrange.filter("SubRangeDesc is not null").show()

# COMMAND ----------

# Processing node EXP_DEFAULT, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
JNR_Subrange_temp = JNR_Subrange.toDF(*["JNR_Subrange___" + col for col in JNR_Subrange.columns])

# .selectExpr(
# 	"JNR_Subrange___SubRangeCD as in_SubRangeCD",
# 	"JNR_Subrange___SubRangeDesc as in_SubRangeDesc",
# 	"JNR_Subrange___VendorNbr as i_VendorNbr",
# 	"JNR_Subrange___VendorAccountGroup as VendorAccountGroup")
EXP_DEFAULT = JNR_Subrange_temp.selectExpr(
	"JNR_Subrange___sys_row_id as sys_row_id",
	# "JNR_Subrange___SubRangeCD as SubRangeCD",
	"IF (JNR_Subrange___SubRangeCD IS NULL, ' ', JNR_Subrange___SubRangeCD) as SubRangeCD",
	# "JNR_Subrange___in_SubRangeDesc as in_SubRangeDesc",
	"IF (JNR_Subrange___SubRangeDesc IS NULL, ' ', JNR_Subrange___SubRangeDesc) as SubRangeDesc",
	"LTRIM('0', JNR_Subrange___VendorNbr) as VendorNbr",
	"JNR_Subrange___VendorAccountGroup as VendorAccountGroup"
)

# COMMAND ----------

# EXP_DEFAULT.filter("length(VendorAccountGroup) < 5").show()

# COMMAND ----------

# Processing node LKP_VENDOR_TYPE, type LOOKUP_FROM_PRECACHED_DATASET 
# COLUMN COUNT: 3


LKP_VENDOR_TYPE_lookup_result = EXP_DEFAULT.join(LKP_VENDOR_TYPE_SRC, (col('VENDOR_ACCOUNT_GROUP_CD') == col('VendorAccountGroup')), 'left') \
.withColumn('row_num_VENDOR_TYPE_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("VENDOR_TYPE_ID")))
# LKP_VENDOR_TYPE_lookup_result.count()
# LKP_VENDOR_TYPE_lookup_result.filter("row_num_VENDOR_TYPE_ID > 1").show()
LKP_VENDOR_TYPE = LKP_VENDOR_TYPE_lookup_result.filter(col("row_num_VENDOR_TYPE_ID") == 1).select(
	LKP_VENDOR_TYPE_lookup_result.sys_row_id,
	col('VENDOR_TYPE_ID')
#     col('VendorAccountGroup')
)
# LKP_VENDOR_TYPE.filter("VENDOR_TYPE_ID IS NULL").show()

# COMMAND ----------

# Processing node EXP_Vendor_type, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
LKP_VENDOR_TYPE_temp = LKP_VENDOR_TYPE.toDF(*["LKP_VENDOR_TYPE___" + col for col in LKP_VENDOR_TYPE.columns])
EXP_DEFAULT_temp = EXP_DEFAULT.toDF(*["EXP_DEFAULT___" + col for col in EXP_DEFAULT.columns])

# Joining dataframes EXP_DEFAULT, LKP_VENDOR_TYPE to form EXP_Vendor_type
EXP_Vendor_type_joined = EXP_DEFAULT_temp.join(LKP_VENDOR_TYPE_temp, col('EXP_DEFAULT___sys_row_id') == col('LKP_VENDOR_TYPE___sys_row_id'), 'inner')
# This doesn't look right but its what the info code is doing.
EXP_Vendor_type = EXP_Vendor_type_joined.selectExpr(
	"EXP_DEFAULT___VendorNbr as VendorNbr",
	"LKP_VENDOR_TYPE___VENDOR_TYPE_ID as in_VENDOR_TYPE_ID",
	"EXP_DEFAULT___SubRangeCD as SubRangeCD",
	"EXP_DEFAULT___SubRangeDesc as SubRangeDesc",
	"EXP_DEFAULT___sys_row_id as sys_row_id") \
	.selectExpr(
	"sys_row_id as sys_row_id",
	"VendorNbr as VendorNbr",
	"IF (in_VENDOR_TYPE_ID IS NULL, 21, in_VENDOR_TYPE_ID) as VENDOR_TYPE_ID",
# 	"IF (VendorAccountGroup = '', 21, in_VENDOR_TYPE_ID) as VENDOR_TYPE_ID",  
	"SubRangeCD as SubRangeCD",
	"SubRangeDesc as SubRangeDesc"
)
# EXP_Vendor_type_joined.show()
# EXP_Vendor_type.filter("trim(SubRangeCD) <> ''").show()
# EXP_Vendor_type.filter("VENDOR_TYPE_ID is null").show()

# COMMAND ----------

# Processing node Shortcut_to_VENDOR_SUBRANGE_PRE, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_VENDOR_SUBRANGE_PRE = EXP_Vendor_type.selectExpr(
	"CAST(VendorNbr AS STRING) as VENDOR_NBR",
	"CAST(VENDOR_TYPE_ID as tinyint) as VENDOR_TYPE_ID",
	"CAST(SubRangeCD AS STRING) as VENDOR_SUBRANGE_CD",
	"CAST(SubRangeDesc AS STRING) as VENDOR_SUBRANGE_DESC"
)
# Shortcut_to_VENDOR_SUBRANGE_PRE.show()
Shortcut_to_VENDOR_SUBRANGE_PRE.write.saveAsTable(f'{raw}.VENDOR_SUBRANGE_PRE', mode = 'overwrite')

# COMMAND ----------


