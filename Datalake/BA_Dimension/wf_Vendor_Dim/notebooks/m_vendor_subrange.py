# Databricks notebook source
#Code converted on 2023-09-25 15:41:21
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
# Comment

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

# Processing node LKP_VENDOR_PROFILE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_VENDOR_PROFILE_SRC = spark.sql(f"""SELECT
VENDOR_ID,
VENDOR_TYPE_ID,
VENDOR_NBR
FROM {legacy}.VENDOR_PROFILE""")
# Conforming fields names to the component layout
# LKP_VENDOR_PROFILE_SRC = LKP_VENDOR_PROFILE_SRC \
# 	.withColumnRenamed(LKP_VENDOR_PROFILE_SRC.columns[0],'')

# COMMAND ----------

# LKP_VENDOR_PROFILE_SRC.show()

# COMMAND ----------

# Processing node LKP_VENDOR_SUBRANGE1_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

LKP_VENDOR_SUBRANGE1_SRC = spark.sql(f"""SELECT VENDOR_SUBRANGE.VENDOR_ID as VENDOR_ID, Trim(VENDOR_SUBRANGE.VENDOR_SUBRANGE_CD) as VENDOR_SUBRANGE_CD FROM {legacy}.VENDOR_SUBRANGE""")
# Conforming fields names to the component layout
# LKP_VENDOR_SUBRANGE1_SRC = LKP_VENDOR_SUBRANGE1_SRC \
# 	.withColumnRenamed(LKP_VENDOR_SUBRANGE1_SRC.columns[0],'')

# COMMAND ----------

# LKP_VENDOR_SUBRANGE1_SRC.show()

# COMMAND ----------

# Processing node SQ_Shortcut_to_VENDOR_SUBRANGE_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_VENDOR_SUBRANGE_PRE = spark.sql(f"""SELECT
VENDOR_NBR,
VENDOR_TYPE_ID,
VENDOR_SUBRANGE_CD,
VENDOR_SUBRANGE_DESC
FROM {raw}.VENDOR_SUBRANGE_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_VENDOR_SUBRANGE_PRE = SQ_Shortcut_to_VENDOR_SUBRANGE_PRE \
	.withColumnRenamed(SQ_Shortcut_to_VENDOR_SUBRANGE_PRE.columns[0],'VENDOR_NBR') \
	.withColumnRenamed(SQ_Shortcut_to_VENDOR_SUBRANGE_PRE.columns[1],'VENDOR_TYPE_ID') \
	.withColumnRenamed(SQ_Shortcut_to_VENDOR_SUBRANGE_PRE.columns[2],'VENDOR_SUBRANGE_CD') \
	.withColumnRenamed(SQ_Shortcut_to_VENDOR_SUBRANGE_PRE.columns[3],'VENDOR_SUBRANGE_DESC')

# COMMAND ----------

# Processing node EXP_Subrange, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_VENDOR_SUBRANGE_PRE_temp = SQ_Shortcut_to_VENDOR_SUBRANGE_PRE.toDF(*["SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___" + col for col in SQ_Shortcut_to_VENDOR_SUBRANGE_PRE.columns])

EXP_Subrange = SQ_Shortcut_to_VENDOR_SUBRANGE_PRE_temp.selectExpr(
	"SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___VENDOR_SUBRANGE_CD as SubRangeCD",
	"SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___VENDOR_NBR as VendorNbr",
	"SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___VENDOR_SUBRANGE_DESC as SubRangeDesc",
	"SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___VENDOR_TYPE_ID as VENDOR_TYPE_ID",
    "SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___sys_row_id as sys_row_id")
# .selectExpr(
# 	"SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___sys_row_id as sys_row_id",
# 	"SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___SubRangeCD as SubRangeCD",
# 	"SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___VendorNbr as VendorNbr",
# 	"SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___SubRangeDesc as SubRangeDesc",
# 	"SQ_Shortcut_to_VENDOR_SUBRANGE_PRE___VENDOR_TYPE_ID as VENDOR_TYPE_ID"
# )

# COMMAND ----------

# EXP_Subrange.show()

# COMMAND ----------

# Processing node LKP_VENDOR_PROFILE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# ,"sys_row_id as sys_row_id_vp"
LKP_VENDOR_PROFILE_lookup_result = EXP_Subrange.selectExpr(
	"VendorNbr as in_VendorNbr",
	"VENDOR_TYPE_ID as in_Vendor_Type_Id","sys_row_id as sys_row_id").join(LKP_VENDOR_PROFILE_SRC, (col('VENDOR_TYPE_ID') == col('in_Vendor_Type_Id')) & (col('VENDOR_NBR') == col('in_VendorNbr')), 'left') \
.withColumn('row_num_VENDOR_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("VENDOR_ID")))

LKP_VENDOR_PROFILE = LKP_VENDOR_PROFILE_lookup_result.filter(col("row_num_VENDOR_ID") == 1).select(
	LKP_VENDOR_PROFILE_lookup_result.sys_row_id,
	col('VENDOR_ID')
)

# COMMAND ----------

# LKP_VENDOR_PROFILE.show()

# COMMAND ----------

# Processing node EXP_Final, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
LKP_VENDOR_PROFILE_temp = LKP_VENDOR_PROFILE.toDF(*["LKP_VENDOR_PROFILE___" + col for col in LKP_VENDOR_PROFILE.columns])
EXP_Subrange_temp = EXP_Subrange.toDF(*["EXP_Subrange___" + col for col in EXP_Subrange.columns])

# Joining dataframes EXP_Subrange, LKP_VENDOR_PROFILE to form EXP_Final
EXP_Final_joined = EXP_Subrange_temp.join(LKP_VENDOR_PROFILE_temp, col('EXP_Subrange___sys_row_id') == col('LKP_VENDOR_PROFILE___sys_row_id'), 'inner')
EXP_Final = EXP_Final_joined.selectExpr(
	"LKP_VENDOR_PROFILE___VENDOR_ID as VENDOR_ID",
	"EXP_Subrange___SubRangeCD as i_SubRangeCD",
	"EXP_Subrange___SubRangeDesc as SubRangeDesc",
 	"EXP_Subrange___sys_row_id as sys_row_id",
 ).selectExpr(
	"sys_row_id as sys_row_id",
	"VENDOR_ID as VENDOR_ID",
	"ltrim ( rtrim ( i_SubRangeCD ) ) as lkp_SubRangeCD1",
	"IF (i_SubRangeCD IS NULL, ' ', i_SubRangeCD) as SubRangeCD",
	"SubRangeDesc as SubRangeDesc"
)

# COMMAND ----------

# Processing node LKP_VENDOR_SUBRANGE1, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4


LKP_VENDOR_SUBRANGE1_lookup_result = EXP_Final.selectExpr(
	"VENDOR_ID as VENDOR_ID1",
	"lkp_SubRangeCD1 as SubRangeCD","sys_row_id as sys_row_id").join(LKP_VENDOR_SUBRANGE1_SRC, (col('VENDOR_ID') == col('VENDOR_ID1')) & (col('VENDOR_SUBRANGE_CD') == col('SubRangeCD')), 'left') \
.withColumn('row_num_VENDOR_ID', row_number().over(Window.partitionBy("sys_row_id").orderBy("VENDOR_ID")))

LKP_VENDOR_SUBRANGE1 = LKP_VENDOR_SUBRANGE1_lookup_result.filter(col("row_num_VENDOR_ID") == 1).select(
	LKP_VENDOR_SUBRANGE1_lookup_result.sys_row_id,
	col('VENDOR_ID')
)

# COMMAND ----------

# Processing node EXP_UPD, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
EXP_Final_temp = EXP_Final.toDF(*["EXP_Final___" + col for col in EXP_Final.columns])
LKP_VENDOR_SUBRANGE1_temp = LKP_VENDOR_SUBRANGE1.toDF(*["LKP_VENDOR_SUBRANGE1___" + col for col in LKP_VENDOR_SUBRANGE1.columns])

# Joining dataframes EXP_Final, LKP_VENDOR_SUBRANGE1 to form EXP_UPD
EXP_UPD_joined = EXP_Final_temp.join(LKP_VENDOR_SUBRANGE1_temp, col('EXP_Final___sys_row_id') == col('LKP_VENDOR_SUBRANGE1___sys_row_id'), 'inner')
EXP_UPD = EXP_UPD_joined.selectExpr(
	"LKP_VENDOR_SUBRANGE1___VENDOR_ID as VENDOR_ID1",
	"EXP_Final___VENDOR_ID as VENDOR_ID",
	"EXP_Final___SubRangeCD as SubRangeCD",
	"EXP_Final___SubRangeDesc as SubRangeDesc",
	"EXP_Final___sys_row_id as sys_row_id",
 ).selectExpr(
	"sys_row_id as sys_row_id",
	"VENDOR_ID1 as VENDOR_ID1",
	"VENDOR_ID as VENDOR_ID",
	"SubRangeCD as SubRangeCD",
	"SubRangeDesc as SubRangeDesc"
)

# COMMAND ----------

# Processing node UPD_UPDATE, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
EXP_UPD_temp = EXP_UPD.toDF(*["EXP_UPD___" + col for col in EXP_UPD.columns])

UPD_UPDATE = EXP_UPD_temp.selectExpr(
	"EXP_UPD___VENDOR_ID as VENDOR_ID3",
	"EXP_UPD___SubRangeCD as SubRangeCd3",
	"EXP_UPD___SubRangeDesc as SubRangeDesc3",
	"EXP_UPD___VENDOR_ID1 as VENDOR_ID") \
	.withColumn('pyspark_data_action', when((col('VENDOR_ID').isNull()) ,(lit(0))) .otherwise(lit(1)))

# COMMAND ----------

# Processing node UPD_VENDOR_SUBRANGE, type TARGET 
# COLUMN COUNT: 3


UPD_VENDOR_SUBRANGE = UPD_UPDATE.selectExpr(
	"CAST(VENDOR_ID3 as bigint) as VENDOR_ID",
	"CAST(SubRangeCd3 AS CHAR(10)) as VENDOR_SUBRANGE_CD",
	"CAST(SubRangeDesc3 AS CHAR(10)) as VENDOR_SUBRANGE_DESC",
	"pyspark_data_action as pyspark_data_action"
)
# UPD_VENDOR_SUBRANGE.write.saveAsTable(f'{refine}.VENDOR_SUBRANGE', mode = 'append')
# UPD_VENDOR_SUBRANGE.show()
try:
  primary_key = """source.VENDOR_ID = target.VENDOR_ID AND source.VENDOR_SUBRANGE_CD = target.VENDOR_SUBRANGE_CD"""
  refined_perf_table = f"{legacy}.VENDOR_SUBRANGE"
  executeMerge(UPD_VENDOR_SUBRANGE, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("VENDOR_SUBRANGE", "VENDOR_SUBRANGE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("VENDOR_SUBRANGE", "VENDOR_SUBRANGE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	

# COMMAND ----------

# UPD_VENDOR_SUBRANGE.show()

# COMMAND ----------


