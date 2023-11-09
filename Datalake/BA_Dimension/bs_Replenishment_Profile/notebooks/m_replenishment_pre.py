# Databricks notebook source
#Code converted on 2023-09-26 15:05:18
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

# currdate = starttime.strftime('%Y%m%d')

# dbutils.widgets.text(name = 'file_path', defaultValue = 'gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/replenishment_profile/')
# file_path = dbutils.widgets.get('file_path') # + f'/{currdate}'

# source_bucket = dbutils.widgets.get('source_bucket')


file_path=getParameterValue(raw,'BA_Dimension_Parameter.prm','BA_Dimension.WF:bs_Replenishment_Profile.M:m_replenishment_pre','source_bucket')
source_file = get_src_file('rep', file_path)

#source_file='gs://petm-bdpl-prod-raw-p1-gcs-gbl/sap/replenishment_profile/20231028/rep20231027_172055.dat'

# COMMAND ----------

# Processing node SQ_Shortcut_To_REPLENISHMENT_FILE, type SOURCE 
# COLUMN COUNT: 9

fixed_width_data = spark.read.text(source_file)
columns = [
	expr('substring(value, 1, 1)').alias('DELETE_IND'),
	expr('substring(value, 2, 18)').alias('SKU_NBR'),
	expr('substring(value, 20, 4)').alias('STORE_NBR'),
	expr('substring(value, 39, 9)').alias('SAFETY_QTY'),
	expr('substring(value, 48, 4)').alias('SERVICE_LVL_RT'),
	expr('substring(value, 62, 9)').alias('REORDER_POINT_QTY'),
	expr('substring(value, 72, 3)').alias('PLAN_DELIV_DAYS'),
	expr('substring(value, 109, 9)').alias('TARGET_STOCK_QTY'),
	expr('substring(value, 121, 9)').alias('PRESENT_QTY')
]


SQ_Shortcut_To_REPLENISHMENT_FILE = fixed_width_data.select(*columns).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_To_REPLENISHMENT_FILE = SQ_Shortcut_To_REPLENISHMENT_FILE \
	.withColumnRenamed(SQ_Shortcut_To_REPLENISHMENT_FILE.columns[0],'DELETE_IND') \
	.withColumnRenamed(SQ_Shortcut_To_REPLENISHMENT_FILE.columns[1],'SKU_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_REPLENISHMENT_FILE.columns[2],'STORE_NBR') \
	.withColumnRenamed(SQ_Shortcut_To_REPLENISHMENT_FILE.columns[3],'SAFETY_QTY') \
	.withColumnRenamed(SQ_Shortcut_To_REPLENISHMENT_FILE.columns[4],'SERVICE_LVL_RT') \
	.withColumnRenamed(SQ_Shortcut_To_REPLENISHMENT_FILE.columns[5],'REORDER_POINT_QTY') \
	.withColumnRenamed(SQ_Shortcut_To_REPLENISHMENT_FILE.columns[6],'PLAN_DELIV_DAYS') \
	.withColumnRenamed(SQ_Shortcut_To_REPLENISHMENT_FILE.columns[7],'TARGET_STOCK_QTY') \
	.withColumnRenamed(SQ_Shortcut_To_REPLENISHMENT_FILE.columns[8],'PRESENT_QTY')\
    .withColumn("o_CURRENT_DATE" , expr("DATE_TRUNC('dd',current_timestamp())"))

# SQ_Shortcut_To_REPLENISHMENT_FILE.show()


# COMMAND ----------

# Processing node Shortcut_To_EXP_COMMON_DATE_TRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9

# for each involved DataFrame, append the dataframe name to each column
# MMDDYYYY_temp = MMDDYYYY.toDF(*["MMDDYYYY___" + col for col in MMDDYYYY.columns])
# YYYYMMDD_temp = YYYYMMDD.toDF(*["YYYYMMDD___" + col for col in YYYYMMDD.columns])
# DD_temp = DD.toDF(*["DD___" + col for col in DD.columns])
# 31_temp = 31.toDF(*["31___" + col for col in 31.columns])
# SQ_Shortcut_To_REPLENISHMENT_FILE_temp = SQ_Shortcut_To_REPLENISHMENT_FILE.toDF(*["SQ_Shortcut_To_REPLENISHMENT_FILE___" + col for col in SQ_Shortcut_To_REPLENISHMENT_FILE.columns])

# Shortcut_To_EXP_COMMON_DATE_TRANS = SQ_Shortcut_To_REPLENISHMENT_FILE_temp.selectExpr(
# 	"SQ_Shortcut_To_REPLENISHMENT_FILE___DELETE_IND as i_DAY_ONLY") \
# 	.withColumn('i_TIME_ONLY', NULL) \
# 	.selectExpr(
# 	"SQ_Shortcut_To_REPLENISHMENT_FILE___sys_row_id as sys_row_id",
# 	"IF (SQ_Shortcut_To_REPLENISHMENT_FILE___i_DAY_ONLY = '00000000', null, TO_DATE ( SQ_Shortcut_To_REPLENISHMENT_FILE___i_DAY_ONLY , 'MMDDYYYY' )) as o_MMDDYYYY_W_DEFAULT_TIME",
# 	"IF (SQ_Shortcut_To_REPLENISHMENT_FILE___i_DAY_ONLY = '00000000', null, TO_DATE ( SQ_Shortcut_To_REPLENISHMENT_FILE___i_DAY_ONLY , 'yyyyMMdd' )) as o_YYYYMMDD_W_DEFAULT_TIME",
# 	"TO_DATE ( (concat( '9999-12-31.' , SQ_Shortcut_To_REPLENISHMENT_FILE___i_TIME_ONLY )) , 'YYYY-MM-DD___HH24MISS' ) as o_TIME_W_DEFAULT_DATE",
# 	"IF (SQ_Shortcut_To_REPLENISHMENT_FILE___i_DAY_ONLY = '00000000', null, TO_DATE ( (concat( SQ_Shortcut_To_REPLENISHMENT_FILE___i_DAY_ONLY , '.' , SQ_Shortcut_To_REPLENISHMENT_FILE___i_TIME_ONLY )) , 'MMDDYYYY___HH24:MI:SS' )) as o_MMDDYYYY_W_TIME",
# 	"IF (SQ_Shortcut_To_REPLENISHMENT_FILE___i_DAY_ONLY = '00000000', null, TO_DATE ( (concat( SQ_Shortcut_To_REPLENISHMENT_FILE___i_DAY_ONLY , '.' , SQ_Shortcut_To_REPLENISHMENT_FILE___i_TIME_ONLY )) , 'YYYYMMDD___HH24:MI:SS' )) as o_YYYYMMDD_W_TIME",
# 	"TRUNC ( (CURRENT_TIMESTAMP) ) as o_CURRENT_DATE",
# 	"DATE_ADD(- 1, SQ_Shortcut_To_REPLENISHMENT_FILE___v_CURRENT_DATE) as o_CURRENT_DATE_MINUS1",
# 	"TO_DATE ( '0001-01-01' , 'yyyy-MM-dd' ) as o_DEFAULT_EFF_DATE",
# 	"TO_DATE ( '9999-12-31' , 'yyyy-MM-dd' ) as o_DEFAULT_END_DATE"
# ).withColumn("v_CURRENT_DATE", TRUNC ( (CURRENT_TIMESTAMP) ))

# COMMAND ----------

# Processing node Shortcut_To_REPLENISHMENT_PRE, type TARGET 
# COLUMN COUNT: 11

# Joining dataframes SQ_Shortcut_To_REPLENISHMENT_FILE, Shortcut_To_EXP_COMMON_DATE_TRANS to form Shortcut_To_REPLENISHMENT_PRE
# Shortcut_To_REPLENISHMENT_PRE_joined = SQ_Shortcut_To_REPLENISHMENT_FILE.join(Shortcut_To_EXP_COMMON_DATE_TRANS, SQ_Shortcut_To_REPLENISHMENT_FILE.sys_row_id == Shortcut_To_EXP_COMMON_DATE_TRANS.sys_row_id, 'inner')

Shortcut_To_REPLENISHMENT_PRE = SQ_Shortcut_To_REPLENISHMENT_FILE.selectExpr(
	"CAST(SKU_NBR as int) as SKU_NBR",
	"CAST(STORE_NBR as int) as STORE_NBR",
	"CAST(DELETE_IND AS STRING) as DELETE_IND",
	"CAST(SAFETY_QTY as int) as SAFETY_QTY",
	"CAST(SERVICE_LVL_RT AS DECIMAL(3,1)) as SERVICE_LVL_RT",
	"CAST(REORDER_POINT_QTY as int) as REORDER_POINT_QTY",
	"CAST(PLAN_DELIV_DAYS as smallint) as PLAN_DELIV_DAYS",
	"CAST(TARGET_STOCK_QTY as int) as TARGET_STOCK_QTY",
	"CAST(PRESENT_QTY as int) as PRESENT_QTY",
	"CAST(NULL as int) as PROMO_QTY",
	"CAST(o_CURRENT_DATE AS TIMESTAMP) as LOAD_DT"
)
# Shortcut_To_REPLENISHMENT_PRE.show()
Shortcut_To_REPLENISHMENT_PRE.write.saveAsTable(f'{raw}.REPLENISHMENT_PRE', mode = 'overwrite')

# COMMAND ----------


