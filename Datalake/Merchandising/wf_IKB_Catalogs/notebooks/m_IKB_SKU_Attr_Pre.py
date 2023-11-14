# Databricks notebook source
# Code converted on 2023-08-30 11:25:57
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

(username,password,connection_string) = ckb_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_ix_spc_product, type SOURCE 
# COLUMN COUNT: 8

SQ_ix_spc_product = jdbcSqlServerConnection(f"""(SELECT DISTINCT ix_spc_product.ID, ix_spc_performance.Capacity, ix_spc_performance.Facings, ix_spc_product.Height, ix_spc_product.Depth, ix_spc_product.Width, ix_spc_product.UOM, ix_spc_product.DateModified 

FROM

 CKB_PRD.dbo.ix_spc_product, CKB_PRD.dbo.ix_spc_performance 

WHERE

 ix_spc_performance.Facings > 0

AND 

ix_spc_product.ID < 'A' 

AND

 ix_spc_performance.DBParentProductKey = ix_spc_product.DBKey 

) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_ix_spc_product = SQ_ix_spc_product \
	.withColumnRenamed(SQ_ix_spc_product.columns[0],'ID') \
	.withColumnRenamed(SQ_ix_spc_product.columns[1],'Capacity') \
	.withColumnRenamed(SQ_ix_spc_product.columns[2],'Facings') \
	.withColumnRenamed(SQ_ix_spc_product.columns[3],'Height') \
	.withColumnRenamed(SQ_ix_spc_product.columns[4],'Depth') \
	.withColumnRenamed(SQ_ix_spc_product.columns[5],'Width') \
	.withColumnRenamed(SQ_ix_spc_product.columns[6],'UOM') \
	.withColumnRenamed(SQ_ix_spc_product.columns[7],'DateModified')

# COMMAND ----------

# Processing node EXP_Datatypes, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_ix_spc_product_temp = SQ_ix_spc_product.toDF(*["SQ_ix_spc_product___" + col for col in SQ_ix_spc_product.columns])

EXP_Datatypes = SQ_ix_spc_product_temp.selectExpr(
	"cast(SQ_ix_spc_product___ID as int) as ID",
	"ROUND(SQ_ix_spc_product___Capacity, 0) as SKU_CAPACITY_QTY",
	"ROUND(SQ_ix_spc_product___Facings, 0) as SKU_FACINGS_QTY",
	"ROUND(SQ_ix_spc_product___Height, 2) as SKU_HEIGHT_IN",
	"ROUND(SQ_ix_spc_product___Depth, 2) as SKU_DEPTH_IN",
	"ROUND(SQ_ix_spc_product___Width, 2) as SKU_WIDTH_IN",
	"SQ_ix_spc_product___UOM as UNIT_OF_MEASURE",
	"SQ_ix_spc_product___DateModified as LAST_CHNG_DT"
)

# COMMAND ----------

# Processing node AGG_Max_Change_Dt, type AGGREGATOR 
# COLUMN COUNT: 8
AGG_Max_Change_Dt = EXP_Datatypes.select("*"). \
    withColumn('rnk', row_number().over(Window.partitionBy("ID").orderBy(desc("SKU_CAPACITY_QTY"),desc("SKU_FACINGS_QTY")))).\
      withColumn('MAX_LAST_CHNG_DT',max('LAST_CHNG_DT').over(Window.partitionBy("ID"))).\
          filter("rnk=1").\
              select('ID','SKU_CAPACITY_QTY','SKU_FACINGS_QTY','SKU_HEIGHT_IN','SKU_DEPTH_IN','SKU_WIDTH_IN','UNIT_OF_MEASURE','MAX_LAST_CHNG_DT')

# AGG_Max_Change_Dt = EXP_Datatypes \
# 	.groupBy("ID") \
# 	.agg( \
# 	min(EXP_Datatypes.SKU_CAPACITY_QTY).alias('SKU_CAPACITY_QTY'),
# 	min(EXP_Datatypes.SKU_FACINGS_QTY).alias('SKU_FACINGS_QTY'),
# 	min(EXP_Datatypes.SKU_HEIGHT_IN).alias('SKU_HEIGHT_IN'),
# 	min(EXP_Datatypes.SKU_DEPTH_IN).alias('SKU_DEPTH_IN'),
# 	min(EXP_Datatypes.SKU_WIDTH_IN).alias('SKU_WIDTH_IN'),
# 	min(EXP_Datatypes.UNIT_OF_MEASURE).alias('UNIT_OF_MEASURE'),
# 	max(EXP_Datatypes.LAST_CHNG_DT).alias('MAX_LAST_CHNG_DT')
# 	) \
# 	.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_IKB_SKU_ATTR_PRE, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_IKB_SKU_ATTR_PRE = AGG_Max_Change_Dt.selectExpr(
	"CAST(ID AS INT) as SKU_NBR",
	"CAST(SKU_CAPACITY_QTY AS INT) as SKU_CAPACITY_QTY",
	"CAST(SKU_FACINGS_QTY AS INT) as SKU_FACINGS_QTY",
	"CAST(SKU_HEIGHT_IN AS DECIMAL(7,2)) as SKU_HEIGHT_IN",
	"CAST(SKU_DEPTH_IN AS DECIMAL(7,2)) as SKU_DEPTH_IN",
	"CAST(SKU_WIDTH_IN AS DECIMAL(7,2)) as SKU_WIDTH_IN",
	"CAST(UNIT_OF_MEASURE AS STRING) as UNIT_OF_MEASURE",
	"CAST(MAX_LAST_CHNG_DT AS DATE) as LAST_CHNG_DT"
)
Shortcut_to_IKB_SKU_ATTR_PRE.write.mode("overwrite").saveAsTable(f'{raw}.IKB_SKU_ATTR_PRE')

# COMMAND ----------


