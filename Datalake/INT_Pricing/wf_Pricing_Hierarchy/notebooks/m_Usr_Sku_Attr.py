# Databricks notebook source
#Code converted on 2023-09-19 11:15:43
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

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name='env', defaultValue='dev')
env = dbutils.widgets.get('env')

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

(username, password, connection_string) = UserDataFeed_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_USR_SKU_ATTR, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""
SELECT
SKU_NBR,
ATTR_VALUE
FROM userdatafeed.dbo.USR_SKU_ATTR"""

SQ_Shortcut_to_USR_SKU_ATTR = jdbcSqlServerConnection(f"({_sql}) as src", username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE_RPT, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE_RPT = spark.sql(f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE_RPT""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_SKU_NBR, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_USR_SKU_ATTR_temp = SQ_Shortcut_to_USR_SKU_ATTR.toDF(*["SQ_Shortcut_to_USR_SKU_ATTR___" + col for col in SQ_Shortcut_to_USR_SKU_ATTR.columns])
SQ_Shortcut_to_SKU_PROFILE_RPT_temp = SQ_Shortcut_to_SKU_PROFILE_RPT.toDF(*["SQ_Shortcut_to_SKU_PROFILE_RPT___" + col for col in SQ_Shortcut_to_SKU_PROFILE_RPT.columns])

JNR_SKU_NBR = SQ_Shortcut_to_USR_SKU_ATTR_temp.join(SQ_Shortcut_to_SKU_PROFILE_RPT_temp,[SQ_Shortcut_to_USR_SKU_ATTR_temp.SQ_Shortcut_to_USR_SKU_ATTR___SKU_NBR == SQ_Shortcut_to_SKU_PROFILE_RPT_temp.SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_NBR],'inner').selectExpr(
	"SQ_Shortcut_to_SKU_PROFILE_RPT___PRODUCT_ID as PRODUCT_ID",
	"SQ_Shortcut_to_SKU_PROFILE_RPT___SKU_NBR as SKU_NBR",
	"SQ_Shortcut_to_USR_SKU_ATTR___SKU_NBR as SKU_NBR_SKU_PROF",
	"SQ_Shortcut_to_USR_SKU_ATTR___ATTR_VALUE as ATTR_VALUE")

# COMMAND ----------

# Processing node EXP_VALIDATION, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
JNR_SKU_NBR_temp = JNR_SKU_NBR.toDF(*["JNR_SKU_NBR___" + col for col in JNR_SKU_NBR.columns])

EXP_VALIDATION = JNR_SKU_NBR_temp.selectExpr(
	# "JNR_SKU_NBR___sys_row_id as sys_row_id",
	"JNR_SKU_NBR___PRODUCT_ID as PRODUCT_ID",
	"JNR_SKU_NBR___SKU_NBR as SKU_NBR",
	"IF (JNR_SKU_NBR___ATTR_VALUE = 'KVI', '1', '0') as KVI_FLAG",
	"IF (JNR_SKU_NBR___ATTR_VALUE = 'KOI', '1', '0') as KOI_FLAG",
	"CURRENT_TIMESTAMP () as LOAD_TSTMP"
)

# COMMAND ----------

# Processing node AGG_SKU_NBR, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

AGG_SKU_NBR = EXP_VALIDATION.selectExpr(
	"EXP_VALIDATION.PRODUCT_ID as PRODUCT_ID",
	"EXP_VALIDATION.SKU_NBR as SKU_NBR",
	"EXP_VALIDATION.KVI_FLAG as i_KVI_FLAG",
	"EXP_VALIDATION.KOI_FLAG as i_KOI_FLAG",
	"EXP_VALIDATION.LOAD_TSTMP as LOAD_TSTMP") \
	.groupBy("PRODUCT_ID","SKU_NBR", "LOAD_TSTMP") \
	.agg( 
        max(col('i_KVI_FLAG')).alias("KVI_FLAG"),
        max(col('i_KOI_FLAG')).alias("KOI_FLAG")
	) 

# COMMAND ----------

# Processing node Shortcut_to_USR_SKU_ATTR_1, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_USR_SKU_ATTR_1 = AGG_SKU_NBR.selectExpr(
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(SKU_NBR AS INT) as SKU_NBR",
	"KVI_FLAG",
	"KOI_FLAG",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)

Shortcut_to_USR_SKU_ATTR_1.write.mode("overwrite").saveAsTable(f'{legacy}.USR_SKU_ATTR')

#########################################{legacy}.USR_SKU_ATTR' DDL missing
