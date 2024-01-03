# Databricks notebook source
# Code converted on 2023-08-24 09:26:48
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

(username,password,connection_string) = or_kro_read_edhp1(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SL_CAT_CODE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SL_CAT_CODE = jdbcOracleConnection(f"""select trim(cat_code) cat_code, max(cat_description) cat_desc

  from wmspadm.sl_cat_code cc

 where cc.CAT_ID=4

 group by trim(cat_code)

UNION 

select '00' cat_code, 'No Status' cat_description from dual

 order by 1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_SL_CAT_CODE = SQ_Shortcut_to_SL_CAT_CODE \
	.withColumnRenamed(SQ_Shortcut_to_SL_CAT_CODE.columns[0],'CAT_CODE') \
	.withColumnRenamed(SQ_Shortcut_to_SL_CAT_CODE.columns[1],'CAT_DESCRIPTION')

# COMMAND ----------

# Processing node Shortcut_to_SLOT_STATUS_CODES_PRE, type TARGET 
# COLUMN COUNT: 2


Shortcut_to_SLOT_STATUS_CODES_PRE = SQ_Shortcut_to_SL_CAT_CODE.selectExpr(
	"CAST(CAT_CODE AS STRING) as SL_STATUS_CD",
	"CAST(CAT_DESCRIPTION AS STRING) as SL_STATUS_DESC"
)
# overwriteDeltaPartition(Shortcut_to_SLOT_STATUS_CODES_PRE,'DC_NBR',dcnbr,f'{raw}.SLOT_STATUS_CODES_PRE')
Shortcut_to_SLOT_STATUS_CODES_PRE.write.mode("overwrite").saveAsTable(f'{raw}.SLOT_STATUS_CODES_PRE')
