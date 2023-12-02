# Databricks notebook source
#Code converted on 2023-09-19 11:15:42
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

# Processing node SQ_Shortcut_to_UDH_ONLINE_ROLE_LKUP, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""
SELECT
ONLINE_CATEGORY_ROLE_ID,
ONLINE_CATEGORY_ROLE_DESC
FROM userdatafeed.dbo.UDH_ONLINE_ROLE_LKUP
"""

EXP_PASS_THRU = jdbcSqlServerConnection(f"({_sql}) as src", username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node Shortcut_to_UDH_ONLINE_ROLE_LKUP1, type TARGET 
# COLUMN COUNT: 2


Shortcut_to_UDH_ONLINE_ROLE_LKUP1 = EXP_PASS_THRU.selectExpr(
	"CAST(ONLINE_CATEGORY_ROLE_ID AS INT) as ONLINE_CATEGORY_ROLE_ID",
	"CAST(ONLINE_CATEGORY_ROLE_DESC AS STRING) as ONLINE_CATEGORY_ROLE_DESC"
)
Shortcut_to_UDH_ONLINE_ROLE_LKUP1.write.mode("overwrite").saveAsTable(f'{legacy}.UDH_ONLINE_ROLE_LKUP')
