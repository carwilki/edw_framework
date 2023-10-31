# Databricks notebook source
#Code converted on 2023-09-26 11:24:08
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

# Read in relation source variables
(username, password, connection_string) = esdh_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_Address, type SOURCE 
# COLUMN COUNT: 3

_sql = f"""
select a.latitude,a.longitude,s.storenumber
from storelocator.dbo.store s inner join storelocator.dbo.address a 
on s.addressid=a.addressid
--order by s.storenumber
"""

SQ_Shortcut_to_Address = jdbcSqlServerConnection(f"({_sql}) as src", username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_Shortcut_to_Address = SQ_Shortcut_to_Address \
	.withColumnRenamed(SQ_Shortcut_to_Address.columns[0],'Latitude') \
	.withColumnRenamed(SQ_Shortcut_to_Address.columns[1],'Longitude') \
	.withColumnRenamed(SQ_Shortcut_to_Address.columns[2],'StoreNumber')

# COMMAND ----------

# Processing node Shortcut_to_STORE_LAT_LONG_PRE, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_STORE_LAT_LONG_PRE = SQ_Shortcut_to_Address.selectExpr(
	"CAST(StoreNumber AS DECIMAL(18,13)) as STORE_NBR",
	"CAST(Latitude AS DECIMAL(12,6)) as LAT",
	"CAST(Longitude AS DECIMAL(12,6)) as LON"
)
Shortcut_to_STORE_LAT_LONG_PRE.write.mode("overwrite").saveAsTable(f'{raw}.STORE_LAT_LONG_PRE')
