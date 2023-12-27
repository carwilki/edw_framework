# Databricks notebook source
#Code converted on 2023-09-12 13:31:03
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
(username, password, connection_string) = ckb_prd_sqlServer(env)
db_name = "CKB_PRD"


# COMMAND ----------

# Processing node SQ_ix_flr_floorplan, type SOURCE 
# COLUMN COUNT: 10

_sql = f"""SELECT
ix_flr_floorplan.DBKey,
ix_flr_floorplan.DBDateEffectiveFrom,
ix_flr_floorplan.DBDateEffectiveTo,
ix_flr_floorplan.Name,
ix_flr_floorplan.Desc2,
ix_flr_floorplan.DBVersionKey,
ix_flr_floorplan.Desc14,
ix_flr_floorplan.DBStatus,
ix_flr_floorplan.DBTime,
ix_flr_floorplan.DBUser
FROM {db_name}.dbo.ix_flr_floorplan
WHERE ix_flr_floorplan.Desc14= 'MERCH'"""

SQ_ix_flr_floorplan = jdbcSqlServerConnection(f"({_sql}) as x",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FLOORPLAN_VERSION, type TARGET 
# COLUMN COUNT: 11


FLOORPLAN_VERSION = SQ_ix_flr_floorplan.filter("DBDateEffectiveFrom is NOT null").selectExpr(			# a filter can be removed once the data is good
	"CAST(DBKey AS INT) as FLOORPLAN_DBKEY",
	"CAST(DBDateEffectiveFrom AS DATE) as FLOORPLAN_EFF_FROM_DT",
	"CAST(DBDateEffectiveTo AS DATE) as FLOORPLAN_EFF_TO_DT",
	"CAST(Name AS STRING) as FLOORPLAN_NM",
	"CAST(Desc2 AS STRING) as FLOORPLAN_LOCATION",
	"CAST(DBVersionKey AS INT) as DB_VERSION_KEY",
	"CAST(Desc14 AS STRING) as FLOORPLAN_TYPE",
	"CAST(DBStatus AS INT) as FLOORPLAN_IKB_STATUS_ID",
	"CAST(DBTime AS TIMESTAMP) as DB_TIME",
	"CAST(DBUser AS STRING) as DB_USER",
	"CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as LOAD_TSTMP"
)
FLOORPLAN_VERSION.write.mode("overwrite").saveAsTable(f'{legacy}.FLOORPLAN_VERSION')

# COMMAND ----------


