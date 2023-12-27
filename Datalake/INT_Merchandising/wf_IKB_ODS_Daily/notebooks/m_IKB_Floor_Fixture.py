# Databricks notebook source
#Code converted on 2023-09-12 13:31:05
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

# Processing node SQ_ix_flr_fixture, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""
SELECT ix_flr_fixture.DBParentFloorplanKey, ix_flr_fixture.DBKey
FROM {db_name}.dbo.ix_flr_fixture, {db_name}.dbo.ix_flr_floorplan
WHERE ix_flr_floorplan.DBKey=ix_flr_fixture.DBParentFloorplanKey
  AND ix_flr_floorplan.Desc14 = 'MERCH'
"""

SQ_ix_flr_fixture = jdbcSqlServerConnection(f"({_sql}) as x",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_FLOOR_FIXTURE_ins, type TARGET 
# COLUMN COUNT: 3


Shortcut_to_FLOOR_FIXTURE_ins = SQ_ix_flr_fixture.selectExpr(
	"CAST(DBParentFloorplanKey AS INT) as FLOORPLAN_DBKEY",
	"CAST(DBKey AS INT) as FIXTURE_DBKEY",
	"CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_FLOOR_FIXTURE_ins.write.mode("overwrite").saveAsTable(f'{legacy}.FLOOR_FIXTURE')

# COMMAND ----------


