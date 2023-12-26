# Databricks notebook source
#Code converted on 2023-09-12 13:31:06
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
(username, password, connection_string) = IKB_ODS_Daily_prd_sqlServer(env)
db_name = "CKB_PRD"

# COMMAND ----------

# Processing node SQ_Shortcut_to_ix_flr_section, type SOURCE 
# COLUMN COUNT: 6

_sql = f"""
SELECT
    ix_flr_section.DBParentFloorplanKey,
    ix_flr_section.DBParentPlanogramKey,
    ix_flr_section.DBParentFixtureKey,
    ix_flr_section.DBKey,
    ix_flr_section.DBTime,
    ix_flr_section.DBUser
FROM {db_name}.dbo.ix_spc_planogram, {db_name}.dbo.ix_flr_floorplan, {db_name}.dbo.ix_flr_section
WHERE ix_flr_section.DBParentFloorplanKey = ix_flr_floorplan.DBKey 
  and ix_flr_section.DBParentPlanogramKey=ix_spc_planogram.DBKey
  AND ix_flr_floorplan.Desc14 = 'MERCH'
  and ix_flr_section.DBParentPlanogramKey is not null
  and ix_flr_section.DBParentFixtureKey is not null
  and ix_spc_planogram.Desc2= 'INLINE'"""

SQ_Shortcut_to_ix_flr_section = jdbcSqlServerConnection(f"({_sql}) as x",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_to_POG_FLOOR_FIXTURE_SECTION_ins, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_POG_FLOOR_FIXTURE_SECTION_ins = SQ_Shortcut_to_ix_flr_section.selectExpr(
	"CAST(DBParentFloorplanKey AS INT) as FLOORPLAN_DBKEY",
	"CAST(DBParentPlanogramKey AS INT) as POG_DBKEY",
	"CAST(DBParentFixtureKey AS INT) as FIXTURE_DBKEY",
	"CAST(DBKey AS INT) as SECTION_DBKEY",
	"CAST(DBTime AS TIMESTAMP) as DB_TIME",
	"CAST(DBUser AS STRING) as DB_USER",
	"CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_POG_FLOOR_FIXTURE_SECTION_ins.write.mode("overwrite").saveAsTable(f'{legacy}.POG_FLOOR_FIXTURE_SECTION')

# COMMAND ----------


