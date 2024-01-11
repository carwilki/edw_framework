# Databricks notebook source
#Code converted on 2023-09-12 13:31:02
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

# Processing node SQ_ix_flr_performance, type SOURCE 
# COLUMN COUNT: 8

_sql = f"""
SELECT DBParentFloorplanKey,
       DBKey,
       Name,
       FLOORPLAN_LOCATION,
       POG_STATUS,
       FP_STATUS,
       DBTime,
       DBUser 
  FROM (SELECT ix_flr_performance.DBParentFloorplanKey,
			   ix_spc_planogram.DBKey,
			   ix_flr_floorplan.Name,
			   ix_flr_floorplan.Desc2 AS FLOORPLAN_LOCATION,
			   ix_spc_planogram.DBStatus AS POG_STATUS,
			   ix_flr_floorplan.DBStatus AS FP_STATUS,
			   ix_flr_performance.DBTime,
			   ix_flr_performance.DBUser,
			   RANK() OVER(PARTITION BY DBParentFloorplanKey, DBParentPlanogramKey 
			   ORDER BY ix_spc_planogram.DBDateEffectiveFrom DESC) AS RNK
		  FROM {db_name}.dbo.ix_flr_performance,
			     {db_name}.dbo.ix_flr_floorplan,
			     {db_name}.dbo.ix_spc_planogram            
		 WHERE ix_flr_performance.DBParentFloorplanKey = ix_flr_floorplan.DBKey
		   AND ix_flr_performance.DBParentPlanogramKey = ix_spc_planogram.DBVersionKey
		   AND ix_flr_floorplan.Desc14 = 'MERCH'
		   AND ix_flr_floorplan.DBStatus <> 4
		   AND ix_spc_planogram.DBStatus <> 4
		   AND ix_spc_planogram.Desc2 = 'INLINE'
		   AND ix_spc_planogram.DBDateEffectiveFrom <= ix_flr_floorplan.DBDateEffectiveFrom) TEMP
 WHERE RNK = 1   
-- ORDER BY Name,
--          FP_STATUS,
--          DBParentFloorplanKey
"""

SQ_ix_flr_performance = jdbcSqlServerConnection(f"({_sql}) as x",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# Conforming fields names to the component layout
SQ_ix_flr_performance = SQ_ix_flr_performance \
	.withColumnRenamed(SQ_ix_flr_performance.columns[0],'DBParentFloorplanKey') \
	.withColumnRenamed(SQ_ix_flr_performance.columns[1],'DBParentPlanogramKey') \
	.withColumnRenamed(SQ_ix_flr_performance.columns[2],'Name') \
	.withColumnRenamed(SQ_ix_flr_performance.columns[3],'Desc12') \
	.withColumnRenamed(SQ_ix_flr_performance.columns[4],'POG_DBStatus') \
	.withColumnRenamed(SQ_ix_flr_performance.columns[5],'FP_DBStatus') \
	.withColumnRenamed(SQ_ix_flr_performance.columns[6],'DBTime') \
	.withColumnRenamed(SQ_ix_flr_performance.columns[7],'DBUser')

# COMMAND ----------

# Processing node Shortcut_to_POG_FLOOR_VERSION, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_POG_FLOOR_VERSION = SQ_ix_flr_performance.selectExpr(
	"CAST(DBParentFloorplanKey AS INT) as FLOORPLAN_DBKEY",
	"CAST(DBParentPlanogramKey AS INT) as POG_DBKEY",
	"CAST(Name AS STRING) as FLOORPLAN_NM",
	"CAST(Desc12 AS STRING) as FLOORPLAN_LOCATION",
	"CAST(POG_DBStatus AS INT) as POG_IKB_STATUS_ID",
	"CAST(FP_DBStatus AS INT) as FLOORPLAN_IKB_STATUS_ID",
	"CAST(DBTime AS TIMESTAMP) as DB_TIME",
	"CAST(DBUser AS STRING) as DB_USER",
	"CAST(CURRENT_TIMESTAMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_POG_FLOOR_VERSION.write.mode("overwrite").saveAsTable(f'{legacy}.POG_FLOOR_VERSION')

# COMMAND ----------


