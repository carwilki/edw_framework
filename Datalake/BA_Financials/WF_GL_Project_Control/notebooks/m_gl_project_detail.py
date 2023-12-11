# Databricks notebook source
#Code converted on 2023-09-08 09:28:24
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


# COMMAND ----------

# Processing node SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE, type SOURCE 
# COLUMN COUNT: 8

_sql = f"""
SELECT pre.gl_project_gid
      ,pre.plan_amt
      ,pre.budget_amt
      ,pre.actual_amt
      ,pre.commitment_amt
      ,CURRENT_DATE AS update_dt
      ,NVL (gpd.load_dt, CURRENT_DATE) AS load_dt
      ,CASE
          WHEN gpd.gl_project_gid IS NULL
             THEN 1
          ELSE 2
       END load_flag
  FROM (SELECT gp.gl_project_gid
              ,diff.plan_amt
              ,diff.budget_amt
              ,diff.actual_amt
              ,diff.commitment_amt
          FROM {raw}.gl_project_detail_diff_pre diff, {legacy}.gl_project gp
         WHERE diff.object_cd = gp.wbs_object_cd
        ) pre
   LEFT OUTER JOIN {legacy}.gl_project_detail gpd ON pre.gl_project_gid = gpd.gl_project_gid
"""

SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE = spark.sql(_sql)

# Conforming fields names to the component layout
SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE = SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE \
	.withColumnRenamed(SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.columns[0],'GL_PROJECT_GID') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.columns[1],'GL_PROJ_PLAN_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.columns[2],'GL_PROJ_BUDGET_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.columns[3],'GL_PROJ_ACTUAL_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.columns[4],'GL_PROJ_COMMITMENT_AMT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.columns[5],'UPDATE_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.columns[6],'LOAD_DT') \
	.withColumnRenamed(SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.columns[7],'LOAD_FLAG')

# COMMAND ----------

# Processing node UPD_TRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE_temp = SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.toDF(*["SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE___" + col for col in SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE.columns])

UPD_TRANS = SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE_temp.selectExpr(
	"SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE___GL_PROJECT_GID as GL_PROJECT_GID",
	"SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE___GL_PROJ_PLAN_AMT as GL_PROJ_PLAN_AMT",
	"SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE___GL_PROJ_BUDGET_AMT as GL_PROJ_BUDGET_AMT",
	"SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE___GL_PROJ_ACTUAL_AMT as GL_PROJ_ACTUAL_AMT",
	"SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE___GL_PROJ_COMMITMENT_AMT as GL_PROJ_COMMITMENT_AMT",
	"SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE___UPDATE_DT as UPDATE_DT",
	"SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE___LOAD_DT as LOAD_DT",
	"SQ_Shortcut_to_GL_PROJECT_DETAIL_DIFF_PRE___LOAD_FLAG as LOAD_FLAG") \
	.withColumn('pyspark_data_action', when((col("LOAD_FLAG") == lit(1)), (lit(0))).otherwise(lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_GL_PROJECT_DETAIL1, type TARGET 
# COLUMN COUNT: 7


Shortcut_to_GL_PROJECT_DETAIL1 = UPD_TRANS.selectExpr(
	"CAST(GL_PROJECT_GID AS BIGINT) as GL_PROJECT_GID",
	"CAST(GL_PROJ_PLAN_AMT AS DECIMAL(15,2)) as GL_PROJ_PLAN_AMT",
	"CAST(GL_PROJ_BUDGET_AMT AS DECIMAL(15,2)) as GL_PROJ_BUDGET_AMT",
	"CAST(GL_PROJ_ACTUAL_AMT AS DECIMAL(15,2)) as GL_PROJ_ACTUAL_AMT",
	"CAST(GL_PROJ_COMMITMENT_AMT AS DECIMAL(15,2)) as GL_PROJ_COMMITMENT_AMT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.GL_PROJECT_GID = target.GL_PROJECT_GID"""
	refined_perf_table = f"{legacy}.GL_PROJECT_DETAIL"
	executeMerge(Shortcut_to_GL_PROJECT_DETAIL1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("GL_PROJECT_DETAIL", "GL_PROJECT_DETAIL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("GL_PROJECT_DETAIL", "GL_PROJECT_DETAIL","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		

# COMMAND ----------


