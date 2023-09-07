# Databricks notebook source
#Code converted on 2023-08-09 13:02:52
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

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")



if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node ASQ_Shortcut_To_GL_SKAT_PRE, type SOURCE 
# COLUMN COUNT: 2

_sql = f"""
SELECT gl_acct_nbr
    ,acct_grp_cd as GL_ACCT_GRP_CD
FROM (
    SELECT s.gl_acct_nbr
        ,TRIM(x.acct_grp_cd) acct_grp_cd
        ,RANK() OVER (
            PARTITION BY s.gl_acct_nbr ORDER BY TRIM(x.acct_grp_cd)
            ) AS RNK
    FROM {raw}.GL_SKAT_PRE s
        ,{raw}.GL_T077S_PRE x
    WHERE s.chart_of_accts_cd = 'PCOA'
        AND s.chart_of_accts_cd = x.chart_of_accts_cd
        AND s.gl_acct_nbr BETWEEN x.from_gl_nbr
            AND x.to_gl_nbr
    ) X
WHERE RNK = 1
"""

ASQ_Shortcut_To_GL_SKAT_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node Shortcut_To_GL_ACCOUNT, type TARGET 
# COLUMN COUNT: 2


Shortcut_To_GL_ACCOUNT = ASQ_Shortcut_To_GL_SKAT_PRE.selectExpr(
	"CAST(GL_ACCT_NBR AS INT) as GL_ACCT_NBR",
	"CAST(GL_ACCT_GRP_CD AS STRING) as GL_ACCT_GRP_CD"
)


# COMMAND ----------

try:
  refined_perf_table = f"{legacy}.GL_ACCOUNT"
  Shortcut_To_GL_ACCOUNT.createOrReplaceTempView('temp_GL_ACCOUNT')
  merge_sql = f"""MERGE INTO {refined_perf_table} as target
                  USING temp_GL_ACCOUNT as source
                  ON source.GL_ACCT_NBR = target.GL_ACCT_NBR
                  WHEN MATCHED THEN
                    UPDATE SET *
                  WHEN NOT MATCHED THEN
                    INSERT *
                  """
  spark.sql(merge_sql)
except Exception as e:
  raise e
