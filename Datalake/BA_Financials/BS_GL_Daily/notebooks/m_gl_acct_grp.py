# Databricks notebook source
#Code converted on 2023-08-09 13:02:54
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

# Processing node ASQ_Shortcut_To_GL_T077S_PRE, type SOURCE 
# COLUMN COUNT: 4

_sql = f"""
SELECT  TRIM(acct_grp_cd) AS ACCOUNT_GRP_CD,
        TRIM(acct_grp_type_cd) AS ACCT_GRP_TYPE_CD,
        CASE WHEN TRIM(acct_grp_cd) IN ('CASH','CARD','INVS','RECV','INV','PREP','FA','OTHA','LIAB','EQ') THEN 'X'
             ELSE ' '
        END AS GL_BAL_SHEET_IND ,
        CASE WHEN TRIM(acct_grp_cd) IN ('REV','COGS','OPEX','OINC','OEXP','CLER') THEN 'X'
        ELSE ' '
        END AS GL_PL_IND
FROM {raw}.gl_t077s_pre
WHERE  chart_of_accts_cd = 'PCOA'
"""

ASQ_Shortcut_To_GL_T077S_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())


# COMMAND ----------

# Processing node Shortcut_To_GL_ACCT_GRP, type TARGET 
# COLUMN COUNT: 4


Shortcut_To_GL_ACCT_GRP = ASQ_Shortcut_To_GL_T077S_PRE.selectExpr(
	"CAST(ACCOUNT_GRP_CD AS STRING) as GL_ACCT_GRP_CD",
	"CAST(ACCT_GRP_TYPE_CD AS STRING) as GL_ACCT_GRP_DESC",
	"CAST(GL_BAL_SHEET_IND AS STRING) as GL_BAL_SHEET_IND",
	"CAST(GL_PL_IND AS STRING) as GL_PL_IND"
)

# COMMAND ----------

try:
  refined_perf_table = f"{legacy}.GL_ACCT_GRP"
  Shortcut_To_GL_ACCT_GRP.createOrReplaceTempView('temp_GL_ACCT_GRP')
  merge_sql = f"""MERGE INTO {refined_perf_table} as target
                  USING temp_GL_ACCT_GRP as source
                  ON source.GL_ACCT_GRP_CD = target.GL_ACCT_GRP_CD
                  WHEN MATCHED THEN
                    UPDATE SET *
                  WHEN NOT MATCHED THEN
                    INSERT *
                  """
  spark.sql(merge_sql)
except Exception as e:
  raise e
