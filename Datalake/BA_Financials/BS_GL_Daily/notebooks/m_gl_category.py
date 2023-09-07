# Databricks notebook source
#Code converted on 2023-08-09 13:02:51
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

# Processing node ASQ_Shortcut_To_GL_TSAB_GL_CAT_PRE, type SOURCE 
# COLUMN COUNT: 2

ASQ_Shortcut_To_GL_TSAB_GL_CAT_PRE = spark.sql(f"""select a.gl_category_cd, a.description
                                               from
                                               (select trim(gl_category_cd) as gl_category_cd
                                               , trim(description) as description
                                               , rank() over (partition by gl_category_cd order by trim(description)) as rnk 
                                               from {raw}.GL_TSAB_GL_CAT_PRE) a
                                               where a.rnk=1""").withColumn("sys_row_id", monotonically_increasing_id())



# COMMAND ----------

# Processing node Shortcut_To_GL_CATEGORY, type TARGET 
# COLUMN COUNT: 2


Shortcut_To_GL_CATEGORY = ASQ_Shortcut_To_GL_TSAB_GL_CAT_PRE.selectExpr(
	"CAST(GL_CATEGORY_CD AS STRING) as GL_CATEGORY_CD",
	"CAST(DESCRIPTION AS STRING) as GL_CATEGORY_DESC"
)


# COMMAND ----------

try:
  refined_perf_table = f"{legacy}.GL_CATEGORY"
  Shortcut_To_GL_CATEGORY.createOrReplaceTempView('temp_GL_CATEGORY')
  merge_sql = f"""MERGE INTO {refined_perf_table} as target
                  USING temp_GL_CATEGORY as source
                  ON source.GL_CATEGORY_CD = target.GL_CATEGORY_CD
                  WHEN MATCHED THEN
                    UPDATE SET *
                  WHEN NOT MATCHED THEN
                    INSERT *
                  """
  spark.sql(merge_sql)
except Exception as e:
  raise e
