# Databricks notebook source
#Code converted on 2023-08-09 13:02:58
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

# Processing node ASQ_Shortcut_To_GL_GLPCT_PLAN_PRE, type SOURCE 
# COLUMN COUNT: 4

_sql = f"""
SELECT new_pc.profit_ctr,
       COALESCE(l.location_id, 90000) as location_id,
       COALESCE(l.country_cd, 'US') as country_cd,
       CURRENT_DATE as current_date
FROM (
    SELECT DISTINCT RTRIM(p.profit_ctr) as profit_ctr
    FROM {raw}.gl_glpct_plan_pre p
    LEFT OUTER JOIN {legacy}.gl_profit_center pc ON RTRIM(p.profit_ctr) = pc.gl_profit_ctr_cd
    WHERE pc.load_dt IS NULL
) new_pc
LEFT OUTER JOIN {legacy}.site_profile l ON CAST(
    CASE 
        WHEN SUBSTR(new_pc.profit_ctr, 1, 4) = '0000' AND SUBSTR(new_pc.profit_ctr, 7, 1) <> '3' THEN SUBSTR(new_pc.profit_ctr, 7, 4)
        WHEN SUBSTR(new_pc.profit_ctr, 1, 4) = '0000' AND SUBSTR(new_pc.profit_ctr, 7, 1) = '3' THEN SUBSTR(new_pc.profit_ctr, 9, 2)
        WHEN SUBSTR(new_pc.profit_ctr, 5, 1) = '-' THEN SUBSTR(new_pc.profit_ctr, 1, 4)
    END AS NUMERIC) = l.store_nbr;
"""

ASQ_Shortcut_To_GL_GLPCT_PLAN_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_GL_GLPCT_PLAN_PRE = ASQ_Shortcut_To_GL_GLPCT_PLAN_PRE \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_PRE.columns[0],'PROFIT_CTR') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_PRE.columns[1],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_PRE.columns[2],'COUNTRY_CD') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_PRE.columns[3],'LOAD_DT')

# COMMAND ----------

# Processing node INSERT_PROFIT_CENTER, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

INSERT_PROFIT_CENTER = ASQ_Shortcut_To_GL_GLPCT_PLAN_PRE.selectExpr(
    "PROFIT_CTR as GL_PROFIT_CTR_CD",
    "'UNKNOWN' as GL_PROFIT_CTR_DESC",
    "LOCATION_ID as LOCATION_ID",
    "LOAD_DT as VALID_FROM_DT",
    "TO_DATE('12319999', 'MMddyyyy') as EXP_DT",
    "concat(RTRIM(COUNTRY_CD) , 'D' ) as CURRENCY_ID",
    "LOAD_DT as LOAD_DT").withColumn('pyspark_data_action', lit(0))


# COMMAND ----------

# Processing node Shortcut_To_GL_PROFIT_CENTER1, type TARGET 
# COLUMN COUNT: 7


Shortcut_To_GL_PROFIT_CENTER1 = INSERT_PROFIT_CENTER.selectExpr(
	"CAST(GL_PROFIT_CTR_CD AS STRING) as GL_PROFIT_CTR_CD",
	"CAST(GL_PROFIT_CTR_DESC AS STRING) as GL_PROFIT_CTR_DESC",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(VALID_FROM_DT AS TIMESTAMP) as VALID_FROM_DT",
	"CAST(EXP_DT AS TIMESTAMP) as EXP_DT",
	"CAST(CURRENCY_ID AS STRING) as CURRENCY_ID",
	"CAST(LOAD_DT AS TIMESTAMP) as LOAD_DT"
)
Shortcut_To_GL_PROFIT_CENTER1.write.mode("append").saveAsTable(f'{legacy}.GL_PROFIT_CENTER')

# COMMAND ----------


