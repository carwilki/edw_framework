# Databricks notebook source
#Code converted on 2023-08-09 13:03:01
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

# Processing node ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE, type SOURCE 
# COLUMN COUNT: 8

_sql = f"""
SELECT 
    profit_ctr,
    fiscal_mo,
    gl_category_cd,
    gl_acct_nbr,
    p.location_id,
    p.currency_id,
    SUM(
        CASE
            WHEN m.fiscal_mo_nbr = 1 THEN loc1_comp_amt
            WHEN m.fiscal_mo_nbr = 2 THEN loc2_comp_amt
            WHEN m.fiscal_mo_nbr = 3 THEN loc3_comp_amt
            WHEN m.fiscal_mo_nbr = 4 THEN loc4_comp_amt
            WHEN m.fiscal_mo_nbr = 5 THEN loc5_comp_amt
            WHEN m.fiscal_mo_nbr = 6 THEN loc6_comp_amt
            WHEN m.fiscal_mo_nbr = 7 THEN loc7_comp_amt
            WHEN m.fiscal_mo_nbr = 8 THEN loc8_comp_amt
            WHEN m.fiscal_mo_nbr = 9 THEN loc9_comp_amt
            WHEN m.fiscal_mo_nbr = 10 THEN loc10_comp_amt
            WHEN m.fiscal_mo_nbr = 11 THEN loc11_comp_amt
            WHEN m.fiscal_mo_nbr = 12 THEN loc12_comp_amt
            ELSE 0
        END
    ) AS US_PLAN_AMT,
    SUM(
        CASE
            WHEN m.fiscal_mo_nbr = 1 THEN per1_comp_amt
            WHEN m.fiscal_mo_nbr = 2 THEN per2_comp_amt
            WHEN m.fiscal_mo_nbr = 3 THEN per3_comp_amt
            WHEN m.fiscal_mo_nbr = 4 THEN per4_comp_amt
            WHEN m.fiscal_mo_nbr = 5 THEN per5_comp_amt
            WHEN m.fiscal_mo_nbr = 6 THEN per6_comp_amt
            WHEN m.fiscal_mo_nbr = 7 THEN per7_comp_amt
            WHEN m.fiscal_mo_nbr = 8 THEN per8_comp_amt
            WHEN m.fiscal_mo_nbr = 9 THEN per9_comp_amt
            WHEN m.fiscal_mo_nbr = 10 THEN per10_comp_amt
            WHEN m.fiscal_mo_nbr = 11 THEN per11_comp_amt
            WHEN m.fiscal_mo_nbr = 12 THEN per12_comp_amt
            ELSE 0
        END
    ) AS LOC_PLAN_AMT
FROM {raw}.gl_glpct_plan_pre GL JOIN enterprise.months M ON gl.fiscal_yr = m.fiscal_yr
                                JOIN {legacy}.gl_profit_center P ON RTRIM(gl.profit_ctr) = p.gl_profit_ctr_cd
WHERE gl.fiscal_yr > 2002
GROUP BY 
    profit_ctr,
    fiscal_mo,
    gl_category_cd,
    gl_acct_nbr,
    p.location_id,
    p.currency_id
HAVING 
    SUM(
        CASE
            WHEN m.fiscal_mo_nbr = 1 THEN loc1_comp_amt
            WHEN m.fiscal_mo_nbr = 2 THEN loc2_comp_amt
            WHEN m.fiscal_mo_nbr = 3 THEN loc3_comp_amt
            WHEN m.fiscal_mo_nbr = 4 THEN loc4_comp_amt
            WHEN m.fiscal_mo_nbr = 5 THEN loc5_comp_amt
            WHEN m.fiscal_mo_nbr = 6 THEN loc6_comp_amt
            WHEN m.fiscal_mo_nbr = 7 THEN loc7_comp_amt
            WHEN m.fiscal_mo_nbr = 8 THEN loc8_comp_amt
            WHEN m.fiscal_mo_nbr = 9 THEN loc9_comp_amt
            WHEN m.fiscal_mo_nbr = 10 THEN loc10_comp_amt
            WHEN m.fiscal_mo_nbr = 11 THEN loc11_comp_amt
            WHEN m.fiscal_mo_nbr = 12 THEN loc12_comp_amt
            ELSE 0
        END
    ) <> 0
"""

ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE = spark.sql(_sql).withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE = ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.columns[0],'PROFIT_CTR') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.columns[1],'FISCAL_MO') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.columns[2],'GL_CATEGORY_CD') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.columns[3],'GL_ACCT_NBR') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.columns[4],'LOCATION_ID') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.columns[5],'CUR') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.columns[6],'PLAN_AMT_US') \
	.withColumnRenamed(ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.columns[7],'PLAN_AMT')

# COMMAND ----------

# Processing node Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE, type TARGET 
# COLUMN COUNT: 8


Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE = ASQ_Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.selectExpr(
	"CAST(PROFIT_CTR AS STRING) as PROFIT_CTR",
	"CAST(FISCAL_MO AS INT) as FISCAL_MO",
	"CAST(GL_CATEGORY_CD AS STRING) as GL_CATEGORY_CD",
	"CAST(GL_ACCT_NBR AS INT) as GL_ACCT_NBR",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(CUR AS STRING) as CURR_CD",
	"CAST(PLAN_AMT AS DECIMAL(15,2)) as LOC_PLAN_AMT",
	"CAST(PLAN_AMT_US AS DECIMAL(15,2)) as US_PLAN_AMT"
)
Shortcut_To_GL_GLPCT_PLAN_MONTH_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_GLPCT_PLAN_MONTH_PRE')

# COMMAND ----------


