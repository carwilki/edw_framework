# Databricks notebook source
#Code converted on 2023-08-09 13:02:57
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

# Processing node SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT, type SOURCE 
# COLUMN COUNT: 38

key = "glpct"
_bucket=getParameterValue(raw,'BA_FINANCIALS_Parameter.prm','BA_FINANCIALS.WF:bs_GL_Daily','source_bucket')
file_path = get_source_file(key,_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")

fixed_width_data = spark.read.text(file_path)

columns = [
  expr("substring(value, 1, 4)").alias("FISCAL_YR"),
  expr("substring(value, 5, 5)").alias("CUR"),
  expr("substring(value, 10, 1)").alias("DEBIT_CD_IND"),
  expr("substring(value, 11, 4)").alias("COMPANY_CD"),
  expr("substring(value, 15, 10)").alias("PROFIT_CTR"),
  expr("substring(value, 25, 10)").alias("GL_ACCT"),
  expr("substring(value, 35, 15)").alias("PER1_COMP_AMT"),
  expr("substring(value, 50, 15)").alias("PER2_COMP_AMT"),
  expr("substring(value, 65, 15)").alias("PER3_COMP_AMT"),
  expr("substring(value, 80, 15)").alias("PER4_COMP_AMT"),
  expr("substring(value, 95, 15)").alias("PER5_COMP_AMT"),
  expr("substring(value, 110, 15)").alias("PER6_COMP_AMT"),
  expr("substring(value, 125, 15)").alias("PER7_COMP_AMT"),
  expr("substring(value, 140, 15)").alias("PER8_COMP_AMT"),
  expr("substring(value, 155, 15)").alias("PER9_COMP_AMT"),
  expr("substring(value, 170, 15)").alias("PER10_COMP_AMT"),
  expr("substring(value, 185, 15)").alias("PER11_COMP_AMT"),
  expr("substring(value, 200, 15)").alias("PER12_COMP_AMT"),
  expr("substring(value, 215, 15)").alias("PER13_COMP_AMT"),
  expr("substring(value, 230, 15)").alias("PER14_COMP_AMT"),
  expr("substring(value, 245, 15)").alias("PER15_COMP_AMT"),
  expr("substring(value, 260, 15)").alias("PER16_COMP_AMT"),
  expr("substring(value, 275, 15)").alias("LOC_AMT_1"),
  expr("substring(value, 290, 15)").alias("LOC_AMT_2"),
  expr("substring(value, 305, 15)").alias("LOC_AMT_3"),
  expr("substring(value, 320, 15)").alias("LOC_AMT_4"),
  expr("substring(value, 335, 15)").alias("LOC_AMT_5"),
  expr("substring(value, 350, 15)").alias("LOC_AMT_6"),
  expr("substring(value, 365, 15)").alias("LOC_AMT_7"),
  expr("substring(value, 380, 15)").alias("LOC_AMT_8"),
  expr("substring(value, 395, 15)").alias("LOC_AMT_9"),
  expr("substring(value, 410, 15)").alias("LOC_AMT_10"),
  expr("substring(value, 425, 15)").alias("LOC_AMT_11"),
  expr("substring(value, 440, 15)").alias("LOC_AMT_12"),
  expr("substring(value, 455, 15)").alias("LOC_AMT_13"),
  expr("substring(value, 470, 15)").alias("LOC_AMT_14"),
  expr("substring(value, 485, 15)").alias("LOC_AMT_15"),
  expr("substring(value, 500, 15)").alias("LOC_AMT_16")
]  

SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT = fixed_width_data.select(*columns).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT_temp = SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT.toDF(*["SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___" + col for col in SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT.columns])

EXPTRANS = SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT_temp.selectExpr(
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___CUR as CUR",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PROFIT_CTR as PROFIT_CTR",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER12_COMP_AMT as PER12_COMP_AMT",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER13_COMP_AMT as PER13_COMP_AMT",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER14_COMP_AMT as PER14_COMP_AMT",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER15_COMP_AMT as PER15_COMP_AMT",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER16_COMP_AMT as PER16_COMP_AMT",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_12 as LOC_AMT_12",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_13 as LOC_AMT_13",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_14 as LOC_AMT_14",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_15 as LOC_AMT_15",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_16 as LOC_AMT_16",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___sys_row_id as sys_row_id",
	"SUBSTR (SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___CUR , 1 , 3 ) as out_CUR",
	"RTRIM (SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PROFIT_CTR ) as out_PROFIT_CTR",
	"IF (SUBSTR ( SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PROFIT_CTR , 5 , 1 ) = '-', SUBSTR ( SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PROFIT_CTR , 6 , 2 ), '00') as OUT_GL_CATEGORY_CD",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER12_COMP_AMT + SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER13_COMP_AMT + SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER14_COMP_AMT + SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER15_COMP_AMT + SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___PER16_COMP_AMT as out_PER12_COMP_AMT",
	"SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_12 + SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_13 + SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_14 + SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_15 + SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT___LOC_AMT_16 as out_PER12_LOC_AMT"
)

# COMMAND ----------

# Processing node Shortcut_To_GL_GLPCT_PLAN_PRE, type TARGET 
# COLUMN COUNT: 32

# Joining dataframes SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT, EXPTRANS to form Shortcut_To_GL_GLPCT_PLAN_PRE
Shortcut_To_GL_GLPCT_PLAN_PRE_joined = SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT.join(EXPTRANS, SQ_Shortcut_To_GL_GLPCT_PLAN_FLAT.sys_row_id == EXPTRANS.sys_row_id, 'inner')

Shortcut_To_GL_GLPCT_PLAN_PRE = Shortcut_To_GL_GLPCT_PLAN_PRE_joined.withColumn("SEQ_NBR", monotonically_increasing_id()).selectExpr(
	"CAST(SEQ_NBR AS BIGINT) as SEQ_NBR",
	"CAST(FISCAL_YR AS SMALLINT) as FISCAL_YR",
	"CAST(out_CUR AS STRING) as CUR",
	"CAST(DEBIT_CD_IND AS STRING) as DEBIT_CR_IND",
	"CAST(COMPANY_CD AS SMALLINT) as COMPANY_CD",
	"CAST(out_PROFIT_CTR AS STRING) as PROFIT_CTR",
	"CAST(OUT_GL_CATEGORY_CD AS STRING) as GL_CATEGORY_CD",
	"CAST(GL_ACCT AS INT) as GL_ACCT_NBR",
	"CAST(PER1_COMP_AMT AS DECIMAL(15,2)) as PER1_COMP_AMT",
	"CAST(PER2_COMP_AMT AS DECIMAL(15,2)) as PER2_COMP_AMT",
	"CAST(PER3_COMP_AMT AS DECIMAL(15,2)) as PER3_COMP_AMT",
	"CAST(PER4_COMP_AMT AS DECIMAL(15,2)) as PER4_COMP_AMT",
	"CAST(PER5_COMP_AMT AS DECIMAL(15,2)) as PER5_COMP_AMT",
	"CAST(PER6_COMP_AMT AS DECIMAL(15,2)) as PER6_COMP_AMT",
	"CAST(PER7_COMP_AMT AS DECIMAL(15,2)) as PER7_COMP_AMT",
	"CAST(PER8_COMP_AMT AS DECIMAL(15,2)) as PER8_COMP_AMT",
	"CAST(PER9_COMP_AMT AS DECIMAL(15,2)) as PER9_COMP_AMT",
	"CAST(PER10_COMP_AMT AS DECIMAL(15,2)) as PER10_COMP_AMT",
	"CAST(PER11_COMP_AMT AS DECIMAL(15,2)) as PER11_COMP_AMT",
	"CAST(out_PER12_COMP_AMT AS DECIMAL(15,2)) as PER12_COMP_AMT",
	"CAST(LOC_AMT_1 AS DECIMAL(15,2)) as LOC1_COMP_AMT",
	"CAST(LOC_AMT_2 AS DECIMAL(15,2)) as LOC2_COMP_AMT",
	"CAST(LOC_AMT_3 AS DECIMAL(15,2)) as LOC3_COMP_AMT",
	"CAST(LOC_AMT_4 AS DECIMAL(15,2)) as LOC4_COMP_AMT",
	"CAST(LOC_AMT_5 AS DECIMAL(15,2)) as LOC5_COMP_AMT",
	"CAST(LOC_AMT_6 AS DECIMAL(15,2)) as LOC6_COMP_AMT",
	"CAST(LOC_AMT_7 AS DECIMAL(15,2)) as LOC7_COMP_AMT",
	"CAST(LOC_AMT_8 AS DECIMAL(15,2)) as LOC8_COMP_AMT",
	"CAST(LOC_AMT_9 AS DECIMAL(15,2)) as LOC9_COMP_AMT",
	"CAST(LOC_AMT_10 AS DECIMAL(15,2)) as LOC10_COMP_AMT",
	"CAST(LOC_AMT_11 AS DECIMAL(15,2)) as LOC11_COMP_AMT",
	"CAST(out_PER12_LOC_AMT AS DECIMAL(15,2)) as LOC12_COMP_AMT"
)
Shortcut_To_GL_GLPCT_PLAN_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_GLPCT_PLAN_PRE')

# COMMAND ----------


