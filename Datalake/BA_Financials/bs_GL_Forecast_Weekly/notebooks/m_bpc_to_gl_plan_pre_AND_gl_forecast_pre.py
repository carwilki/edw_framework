# Databricks notebook source
#Code converted on 2023-08-17 15:37:01
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

# Processing node SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT, type SOURCE 
# COLUMN COUNT: 12


fname = getParameterValue(raw,'BA_Financials_Parameter.prm','BA_Financials.WF:bs_GL_Forecast_Weekly','source_file')
_bucket = getParameterValue(raw,'BA_Financials_Parameter.prm','BA_Financials.WF:bs_GL_Forecast_Weekly','source_bucket')
file_name = get_source_file_bs_wkly(fname, _bucket)

df = spark.read.text(file_name).withColumn("rn", monotonically_increasing_id())
columns = df.filter("rn=4").select("value").first()[0]
cols = columns.split(",")

data_df = df.filter("rn>4")
split_columns = [split(regexp_replace(data_df["value"], '"', ''), ",").getItem(i).alias(cols[i]) for i in range(len(cols))]

SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT = data_df.select(*split_columns).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT_temp = SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT.toDF(*["SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___" + col for col in SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT.columns])


EXPTRANS = SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT_temp.selectExpr(
    "SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___TIME as TIME",
    "SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___SIGNEDDATA as i_GL_AMT",
    "SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___COMP_CODE as COMP_CODE",
    "SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___GL_ACCOUNT as GL_ACCOUNT",
    "SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___sys_row_id as sys_row_id"
).selectExpr(
    "sys_row_id",
    "CAST(CONCAT(SUBSTR(TIME, 1, 4), SUBSTR(TIME, 6, 2) ) as int) as o_FISCAL_MO",
    "ROUND(CAST(i_GL_AMT as decimal(15, 4)),2) as o_GL_FORECAST_AMT", 
    "ROUND(CAST(i_GL_AMT as decimal(15, 4)),2) as o_GL_AMT",
    "SUBSTR(COMP_CODE, 2) as COMPANY_NBR",
    "CAST(SUBSTR (GL_ACCOUNT, 2) as int) as GL_ACCT_NBR"
)

# COMMAND ----------

# Processing node FIL_TRANS_Forecast_Recs, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT_temp = SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT.toDF(*["SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___" + col for col in SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT.columns])
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])

# Joining dataframes SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT, EXPTRANS to form FIL_TRANS_Forecast_Recs
FIL_TRANS_Forecast_Recs_joined = SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT_temp.join(EXPTRANS_temp, col("SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___sys_row_id") == col("EXPTRANS___sys_row_id"), 'inner')

# FIL_TRANS_Forecast_Recs_joined_temp = FIL_TRANS_Forecast_Recs_joined_temp.join(SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT_temp).join(EXPTRANS_temp)

FIL_TRANS_Forecast_Recs = FIL_TRANS_Forecast_Recs_joined.selectExpr(
	"EXPTRANS___o_FISCAL_MO as o_FISCAL_MO",
	"EXPTRANS___COMPANY_NBR as COMPANY_NBR",
	"EXPTRANS___GL_ACCT_NBR as GL_ACCT_NBR",
	"SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___SITE as STORE_NBR",
	"SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___REPORT_CURRENCY as CURRENCY_ID",
	"SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___GL_CATEGORY as GL_CAT_CD",
	"EXPTRANS___o_GL_FORECAST_AMT as o_GL_FORECAST_AMT",
	"SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___CATEGORY as CATEGORY").filter("TO_DATE(o_FISCAL_MO, 'yyyyMM') >= add_months(CURRENT_DATE, -140) AND CATEGORY = 'FF'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node FIL_TRANS_Plan_Recs, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 9


FIL_TRANS_Plan_Recs = FIL_TRANS_Forecast_Recs_joined.selectExpr(
	"EXPTRANS___o_FISCAL_MO as o_FISCAL_MO",
	"SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___CATEGORY as FORECAST_NAME",
	"EXPTRANS___COMPANY_NBR as COMPANY_NBR",
	"EXPTRANS___GL_ACCT_NBR as GL_ACCT_NBR",
	"SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___SITE as STORE_NBR",
	"SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___REPORT_CURRENCY as CURRENCY_ID",
	"SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___GL_CATEGORY as GL_CAT_CD",
	"EXPTRANS___o_GL_AMT as o_GL_AMT",
	"SQ_Shortcut_to_BPC_PLAN_AND_FORECAST_FLAT___CATEGORY as CATEGORY").filter("TO_DATE(o_FISCAL_MO, 'yyyyMM') >= add_months(CURRENT_DATE, -140) AND CATEGORY = 'FA'").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Shortcut_To_GL_FORECAST_PRE, type TARGET 
# COLUMN COUNT: 7


Shortcut_To_GL_FORECAST_PRE = FIL_TRANS_Forecast_Recs.selectExpr(
	"CAST(o_FISCAL_MO AS INT) as FISCAL_MO",
	"CAST(GL_ACCT_NBR AS INT) as GL_ACCT_NBR",
	"CAST(GL_CAT_CD AS STRING) as GL_CAT_CD",
	"CAST(STORE_NBR AS SMALLINT) as STORE_NBR",
	"CAST(CURRENCY_ID AS STRING) as CURRENCY_ID",
	"CAST(COMPANY_NBR AS SMALLINT) as COMPANY_NBR",
	"CAST(o_GL_FORECAST_AMT AS DECIMAL(15,2)) as GL_FORECAST_AMT"
)
Shortcut_To_GL_FORECAST_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_FORECAST_PRE')

# COMMAND ----------

# Processing node Shortcut_to_GL_PLAN_PRE, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_GL_PLAN_PRE = FIL_TRANS_Plan_Recs.selectExpr(
	"CAST(FORECAST_NAME AS STRING) as FORECAST_NAME",
	"CAST(o_FISCAL_MO AS INT) as FISCAL_MO",
	"CAST(GL_ACCT_NBR AS INT) as GL_ACCT_NBR",
	"CAST(GL_CAT_CD AS STRING) as GL_CAT_CD",
	"CAST(STORE_NBR AS SMALLINT) as STORE_NBR",
	"CAST(CURRENCY_ID AS STRING) as CURRENCY_ID",
	"CAST(COMPANY_NBR AS SMALLINT) as COMPANY_NBR",
	"CAST(o_GL_AMT AS DECIMAL(15,4)) as GL_AMT"
)
Shortcut_to_GL_PLAN_PRE.write.mode("overwrite").saveAsTable(f'{raw}.GL_PLAN_PRE')
