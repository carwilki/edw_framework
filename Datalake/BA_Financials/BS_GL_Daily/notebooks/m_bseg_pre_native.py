# Databricks notebook source
#Code converted on 2023-08-09 13:02:56
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

# dbutils.widgets.text(name = 'target_folder', defaultValue = 'gs://mnt/ssgnas01/devl-edwshare/DataLake/NZ_Migration_Test')
# target_folder = dbutils.widgets.get("target_folder")



if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------

# Processing node SQ_Shortcut_to_GL_BSEG_PRE_Flat_File, type SOURCE 
# COLUMN COUNT: 24

key = "gl_bseg"  # "gl_bseg_pre"
_bucket=getParameterValue(raw,'BA_FINANCIALS_Parameter.prm','BA_FINANCIALS.WF:bs_GL_Daily','source_bucket')
file_path = get_source_file(key,_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")

fixed_width_data = spark.read.text(file_path)

columns = [
  expr("substring(value, 1, 4)").alias("COMPANY_CD"),
  expr("substring(value, 5, 10)").alias("ACCT_DOC_NBR"),
  expr("substring(value, 15, 4)").alias("FISCAL_YR"),
  expr("substring(value, 19, 3)").alias("LINE_NBR"),
  expr("substring(value, 22, 2)").alias("DOC_TYPE_CD"),
  expr("substring(value, 24, 8)").alias("DOC_DT"),
  expr("substring(value, 32, 8)").alias("POSTING_DT"),
  expr("substring(value, 40, 2)").alias("FISCAL_PERIOD"),
  expr("substring(value, 42, 5)").alias("CURR_KEY"),
  expr("substring(value, 47, 7)").alias("XRATE"),
  expr("substring(value, 54, 7)").alias("XRATE_L2"),
  expr("substring(value, 61, 5)").alias("LOC_CURR"),
  expr("substring(value, 66, 1)").alias("DB_CR_IND"),
  expr("substring(value, 67, 16)").alias("LOC_CURR_AMT"),
  expr("substring(value, 83, 16)").alias("DOC_CURR_AMT"),
  expr("substring(value, 99, 16)").alias("LOC2_CURR_AMT"),
  expr("substring(value, 115, 3)").alias("TRANS_TYPE_CD"),
  expr("substring(value, 118, 4)").alias("CONTROL_AREA"),
  expr("substring(value, 122, 10)").alias("GL_ACCT_NBR"),
  expr("substring(value, 132, 10)").alias("VENDOR_ID"),
  expr("substring(value, 142, 1)").alias("BAL_SHEET_IND"),
  expr("substring(value, 143, 1)").alias("P_L_IND"),
  expr("substring(value, 144, 11)").alias("PURCH_DOC_NBR"),
  expr("substring(value, 155, 10)").alias("PROFIT_CTR")
]


SQ_Shortcut_to_GL_BSEG_PRE_Flat_File = fixed_width_data.select(*columns).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

FILTRANS = SQ_Shortcut_to_GL_BSEG_PRE_Flat_File.selectExpr(
	"COMPANY_CD as COMPANY_CD",
	"ACCT_DOC_NBR as ACCT_DOC_NBR",
	"FISCAL_YR as FISCAL_YR",
	"LINE_NBR as LINE_NBR",
	"DOC_TYPE_CD as DOC_TYPE_CD",
	"CASE WHEN DOC_DT = '00000000' THEN NULL ELSE TO_DATE(DOC_DT, 'yyyyMMdd') END AS DOC_DT",
	"CASE WHEN POSTING_DT = '00000000' THEN NULL ELSE TO_DATE(POSTING_DT, 'yyyyMMdd') END AS POSTING_DT",
	"FISCAL_PERIOD as FISCAL_PERIOD",
	"CURR_KEY as CURR_KEY",
	"XRATE as XRATE",
	"XRATE_L2 as XRATE_L2",
	"LOC_CURR as LOC_CURR",
	"DB_CR_IND as DB_CR_IND",
	"LOC_CURR_AMT as LOC_CURR_AMT",
	"DOC_CURR_AMT as DOC_CURR_AMT",
	"LOC2_CURR_AMT as LOC2_CURR_AMT",
	"TRANS_TYPE_CD as TRANS_TYPE_CD",
	"CONTROL_AREA as CONTROL_AREA",
	"GL_ACCT_NBR as GL_ACCT_NBR",
	"VENDOR_ID as VENDOR_ID",
	"BAL_SHEET_IND as BAL_SHEET_IND",
	"P_L_IND as P_L_IND",
	"PURCH_DOC_NBR as PURCH_DOC_NBR",
	"PROFIT_CTR as PROFIT_CTR").filter("LENGTH(TRIM(PROFIT_CTR)) > 0")

# COMMAND ----------

# Processing node Shortcut_to_GL_BSEG_PRE_Flat_File1, type TARGET 
# COLUMN COUNT: 24


Shortcut_to_GL_BSEG_PRE_Flat_File1 = FILTRANS.selectExpr(
	"CAST(FISCAL_YR AS SMALLINT) as FISCAL_YR",
	"CAST(FISCAL_PERIOD AS TINYINT) as FISCAL_PERIOD",
	"CAST(DOC_TYPE_CD AS STRING) as DOC_TYPE_CD",
	"CAST(GL_ACCT_NBR AS INT) as GL_ACCT_NBR",
	"CAST(ACCT_DOC_NBR AS BIGINT) as ACCT_DOC_NBR",
	"CAST(LINE_NBR AS SMALLINT) as LINE_NBR",
	"CAST(PROFIT_CTR AS STRING) as PROFIT_CTR",
	"CAST(COMPANY_CD AS SMALLINT) as COMPANY_CD",
	"CAST(DOC_DT AS TIMESTAMP) as DOC_DT",
	"CAST(POSTING_DT AS TIMESTAMP) as POSTING_DT",
	"CAST(CURR_KEY AS STRING) as CURR_KEY",
	"CAST(XRATE AS DECIMAL(5, 4)) as XRATE",
	"CAST(XRATE_L2 AS DECIMAL(5, 4)) as XRATE_L2",
	"CAST(LOC_CURR AS STRING) as LOC_CURR",
	"CAST(DB_CR_IND AS STRING) as DB_CR_IND",
	"CAST(LOC_CURR_AMT AS DECIMAL(15, 2)) as LOC_CURR_AMT",
	"CAST(DOC_CURR_AMT AS DECIMAL(15, 2)) as DOC_CURR_AMT",
	"CAST(LOC2_CURR_AMT AS DECIMAL(15, 2)) as LOC2_CURR_AMT",
	"CAST(TRANS_TYPE_CD AS STRING) as TRANS_TYPE_CD",
	"CAST(CONTROL_AREA AS STRING) as CONTROL_AREA",
	"CAST(VENDOR_ID AS STRING) as VENDOR_ID",
	"CAST(BAL_SHEET_IND AS STRING) as BAL_SHEET_IND",
	"CAST(P_L_IND AS STRING) as P_L_IND",
	"CAST(PURCH_DOC_NBR AS STRING) as PURCH_DOC_NBR"
)


# COMMAND ----------

# column_widths = [5, 3, 2, 10, 19, 5, 10, 5, 29, 29, 5, 5, 5, 5, 1, 15, 15, 15, 3, 4, 10, 1, 1, 11]

# df = Shortcut_to_GL_BSEG_PRE_Flat_File1
# columns = df.columns

# # Apply format_string to pad values to column widths
# formatted_cols = [lpad(col, width, " ").alias(col) for col, width in zip(columns, column_widths)]

# # Select formatted columns and concatenate
# df = df.select(*formatted_cols)

# COMMAND ----------

# df_line = df.select(concat_ws("", *columns).alias("line"))

# # Save the DataFrame as a text file with fixed-width columns
# #_path = 'gs://petm-bdpl-dev-nas-p1-gcs-gbl/shortcut_to_gl_bseg_pre_flat_file1.out'
# #df_line.write.mode("overwrite").text(_path)

# write_target_file(df_line, 'shortcut_to_gl_bseg_pre_flat_file1.out')


# COMMAND ----------

Shortcut_to_GL_BSEG_PRE_Flat_File1.write.mode("overwrite").saveAsTable(f'{raw}.gl_bseg_pre')
