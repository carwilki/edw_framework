# Databricks notebook source
#Code converted on 2023-08-09 13:02:40
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

# Processing node SQ_Shortcut_To_GL_CSKS_COST_CTR_FLAT, type SOURCE 
# COLUMN COUNT: 14

key = "csks"
_bucket=getParameterValue(raw,'BA_FINANCIALS_Parameter.prm','BA_FINANCIALS.WF:bs_GL_Daily','source_bucket')
file_path = get_source_file(key,_bucket)

if not file_path:
    raise FileNotFoundError(f"Unexpected Error: cannot find source data file for {key}")
    
fixed_width_data = spark.read.text(file_path)

columns = [
    expr("substring(value, 1, 4)").alias("CO_area_Id"),
    expr("substring(value, 5, 10)").alias("Cost_center_Id"),
    expr("substring(value, 15, 8)").alias("Exp_dt"),
    expr("substring(value, 23, 8)").alias("Valid_from_dt"),
    expr("substring(value, 31, 4)").alias("Company_Code"),
    expr("substring(value, 35, 1)").alias("CCtr_category_cd"),
    expr("substring(value, 36, 20)").alias("Person_resp"),
    expr("substring(value, 56, 5)").alias("Currency"),
    expr("substring(value, 61, 6)").alias("Costing_Sheet"),
    expr("substring(value, 67, 15)").alias("Tax_Jusisdiction"),
    expr("substring(value, 82, 10)").alias("Profit_ctr_cd"),
    expr("substring(value, 92, 4)").alias("Site"),
    expr("substring(value, 96, 8)").alias("Created_on_dt"),
    expr("substring(value, 104, 12)").alias("Created_By")
] 

SQ_Shortcut_To_GL_CSKS_COST_CTR_FLAT = fixed_width_data.select(*columns)

# COMMAND ----------

Shortcut_To_GL_CSKS_COST_CTR_PRE1 = SQ_Shortcut_To_GL_CSKS_COST_CTR_FLAT.selectExpr(
	"CAST(Cost_center_Id AS STRING) as COST_CENTER_ID",
  "CAST(IF (Valid_from_dt = '00000000', NULL, TO_DATE(Valid_from_dt, 'yyyyMMdd')) AS TIMESTAMP) as VALID_FROM_DT",
  "CAST(IF (Exp_dt = '00000000', NULL, TO_DATE(Exp_dt, 'yyyyMMdd')) AS TIMESTAMP) as EXP_DT",
  "CAST(CO_area_Id AS STRING) as CO_AREA_ID",
  "CAST(COMPANY_CODE AS SMALLINT) as COMPANY_CODE",
	"CAST(CCtr_category_cd AS STRING) as CCTR_CATEGORY_CD",
	"CAST(Person_resp AS STRING) as PERSON_RESP",
	"CAST(Currency AS STRING) as CURRENCY_CD",
	"CAST(Costing_Sheet AS STRING) as COSTING_SHEET",
	"CAST(Tax_Jusisdiction AS STRING) as TAX_JURISDICTION_CD",
	"CAST(Profit_ctr_cd AS STRING) as PROFIT_CENTER_ID",
	"CAST(Site AS STRING) as STORE_NBR",
	"CAST(IF (Created_on_dt = '00000000', NULL, TO_DATE(Created_on_dt, 'yyyyMMdd')) AS TIMESTAMP) as CREATED_ON_DT",
	"CAST(Created_By AS STRING) as CREATED_BY"
)

Shortcut_To_GL_CSKS_COST_CTR_PRE1.write.mode("overwrite").saveAsTable(f'{raw}.GL_CSKS_COST_CTR_PRE')

# COMMAND ----------


