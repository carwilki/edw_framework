# Databricks notebook source
#Code converted on 2023-10-13 12:47:09
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

from datetime import date, datetime, timedelta
def get_last_load_date(target_table):
    sql = f"SELECT MAX(LOAD_TSTMP) dt FROM {target_table}"
    df = spark.sql(sql)
    result = df.first()[0]
    prev_run_dt = str(date.today() - timedelta(days=2))
    return prev_run_dt if result is None else df.first()[0].strftime("%Y-%m-%d")

LAST_LOAD_DT = get_last_load_date(f'{raw}.CO_VARIANCE_PRE')

print(LAST_LOAD_DT)

# COMMAND ----------

(username, password, connection_string) = or_stxp1_read(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_CO_VARIANCE, type SOURCE 
# COLUMN COUNT: 8

orcl_schema = 'STXADM'

_sql = f"""SELECT
SALES_DT,
SITE_NBR,
COUNTRY_CD,
VARIANCE_TYPE_CD,
TENDER_TYPE_ID,
CASHIER_NBR,
VARIANCE_AMT,
LOAD_DT
FROM {orcl_schema}.CO_VARIANCE
WHERE LOAD_DT > date'{LAST_LOAD_DT}'"""

SQ_Shortcut_to_CO_VARIANCE = jdbcOracleConnection(_sql,username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_SET_MAX_VAR, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_CO_VARIANCE_temp = SQ_Shortcut_to_CO_VARIANCE.toDF(*["SQ_Shortcut_to_CO_VARIANCE___" + col for col in SQ_Shortcut_to_CO_VARIANCE.columns])

EXP_SET_MAX_VAR = SQ_Shortcut_to_CO_VARIANCE_temp.selectExpr(
    "SQ_Shortcut_to_CO_VARIANCE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_CO_VARIANCE___SALES_DT as SALES_DT",
	"SQ_Shortcut_to_CO_VARIANCE___SITE_NBR as SITE_NBR",
	"SQ_Shortcut_to_CO_VARIANCE___COUNTRY_CD as COUNTRY_CD",
	"SQ_Shortcut_to_CO_VARIANCE___VARIANCE_TYPE_CD as VARIANCE_TYPE_CD",
	"SQ_Shortcut_to_CO_VARIANCE___TENDER_TYPE_ID as TENDER_TYPE_ID",
	"SQ_Shortcut_to_CO_VARIANCE___CASHIER_NBR as CASHIER_NBR",
	"SQ_Shortcut_to_CO_VARIANCE___VARIANCE_AMT as VARIANCE_AMT",
	"SQ_Shortcut_to_CO_VARIANCE___LOAD_DT as i_LOAD_DT").selectExpr(
	"sys_row_id as sys_row_id",
	"SALES_DT as SALES_DT",
	"SITE_NBR as SITE_NBR",
	"COUNTRY_CD as COUNTRY_CD",
	"VARIANCE_TYPE_CD as VARIANCE_TYPE_CD",
	"TENDER_TYPE_ID as TENDER_TYPE_ID",
	"CASHIER_NBR as CASHIER_NBR",
	"VARIANCE_AMT as VARIANCE_AMT"
)

# COMMAND ----------

# Processing node Shortcut_to_CO_VARIANCE_PRE, type TARGET 
# COLUMN COUNT: 8


Shortcut_to_CO_VARIANCE_PRE = EXP_SET_MAX_VAR.selectExpr(
	"CAST(SALES_DT AS TIMESTAMP) as SALES_DT",
	"CAST(SITE_NBR AS SMALLINT) as  SITE_NBR",
	"CAST(COUNTRY_CD AS STRING) as COUNTRY_CD",
	"CAST(VARIANCE_TYPE_CD AS STRING) as VARIANCE_TYPE_CD",
	"CAST(TENDER_TYPE_ID AS SMALLINT) as  TENDER_TYPE_ID",
	"CAST(CASHIER_NBR AS INT) as CASHIER_NBR",
	"CAST(VARIANCE_AMT AS DECIMAL(10,2)) as VARIANCE_AMT",
	"CAST(CURRENT_DATE AS TIMESTAMP) as LOAD_TSTMP"
)

Shortcut_to_CO_VARIANCE_PRE.write.mode("overwrite").saveAsTable(f'{raw}.CO_VARIANCE_PRE')

# COMMAND ----------


