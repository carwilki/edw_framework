# Databricks notebook source
#Code converted on 2023-10-13 12:47:11
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

LAST_LOAD_DT = get_last_load_date(f'{raw}.CO_DEPOSIT_PRE')

print(LAST_LOAD_DT)

# COMMAND ----------

(username, password, connection_string) = or_stxp1_read(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_CO_DEPOSIT, type SOURCE 
# COLUMN COUNT: 11

orcl_schema = 'STXADM'

_sql = f"""
SELECT
SALES_DT,
SITE_NBR,
COUNTRY_CD,
DEPOSIT_TYPE_CD,
GL_ACCT,
TENDER_TYPE_ID,
DEPOSIT_SLIP_NBR,
DEPOSIT_BAG_NBR,
SEQ_NBR,
DEPOSIT_AMT,
LOAD_DT
FROM {orcl_schema}.CO_DEPOSIT
WHERE LOAD_DT > date'{LAST_LOAD_DT}'"""

SQ_Shortcut_to_CO_DEPOSIT = jdbcOracleConnection(_sql,username, password, connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_SET_MAX_VAR, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_CO_DEPOSIT_temp = SQ_Shortcut_to_CO_DEPOSIT.toDF(*["SQ_Shortcut_to_CO_DEPOSIT___" + col for col in SQ_Shortcut_to_CO_DEPOSIT.columns])

EXP_SET_MAX_VAR = SQ_Shortcut_to_CO_DEPOSIT_temp.selectExpr(
    "SQ_Shortcut_to_CO_DEPOSIT___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_CO_DEPOSIT___SALES_DT as SALES_DT",
	"SQ_Shortcut_to_CO_DEPOSIT___SITE_NBR as SITE_NBR",
	"SQ_Shortcut_to_CO_DEPOSIT___COUNTRY_CD as COUNTRY_CD",
	"SQ_Shortcut_to_CO_DEPOSIT___DEPOSIT_TYPE_CD as DEPOSIT_TYPE_CD",
	"SQ_Shortcut_to_CO_DEPOSIT___GL_ACCT as GL_ACCT",
	"SQ_Shortcut_to_CO_DEPOSIT___TENDER_TYPE_ID as TENDER_TYPE_ID",
	"SQ_Shortcut_to_CO_DEPOSIT___DEPOSIT_SLIP_NBR as DEPOSIT_SLIP_NBR",
	"SQ_Shortcut_to_CO_DEPOSIT___DEPOSIT_BAG_NBR as DEPOSIT_BAG_NBR",
	"SQ_Shortcut_to_CO_DEPOSIT___SEQ_NBR as SEQ_NBR",
	"SQ_Shortcut_to_CO_DEPOSIT___DEPOSIT_AMT as DEPOSIT_AMT",
	"SQ_Shortcut_to_CO_DEPOSIT___LOAD_DT as i_LOAD_DT").selectExpr(
	"sys_row_id as sys_row_id",
	"SALES_DT as SALES_DT",
	"SITE_NBR as SITE_NBR",
	"COUNTRY_CD as COUNTRY_CD",
	"DEPOSIT_TYPE_CD as DEPOSIT_TYPE_CD",
	"GL_ACCT as GL_ACCT",
	"TENDER_TYPE_ID as TENDER_TYPE_ID",
	"DEPOSIT_SLIP_NBR as DEPOSIT_SLIP_NBR",
	"DEPOSIT_BAG_NBR as DEPOSIT_BAG_NBR",
	"SEQ_NBR as SEQ_NBR",
	"DEPOSIT_AMT as DEPOSIT_AMT"
)

# COMMAND ----------

# Processing node Shortcut_to_CO_DEPOSIT_PRE, type TARGET 
# COLUMN COUNT: 11


Shortcut_to_CO_DEPOSIT_PRE = EXP_SET_MAX_VAR.selectExpr(
	"CAST(SALES_DT AS TIMESTAMP) as SALES_DT",
	"CAST(SITE_NBR AS SMALLINT) as  SITE_NBR",
	"CAST(COUNTRY_CD AS STRING) as COUNTRY_CD",
	"CAST(DEPOSIT_TYPE_CD AS STRING) as DEPOSIT_TYPE_CD",
	"CAST(GL_ACCT AS STRING) as GL_ACCT",
	"CAST(TENDER_TYPE_ID AS SMALLINT) as  TENDER_TYPE_ID",
	"CAST(DEPOSIT_SLIP_NBR AS BIGINT) as  DEPOSIT_SLIP_NBR",
	"CAST(DEPOSIT_BAG_NBR AS BIGINT) as  DEPOSIT_BAG_NBR",
	"CAST(SEQ_NBR AS BIGINT) as  SEQ_NBR",
	"CAST(DEPOSIT_AMT AS DECIMAL(10,2)) as DEPOSIT_AMT",
	"CAST(CURRENT_DATE AS TIMESTAMP) as LOAD_TSTMP"
)

Shortcut_to_CO_DEPOSIT_PRE.write.mode("overwrite").saveAsTable(f'{raw}.CO_DEPOSIT_PRE')

# COMMAND ----------


