# Databricks notebook source
#Code converted on 2023-08-17 15:37:09
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

# Processing node SQ_Shortcut_to_GL_FORECAST_PRE, type SOURCE 
# COLUMN COUNT: 3

# SQ_Shortcut_to_GL_FORECAST_PRE = spark.sql(f"""
# SELECT CURRENT_TIMESTAMP AS START_TSTMP,
#        'GL_PLAN_MONTH_PRE' AS TABLE_NAME,
#        COUNT(*) AS BEGIN_ROW_CNT
#   FROM {raw}.GL_PLAN_MONTH_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
  
# # Conforming fields names to the component layout
# SQ_Shortcut_to_GL_FORECAST_PRE = SQ_Shortcut_to_GL_FORECAST_PRE \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_FORECAST_PRE.columns[0],'START_TSTMP') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_FORECAST_PRE.columns[1],'TABLE_NAME') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_FORECAST_PRE.columns[2],'BEGIN_ROW_CNT')

# COMMAND ----------

# Processing node SQL_INS_and_DUPS_CHECK, type SQL_TRANSFORM 
# COLUMN COUNT: 9

_sql = f"""
INSERT OVERWRITE {raw}.GL_PLAN_MONTH_PRE
SELECT FISCAL_MO
       ,GL_ACCT_NBR
       ,GL_CAT_CD
       ,PROFIT_CTR
       ,LOCATION_ID
       ,CURRENCY_ID
       ,GL_F1_ADJ_AMT_US
       ,GL_F1_ADJ_AMT_LOC
  FROM (
         SELECT PRE.FISCAL_MO
               ,PRE.GL_ACCT_NBR
               ,PRE.GL_CAT_CD
               ,CASE WHEN PRE.STORE_NBR > 8000
                          AND PRE.STORE_NBR < 9000
                          THEN '000000'
                               || LTRIM (TO_CHAR (PRE.STORE_NBR, '0000') )
                     ELSE LTRIM (TO_CHAR (PRE.STORE_NBR, '0000') )
                          || '-'
                          || PRE.GL_CAT_CD
                END AS PROFIT_CTR
               , ROUND(sum(PRE.GL_AMT_US),2) AS GL_F1_ADJ_AMT_US
               , CASE 
                  WHEN sum(PRE.GL_AMT_LOC) IS NULL
                  THEN ROUND(sum(PRE.GL_AMT_US),2)
                  ELSE ROUND(sum(PRE.GL_AMT_LOC),2)
                  END AS GL_F1_ADJ_AMT_LOC
          FROM (
            SELECT 
               FISCAL_MO,
               GL_ACCT_NBR,
               GL_CAT_CD,
               COMPANY_NBR,
               STORE_NBR,
                CASE WHEN CURRENCY_ID = 'USD'
                THEN GL_AMT END  AS GL_AMT_US,
                CASE WHEN CURRENCY_ID = 'CAD'
                THEN GL_AMT END  AS GL_AMT_LOC
           FROM {raw}.GL_PLAN_PRE           
           ) PRE 
 GROUP BY       PRE.FISCAL_MO,
               pre.GL_ACCT_NBR,
               pre.GL_CAT_CD,
               pre.COMPANY_NBR,
               pre.STORE_NBR
        ) grp
       join {legacy}.GL_PROFIT_CENTER P
         on grp.PROFIT_CTR = P.GL_PROFIT_CTR_CD
 ORDER BY PROFIT_CTR, GL_ACCT_NBR
"""

spark.sql(_sql)

# # COMMAND ----------

# _sql = f"""
# SELECT COUNT(*) AS DUPLICATE_ROWS
#   FROM (
#          SELECT FISCAL_MO,
#                GL_ACCT_NBR,
#                GL_CATEGORY_CD,
#                PROFIT_CTR,
#                COUNT(*) AS CNT
#           FROM {raw}.GL_PLAN_MONTH_PRE
#          GROUP BY FISCAL_MO,
#                   GL_ACCT_NBR,
#                   GL_CATEGORY_CD,
#                   PROFIT_CTR
#        ) T
#  WHERE CNT > 1
# """
# spark.sql(_sql)

# # COMMAND ----------

# # quitting here as the rest seems to be irrelevant to the workflow
# # otherwise this command / code blocked should be removed

# try:
#     from pyspark.dbutils import DBUtils
#     dbutils = DBUtils(spark)
#     in_databricks_notebook = True
#     dbutils.notebook.exit(0)
# except ImportError:
#     in_databricks_notebook = False
#     sys.exit(0)

# # COMMAND ----------

# # for each involved DataFrame, append the dataframe name to each column

# SQL_INS_and_DUPS_CHECK = SQ_Shortcut_to_GL_FORECAST_PRE.selectExpr(
# 	"SQ_Shortcut_to_GL_FORECAST_PRE___sys_row_id as sys_row_id"
# )

# # COMMAND ----------

# # Processing node EXP_GET_SESSION_INFO, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# # COLUMN COUNT: 10

# # for each involved DataFrame, append the dataframe name to each column
# SQL_INS_and_DUPS_CHECK_temp = SQL_INS_and_DUPS_CHECK.toDF(*["SQL_INS_and_DUPS_CHECK___" + col for col in SQL_INS_and_DUPS_CHECK.columns])

# EXP_GET_SESSION_INFO = SQL_INS_and_DUPS_CHECK_temp.selectExpr(
# 	"SQL_INS_and_DUPS_CHECK___START_TSTMP_output as i_START_TSTMP",
# 	"SQL_INS_and_DUPS_CHECK___TABLE_NAME_output as TABLE_NAME",
# 	"SQL_INS_and_DUPS_CHECK___BEGIN_ROW_CNT_output as BEGIN_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___NumRowsAffected as INSERT_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___DUPLICATE_ROW_CNT as DUPLICATE_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___SQLError as i_SQL_TRANSFORM_ERROR").selectExpr(
# 	"SQL_INS_and_DUPS_CHECK___sys_row_id as sys_row_id",
# 	"date_format(SQL_INS_and_DUPS_CHECK___i_START_TSTMP, 'MM/DD/YYYY HH24:MI:SS') as START_TSTMP",
# 	"date_format(CURRENT_TIMESTAMP, 'MM/DD/YYYY HH24:MI:SS') as END_TSTMP",
# 	"$PMWorkflowName as WORKFLOW_NAME",
# 	"$PMSessionName as SESSION_NAME",
# 	"$PMMappingName as MAPPING_NAME",
# 	"SQL_INS_and_DUPS_CHECK___TABLE_NAME as TABLE_NAME",
# 	"SQL_INS_and_DUPS_CHECK___BEGIN_ROW_CNT as BEGIN_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___INSERT_ROW_CNT as INSERT_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___DUPLICATE_ROW_CNT as DUPLICATE_ROW_CNT",
# 	"IF (SQL_INS_and_DUPS_CHECK___DUPLICATE_ROW_CNT > 0, 'There are duplicate records in the table', SQL_INS_and_DUPS_CHECK___i_SQL_TRANSFORM_ERROR) as SQL_TRANSFORM_ERROR"
# )

# # COMMAND ----------

# # Processing node AGG, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# # COLUMN COUNT: 10

# AGG = EXP_GET_SESSION_INFO.selectExpr(
# 	"EXP_GET_SESSION_INFO.START_TSTMP as START_TSTMP",
# 	"EXP_GET_SESSION_INFO.END_TSTMP as i_END_TSTMP",
# 	"EXP_GET_SESSION_INFO.WORKFLOW_NAME as WORKFLOW_NAME",
# 	"EXP_GET_SESSION_INFO.SESSION_NAME as SESSION_NAME",
# 	"EXP_GET_SESSION_INFO.MAPPING_NAME as MAPPING_NAME",
# 	"EXP_GET_SESSION_INFO.TABLE_NAME as TABLE_NAME",
# 	"EXP_GET_SESSION_INFO.BEGIN_ROW_CNT as i_BEGIN_ROW_CNT",
# 	"EXP_GET_SESSION_INFO.INSERT_ROW_CNT as i_INSERT_ROW_CNT",
# 	"EXP_GET_SESSION_INFO.SQL_TRANSFORM_ERROR as i_SQL_TRANSFORM_ERROR",
# 	"EXP_GET_SESSION_INFO.DUPLICATE_ROW_CNT as i_DUPLICATE_ROW_CNT") \
# 	.groupBy("START_TSTMP","WORKFLOW_NAME","SESSION_NAME","MAPPING_NAME","TABLE_NAME") \
# 	.agg( \
# 	min("max(col('i_END_TSTMP')) as END_TSTMP"),
# 	min("max(col('i_BEGIN_ROW_CNT')) .cast(StringType()) as BEGIN_ROW_CNT"),
# 	min("sum(col('i_INSERT_ROW_CNT')) .cast(StringType()) as INSERT_ROW_CNT"),
# 	min("max(i_SQL_TRANSFORM_ERROR) as SQL_TRANSFORM_ERROR"),
# 	min("sum(col('i_DUPLICATE_ROW_CNT')) .cast(StringType()) as DUPLICATE_ROW_CNT")
# 	) \
# 	.withColumn("sys_row_id", monotonically_increasing_id())

# # COMMAND ----------

# # Processing node EXP_CREATE_INS_SQL, type EXPRESSION 
# # COLUMN COUNT: 11

# # for each involved DataFrame, append the dataframe name to each column
# AGG_temp = AGG.toDF(*["AGG___" + col for col in AGG.columns])

# EXP_CREATE_INS_SQL = AGG_temp.selectExpr(
# 	"AGG___sys_row_id as sys_row_id",
# 	"AGG___START_TSTMP as START_TSTMP",
# 	"AGG___END_TSTMP as END_TSTMP",
# 	"AGG___WORKFLOW_NAME as WORKFLOW_NAME",
# 	"AGG___SESSION_NAME as SESSION_NAME",
# 	"AGG___MAPPING_NAME as MAPPING_NAME",
# 	"AGG___TABLE_NAME as TABLE_NAME",
# 	"AGG___BEGIN_ROW_CNT as BEGIN_ROW_CNT",
# 	"AGG___INSERT_ROW_CNT as INSERT_ROW_CNT",
# 	"AGG___DUPLICATE_ROW_CNT as DUPLICATE_ROW_CNT",
# 	"AGG___SQL_TRANSFORM_ERROR as SQL_TRANSFORM_ERROR",
# 	"concat('INSERT INTO SQL_TRANSFORM_LOG VALUES (TO_DATE(' , CHR ( 39 ) , AGG___START_TSTMP , CHR ( 39 ) , ',' , CHR ( 39 ) , 'MM/DD/YYYY HH24:MI:SS' , CHR ( 39 ) , '),TO_DATE(' , CHR ( 39 ) , AGG___END_TSTMP , CHR ( 39 ) , ',' , CHR ( 39 ) , 'MM/DD/YYYY HH24:MI:SS' , CHR ( 39 ) , '), ' , CHR ( 39 ) , AGG___WORKFLOW_NAME , CHR ( 39 ) , ', ' , CHR ( 39 ) , AGG___SESSION_NAME , CHR ( 39 ) , ', ' , CHR ( 39 ) , AGG___MAPPING_NAME , CHR ( 39 ) , ', ' , CHR ( 39 ) , AGG___TABLE_NAME , CHR ( 39 ) , ', ' , CHR ( 39 ) , AGG___BEGIN_ROW_CNT , CHR ( 39 ) , ', ' , CHR ( 39 ) , AGG___INSERT_ROW_CNT , CHR ( 39 ) , ', ' , CHR ( 39 ) , AGG___DUPLICATE_ROW_CNT , CHR ( 39 ) , ',  ' , CHR ( 39 ) , AGG___SQL_TRANSFORM_ERROR , CHR ( 39 ) , ')' ) as INSERT_SQL"
# )

# # COMMAND ----------

# # Processing node SQL_INS_to_SQL_TRANSFORM_LOG, type SQL_TRANSFORM 
# # COLUMN COUNT: 14

# """
# WARNING: SQL Transformation is not yet supported, producing passthrough dataframe:
# SQL query:


# """
# # for each involved DataFrame, append the dataframe name to each column

# SQL_INS_to_SQL_TRANSFORM_LOG = EXP_CREATE_INS_SQL.selectExpr(
# 	"EXP_CREATE_INS_SQL___sys_row_id as sys_row_id"
# )

# # COMMAND ----------

# # Processing node EXP_ABORT_SESSION, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# # COLUMN COUNT: 3

# # for each involved DataFrame, append the dataframe name to each column
# SQL_INS_to_SQL_TRANSFORM_LOG_temp = SQL_INS_to_SQL_TRANSFORM_LOG.toDF(*["SQL_INS_to_SQL_TRANSFORM_LOG___" + col for col in SQL_INS_to_SQL_TRANSFORM_LOG.columns])

# EXP_ABORT_SESSION = SQL_INS_to_SQL_TRANSFORM_LOG_temp.selectExpr(
# 	"SQL_INS_to_SQL_TRANSFORM_LOG___DUPLICATE_ROW_CNT_output as DUPLICATE_ROW_CNT",
# 	"SQL_INS_to_SQL_TRANSFORM_LOG___SQL_TRANSFORM_ERROR_output as SQL_TRANSFORM_ERROR").selectExpr(
# 	"SQL_INS_to_SQL_TRANSFORM_LOG___sys_row_id as sys_row_id",
# 	"SQL_INS_to_SQL_TRANSFORM_LOG___DUPLICATE_ROW_CNT as DUPLICATE_ROW_CNT",
# 	"SQL_INS_to_SQL_TRANSFORM_LOG___SQL_TRANSFORM_ERROR as SQL_TRANSFORM_ERROR",
# 	"IF (cast(SQL_INS_to_SQL_TRANSFORM_LOG___DUPLICATE_ROW_CNT as int) > 0, ABORT ( 'There are duplicates rows in the table' ), IF (SQL_INS_to_SQL_TRANSFORM_LOG___SQL_TRANSFORM_ERROR IS NOT NULL, ABORT ( 'There is an error in the INSERT statement' ), NULL)) as ABORT_SESSION"
# )

# # COMMAND ----------

# # Processing node Shortcut_to_SQL_TRANSFORM_DUMMY_TARGET, type TARGET 
# # COLUMN COUNT: 3


# Shortcut_to_SQL_TRANSFORM_DUMMY_TARGET = EXP_ABORT_SESSION.selectExpr(
# 	"CAST(DUPLICATE_ROW_CNT AS STRING) as DUPLICATE_ROW_CNT",
# 	"CAST(SQL_TRANSFORM_ERROR AS STRING) as SQL_TRANSFORM_ERROR",
# 	"CAST(ABORT_SESSION AS STRING) as ABORT_SESSION"
# )
# Shortcut_to_SQL_TRANSFORM_DUMMY_TARGET.write.saveAsTable(f'{raw}.SQL_TRANSFORM_DUMMY_TARGET')
