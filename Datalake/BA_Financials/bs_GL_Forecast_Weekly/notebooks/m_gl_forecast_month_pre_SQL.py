# Databricks notebook source
#Code converted on 2023-08-17 15:36:58
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

# dbutils.widgets.text(name='PMWorkflowName', defaultValue='bs_GL_Forecast_Weekly')
# PMWorkflowName = dbutils.widgets.get('PMWorkflowName')

# dbutils.widgets.text(name='PMSessionName', defaultValue='s_gl_forecast_month_pre_SQL')
# PMSessionName = dbutils.widgets.get('PMSessionName')

# dbutils.widgets.text(name='PMMappingName', defaultValue='m_gl_forecast_month_pre_SQL')
# PMMappingName = dbutils.widgets.get('PMMappingName')


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
#        'GL_FORECAST_MONTH_PRE' AS TABLE_NAME,
#        COUNT(*) AS BEGIN_ROW_CNT
# FROM {raw}.GL_FORECAST_MONTH_PRE""").withColumn("sys_row_id", monotonically_increasing_id())
  
# # Conforming fields names to the component layout
# SQ_Shortcut_to_GL_FORECAST_PRE = SQ_Shortcut_to_GL_FORECAST_PRE \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_FORECAST_PRE.columns[0],'START_TSTMP') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_FORECAST_PRE.columns[1],'TABLE_NAME') \
# 	.withColumnRenamed(SQ_Shortcut_to_GL_FORECAST_PRE.columns[2],'BEGIN_ROW_CNT')

# COMMAND ----------

# Processing node SQL_INS_and_DUPS_CHECK, type SQL_TRANSFORM 
# COLUMN COUNT: 9

_sql = f"""
INSERT OVERWRITE {raw}.GL_FORECAST_MONTH_PRE
SELECT
    FISCAL_MO,
    GL_ACCT_NBR,
    GL_CAT_CD,
    PROFIT_CTR,
    LOCATION_ID,
    CURRENCY_ID,
    GL_FORECAST_AMT_US AS FORECAST_AMT_US,
    GL_FORECAST_AMT_LOC AS FORECAST_AMT_LOC
FROM (
    SELECT
        PRE.FISCAL_MO,
        PRE.GL_ACCT_NBR,
        PRE.GL_CAT_CD,
        PRE.COMPANY_NBR,
        CASE
            WHEN PRE.STORE_NBR > 8000 AND PRE.STORE_NBR < 9000
            THEN CONCAT('000000', CAST(PRE.STORE_NBR AS STRING))
            ELSE CONCAT(LTRIM(CAST(PRE.STORE_NBR AS STRING), '0'), '-', PRE.GL_CAT_CD)
        END AS PROFIT_CTR,
        SUM(CASE WHEN CURRENCY_ID = 'USD' THEN GL_FORECAST_AMT END) AS GL_FORECAST_AMT_US,
        COALESCE(SUM(CASE WHEN CURRENCY_ID = 'CAD' THEN GL_FORECAST_AMT END),
                 SUM(CASE WHEN CURRENCY_ID = 'USD' THEN GL_FORECAST_AMT END)) AS GL_FORECAST_AMT_LOC
    FROM (
        SELECT
            FISCAL_MO,
            GL_ACCT_NBR,
            GL_CAT_CD,
            COMPANY_NBR,
            STORE_NBR,
            CURRENCY_ID,
            GL_FORECAST_AMT
        FROM {raw}.GL_FORECAST_PRE
    ) PRE
    GROUP BY
        PRE.FISCAL_MO,
        PRE.GL_ACCT_NBR,
        PRE.GL_CAT_CD,
        PRE.COMPANY_NBR,
        PRE.STORE_NBR,
        PRE.CURRENCY_ID
) GRP
JOIN {legacy}.GL_PROFIT_CENTER P
ON GRP.PROFIT_CTR = P.GL_PROFIT_CTR_CD
WHERE GL_FORECAST_AMT_US IS NOT NULL
ORDER BY GRP.PROFIT_CTR, GL_ACCT_NBR
"""

spark.sql(_sql)

# _sql = f"""
# SELECT COUNT(*) AS DUPLICATE_ROWS
#   FROM (SELECT FISCAL_MO,
#                GL_ACCT_NBR,
#                GL_CATEGORY_CD,
#                PROFIT_CTR,
#                COUNT(*) AS CNT
#           FROM {raw}.GL_FORECAST_MONTH_PRE
#          GROUP BY FISCAL_MO,
#                   GL_ACCT_NBR,
#                   GL_CATEGORY_CD,
#                   PROFIT_CTR
# ) T
# WHERE CNT > 1
# """

# spark.sql(_sql)

# COMMAND ----------

# quitting here as the rest seems to be irrelevant to the workflow
# otherwise this command / code blocked should be removed

try:
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)
    in_databricks_notebook = True
    dbutils.notebook.exit(0)
except ImportError:
    in_databricks_notebook = False
    sys.exit()



# COMMAND ----------

# for each involved DataFrame, append the dataframe name to each column

# SQL_INS_and_DUPS_CHECK = SQ_Shortcut_to_GL_FORECAST_PRE.selectExpr(
# 	"SQ_Shortcut_to_GL_FORECAST_PRE___sys_row_id as sys_row_id"
# )

# COMMAND ----------

# Processing node EXP_GET_SESSION_INFO, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
# SQL_INS_and_DUPS_CHECK_temp = SQL_INS_and_DUPS_CHECK.toDF(*["SQL_INS_and_DUPS_CHECK___" + col for col in SQL_INS_and_DUPS_CHECK.columns])

# EXP_GET_SESSION_INFO = SQL_INS_and_DUPS_CHECK_temp.selectExpr(
# 	"SQL_INS_and_DUPS_CHECK___START_TSTMP_output as i_START_TSTMP",
# 	"SQL_INS_and_DUPS_CHECK___TABLE_NAME_output as TABLE_NAME",
# 	"SQL_INS_and_DUPS_CHECK___BEGIN_ROW_CNT_output as BEGIN_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___NumRowsAffected as INSERT_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___DUPLICATE_ROW_CNT as DUPLICATE_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___SQLError as i_SQL_TRANSFORM_ERROR",
# 	"SQL_INS_and_DUPS_CHECK___sys_row_id as sys_row_id",
# 	"date_format(SQL_INS_and_DUPS_CHECK___i_START_TSTMP, 'MM/DD/YYYY HH24:MI:SS') as START_TSTMP",
# 	"date_format(CURRENT_TIMESTAMP, 'MM/DD/YYYY HH24:MI:SS') as END_TSTMP",
# 	f"'{PMWorkflowName}' as WORKFLOW_NAME",
# 	f"'{PMSessionName}' as SESSION_NAME",
# 	f"'{PMMappingName}' as MAPPING_NAME",
# 	"SQL_INS_and_DUPS_CHECK___TABLE_NAME as TABLE_NAME",
# 	"SQL_INS_and_DUPS_CHECK___BEGIN_ROW_CNT as BEGIN_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___INSERT_ROW_CNT as INSERT_ROW_CNT",
# 	"SQL_INS_and_DUPS_CHECK___DUPLICATE_ROW_CNT as DUPLICATE_ROW_CNT",
# 	"IF (SQL_INS_and_DUPS_CHECK___DUPLICATE_ROW_CNT > 0, 'There are duplicate records in the table', SQL_INS_and_DUPS_CHECK___i_SQL_TRANSFORM_ERROR) as SQL_TRANSFORM_ERROR"
# )

# COMMAND ----------

# Processing node AGG, type AGGREGATOR . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

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
# 	.agg(max(col('i_END_TSTMP')).alias("END_TSTMP"),
#          max(col('i_BEGIN_ROW_CNT')).cast(StringType()).alias("BEGIN_ROW_CNT"), 
#          sum(col('i_INSERT_ROW_CNT')).cast(StringType()).alias("INSERT_ROW_CNT"), 
#          max(i_SQL_TRANSFORM_ERROR).alias("SQL_TRANSFORM_ERROR"), 
#          sum(col('i_DUPLICATE_ROW_CNT')).cast(StringType()).alias("DUPLICATE_ROW_CNT") 
# 	).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_CREATE_INS_SQL, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
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
# 	concat(
#         lit("INSERT INTO SQL_TRANSFORM_LOG VALUES (TO_DATE('"),
#         col("AGG___START_TSTMP"),
#         lit("','MM/DD/YYYY HH24:MI:SS'),TO_DATE('"),
#         col("AGG___END_TSTMP"),
#         lit("','MM/DD/YYYY HH24:MI:SS'), '"),
#         col("AGG___WORKFLOW_NAME"),
#         lit("', '"),
#         col("AGG___SESSION_NAME"),
#         lit("', '"),
#         col("AGG___MAPPING_NAME"),
#         lit("', '"),
#         col("AGG___TABLE_NAME"),
#         lit("', '"),
#         col("AGG___BEGIN_ROW_CNT"),
#         lit("', '"),
#         col("AGG___INSERT_ROW_CNT"),
#         lit("', '"),
#         col("AGG___DUPLICATE_ROW_CNT"),
#         lit("',  '"),
#         col("AGG___SQL_TRANSFORM_ERROR"),
#         lit("')')"
#     ).alias("INSERT_SQL")
# )

# # COMMAND ----------

# # Processing node SQL_INS_to_SQL_TRANSFORM_LOG, type SQL_TRANSFORM 
# # COLUMN COUNT: 14

# # """
# # WARNING: SQL Transformation is not yet supported, producing passthrough dataframe:
# # SQL query:


# # """

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
# 	"SQL_INS_to_SQL_TRANSFORM_LOG___SQL_TRANSFORM_ERROR_output as SQL_TRANSFORM_ERROR",
# 	"SQL_INS_to_SQL_TRANSFORM_LOG___sys_row_id as sys_row_id",
# 	"SQL_INS_to_SQL_TRANSFORM_LOG___DUPLICATE_ROW_CNT as DUPLICATE_ROW_CNT",
# 	"SQL_INS_to_SQL_TRANSFORM_LOG___SQL_TRANSFORM_ERROR as SQL_TRANSFORM_ERROR",
# 	expr("""
#         IF(
#             CAST(SQL_INS_to_SQL_TRANSFORM_LOG___DUPLICATE_ROW_CNT_output AS INT) > 0,
#             'There are duplicates rows in the table',
#             IF(
#                 SQL_INS_to_SQL_TRANSFORM_LOG___SQL_TRANSFORM_ERROR_output IS NOT NULL,
#                 'There is an error in the INSERT statement',
#                 NULL
#             )
#         ) AS ABORT_SESSION
#     """)
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
