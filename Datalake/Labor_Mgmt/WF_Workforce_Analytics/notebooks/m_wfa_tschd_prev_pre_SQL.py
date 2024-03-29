#Code converted on 2023-08-08 15:42:31
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

parser = argparse.ArgumentParser()
spark = SparkSession.getActiveSession()
parser.add_argument('env', type=str, help='Env Variable')

args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
empl_protected = getEnvPrefix(env) + 'empl_protected'


# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TSCHD_PRE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_WFA_TSCHD_PRE = spark.sql(f"""SELECT CURRENT_TIMESTAMP AS start_tstmp
      ,'WFA_TSCHD_PREV_PRE' AS table_name
      ,COUNT (*) AS begin_row_cnt
  FROM {empl_protected}.raw_wfa_tschd_pre""").withColumn("sys_row_id", monotonically_increasing_id())
# Conforming fields names to the component layout
SQ_Shortcut_to_WFA_TSCHD_PRE = SQ_Shortcut_to_WFA_TSCHD_PRE \
	.withColumnRenamed(SQ_Shortcut_to_WFA_TSCHD_PRE.columns[0],'START_TSTMP') \
	.withColumnRenamed(SQ_Shortcut_to_WFA_TSCHD_PRE.columns[1],'TABLE_NAME') \
	.withColumnRenamed(SQ_Shortcut_to_WFA_TSCHD_PRE.columns[2],'BEGIN_ROW_CNT')

# COMMAND ----------
# Processing node SQL_INS_and_DUPS_CHECK, type SQL_TRANSFORM 
# COLUMN COUNT: 9
spark.sql(f"""TRUNCATE table {empl_protected}.raw_wfa_tschd_prev_pre""")

spark.sql(f"""INSERT INTO {empl_protected}.raw_wfa_tschd_prev_pre
SELECT TSCHD_ID,
       TSCHD_DAT,
       EMP_SKEY,
       STRT_DTM,
       END_DTM,
       PAYCD_SKEY,
       CORE_HRS_SWT,
       PRI_ORG_SKEY,
       ORG_SKEY,
       SHIFT_ID,
       MONEY_AMT,
       DRTN_AMT,
       CORE_AMT,
       NON_CORE_AMT,
       DRTN_HRS,
       CORE_HRS,
       NON_CORE_HRS
  FROM {empl_protected}.raw_WFA_TSCHD_PRE""")
  
  
# spark.sql(f"""SELECT COUNT(*) AS DUPLICATE_ROWS
#   FROM (SELECT TSCHD_ID,
#                TSCHD_DAT,
#                COUNT(*) AS CNT
#           FROM {empl_protected}.raw_wfa_tschd_prev_pre
#          GROUP BY TSCHD_ID,
#                   TSCHD_DAT
#        ) T
#  WHERE CNT > 1""")

# # for each involved DataFrame, append the dataframe name to each column

# SQL_INS_and_DUPS_CHECK = SQ_Shortcut_to_WFA_TSCHD_PRE.selectExpr(
# 	"sys_row_id as sys_row_id"
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
# # Processing node Shortcut_to_SQL_TRANSFORM_DUMMY_TARGET1, type TARGET 
# # COLUMN COUNT: 3


# Shortcut_to_SQL_TRANSFORM_DUMMY_TARGET1 = EXP_ABORT_SESSION.selectExpr(
# 	"CAST(DUPLICATE_ROW_CNT AS STRING) as DUPLICATE_ROW_CNT",
# 	"CAST(SQL_TRANSFORM_ERROR AS STRING) as SQL_TRANSFORM_ERROR",
# 	"CAST(ABORT_SESSION AS STRING) as ABORT_SESSION"
# )
# Shortcut_to_SQL_TRANSFORM_DUMMY_TARGET1.write.saveAsTable(f'{raw}.SQL_TRANSFORM_DUMMY_TARGET')
# COMMAND ----------
