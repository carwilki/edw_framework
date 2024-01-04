# Databricks notebook source
#Code converted on 2023-09-12 13:31:05
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

# Read in relation source variables
# (username, password, connection_string) = mtx_prd_sqlServer(env)
# db_name = "CKB_PRD"

# COMMAND ----------

# Processing node SQ_Shortcut_to_ZTB_POG_GRP_RLS_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_ZTB_POG_GRP_RLS_PRE = spark.sql(f"""SELECT DISTINCT
FLRPLN_EXCLUSION,
POG_GROUP
FROM {raw}.ZTB_POG_GRP_RLS_PRE
WHERE TRIM(FLRPLN_EXCLUSION) <> ''""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_POG_GROUP_EXCLUSION_LIST, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_POG_GROUP_EXCLUSION_LIST = spark.sql(f"""SELECT
FLOORPLAN_NM,
POG_GROUP_DESC,
DELETE_TSTMP,
LOAD_TSTMP
FROM {legacy}.POG_GROUP_EXCLUSION_LIST""")  #.withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_All_POGs, type JOINER 
# COLUMN COUNT: 6

JNR_All_POGs = SQ_Shortcut_to_POG_GROUP_EXCLUSION_LIST.join(SQ_Shortcut_to_ZTB_POG_GRP_RLS_PRE,[SQ_Shortcut_to_POG_GROUP_EXCLUSION_LIST.FLOORPLAN_NM == SQ_Shortcut_to_ZTB_POG_GRP_RLS_PRE.FLRPLN_EXCLUSION, SQ_Shortcut_to_POG_GROUP_EXCLUSION_LIST.POG_GROUP_DESC == SQ_Shortcut_to_ZTB_POG_GRP_RLS_PRE.POG_GROUP],'fullouter')

# COMMAND ----------


# Processing node EXP_InsUpdDel, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
JNR_All_POGs_temp = JNR_All_POGs.toDF(*["JNR_All_POGs___" + col for col in JNR_All_POGs.columns])

EXP_InsUpdDel = JNR_All_POGs_temp.selectExpr(
	"JNR_All_POGs___sys_row_id as sys_row_id",
	"JNR_All_POGs___FLRPLN_EXCLUSION as i_FLRPLN_EXCLUSION",
	"JNR_All_POGs___POG_GROUP as i_POG_GROUP",
	"JNR_All_POGs___FLOORPLAN_NM as i_FLOORPLAN_NM",
	"JNR_All_POGs___POG_GROUP_DESC as i_POG_GROUP_DESC",
	"JNR_All_POGs___DELETE_TSTMP as i_DELETE_TSTMP",
	"IF (JNR_All_POGs___FLRPLN_EXCLUSION IS NULL, JNR_All_POGs___FLOORPLAN_NM, JNR_All_POGs___FLRPLN_EXCLUSION) as FLOORPLAN_NM",
	"IF (JNR_All_POGs___POG_GROUP IS NULL, JNR_All_POGs___POG_GROUP_DESC, JNR_All_POGs___POG_GROUP) as POG_GROUP_DESC",
	"IF (JNR_All_POGs___POG_GROUP IS NULL AND JNR_All_POGs___POG_GROUP_DESC IS NOT NULL AND JNR_All_POGs___DELETE_TSTMP IS NULL, CURRENT_TIMESTAMP, NULL) as DELETE_TSTMP",
	"IF (JNR_All_POGs___LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNR_All_POGs___LOAD_TSTMP) as LOAD_TSTMP",
	"IF (JNR_All_POGs___FLOORPLAN_NM IS NULL, 1, 2) as ACTION_FLAG"
)

# COMMAND ----------

# Processing node UPD_Define_Action, type UPDATE_STRATEGY 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
EXP_InsUpdDel_temp = EXP_InsUpdDel.toDF(*["EXP_InsUpdDel___" + col for col in EXP_InsUpdDel.columns])

UPD_Define_Action = EXP_InsUpdDel_temp.selectExpr(
	"EXP_InsUpdDel___FLOORPLAN_NM as FLOORPLAN_NM",
	"EXP_InsUpdDel___POG_GROUP_DESC as POG_GROUP_DESC",
	"EXP_InsUpdDel___DELETE_TSTMP as DELETE_TSTMP",
	"EXP_InsUpdDel___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_InsUpdDel___ACTION_FLAG as ACTION_FLAG",
	"EXP_InsUpdDel___i_FLRPLN_EXCLUSION as i_FLRPLN_EXCLUSION",
	"EXP_InsUpdDel___i_POG_GROUP as i_POG_GROUP",
	"EXP_InsUpdDel___i_FLOORPLAN_NM as i_FLOORPLAN_NM",
	"EXP_InsUpdDel___i_POG_GROUP_DESC as i_POG_GROUP_DESC",
	"EXP_InsUpdDel___i_DELETE_TSTMP as i_DELETE_TSTMP") \
	.withColumn('pyspark_data_action', 
                   when(col("i_POG_GROUP").isNull() & col("i_POG_GROUP_DESC").isNotNull(), lit(0))
                   .when(col("i_POG_GROUP").isNotNull() & col("i_POG_GROUP_DESC").isNull() & col("i_DELETE_TSTMP").isNull(), lit(1))
                   .when(col("i_POG_GROUP").isNotNull() & col("i_DELETE_TSTMP").isNotNull(), lit(1))
                   .otherwise(lit(3)))

# COMMAND ----------

# Processing node Shortcut_to_POG_GROUP_EXCLUSION_LIST1, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_POG_GROUP_EXCLUSION_LIST1 = UPD_Define_Action.selectExpr(
	"CAST(FLOORPLAN_NM AS STRING) as FLOORPLAN_NM",
	"CAST(POG_GROUP_DESC AS STRING) as POG_GROUP_DESC",
	"CAST(DELETE_TSTMP AS TIMESTAMP) as DELETE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.FLOORPLAN_NM = target.FLOORPLAN_NM AND source.POG_GROUP_DESC = target.POG_GROUP_DESC"""
	refined_perf_table = f"{legacy}.POG_GROUP_EXCLUSION_LIST"
	executeMerge(Shortcut_to_POG_GROUP_EXCLUSION_LIST1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("POG_GROUP_EXCLUSION_LIST", "POG_GROUP_EXCLUSION_LIST", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("POG_GROUP_EXCLUSION_LIST", "POG_GROUP_EXCLUSION_LIST","Failed",str(e), f"{raw}.log_run_details", )
	raise e
		

# COMMAND ----------


