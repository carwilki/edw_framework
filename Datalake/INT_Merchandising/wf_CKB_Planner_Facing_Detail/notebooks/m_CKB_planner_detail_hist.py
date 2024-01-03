# Databricks notebook source
# Code converted on 2023-10-27 08:40:26
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
from Datalake.utils.pk import *

# COMMAND ----------

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'

(username,password,connection_string) = ckb_prd_sqlServer(env)


# COMMAND ----------

# Processing node SQ_Shortcut_to_SKU_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SKU_PROFILE = spark.sql(f"""SELECT
SKU_PROFILE.PRODUCT_ID,
SKU_PROFILE.SKU_NBR
FROM {legacy}.SKU_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_pm_dmd_planner_detail, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_pm_dmd_planner_detail = jdbcSqlServerConnection(f"""(SELECT
pm_dmd_planner_detail.LoadTime,
pm_dmd_planner_detail.Store,
pm_dmd_planner_detail.PogDBKey,
pm_dmd_planner_detail.PogVersionKey,
pm_dmd_planner_detail.PogGroup,
pm_dmd_planner_detail.SKU,
pm_dmd_planner_detail.StartDate,
pm_dmd_planner_detail.CalcEndDate,
pm_dmd_planner_detail.EOT_Flag,
pm_dmd_planner_detail.MinFill,
pm_dmd_planner_detail.Capacity
FROM CKB_PRD.dbo.pm_dmd_planner_detail) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_INT_CONV, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_pm_dmd_planner_detail_temp = SQ_Shortcut_to_pm_dmd_planner_detail.toDF(*["SQ_Shortcut_to_pm_dmd_planner_detail___" + col for col in SQ_Shortcut_to_pm_dmd_planner_detail.columns])

EXP_INT_CONV = SQ_Shortcut_to_pm_dmd_planner_detail_temp.selectExpr(
	"SQ_Shortcut_to_pm_dmd_planner_detail___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_pm_dmd_planner_detail___LoadTime as LoadTime",
	"cast(SQ_Shortcut_to_pm_dmd_planner_detail___Store as int) as o_Store",
	"SQ_Shortcut_to_pm_dmd_planner_detail___PogDBKey as PogDBKey",
	"SQ_Shortcut_to_pm_dmd_planner_detail___PogVersionKey as PogVersionKey",
	"SQ_Shortcut_to_pm_dmd_planner_detail___PogGroup as PogGroup",
	"cast(SQ_Shortcut_to_pm_dmd_planner_detail___SKU as int) as o_SKU",
	"SQ_Shortcut_to_pm_dmd_planner_detail___StartDate as StartDate",
	"SQ_Shortcut_to_pm_dmd_planner_detail___CalcEndDate as CalcEndDate",
	"SQ_Shortcut_to_pm_dmd_planner_detail___EOT_Flag as EOT_Flag",
	"SQ_Shortcut_to_pm_dmd_planner_detail___MinFill as MinFill",
	"SQ_Shortcut_to_pm_dmd_planner_detail___Capacity as Capacity"
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE = spark.sql(f"""SELECT
SITE_PROFILE.LOCATION_ID,
SITE_PROFILE.STORE_NBR
FROM {legacy}.SITE_PROFILE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_SITE_PROFILE, type JOINER 
# COLUMN COUNT: 13

JNR_SITE_PROFILE = SQ_Shortcut_to_SITE_PROFILE.join(EXP_INT_CONV,[SQ_Shortcut_to_SITE_PROFILE.STORE_NBR == EXP_INT_CONV.o_Store],'inner')

# COMMAND ----------

# Processing node JNR_SKU_PROFILE, type JOINER 
# COLUMN COUNT: 13

JNR_SKU_PROFILE = SQ_Shortcut_to_SKU_PROFILE.join(JNR_SITE_PROFILE,[SQ_Shortcut_to_SKU_PROFILE.SKU_NBR == JNR_SITE_PROFILE.o_SKU],'inner')

# COMMAND ----------

# Processing node EXP_SNAPSHOT_DT, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
JNR_SKU_PROFILE_temp = JNR_SKU_PROFILE.toDF(*["JNR_SKU_PROFILE___" + col for col in JNR_SKU_PROFILE.columns])

EXP_SNAPSHOT_DT = JNR_SKU_PROFILE_temp.selectExpr(
	# "JNR_SKU_PROFILE___sys_row_id as sys_row_id",
	"current_timestamp as o_SNAPSHOT_DT",
	"JNR_SKU_PROFILE___LOCATION_ID as LOCATION_ID",
	"JNR_SKU_PROFILE___LoadTime as LoadTime",
	"JNR_SKU_PROFILE___PogDBKey as PogDBKey",
	"JNR_SKU_PROFILE___PogVersionKey as PogVersionKey",
	"JNR_SKU_PROFILE___PogGroup as PogGroup",
	"JNR_SKU_PROFILE___StartDate as StartDate",
	"JNR_SKU_PROFILE___CalcEndDate as CalcEndDate",
	"DECODE ( LTRIM ( RTRIM ( UPPER ( JNR_SKU_PROFILE___EOT_Flag ) ) ) , 'Y','1' , '1','1','0' ) as o_EOT_Flag",
	"JNR_SKU_PROFILE___MinFill as MinFill",
	"JNR_SKU_PROFILE___Capacity as Capacity",
	"JNR_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID"
)

# COMMAND ----------

# Processing node Shortcut_to_CKB_PLANNER_DETAIL_HIST, type TARGET 
# COLUMN COUNT: 13


Shortcut_to_CKB_PLANNER_DETAIL_HIST = EXP_SNAPSHOT_DT.selectExpr(
	"CAST(o_SNAPSHOT_DT AS DATE) as SNAPSHOT_DT",
	"CAST(LOCATION_ID AS INT) as LOCATION_ID",
	"CAST(PogDBKey AS INT) as POG_DBKEY",
	"CAST(PogVersionKey AS INT) as POG_VERSION_KEY",
	"CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
	"CAST(StartDate AS DATE) as EFF_START_DT",
	"CAST(CalcEndDate AS DATE) as EFF_END_DT",
	"CAST(PogGroup AS STRING) as POG_GROUP",
	"CAST(o_EOT_FLAG AS TINYINT) as EOT_FLAG",
	"CAST(MinFill AS INT) as MIN_FILL",
	"CAST(Capacity AS INT) as CAPACITY",
	"CAST(LoadTime AS TIMESTAMP) as CKB_LOAD_TSTMP",
	"CAST(current_timestamp() AS TIMESTAMP) as LOAD_TSTMP"
)
try:
	chk=DuplicateChecker()
	chk.check_for_duplicate_primary_keys(Shortcut_to_CKB_PLANNER_DETAIL_HIST,["SNAPSHOT_DT","LOCATION_ID","POG_DBKEY","POG_VERSION_KEY","PRODUCT_ID","EFF_START_DT"])
	Shortcut_to_CKB_PLANNER_DETAIL_HIST.write.mode("append").saveAsTable(f'{legacy}.CKB_PLANNER_DETAIL_HIST')
except Exception as e:
	raise e
