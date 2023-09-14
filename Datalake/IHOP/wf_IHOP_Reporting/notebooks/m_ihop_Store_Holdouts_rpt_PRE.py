# Databricks notebook source
# Code converted on 2023-08-24 13:53:58
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

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

(username,password,connection_string) = mtx_prd_sqlServer(env)

# COMMAND ----------

# Processing node SQ_Shortcut_to_StoreHoldOuts, type SOURCE 
# COLUMN COUNT: 13

SQ_Shortcut_to_StoreHoldOuts = jdbcSqlServerConnection(f"""(SELECT
StoreHoldOuts.StoreHoldOutId,
StoreHoldOuts.StoreId,
StoreHoldOuts.SKU,
StoreHoldOuts.EventId,
StoreHoldOuts.HoldQuantity,
StoreHoldOuts.BeginDate,
StoreHoldOuts.EndDate,
StoreHoldOuts.FlaggedForRemoval,
StoreHoldOuts.LastIssuedOn,
StoreHoldOuts.UserKey,
StoreHoldOuts.IsPending,
StoreHoldOuts.ModifiedBy,
StoreHoldOuts.ModifiedOn
FROM HolidayPlanning.dbo.StoreHoldOuts) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_StoreHoldOuts_temp = SQ_Shortcut_to_StoreHoldOuts.toDF(*["SQ_Shortcut_to_StoreHoldOuts___" + col for col in SQ_Shortcut_to_StoreHoldOuts.columns])

EXPTRANS = SQ_Shortcut_to_StoreHoldOuts_temp.selectExpr(
	"SQ_Shortcut_to_StoreHoldOuts___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_StoreHoldOuts___StoreHoldOutId as StoreHoldOutId",
	"SQ_Shortcut_to_StoreHoldOuts___StoreId as StoreId",
	"SQ_Shortcut_to_StoreHoldOuts___SKU as SKU",
	"SQ_Shortcut_to_StoreHoldOuts___EventId as EventId",
	"SQ_Shortcut_to_StoreHoldOuts___HoldQuantity as HoldQuantity",
	"SQ_Shortcut_to_StoreHoldOuts___BeginDate as BeginDate",
	"SQ_Shortcut_to_StoreHoldOuts___EndDate as EndDate",
	"SQ_Shortcut_to_StoreHoldOuts___FlaggedForRemoval as FlaggedForRemoval",
	"SQ_Shortcut_to_StoreHoldOuts___LastIssuedOn as LastIssuedOn",
	"SQ_Shortcut_to_StoreHoldOuts___UserKey as UserKey",
	"SQ_Shortcut_to_StoreHoldOuts___IsPending as IsPending",
	"SQ_Shortcut_to_StoreHoldOuts___ModifiedBy as ModifiedBy",
	"SQ_Shortcut_to_StoreHoldOuts___ModifiedOn as ModifiedOn",
	"CURRENT_TIMESTAMP as INSERT_DT"
)

# COMMAND ----------

# Processing node Shortcut_to_IHOP_STORE_HOLDOUTS_RPT_PRE, type TARGET 
# COLUMN COUNT: 14


Shortcut_to_IHOP_STORE_HOLDOUTS_RPT_PRE = EXPTRANS.selectExpr(
	"CAST(StoreHoldOutId AS INT) as STORE_HOLDOUT_ID",
	"CAST(StoreId AS INT) as STORE_ID",
	"CAST(SKU AS INT) as SKU_NBR",
	"CAST(EventId AS INT) as EVENT_ID",
	"CAST(HoldQuantity AS INT) as HOLD_QTY",
	"CAST(BeginDate AS TIMESTAMP) as BEGIN_DATE",
	"CAST(EndDate AS TIMESTAMP) as END_DATE",
	"CAST(FlaggedForRemoval AS TINYINT) as FLAGGED_FOR_REMOVAL",
	"CAST(LastIssuedOn AS TIMESTAMP) as LAST_ISSUED_ON",
	"CAST(UserKey AS STRING) as USER_KEY",
	"CAST(IsPending AS TINYINT) as IS_PENDING",
	"CAST(ModifiedBy AS STRING) as MODIFIEDBY",
	"CAST(ModifiedOn AS TIMESTAMP) as MODIFIEDON",
	"CAST(INSERT_DT AS TIMESTAMP) as INSERT_DT"
)
# overwriteDeltaPartition(Shortcut_to_IHOP_STORE_HOLDOUTS_RPT_PRE,'DC_NBR',dcnbr,f'{raw}.IHOP_STORE_HOLDOUTS_RPT_PRE')
Shortcut_to_IHOP_STORE_HOLDOUTS_RPT_PRE.write.mode("overwrite").saveAsTable(f'{raw}.IHOP_STORE_HOLDOUTS_RPT_PRE')

# COMMAND ----------


