# Databricks notebook source
# Code converted on 2023-08-24 13:53:59
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

# Processing node SQ_Shortcut_to_DistributionCenterHoldOuts, type SOURCE 
# COLUMN COUNT: 14

SQ_Shortcut_to_DistributionCenterHoldOuts = jdbcSqlServerConnection(f"""(SELECT
DistributionCenterHoldOuts.DistributionCenterHoldOutId,
DistributionCenterHoldOuts.DistributionCenterId,
DistributionCenterHoldOuts.SKU,
DistributionCenterHoldOuts.EventId,
DistributionCenterHoldOuts.ReferenceCode,
DistributionCenterHoldOuts.HoldQuantity,
DistributionCenterHoldOuts.BeginDate,
DistributionCenterHoldOuts.EndDate,
DistributionCenterHoldOuts.FlaggedForRemoval,
DistributionCenterHoldOuts.LastIssuedOn,
DistributionCenterHoldOuts.UserKey,
DistributionCenterHoldOuts.IsPending,
DistributionCenterHoldOuts.ModifiedBy,
DistributionCenterHoldOuts.ModifiedOn
FROM HolidayPlanning.dbo.DistributionCenterHoldOuts) as src""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_DistributionCenterHoldOuts_temp = SQ_Shortcut_to_DistributionCenterHoldOuts.toDF(*["SQ_Shortcut_to_DistributionCenterHoldOuts___" + col for col in SQ_Shortcut_to_DistributionCenterHoldOuts.columns])

EXPTRANS = SQ_Shortcut_to_DistributionCenterHoldOuts_temp.selectExpr(
	"SQ_Shortcut_to_DistributionCenterHoldOuts___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___DistributionCenterHoldOutId as DistributionCenterHoldOutId",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___DistributionCenterId as DistributionCenterId",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___SKU as SKU",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___EventId as EventId",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___ReferenceCode as ReferenceCode",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___HoldQuantity as HoldQuantity",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___BeginDate as BeginDate",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___EndDate as EndDate",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___FlaggedForRemoval as FlaggedForRemoval",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___LastIssuedOn as LastIssuedOn",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___UserKey as UserKey",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___IsPending as IsPending",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___ModifiedBy as ModifiedBy",
	"SQ_Shortcut_to_DistributionCenterHoldOuts___ModifiedOn as ModifiedOn",
	"CURRENT_TIMESTAMP as INSERT_DT"
)

# COMMAND ----------

# Processing node Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE, type TARGET 
# COLUMN COUNT: 15


Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE = EXPTRANS.selectExpr(
	"CAST(DistributionCenterHoldOutId AS INT) as DC_HOLDOUT_ID",
	"CAST(DistributionCenterId AS INT) as DC_ID",
	"CAST(SKU AS INT) as SKU_NBR",
	"CAST(EventId AS INT) as EVENT_ID",
	"CAST(ReferenceCode AS STRING) as REFERENCE_CD",
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
# overwriteDeltaPartition(Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE,'DC_NBR',dcnbr,f'{raw}.IHOP_DC_HOLDOUTS_RPT_PRE')
Shortcut_to_IHOP_DC_HOLDOUTS_RPT_PRE.write.mode("overwrite").saveAsTable(f'{raw}.IHOP_DC_HOLDOUTS_RPT_PRE')

# COMMAND ----------


