# Databricks notebook source
# Code converted on 2023-08-03 11:48:57
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
# spark = SparkSession.getActiveSession()
# parser.add_argument("env", type=str, help="Env Variable")

# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"
enterprise = getEnvPrefix(env) + "enterprise"


# COMMAND ----------

# Processing node SQ_Shortcut_to_DAYS, type SOURCE
# COLUMN COUNT: 3

SQ_Shortcut_to_DAYS = spark.sql(
    f"""SELECT
DAYS.DAY_DT,
DAYS.HOLIDAY_FLAG,
DAYS.DAY_OF_WK_NBR
FROM {enterprise}.DAYS"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node Fil_Day_Dt, type FILTER
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_DAYS_temp = SQ_Shortcut_to_DAYS.toDF(
    *["SQ_Shortcut_to_DAYS___" + col for col in SQ_Shortcut_to_DAYS.columns]
)

Fil_Day_Dt = (
    SQ_Shortcut_to_DAYS_temp.selectExpr(
        "SQ_Shortcut_to_DAYS___DAY_DT as DAY_DT",
        "SQ_Shortcut_to_DAYS___DAY_OF_WK_NBR as DAY_OF_WK_NBR",
        "SQ_Shortcut_to_DAYS___HOLIDAY_FLAG as HOLIDAY_FLAG",
    )
    .filter("DAY_DT >= current_date()  AND DAY_DT < DATE_ADD (current_date(), 15 )")
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node SQ_Shortcut_to_SITE_PROFILE_RPT, type SOURCE
# COLUMN COUNT: 2

SQ_Shortcut_to_SITE_PROFILE_RPT = spark.sql(
    f"""SELECT
SITE_PROFILE_RPT.LOCATION_TYPE_ID,
SITE_PROFILE_RPT.LOCATION_NBR
FROM {legacy}.SITE_PROFILE_RPT"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXPTRANS, type EXPRESSION
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
Fil_Day_Dt_temp = Fil_Day_Dt.toDF(
    *["Fil_Day_Dt___" + col for col in Fil_Day_Dt.columns]
)

EXPTRANS = Fil_Day_Dt_temp.selectExpr(
    "Fil_Day_Dt___sys_row_id as sys_row_id",
    "Fil_Day_Dt___DAY_DT as DAY_DT",
    "Fil_Day_Dt___DAY_OF_WK_NBR as DAY_OF_WK_NBR",
    "1 as Join",
    "Fil_Day_Dt___HOLIDAY_FLAG as HOLIDAY_FLAG",
)

# COMMAND ----------

# Processing node Fil_Location_Type, type FILTER
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SITE_PROFILE_RPT_temp = SQ_Shortcut_to_SITE_PROFILE_RPT.toDF(
    *[
        "SQ_Shortcut_to_SITE_PROFILE_RPT___" + col
        for col in SQ_Shortcut_to_SITE_PROFILE_RPT.columns
    ]
)

Fil_Location_Type = (
    SQ_Shortcut_to_SITE_PROFILE_RPT_temp.selectExpr(
        "SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
        "SQ_Shortcut_to_SITE_PROFILE_RPT___LOCATION_NBR as LOCATION_NBR",
    )
    .filter("LOCATION_TYPE_ID = 19")
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------

# Processing node EXPTRANS1, type EXPRESSION
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
Fil_Location_Type_temp = Fil_Location_Type.toDF(
    *["Fil_Location_Type___" + col for col in Fil_Location_Type.columns]
)

EXPTRANS1 = Fil_Location_Type_temp.selectExpr(
    "Fil_Location_Type___sys_row_id as sys_row_id",
    "Fil_Location_Type___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
    "Fil_Location_Type___LOCATION_NBR as LOCATION_NBR",
    "1 as Join",
)

# COMMAND ----------

# Processing node JNRTRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])
EXPTRANS1_temp = EXPTRANS1.toDF(*["EXPTRANS1___" + col for col in EXPTRANS1.columns])

JNRTRANS = EXPTRANS_temp.join(
    EXPTRANS1_temp,
    [EXPTRANS_temp.EXPTRANS___Join == EXPTRANS1_temp.EXPTRANS1___Join],
    "inner",
).selectExpr(
    "EXPTRANS1___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
    "EXPTRANS1___LOCATION_NBR as LOCATION_NBR",
    "EXPTRANS1___Join as Join",
    "EXPTRANS___sys_row_id as sys_row_id",
    "EXPTRANS___DAY_DT as DAY_DT",
    "EXPTRANS___DAY_OF_WK_NBR as DAY_OF_WK_NBR",
    "EXPTRANS___HOLIDAY_FLAG as HOLIDAY_FLAG",
    "EXPTRANS___Join as Join1",
)

# COMMAND ----------

# Processing node EXPTRANS2, type EXPRESSION
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
JNRTRANS_temp = JNRTRANS.toDF(*["JNRTRANS___" + col for col in JNRTRANS.columns])

EXPTRANS2 = JNRTRANS_temp.selectExpr(
    "JNRTRANS___sys_row_id as sys_row_id",
    "JNRTRANS___DAY_DT as DAY_DT",
    "'Vendor' as BUSINESS_AREA",
    "JNRTRANS___LOCATION_NBR as LOCATION_NBR",
    "JNRTRANS___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
    "0 as IS_CLOSE_FLAG",
    "JNRTRANS___DAY_DT as OPEN_TSTMP",
    "(DATE_ADD(JNRTRANS___DAY_DT, 1)) - INTERVAL 1 MILLISECOND as CLOSE_TSTMP",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)

# COMMAND ----------

# Processing node Shortcut_to_SITE_HOURS_DAY_PRE, type TARGET
# COLUMN COUNT: 8


Shortcut_to_SITE_HOURS_DAY_PRE = EXPTRANS2.selectExpr(
    "CAST(DAY_DT AS TIMESTAMP) as DAY_DT",
    "CAST(LOCATION_NBR AS STRING) as LOCATION_NBR",
    "CAST (LOCATION_TYPE_ID AS TINYINT) as LOCATION_TYPE_ID",
    "CAST(BUSINESS_AREA AS STRING) as BUSINESS_AREA",
    "CAST(OPEN_TSTMP AS TIMESTAMP) as OPEN_TSTMP",
    "CAST(CLOSE_TSTMP AS TIMESTAMP) as CLOSE_TSTMP",
    "CAST(IS_CLOSE_FLAG AS SMALLINT) as IS_CLOSED",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)
# overwriteDeltaPartition(Shortcut_to_SITE_HOURS_DAY_PRE,'DC_NBR',dcnbr,f'{raw}.SITE_HOURS_DAY_PRE')
Shortcut_to_SITE_HOURS_DAY_PRE.write.mode("append").saveAsTable(
    f"{raw}.SITE_HOURS_DAY_PRE"
)
