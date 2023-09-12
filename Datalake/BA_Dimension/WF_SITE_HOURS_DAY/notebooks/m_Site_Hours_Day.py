# Databricks notebook source
# Code converted on 2023-08-03 11:49:00
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


# COMMAND ----------
# Processing node LKP_SITE_PROFILE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 7

LKP_SITE_PROFILE_SRC = spark.sql(
    f"""SELECT
LOCATION_ID,
TIME_ZONE,
STORE_NBR,
LOCATION_NBR,
LOCATION_TYPE_ID
FROM {legacy}.SITE_PROFILE_RPT"""
)
# Conforming fields names to the component layout
LKP_SITE_PROFILE_SRC = (
    LKP_SITE_PROFILE_SRC.withColumnRenamed(
        LKP_SITE_PROFILE_SRC.columns[0], "LOCATION_ID"
    )
    .withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[4], "LOCATION_TYPE_ID")
    .withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[1], "TIME_ZONE")
    .withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[3], "LOCATION_NBR")
    .withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[2], "STORE_NBR")
    .withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[4], "LOCATION_TYPE_ID2")
    .withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[3], "LOCATION_NBR2")
)

# COMMAND ----------
# Processing node SQ_SITE_HOURS_DAY, type SOURCE
# COLUMN COUNT: 10

SQ_SITE_HOURS_DAY = spark.sql(
    f"""SELECT
SITE_HOURS_DAY.DAY_DT,
SITE_HOURS_DAY.LOCATION_ID,
SITE_HOURS_DAY.BUSINESS_AREA,
SITE_HOURS_DAY.LOCATION_TYPE_ID,
SITE_HOURS_DAY.STORE_NBR,
SITE_HOURS_DAY.CLOSE_FLAG,
SITE_HOURS_DAY.TIME_ZONE,
SITE_HOURS_DAY.OPEN_TSTMP,
SITE_HOURS_DAY.CLOSE_TSTMP,
SITE_HOURS_DAY.LOAD_TSTMP
FROM {legacy}.SITE_HOURS_DAY"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_SITE_HOURS_DAY_PRE, type SOURCE
# COLUMN COUNT: 7

SQ_SITE_HOURS_DAY_PRE = spark.sql(
    f"""SELECT
SITE_HOURS_DAY_PRE.DAY_DT,
SITE_HOURS_DAY_PRE.LOCATION_NBR,
SITE_HOURS_DAY_PRE.LOCATION_TYPE_ID,
SITE_HOURS_DAY_PRE.BUSINESS_AREA,
SITE_HOURS_DAY_PRE.OPEN_TSTMP,
SITE_HOURS_DAY_PRE.CLOSE_TSTMP,
SITE_HOURS_DAY_PRE.IS_CLOSED
FROM {legacy}.SITE_HOURS_DAY_PRE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Fil_Site_Hours_Day_1, type FILTER
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
SQ_SITE_HOURS_DAY_temp = SQ_SITE_HOURS_DAY.toDF(
    *["SQ_SITE_HOURS_DAY___" + col for col in SQ_SITE_HOURS_DAY.columns]
)

Fil_Site_Hours_Day_1 = (
    SQ_SITE_HOURS_DAY_temp.selectExpr(
        "SQ_SITE_HOURS_DAY___DAY_DT as DAY_DT",
        "SQ_SITE_HOURS_DAY___LOCATION_ID as LOCATION_ID",
        "SQ_SITE_HOURS_DAY___BUSINESS_AREA as BUSINESS_AREA",
        "SQ_SITE_HOURS_DAY___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
        "SQ_SITE_HOURS_DAY___STORE_NBR as STORE_NBR",
        "SQ_SITE_HOURS_DAY___CLOSE_FLAG as CLOSE_FLAG",
        "SQ_SITE_HOURS_DAY___TIME_ZONE as TIME_ZONE",
        "date_format(SQ_SITE_HOURS_DAY___OPEN_TSTMP, 'yyyy-MM-dd hh:MM:SS') as OPEN_TSTMP",
        "date_format(SQ_SITE_HOURS_DAY___CLOSE_TSTMP, 'yyyy-MM-dd hh:MM:SS') as CLOSE_TSTMP",
        "SQ_SITE_HOURS_DAY___LOAD_TSTMP as LOAD_TSTMP",
    )
    .filter("DAY_DT >= date_add( current_date() ,- 15)")
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------
# Processing node Exp_Site_Hours_Day, type EXPRESSION
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
Fil_Site_Hours_Day_1_temp = Fil_Site_Hours_Day_1.toDF(
    *["Fil_Site_Hours_Day_1___" + col for col in Fil_Site_Hours_Day_1.columns]
)

Exp_Site_Hours_Day = Fil_Site_Hours_Day_1_temp.selectExpr(
    "Fil_Site_Hours_Day_1___sys_row_id as sys_row_id",
    "Fil_Site_Hours_Day_1___DAY_DT as DAY_DT",
    "Fil_Site_Hours_Day_1___LOCATION_ID as LOCATION_ID",
    "Fil_Site_Hours_Day_1___BUSINESS_AREA as BUSINESS_AREA",
    "Fil_Site_Hours_Day_1___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
    "Fil_Site_Hours_Day_1___STORE_NBR as STORE_NBR",
    "Fil_Site_Hours_Day_1___CLOSE_FLAG as CLOSE_FLAG",
    "Fil_Site_Hours_Day_1___TIME_ZONE as TIME_ZONE",
    "Fil_Site_Hours_Day_1___OPEN_TSTMP as OPEN_TSTMP",
    "Fil_Site_Hours_Day_1___CLOSE_TSTMP as CLOSE_TSTMP",
    "Fil_Site_Hours_Day_1___LOAD_TSTMP as LOAD_TSTMP",
    "MD5 (concat_ws( cast(Fil_Site_Hours_Day_1___LOCATION_TYPE_ID as string), Fil_Site_Hours_Day_1___TIME_ZONE , Fil_Site_Hours_Day_1___OPEN_TSTMP , Fil_Site_Hours_Day_1___CLOSE_TSTMP, cast(Fil_Site_Hours_Day_1___CLOSE_FLAG as string), cast(Fil_Site_Hours_Day_1___STORE_NBR as string)) ) as _md5FINAL",
)


# COMMAND ----------
# Processing node LKP_SITE_PROFILE, type LOOKUP_FROM_PRECACHED_DATASET . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7


LKP_SITE_PROFILE_lookup_result = (
    SQ_SITE_HOURS_DAY_PRE.selectExpr(
        "LOCATION_TYPE_ID as LOCATION_TYPE_ID1",
        "LOCATION_NBR as LOCATION_NBR1",
        "sys_row_id",
    )
    .join(
        LKP_SITE_PROFILE_SRC,
        (col("LOCATION_NBR1") == col("LOCATION_NBR2"))
        & (col("LOCATION_TYPE_ID1") == col("LOCATION_TYPE_ID2")),
        "left",
    )
    .withColumn(
        "row_num_LOCATION_ID",
        row_number().over(Window.partitionBy("sys_row_id").orderBy("LOCATION_ID")),
    )
)

LKP_SITE_PROFILE = LKP_SITE_PROFILE_lookup_result.filter(
    col("row_num_LOCATION_ID") == 1
).select(
    LKP_SITE_PROFILE_lookup_result.sys_row_id,
    col("LOCATION_ID"),
    col("TIME_ZONE"),
    col("STORE_NBR"),
)

# COMMAND ----------
# Processing node Exp_Site_Hours_Day_Pre, type EXPRESSION
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
LKP_SITE_PROFILE_temp = LKP_SITE_PROFILE.toDF(
    *["LKP_SITE_PROFILE___" + col for col in LKP_SITE_PROFILE.columns]
)
SQ_SITE_HOURS_DAY_PRE_temp = SQ_SITE_HOURS_DAY_PRE.toDF(
    *["SQ_SITE_HOURS_DAY_PRE___" + col for col in SQ_SITE_HOURS_DAY_PRE.columns]
)

# Joining dataframes SQ_SITE_HOURS_DAY_PRE, LKP_SITE_PROFILE to form Exp_Site_Hours_Day_Pre
Exp_Site_Hours_Day_Pre_joined = SQ_SITE_HOURS_DAY_PRE_temp.join(
    LKP_SITE_PROFILE_temp,
    SQ_SITE_HOURS_DAY_PRE_temp.SQ_SITE_HOURS_DAY_PRE___sys_row_id
    == LKP_SITE_PROFILE_temp.LKP_SITE_PROFILE___sys_row_id,
    "inner",
)
Exp_Site_Hours_Day_Pre = Exp_Site_Hours_Day_Pre_joined.selectExpr(
    "SQ_SITE_HOURS_DAY_PRE___sys_row_id as sys_row_id",
    "SQ_SITE_HOURS_DAY_PRE___DAY_DT as DAY_DT",
    "LKP_SITE_PROFILE___LOCATION_ID as LOCATION_ID",
    "SQ_SITE_HOURS_DAY_PRE___BUSINESS_AREA as BUSINESS_AREA",
    "SQ_SITE_HOURS_DAY_PRE___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
    "LKP_SITE_PROFILE___TIME_ZONE as TIME_ZONE",
    "SQ_SITE_HOURS_DAY_PRE___OPEN_TSTMP as OPEN_TSTMP",
    "SQ_SITE_HOURS_DAY_PRE___CLOSE_TSTMP as CLOSE_TSTMP",
    "SQ_SITE_HOURS_DAY_PRE___IS_CLOSED as IS_CLOSED",
    "LKP_SITE_PROFILE___STORE_NBR as STORE_NBR",
    "MD5 (concat_ws( cast(SQ_SITE_HOURS_DAY_PRE___LOCATION_TYPE_ID as string) , LKP_SITE_PROFILE___TIME_ZONE , date_format(SQ_SITE_HOURS_DAY_PRE___OPEN_TSTMP, 'YYYY-MM-DD hh:MM:SS') , date_format(SQ_SITE_HOURS_DAY_PRE___CLOSE_TSTMP, 'YYYY-MM-DD hh:MM:SS') , cast(SQ_SITE_HOURS_DAY_PRE___IS_CLOSED as string) , cast(LKP_SITE_PROFILE___STORE_NBR as string) )) as _md5PRE",
)
# COMMAND ----------
# Processing node Jnr_Site_Hours_Day, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
Exp_Site_Hours_Day_Pre_temp = Exp_Site_Hours_Day_Pre.toDF(
    *["Exp_Site_Hours_Day_Pre___" + col for col in Exp_Site_Hours_Day_Pre.columns]
)
Exp_Site_Hours_Day_temp = Exp_Site_Hours_Day.toDF(
    *["Exp_Site_Hours_Day___" + col for col in Exp_Site_Hours_Day.columns]
)

Jnr_Site_Hours_Day = Exp_Site_Hours_Day_temp.join(
    Exp_Site_Hours_Day_Pre_temp,
    [
        Exp_Site_Hours_Day_temp.Exp_Site_Hours_Day___DAY_DT
        == Exp_Site_Hours_Day_Pre_temp.Exp_Site_Hours_Day_Pre___DAY_DT,
        Exp_Site_Hours_Day_temp.Exp_Site_Hours_Day___LOCATION_ID
        == Exp_Site_Hours_Day_Pre_temp.Exp_Site_Hours_Day_Pre___LOCATION_ID,
        Exp_Site_Hours_Day_temp.Exp_Site_Hours_Day___BUSINESS_AREA
        == Exp_Site_Hours_Day_Pre_temp.Exp_Site_Hours_Day_Pre___BUSINESS_AREA,
    ],
    "fullouter",
).selectExpr(
    "Exp_Site_Hours_Day_Pre___DAY_DT as DAY_DT1",
    "Exp_Site_Hours_Day_Pre___LOCATION_ID as LOCATION_ID1",
    "Exp_Site_Hours_Day_Pre___BUSINESS_AREA as BUSINESS_AREA1",
    "Exp_Site_Hours_Day_Pre___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
    "Exp_Site_Hours_Day_Pre___TIME_ZONE as TIME_ZONE",
    "Exp_Site_Hours_Day_Pre___OPEN_TSTMP as OPEN_TSTMP",
    "Exp_Site_Hours_Day_Pre___CLOSE_TSTMP as CLOSE_TSTMP",
    "Exp_Site_Hours_Day_Pre___IS_CLOSED as IS_CLOSED",
    "Exp_Site_Hours_Day_Pre___STORE_NBR as STORE_NBR",
    "Exp_Site_Hours_Day_Pre____md5PRE as _md5PRE",
    "Exp_Site_Hours_Day___DAY_DT as DAY_DT",
    "Exp_Site_Hours_Day___LOCATION_ID as LOCATION_ID",
    "Exp_Site_Hours_Day___BUSINESS_AREA as BUSINESS_AREA",
    "Exp_Site_Hours_Day___LOCATION_TYPE_ID as LOCATION_TYPE_ID1",
    "Exp_Site_Hours_Day___STORE_NBR as STORE_NBR1",
    "Exp_Site_Hours_Day___CLOSE_FLAG as CLOSE_FLAG",
    "Exp_Site_Hours_Day___TIME_ZONE as TIME_ZONE1",
    "Exp_Site_Hours_Day___OPEN_TSTMP as OPEN_TSTMP1",
    "Exp_Site_Hours_Day___CLOSE_TSTMP as CLOSE_TSTMP1",
    "Exp_Site_Hours_Day___LOAD_TSTMP as LOAD_TSTMP",
    "Exp_Site_Hours_Day____md5FINAL as _md5FINAL",
)

# COMMAND ----------
# Processing node Fil_Site_Hours_Day, type FILTER
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
Jnr_Site_Hours_Day_temp = Jnr_Site_Hours_Day.toDF(
    *["Jnr_Site_Hours_Day___" + col for col in Jnr_Site_Hours_Day.columns]
)

Fil_Site_Hours_Day = (
    Jnr_Site_Hours_Day_temp.selectExpr(
        "Jnr_Site_Hours_Day___DAY_DT1 as DAY_DT1",
        "Jnr_Site_Hours_Day___LOCATION_ID1 as LOCATION_ID1",
        "Jnr_Site_Hours_Day___BUSINESS_AREA1 as BUSINESS_AREA1",
        "Jnr_Site_Hours_Day___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
        "Jnr_Site_Hours_Day___TIME_ZONE as TIME_ZONE",
        "Jnr_Site_Hours_Day___OPEN_TSTMP as OPEN_TSTMP",
        "Jnr_Site_Hours_Day___CLOSE_TSTMP as CLOSE_TSTMP",
        "Jnr_Site_Hours_Day___IS_CLOSED as IS_CLOSED",
        "Jnr_Site_Hours_Day___STORE_NBR as STORE_NBR",
        "Jnr_Site_Hours_Day____md5FINAL as _md5FINAL",
        "Jnr_Site_Hours_Day____md5PRE as _md5PRE",
        "Jnr_Site_Hours_Day___DAY_DT as DAY_DT",
        "Jnr_Site_Hours_Day___LOCATION_ID as LOCATION_ID",
        "Jnr_Site_Hours_Day___BUSINESS_AREA as BUSINESS_AREA",
        "Jnr_Site_Hours_Day___LOCATION_TYPE_ID1 as LOCATION_TYPE_ID1",
        "Jnr_Site_Hours_Day___STORE_NBR1 as STORE_NBR1",
        "Jnr_Site_Hours_Day___CLOSE_FLAG as CLOSE_FLAG",
        "Jnr_Site_Hours_Day___TIME_ZONE1 as TIME_ZONE1",
        "Jnr_Site_Hours_Day___OPEN_TSTMP1 as OPEN_TSTMP1",
        "Jnr_Site_Hours_Day___CLOSE_TSTMP1 as CLOSE_TSTMP1",
        "Jnr_Site_Hours_Day___LOAD_TSTMP as LOAD_TSTMP",
    )
    .filter(
        "( DAY_DT1 is not null AND DAY_DT is null ) OR ( DAY_DT1 is not null AND NOT DAY_DT is null AND _md5FINAL != _md5PRE ) OR ( DAY_DT1 is null AND DAY_DT is not null AND DAY_DT >= trunc ( current_date()- interval 19 day, 'day' ) )"
    )
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------
# Processing node Exp_StoreHours, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
Fil_Site_Hours_Day_temp = Fil_Site_Hours_Day.toDF(
    *["Fil_Site_Hours_Day___" + col for col in Fil_Site_Hours_Day.columns]
)

Exp_StoreHours = Fil_Site_Hours_Day_temp.selectExpr(
    "Fil_Site_Hours_Day___sys_row_id as sys_row_id",
    "Fil_Site_Hours_Day___DAY_DT1 as DAY_DT1",
    "Fil_Site_Hours_Day___LOCATION_ID1 as LOCATION_ID1",
    "Fil_Site_Hours_Day___BUSINESS_AREA1 as BUSINESS_AREA1",
    "Fil_Site_Hours_Day___LOCATION_TYPE_ID as LOCATION_TYPE_ID",
    "Fil_Site_Hours_Day___STORE_NBR as STORE_NBR",
    "Fil_Site_Hours_Day___TIME_ZONE as TIME_ZONE",
    "Fil_Site_Hours_Day___OPEN_TSTMP as OPEN_TSTMP",
    "Fil_Site_Hours_Day___CLOSE_TSTMP as CLOSE_TSTMP",
    "Fil_Site_Hours_Day___IS_CLOSED as IS_CLOSED",
    "Fil_Site_Hours_Day___LOAD_TSTMP as LOAD_TSTMP1",
    "Fil_Site_Hours_Day___DAY_DT as DAY_DT",
    "Fil_Site_Hours_Day___LOCATION_ID as LOCATION_ID",
    "Fil_Site_Hours_Day___BUSINESS_AREA as BUSINESS_AREA",
    "Fil_Site_Hours_Day___LOCATION_TYPE_ID1 as LOCATION_TYPE_ID1",
    "Fil_Site_Hours_Day___STORE_NBR1 as STORE_NBR1",
    "Fil_Site_Hours_Day___CLOSE_FLAG as CLOSE_FLAG",
    "Fil_Site_Hours_Day___TIME_ZONE1 as TIME_ZONE1",
    "Fil_Site_Hours_Day___OPEN_TSTMP1 as OPEN_TSTMP1",
    "Fil_Site_Hours_Day___CLOSE_TSTMP1 as CLOSE_TSTMP1",
).selectExpr(
    "sys_row_id as sys_row_id",
    "IF (DAY_DT IS NULL, 0, 1) as LoadStrategy",
    "IF (DAY_DT1 IS NULL, DAY_DT, DAY_DT1) as o_DAY_DT",
    "IF (DAY_DT1 IS NULL, LOCATION_ID, LOCATION_ID1) as o_LOCATION_ID",
    "IF (DAY_DT1 IS NULL, BUSINESS_AREA, BUSINESS_AREA1) as o_BUSINESS_AREA",
    "IF (DAY_DT1 IS NULL, LOCATION_TYPE_ID1, LOCATION_TYPE_ID) as o_LOCATION_TYPE_ID",
    "IF (DAY_DT1 IS NULL, STORE_NBR1, STORE_NBR) as o_STORE_NBR",
    "IF (DAY_DT1 IS NULL, 1, IS_CLOSED) as o_CLOSE_FLAG",
    "IF (DAY_DT1 IS NULL, TIME_ZONE1, TIME_ZONE) as o_TIME_ZONE",
    "IF (DAY_DT1 IS NULL, OPEN_TSTMP1, OPEN_TSTMP) as o_OPEN_TSTMP",
    "IF (DAY_DT1 IS NULL, CLOSE_TSTMP1, CLOSE_TSTMP) as o_CLOSE_TSTMP",
    "CURRENT_TIMESTAMP as UPDATE_TSTMP",
    "IF (DAY_DT IS NULL, CURRENT_TIMESTAMP, LOAD_TSTMP1) as LOAD_TSTMP",
)

# COMMAND ----------
# Processing node Upd_Site_Hours_Day, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
Exp_StoreHours_temp = Exp_StoreHours.toDF(
    *["Exp_StoreHours___" + col for col in Exp_StoreHours.columns]
)

Upd_Site_Hours_Day = Exp_StoreHours_temp.selectExpr(
    "Exp_StoreHours___o_DAY_DT as DAY_DT",
    "Exp_StoreHours___o_LOCATION_ID as LOCATION_ID",
    "Exp_StoreHours___o_BUSINESS_AREA as BUSINESS_AREA",
    "Exp_StoreHours___o_LOCATION_TYPE_ID as LOCATION_TYPE_ID",
    "Exp_StoreHours___o_STORE_NBR as STORE_NBR",
    "Exp_StoreHours___o_TIME_ZONE as TIME_ZONE",
    "Exp_StoreHours___o_OPEN_TSTMP as OPEN_TSTMP",
    "Exp_StoreHours___o_CLOSE_TSTMP as CLOSE_TSTMP",
    "Exp_StoreHours___o_CLOSE_FLAG as IS_CLOSED",
    "Exp_StoreHours___UPDATE_TSTMP as UPDATE_TSTMP",
    "Exp_StoreHours___LOAD_TSTMP as LOAD_TSTMP",
    "Exp_StoreHours___LoadStrategy as pyspark_data_action",
)

# COMMAND ----------
# Processing node Shortcut_to_SITE_HOURS_DAY_1, type TARGET
# COLUMN COUNT: 11

Shortcut_to_SITE_HOURS_DAY_1 = Upd_Site_Hours_Day.selectExpr(
    "CAST(DAY_DT AS DATE) as DAY_DT",
    "CAST(LOCATION_ID AS INT) as LOCATION_ID",
    "CAST(BUSINESS_AREA AS STRING) as BUSINESS_AREA",
    "CAST(LOCATION_TYPE_ID AS TINYINT) as LOCATION_TYPE_ID",
    "CAST(STORE_NBR AS INT) as STORE_NBR",
    "CAST(IS_CLOSED AS TINYINT) as CLOSE_FLAG",
    "CAST(TIME_ZONE AS STRING) as TIME_ZONE",
    "CAST(OPEN_TSTMP AS TIMESTAMP) as OPEN_TSTMP",
    "CAST(CLOSE_TSTMP AS TIMESTAMP) as CLOSE_TSTMP",
    "CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
    "pyspark_data_action as pyspark_data_action",
)
spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")
try:
    primary_key = """source.DAY_DT = target.DAY_DT AND source.LOCATION_ID = target.LOCATION_ID AND source.BUSINESS_AREA = target.BUSINESS_AREA"""
    refined_perf_table = f"{legacy}.SITE_HOURS_DAY"
    executeMerge(Shortcut_to_SITE_HOURS_DAY_1, refined_perf_table, primary_key)
    logger.info(f"Merge with {refined_perf_table} completed]")
    logPrevRunDt(
        "SITE_HOURS_DAY", "SITE_HOURS_DAY", "Completed", "N/A", f"{raw}.log_run_details"
    )
except Exception as e:
    logPrevRunDt(
        "SITE_HOURS_DAY", "SITE_HOURS_DAY", "Failed", str(e), f"{raw}.log_run_details"
    )
    raise e
