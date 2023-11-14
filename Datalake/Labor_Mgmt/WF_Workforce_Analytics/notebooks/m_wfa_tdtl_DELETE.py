# Code converted on 2023-08-08 15:40:56
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
parser.add_argument("env", type=str, help="Env Variable")

args = parser.parse_args()
env = args.env

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"
empl_protected = getEnvPrefix(env) + "empl_protected"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TDTL_PRE, type SOURCE
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_TDTL_PRE = spark.sql(
    f"""SELECT
raw_WFA_TDTL_PRE.TDTL_ID,
raw_WFA_TDTL_PRE.RECORDED_DAT
FROM {empl_protected}.raw_WFA_TDTL_PRE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TDTL, type SOURCE
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_TDTL = spark.sql(
    f"""SELECT
legacy_WFA_TDTL.DAY_DT,
legacy_WFA_TDTL.TDTL_ID
FROM {empl_protected}.legacy_WFA_TDTL"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node FIL_35DAYS, type FILTER
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_TDTL_temp = SQ_Shortcut_to_WFA_TDTL.toDF(
    *["SQ_Shortcut_to_WFA_TDTL___" + col for col in SQ_Shortcut_to_WFA_TDTL.columns]
)

FIL_35DAYS = (
    SQ_Shortcut_to_WFA_TDTL_temp.selectExpr(
        "SQ_Shortcut_to_WFA_TDTL___DAY_DT as DAY_DT",
        "SQ_Shortcut_to_WFA_TDTL___TDTL_ID as TDTL_ID",
    )
    .filter("DAY_DT > CURRENT_DATE() - 36")
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------
# Processing node JNR_TDTL_PREV_PRE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_TDTL_PRE_temp = SQ_Shortcut_to_WFA_TDTL_PRE.toDF(
    *[
        "SQ_Shortcut_to_WFA_TDTL_PRE___" + col
        for col in SQ_Shortcut_to_WFA_TDTL_PRE.columns
    ]
)
FIL_35DAYS_temp = FIL_35DAYS.toDF(
    *["FIL_35DAYS___" + col for col in FIL_35DAYS.columns]
)

JNR_TDTL_PREV_PRE = SQ_Shortcut_to_WFA_TDTL_PRE_temp.join(
    FIL_35DAYS_temp,
    [
        SQ_Shortcut_to_WFA_TDTL_PRE_temp.SQ_Shortcut_to_WFA_TDTL_PRE___RECORDED_DAT
        == FIL_35DAYS_temp.FIL_35DAYS___DAY_DT,
        SQ_Shortcut_to_WFA_TDTL_PRE_temp.SQ_Shortcut_to_WFA_TDTL_PRE___TDTL_ID
        == FIL_35DAYS_temp.FIL_35DAYS___TDTL_ID,
    ],
    "right_outer",
).selectExpr(
    "FIL_35DAYS___DAY_DT as DAY_DT",
    "FIL_35DAYS___TDTL_ID as TDTL_ID",
    "SQ_Shortcut_to_WFA_TDTL_PRE___TDTL_ID as TDTL_ID_pre",
    "SQ_Shortcut_to_WFA_TDTL_PRE___RECORDED_DAT as RECORDED_DAT_pre",
)

# COMMAND ----------
# Processing node FIL_Deleted_Records, type FILTER
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
JNR_TDTL_PREV_PRE_temp = JNR_TDTL_PREV_PRE.toDF(
    *["JNR_TDTL_PREV_PRE___" + col for col in JNR_TDTL_PREV_PRE.columns]
)

JNR_TDTL_PREV_PRE_temp.selectExpr(
    "JNR_TDTL_PREV_PRE___DAY_DT as DAY_DT",
    "JNR_TDTL_PREV_PRE___TDTL_ID as TDTL_ID",
    "JNR_TDTL_PREV_PRE___TDTL_ID_pre as TDTL_ID_pre",
).filter("TDTL_ID_pre is null").withColumn(
    "sys_row_id", monotonically_increasing_id()
).createOrReplaceTempView(
    "FIL_Deleted_Records"
)

# COMMAND ----------
# Processing node Shortcut_to_WFA_TDTL1, type TARGET
# COLUMN COUNT: 72
# overwriteDeltaPartition(Shortcut_to_WFA_TDTL1,'DC_NBR',dcnbr,f'{raw}.WFA_TDTL')
#Shortcut_to_WFA_TDTL1.write.mode("overwrite").saveAsTable(f'{empl_protected}.legacy_WFA_TDTL')

spark.sql(f"""
MERGE INTO {empl_protected}.legacy_WFA_TDTL target
USING FIL_Deleted_Records source
ON source.DAY_DT = target.DAY_DT AND source.TDTL_ID = target.TDTL_ID
WHEN MATCHED THEN
  DELETE """
)