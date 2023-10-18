# Databricks notebook source
# Code converted on 2023-09-25 13:30:24
import argparse
import os
from datetime import datetime

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

from Datalake.utils.configs import *
from Datalake.utils.genericUtilities import *
from Datalake.utils.logger import *
from Datalake.utils.mergeUtils import *

# COMMAND ----------

# parser = argparse.ArgumentParser()
# parser.add_argument('env', type=str, help='Env Variable')
# args = parser.parse_args()
# env = args.env

spark = SparkSession.getActiveSession()
dbutils = DBUtils(spark)

dbutils.widgets.text(name="env", defaultValue="dev")
env = dbutils.widgets.get("env")

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"


# COMMAND ----------

_bucket = getParameterValue(
    raw,
    "BA_Inventory_Parameter.prm",
    "BA_Inventory.WF:wf_Inventory_Pre",
    "source_bucket",
)
source_bucket = _bucket + "ztb_inv_trans/"


def get_source_file(key, _bucket):
    import builtins

    lst = dbutils.fs.ls(_bucket)
    fldr = builtins.max(lst, key=lambda x: x.name).name
    _path = os.path.join(_bucket, fldr)
    lst = dbutils.fs.ls(_path)
    files = [x.path for x in lst if x.name.startswith(key)]
    return files[0] if files else None


source_file = get_source_file("ZTB_INV_TRANS", source_bucket)

SQ_Shortcut_to_ZTB_INV_TRANS = spark.read.csv(
    source_file, sep="|", header=True
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_To_SITE_PROFILE, type SOURCE
# COLUMN COUNT: 2

SQ_Shortcut_To_SITE_PROFILE = spark.sql(
    f"""SELECT
LOCATION_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_To_SKU_PROFILE, type SOURCE
# COLUMN COUNT: 2

SQ_Shortcut_To_SKU_PROFILE = spark.sql(
    f"""SELECT
PRODUCT_ID,
SKU_NBR
FROM {legacy}.SKU_PROFILE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_INT, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_ZTB_INV_TRANS_temp = SQ_Shortcut_to_ZTB_INV_TRANS.toDF(
    *[
        "SQ_Shortcut_to_ZTB_INV_TRANS___" + col
        for col in SQ_Shortcut_to_ZTB_INV_TRANS.columns
    ]
)

EXP_INT = SQ_Shortcut_to_ZTB_INV_TRANS_temp.selectExpr(
    "SQ_Shortcut_to_ZTB_INV_TRANS___sys_row_id as sys_row_id",
    "cast(SQ_Shortcut_to_ZTB_INV_TRANS___ARTICLE as int) as ARTICLE",
    "cast(SQ_Shortcut_to_ZTB_INV_TRANS___SITE as int) as SITE",
    "SQ_Shortcut_to_ZTB_INV_TRANS___COMMITED as COMMITED",
    "SQ_Shortcut_to_ZTB_INV_TRANS___UNSELLABLE as UNSELLABLE",
)

# COMMAND ----------

# Processing node JNR_SITE_PROFILE, type JOINER
# COLUMN COUNT: 6

JNR_SITE_PROFILE = SQ_Shortcut_To_SITE_PROFILE.join(
    EXP_INT, [SQ_Shortcut_To_SITE_PROFILE.STORE_NBR == EXP_INT.SITE], "right_outer"
)

# COMMAND ----------

# Processing node JNR_SKU_PROFILE, type JOINER
# COLUMN COUNT: 7

JNR_SKU_PROFILE = SQ_Shortcut_To_SKU_PROFILE.join(
    JNR_SITE_PROFILE,
    [SQ_Shortcut_To_SKU_PROFILE.SKU_NBR == JNR_SITE_PROFILE.ARTICLE],
    "right_outer",
)

# COMMAND ----------

# Processing node EXP_DATES, type EXPRESSION
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
JNR_SKU_PROFILE_temp = JNR_SKU_PROFILE.toDF(
    *["JNR_SKU_PROFILE___" + col for col in JNR_SKU_PROFILE.columns]
)

EXP_DATES = JNR_SKU_PROFILE_temp.selectExpr(
    "CURRENT_DATE as DAY_DT",
    "JNR_SKU_PROFILE___LOCATION_ID as LOCATION_ID",
    "JNR_SKU_PROFILE___PRODUCT_ID as PRODUCT_ID",
    "JNR_SKU_PROFILE___COMMITED as COMMITED",
    "JNR_SKU_PROFILE___UNSELLABLE as UNSELLABLE",
    "CURRENT_TIMESTAMP as LOAD_TSTMP",
)

# COMMAND ----------

# Processing node Shortcut_to_INV_UNAVAILABLE_HIST, type TARGET
# COLUMN COUNT: 6


Shortcut_to_INV_UNAVAILABLE_HIST = EXP_DATES.selectExpr(
    "CAST(DAY_DT AS DATE) as DAY_DT",
    "CAST(LOCATION_ID AS INT) as LOCATION_ID",
    "CAST(PRODUCT_ID AS INT) as PRODUCT_ID",
    "CAST(COMMITED AS DECIMAL(13,3)) as COMMITED_QTY",
    "CAST(UNSELLABLE AS DECIMAL(13,3)) as UNSELLABLE_QTY",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
)
DuplicateChecker.check_for_duplicate_primary_keys(
    Shortcut_to_INV_UNAVAILABLE_HIST, ["DAY_DT", "LOCATION_ID", ""]
)
Shortcut_to_INV_UNAVAILABLE_HIST.write.mode("append").saveAsTable(
    f"{legacy}.INV_UNAVAILABLE_HIST"
)
logPrevRunDt(
    "INV_UNAVAILABLE_HIST",
    "INV_UNAVAILABLE_HIST",
    "Completed",
    "N/A",
    f"{raw}.log_run_details",
)
