# Databricks notebook source
# Code converted on 2023-09-25 13:30:25
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
source_bucket = _bucket + "mard/"


def get_source_file(key, _bucket):
    import builtins

    lst = dbutils.fs.ls(_bucket)
    fldr = builtins.max(lst, key=lambda x: x.name).name
    _path = os.path.join(_bucket, fldr)
    lst = dbutils.fs.ls(_path)
    files = [x.path for x in lst if x.name.startswith(key)]
    return files[0] if files else None


source_file = get_source_file("MARD", source_bucket)

SQ_Shortcut_to_MARD = spark.read.csv(source_file, sep="|", header=True)


# COMMAND ----------

EXPTRANS = SQ_Shortcut_to_MARD.filter(
    "cast(MANDT as INT) IS NOT NULL AND cast(MATNR as INT) IS NOT NULL AND cast(WERKS as INT) IS NOT NULL"
)


# COMMAND ----------

# Processing node Shortcut_to_MARD1, type TARGET
# COLUMN COUNT: 7


Shortcut_to_MARD1 = EXPTRANS.selectExpr(
    "CAST(MANDT AS INT) as MANDT",
    "CAST(MATNR AS INT) as MATNR",
    "CAST(WERKS AS INT) as WERKS",
    "CAST(LGORT AS STRING) as LGORT",
    "CAST(LABST AS INT) as LABST",
    "CAST(INSME AS INT) as INSME",
    "CAST(SPEME AS INT) as SPEME",
)
Shortcut_to_MARD1.write.mode("overwrite").saveAsTable(f"{raw}.MARD_PRE")
