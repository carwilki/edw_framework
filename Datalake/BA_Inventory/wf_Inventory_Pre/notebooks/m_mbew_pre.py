# Databricks notebook source
# Code converted on 2023-09-25 13:30:28
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
source_bucket = _bucket + "mbew/"


def get_source_file(key, _bucket):
    import builtins

    lst = dbutils.fs.ls(_bucket)
    fldr = builtins.max(lst, key=lambda x: x.name).name
    _path = os.path.join(_bucket, fldr)
    lst = dbutils.fs.ls(_path)
    files = [x.path for x in lst if x.name.startswith(key)]
    return files[0] if files else None


source_file = get_source_file("MBEW", source_bucket)

SQ_Shortcut_to_MBEW = spark.read.csv(source_file, sep="|", header=True)


# COMMAND ----------

# Processing node FILTRANS, type FILTER
# COLUMN COUNT: 9


FILTRANS = SQ_Shortcut_to_MBEW.filter(
    "cast(MANDT as INT) IS NOT NULL AND cast(MATNR as INT) IS NOT NULL AND cast(BWKEY as INT) IS NOT NULL"
)


# COMMAND ----------

# Processing node Shortcut_to_MBEW_PRE, type TARGET
# COLUMN COUNT: 9


Shortcut_to_MBEW_PRE = FILTRANS.selectExpr(
    "CAST(MANDT AS INT) as MANDT",
    "CAST(MATNR AS INT) as MATNR",
    "CAST(BWKEY AS INT) as BWKEY",
    "CAST(BWTAR AS STRING) as BWTAR",
    "CAST(LBKUM AS INT) as LBKUM",
    "CAST(SALK3 AS DECIMAL(13,2)) as SALK3",
    "CAST(VERPR AS DECIMAL(11,2)) as VERPR",
    "CAST(STPRV AS DECIMAL(11,2)) as STPRV",
    "CAST(to_utc_timestamp(from_unixtime(unix_timestamp(LAEPR, 'MM/dd/yyyy HH:mm:ss')),'UTC') as TIMESTAMP) as LAEPR",
)

Shortcut_to_MBEW_PRE.write.mode("overwrite").saveAsTable(f"{raw}.MBEW_PRE")
