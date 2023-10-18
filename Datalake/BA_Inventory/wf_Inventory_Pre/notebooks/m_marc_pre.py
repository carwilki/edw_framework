# Databricks notebook source
# Code converted on 2023-09-25 13:30:27
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

# Processing node LKP_MARA_PRE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 1

# dbutils.widgets.text(name='source_bucket', defaultValue='gs://petm-bdpl-qa-raw-p1-gcs-gbl/sap/inventory/marc/')
# source_bucket = dbutils.widgets.get('source_bucket')

# def get_source_file(key, _bucket):
#   import builtins

#   lst = dbutils.fs.ls(_bucket)
#   fldr = builtins.max(lst, key=lambda x: x.name).name
#   lst = dbutils.fs.ls(_bucket + fldr)
#   print(lst)
#   files = [x.path for x in lst if x.name.startswith(key)]
#   return files[0] if files else None

# source_file = get_source_file('MARC', source_bucket)
_bucket = getParameterValue(
    raw,
    "BA_Inventory_Parameter.prm",
    "BA_Inventory.WF:wf_Inventory_Pre",
    "source_bucket",
)
source_bucket = _bucket + "marc/"


def get_source_file(key, _bucket):
    import builtins

    lst = dbutils.fs.ls(_bucket)
    fldr = builtins.max(lst, key=lambda x: x.name).name
    _path = os.path.join(_bucket, fldr)
    lst = dbutils.fs.ls(_path)
    files = [x.path for x in lst if x.name.startswith(key)]
    return files[0] if files else None


source_file = get_source_file("MARC", source_bucket)

SQ_Shortcut_to_MARC = spark.read.csv(source_file, sep="|", header=True)

# Conforming fields names to the component layout
SQ_Shortcut_to_MARC = (
    SQ_Shortcut_to_MARC.withColumnRenamed(SQ_Shortcut_to_MARC.columns[0], "MANDT")
    .withColumnRenamed(SQ_Shortcut_to_MARC.columns[1], "MATNR")
    .withColumnRenamed(SQ_Shortcut_to_MARC.columns[2], "WERKS")
    .withColumnRenamed(SQ_Shortcut_to_MARC.columns[3], "TRAME")
    .withColumnRenamed(SQ_Shortcut_to_MARC.columns[4], "BWESB")
)


# COMMAND ----------

# Processing node FILTRANS, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5


FILTRANS = SQ_Shortcut_to_MARC.filter(
    "cast(MANDT as double) IS NOT NULL AND cast(MATNR as double) IS NOT NULL AND cast(WERKS as double) IS NOT NULL"
)


# COMMAND ----------

# Processing node Shortcut_to_MARC1, type TARGET
# COLUMN COUNT: 5


Shortcut_to_MARC1 = FILTRANS.selectExpr(
    "CAST(MANDT AS INT) as MANDT",
    "CAST(MATNR AS INT) as MATNR",
    "CAST(WERKS AS INT) as WERKS",
    "CAST(TRAME AS INT) as TRAME",
    "CAST(BWESB AS INT) as BWESB",
)
Shortcut_to_MARC1.write.mode("overwrite").saveAsTable(f"{raw}.MARC_PRE")
