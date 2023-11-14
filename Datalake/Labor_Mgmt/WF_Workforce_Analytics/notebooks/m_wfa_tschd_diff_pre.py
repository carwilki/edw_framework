# Code converted on 2023-08-08 15:42:25
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


WFA_TSCHD_DIFF_PRE = spark.sql(
    f"""select A.* from {empl_protected}.raw_WFA_TSCHD_PRE A left outer join {empl_protected}.raw_WFA_TSCHD_PREV_PRE B on
A.TSCHD_ID=B.TSCHD_ID
AND A.TSCHD_DAT=B.TSCHD_DAT
AND A.EMP_SKEY=B.EMP_SKEY
AND A.STRT_DTM=B.STRT_DTM
AND A.END_DTM=B.END_DTM
AND A.PAYCD_SKEY=B.PAYCD_SKEY
AND A.CORE_HRS_SWT=B.CORE_HRS_SWT
AND A.PRI_ORG_SKEY=B.PRI_ORG_SKEY
AND A.ORG_SKEY=B.ORG_SKEY
AND A.SHIFT_ID=B.SHIFT_ID
AND A.MONEY_AMT=B.MONEY_AMT
AND A.DRTN_AMT=B.DRTN_AMT
AND A.CORE_AMT=B.CORE_AMT
AND A.NON_CORE_AMT=B.NON_CORE_AMT
AND A.DRTN_HRS=B.DRTN_HRS
AND A.CORE_HRS=B.CORE_HRS
AND A.NON_CORE_HRS=B.NON_CORE_HRS
where B.TSCHD_ID is null"""
).drop("UPDT_DTM")

WFA_TSCHD_DIFF_PRE.write.mode("overwrite").saveAsTable(
    f"{empl_protected}.raw_WFA_TSCHD_DIFF_PRE"
)
