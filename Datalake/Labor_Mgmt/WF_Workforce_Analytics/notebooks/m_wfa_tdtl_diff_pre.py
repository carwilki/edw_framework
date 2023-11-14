
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
 
 
WFA_TDTL_DIFF_PRE = spark.sql(f"""select A.* from {empl_protected}.raw_WFA_TDTL_PRE A left outer join {empl_protected}.raw_WFA_TDTL_PREV_PRE B on A.TDTL_SKEY=B.TDTL_SKEY
AND A.TDTL_ID=B.TDTL_ID
AND A.TSITEM_SKEY=B.TSITEM_SKEY
AND A.RECORDED_DAT=B.RECORDED_DAT
AND A.RECORDED_FOR_DAT=B.RECORDED_FOR_DAT
AND A.ADJ_SWT=B.ADJ_SWT
AND A.EDT_SWT=B.EDT_SWT
AND A.EMP_SKEY=B.EMP_SKEY
AND A.STRT_DTM=B.STRT_DTM
AND A.END_DTM=B.END_DTM
AND A.UNSCHD_STRT_DTM=B.UNSCHD_STRT_DTM
AND A.UNSCHD_END_DTM=B.UNSCHD_END_DTM
AND A.STRT_TZONE_SKEY=B.STRT_TZONE_SKEY
AND A.LBRACCT_SKEY=B.LBRACCT_SKEY
AND A.HM_LBRACCT_SWT=B.HM_LBRACCT_SWT
AND A.HM_LBRACCT_SKEY=B.HM_LBRACCT_SKEY
AND A.FROM_LBRACCT_SKEY=B.FROM_LBRACCT_SKEY
AND A.PAYCD_SKEY=B.PAYCD_SKEY
AND A.CORE_HRS_SWT=B.CORE_HRS_SWT
AND A.FROM_PAYCD_SKEY=B.FROM_PAYCD_SKEY
AND A.SUPV_SKEY=B.SUPV_SKEY
AND A.PRI_JOB_SKEY=B.PRI_JOB_SKEY
AND A.JOB_SKEY=B.JOB_SKEY
AND A.PRI_ORG_SKEY=B.PRI_ORG_SKEY
AND A.ORG_SKEY=B.ORG_SKEY
AND A.PAYPER_SKEY=B.PAYPER_SKEY
AND A.HDAY_SKEY=B.HDAY_SKEY
AND A.STRT_PNCHEVNT_SKEY=B.STRT_PNCHEVNT_SKEY
AND A.END_PNCHEVNT_SKEY=B.END_PNCHEVNT_SKEY
AND A.STRT_DSRC_SKEY=B.STRT_DSRC_SKEY
AND A.END_DSRC_SKEY=B.END_DSRC_SKEY
AND A.DSRC_SKEY=B.DSRC_SKEY
AND A.EMPSTAT_SKEY=B.EMPSTAT_SKEY
AND A.AGE_NBR=B.AGE_NBR
AND A.TENURE_MO_NBR=B.TENURE_MO_NBR
AND A.DFLT_PAY_RULE_SKEY=B.DFLT_PAY_RULE_SKEY
AND A.DFLT_WRK_RULE_SWT=B.DFLT_WRK_RULE_SWT
AND A.DFLT_WRK_RULE_SKEY=B.DFLT_WRK_RULE_SKEY
AND A.WRK_RULE_SKEY=B.WRK_RULE_SKEY
AND A.MONEY_AMT=B.MONEY_AMT
AND A.DRTN_AMT=B.DRTN_AMT
AND A.CORE_AMT=B.CORE_AMT
AND A.NON_CORE_AMT=B.NON_CORE_AMT
AND A.DRTN_DIFF_AMT=B.DRTN_DIFF_AMT
AND A.DRTN_HRS=B.DRTN_HRS
AND A.CORE_HRS=B.CORE_HRS
AND A.NON_CORE_HRS=B.NON_CORE_HRS
AND A.GRP_SCHD_SKEY=B.GRP_SCHD_SKEY
where B.TDTL_SKEY is null""")
 
WFA_TDTL_DIFF_PRE.write.mode("overwrite").saveAsTable(
    f"{empl_protected}.raw_WFA_TDTL_DIFF_PRE"
)
 