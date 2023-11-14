# Code converted on 2023-08-08 15:41:24
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

# args = parser.parse_args()
# env = args.env
env='qa'
if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"
enterprise = getEnvPrefix(env) + "enterprise"
empl_protected = getEnvPrefix(env) + "empl_protected"


# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TDTL, type SOURCE
# COLUMN COUNT: 3

SQ_Shortcut_to_WFA_TDTL = spark.sql(
    f"""SELECT DISTINCT
legacy_WFA_TDTL.DAY_DT,
legacy_WFA_TDTL.TDTL_ID,
legacy_WFA_TDTL.LOAD_DT
FROM {empl_protected}.legacy_WFA_TDTL"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TDTL_DIFF_PRE, type SOURCE
# COLUMN COUNT: 48

SQ_Shortcut_to_WFA_TDTL_DIFF_PRE = spark.sql(
    f"""SELECT
legacy_EMPLOYEE_PROFILE.EMPLOYEE_ID as EMP_ID ,
WFA_ORG_FACT_PRE.LOCATION_ID,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_ID,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_DESC,
WFA_ORG_FACT_PRE.WFA_DEPT_ID,
WFA_ORG_FACT_PRE.WFA_DEPT_DESC,
WFA_ORG_FACT_PRE.WFA_TASK_ID,
WFA_ORG_FACT_PRE.WFA_TASK_DESC,    
WFA_ORG_FACT_PRE_PREV.LOCATION_ID AS PRI_LOCATION_ID,
WFA_ORG_FACT_PRE_PREV.WFA_BUSN_AREA_ID AS PRI_WFA_BUSN_AREA_ID,
WFA_ORG_FACT_PRE_PREV.WFA_BUSN_AREA_DESC AS PRI_WFA_BUSN_AREA_DESC,
WFA_ORG_FACT_PRE_PREV.WFA_DEPT_ID AS PRI_WFA_DEPT_ID,
WFA_ORG_FACT_PRE_PREV.WFA_DEPT_DESC AS PRI_WFA_DEPT_DESC,
WFA_ORG_FACT_PRE_PREV.WFA_TASK_ID AS PRI_WFA_TASK_ID,
WFA_ORG_FACT_PRE_PREV.WFA_TASK_DESC AS PRI_WFA_TASK_DESC,   
raw_WFA_TDTL_DIFF_PRE.TDTL_SKEY,
raw_WFA_TDTL_DIFF_PRE.TDTL_ID,
raw_WFA_TDTL_DIFF_PRE.RECORDED_DAT,
raw_WFA_TDTL_DIFF_PRE.RECORDED_FOR_DAT,
raw_WFA_TDTL_DIFF_PRE.ADJ_SWT,
raw_WFA_TDTL_DIFF_PRE.EDT_SWT,
raw_WFA_TDTL_DIFF_PRE.EMP_SKEY,
raw_WFA_TDTL_DIFF_PRE.STRT_DTM,
raw_WFA_TDTL_DIFF_PRE.END_DTM,
raw_WFA_TDTL_DIFF_PRE.UNSCHD_STRT_DTM,
raw_WFA_TDTL_DIFF_PRE.UNSCHD_END_DTM,
raw_WFA_TDTL_DIFF_PRE.STRT_TZONE_SKEY,
raw_WFA_TDTL_DIFF_PRE.LBRACCT_SKEY,
raw_WFA_TDTL_DIFF_PRE.HM_LBRACCT_SWT,
raw_WFA_TDTL_DIFF_PRE.HM_LBRACCT_SKEY,
raw_WFA_TDTL_DIFF_PRE.FROM_LBRACCT_SKEY,
raw_WFA_TDTL_DIFF_PRE.PAYCD_SKEY,
raw_WFA_TDTL_DIFF_PRE.CORE_HRS_SWT,
raw_WFA_TDTL_DIFF_PRE.FROM_PAYCD_SKEY,
raw_WFA_TDTL_DIFF_PRE.SUPV_SKEY,
raw_WFA_TDTL_DIFF_PRE.PRI_JOB_SKEY,
raw_WFA_TDTL_DIFF_PRE.JOB_SKEY,
raw_WFA_TDTL_DIFF_PRE.PRI_ORG_SKEY,
raw_WFA_TDTL_DIFF_PRE.ORG_SKEY,
raw_WFA_TDTL_DIFF_PRE.PAYPER_SKEY,
raw_WFA_TDTL_DIFF_PRE.HDAY_SKEY,
raw_WFA_TDTL_DIFF_PRE.STRT_PNCHEVNT_SKEY,
raw_WFA_TDTL_DIFF_PRE.END_PNCHEVNT_SKEY,
raw_WFA_TDTL_DIFF_PRE.STRT_DSRC_SKEY,
raw_WFA_TDTL_DIFF_PRE.END_DSRC_SKEY,
raw_WFA_TDTL_DIFF_PRE.DSRC_SKEY,
raw_WFA_TDTL_DIFF_PRE.EMPSTAT_SKEY,
raw_WFA_TDTL_DIFF_PRE.AGE_NBR,
raw_WFA_TDTL_DIFF_PRE.TENURE_MO_NBR,
raw_WFA_TDTL_DIFF_PRE.DFLT_PAY_RULE_SKEY,
raw_WFA_TDTL_DIFF_PRE.DFLT_WRK_RULE_SWT,
raw_WFA_TDTL_DIFF_PRE.DFLT_WRK_RULE_SKEY,
raw_WFA_TDTL_DIFF_PRE.WRK_RULE_SKEY,
raw_WFA_TDTL_DIFF_PRE.MONEY_AMT,
raw_WFA_TDTL_DIFF_PRE.DRTN_AMT,
raw_WFA_TDTL_DIFF_PRE.CORE_AMT,
raw_WFA_TDTL_DIFF_PRE.NON_CORE_AMT,
raw_WFA_TDTL_DIFF_PRE.DRTN_DIFF_AMT,
raw_WFA_TDTL_DIFF_PRE.DRTN_HRS,
raw_WFA_TDTL_DIFF_PRE.CORE_HRS,
raw_WFA_TDTL_DIFF_PRE.NON_CORE_HRS,
raw_WFA_TDTL_DIFF_PRE.LOCKED_SWT,
raw_WFA_TDTL_DIFF_PRE.GRP_SCHD_SKEY,
WFA_PAYCD.PAYCD_ID AS PAYCD_ID,
FROM_WFA_PAYCD.PAYCD_ID AS FROM_PAYCD_ID,
WFA_LBRACCT.LBRACCT_ID AS LBRACCT_ID,
site_profile.location_id AS LBRACCT2_NAM,
HM_WFA_LBRACCT.LBRACCT_ID AS HM_LBRACCT_ID,
FROM_WFA_LBRACCT.LBRACCT_ID AS FROM_LBRACCT_ID,
DAYS.WEEK_DT
FROM {empl_protected}.raw_WFA_TDTL_DIFF_PRE
LEFT JOIN {legacy}.WFA_PAYCD ON raw_WFA_TDTL_DIFF_PRE.PAYCD_SKEY = WFA_PAYCD.PAYCD_SKEY
LEFT JOIN {legacy}.WFA_PAYCD FROM_WFA_PAYCD ON raw_WFA_TDTL_DIFF_PRE.FROM_PAYCD_SKEY = FROM_WFA_PAYCD.PAYCD_SKEY
LEFT JOIN {legacy}.WFA_LBRACCT ON raw_WFA_TDTL_DIFF_PRE.LBRACCT_SKEY = WFA_LBRACCT.LBRACCT_SKEY
LEFT JOIN {legacy}.WFA_LBRACCT HM_WFA_LBRACCT ON raw_WFA_TDTL_DIFF_PRE.HM_LBRACCT_SKEY = HM_WFA_LBRACCT.LBRACCT_SKEY
LEFT JOIN {legacy}.WFA_LBRACCT FROM_WFA_LBRACCT ON raw_WFA_TDTL_DIFF_PRE.FROM_LBRACCT_SKEY = FROM_WFA_LBRACCT.LBRACCT_SKEY
JOIN {enterprise}.DAYS ON raw_WFA_TDTL_DIFF_PRE.RECORDED_DAT = DAYS.DAY_DT
INNER JOIN {raw}.WFA_ORG_FACT_PRE WFA_ORG_FACT_PRE ON raw_WFA_TDTL_DIFF_PRE.ORG_SKEY = wfa_org_fact_pre.org_skey
LEFT JOIN {raw}.WFA_ORG_FACT_PRE WFA_ORG_FACT_PRE_PREV ON raw_WFA_TDTL_DIFF_PRE.PRI_ORG_SKEY = wfa_org_fact_pre_prev.org_skey
LEFT JOIN {legacy}.site_profile on site_profile.store_nbr = cast(WFA_LBRACCT.LBRACCT2_NAM as INTEGER)
JOIN {raw}.WFA_EMP_PRE ON raw_WFA_TDTL_DIFF_PRE.emp_skey = wfa_emp_pre.emp_skey
JOIN {empl_protected}.legacy_EMPLOYEE_PROFILE ON wfa_emp_pre.prsn_nbr_txt = legacy_employee_profile.employee_id
""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node FIL_35DAYS, type FILTER
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_TDTL_temp = SQ_Shortcut_to_WFA_TDTL.toDF(
    *["SQ_Shortcut_to_WFA_TDTL___" + col for col in SQ_Shortcut_to_WFA_TDTL.columns]
)

FIL_35DAYS = (
    SQ_Shortcut_to_WFA_TDTL_temp.selectExpr(
        "SQ_Shortcut_to_WFA_TDTL___DAY_DT as DAY_DT",
        "SQ_Shortcut_to_WFA_TDTL___TDTL_ID as TDTL_ID",
        "SQ_Shortcut_to_WFA_TDTL___LOAD_DT as LOAD_DT",
    )
    .filter("DAY_DT > CURRENT_DATE() - 36")
    .withColumn("sys_row_id", monotonically_increasing_id())
)


# COMMAND ----------
# Processing node JNR_WFA_TDTL, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 67

# for each involved DataFrame, append the dataframe name to each column
FIL_35DAYS_temp = FIL_35DAYS.toDF(
    *["FIL_35DAYS___" + col for col in FIL_35DAYS.columns]
)
JNR_DAYS_temp = SQ_Shortcut_to_WFA_TDTL_DIFF_PRE.toDF(*["JNR_DAYS___" + col for col in SQ_Shortcut_to_WFA_TDTL_DIFF_PRE.columns])

JNR_WFA_TDTL = JNR_DAYS_temp.join(
    FIL_35DAYS_temp,
    [
        JNR_DAYS_temp.JNR_DAYS___RECORDED_DAT == FIL_35DAYS_temp.FIL_35DAYS___DAY_DT,
        JNR_DAYS_temp.JNR_DAYS___TDTL_ID == FIL_35DAYS_temp.FIL_35DAYS___TDTL_ID,
    ],
    "left_outer",
).selectExpr(
    "FIL_35DAYS___DAY_DT as DAY_DT_target",
    "FIL_35DAYS___TDTL_ID as TDTL_ID_target",
    "FIL_35DAYS___LOAD_DT as LOAD_DT_target",
    # "FIL_35DAYS___sys_row_id as sys_row_id",
  	"JNR_DAYS___sys_row_id as sys_row_id",  
    "JNR_DAYS___TDTL_ID as TDTL_ID1",
    "JNR_DAYS___RECORDED_DAT as RECORDED_DAT",
    "JNR_DAYS___STRT_DTM as STRT_DTM",
    "JNR_DAYS___END_DTM as END_DTM",
    "JNR_DAYS___LOCATION_ID as LOCATION_ID",
    "JNR_DAYS___EMP_ID as EMPLOYEE_ID",
    "JNR_DAYS___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
    "JNR_DAYS___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
    "JNR_DAYS___WFA_DEPT_ID as WFA_DEPT_ID",
    "JNR_DAYS___WFA_DEPT_DESC as WFA_DEPT_DESC",
    "JNR_DAYS___WFA_TASK_ID as WFA_TASK_ID",
    "JNR_DAYS___WFA_TASK_DESC as WFA_TASK_DESC",
    "IF (JNR_DAYS___PRI_LOCATION_ID IS NULL, 0, JNR_DAYS___PRI_LOCATION_ID) as LOCATION_ID_org_prev",
    "IF (JNR_DAYS___PRI_WFA_BUSN_AREA_ID IS NULL, 0, JNR_DAYS___PRI_WFA_BUSN_AREA_ID) as WFA_BUSN_AREA_ID_org_prev",
    "IF (JNR_DAYS___PRI_WFA_BUSN_AREA_DESC IS NULL, 'NA', JNR_DAYS___PRI_WFA_BUSN_AREA_DESC) as WFA_BUSN_AREA_DESC_org_prev",
    "IF (JNR_DAYS___PRI_WFA_DEPT_ID IS NULL, 0, JNR_DAYS___PRI_WFA_DEPT_ID) as WFA_DEPT_ID_org_prev",
    "IF (JNR_DAYS___PRI_WFA_DEPT_DESC IS NULL, 'NA', JNR_DAYS___PRI_WFA_DEPT_DESC) as WFA_DEPT_DESC_org_prev",
    "IF (JNR_DAYS___PRI_WFA_TASK_ID IS NULL, 0, JNR_DAYS___PRI_WFA_TASK_ID) as WFA_TASK_ID_org_prev",
    "IF (JNR_DAYS___PRI_WFA_TASK_DESC IS NULL, 'NA', JNR_DAYS___PRI_WFA_TASK_DESC) as WFA_TASK_DESC_org_prev",
    "JNR_DAYS___TDTL_SKEY as TDTL_SKEY",
    "JNR_DAYS___RECORDED_FOR_DAT as RECORDED_FOR_DAT",
    "JNR_DAYS___ADJ_SWT as ADJ_SWT",
    "JNR_DAYS___EDT_SWT as EDT_SWT",
    "JNR_DAYS___UNSCHD_STRT_DTM as UNSCHD_STRT_DTM",
    "JNR_DAYS___UNSCHD_END_DTM as UNSCHD_END_DTM",
    "JNR_DAYS___STRT_TZONE_SKEY as STRT_TZONE_SKEY",
    "JNR_DAYS___LBRACCT_SKEY as LBRACCT_SKEY",
    "JNR_DAYS___LBRACCT_ID as LBRACCT_ID",
    "JNR_DAYS___HM_LBRACCT_SWT as HM_LBRACCT_SWT",
    "JNR_DAYS___HM_LBRACCT_SKEY as HM_LBRACCT_SKEY",
    "JNR_DAYS___HM_LBRACCT_ID as HM_LBRACCT_ID",  #
    "JNR_DAYS___FROM_LBRACCT_SKEY as FROM_LBRACCT_SKEY",
    "JNR_DAYS___FROM_LBRACCT_ID as FROM_LBRACCT_ID",  #
    "JNR_DAYS___PAYCD_SKEY as PAYCD_SKEY",
    "JNR_DAYS___PAYCD_ID as PAYCD_ID",  #
    "JNR_DAYS___CORE_HRS_SWT as CORE_HRS_SWT",
    "JNR_DAYS___FROM_PAYCD_SKEY as FROM_PAYCD_SKEY",
    "JNR_DAYS___FROM_PAYCD_ID as FROM_PAYCD_ID",
    "JNR_DAYS___SUPV_SKEY as SUPV_SKEY",
    "JNR_DAYS___PRI_JOB_SKEY as PRI_JOB_SKEY",
    "JNR_DAYS___JOB_SKEY as JOB_SKEY",
    "JNR_DAYS___PRI_ORG_SKEY as PRI_ORG_SKEY",
    "JNR_DAYS___ORG_SKEY as ORG_SKEY",
    "JNR_DAYS___PAYPER_SKEY as PAYPER_SKEY",
    "JNR_DAYS___HDAY_SKEY as HDAY_SKEY",
    "JNR_DAYS___STRT_PNCHEVNT_SKEY as STRT_PNCHEVNT_SKEY",
    "JNR_DAYS___END_PNCHEVNT_SKEY as END_PNCHEVNT_SKEY",
    "JNR_DAYS___STRT_DSRC_SKEY as STRT_DSRC_SKEY",
    "JNR_DAYS___END_DSRC_SKEY as END_DSRC_SKEY",
    "JNR_DAYS___DSRC_SKEY as DSRC_SKEY",
    "JNR_DAYS___EMPSTAT_SKEY as EMPSTAT_SKEY",
    "JNR_DAYS___AGE_NBR as AGE_NBR",
    "JNR_DAYS___TENURE_MO_NBR as TENURE_MO_NBR",
    "JNR_DAYS___DFLT_PAY_RULE_SKEY as DFLT_PAY_RULE_SKEY",
    "JNR_DAYS___DFLT_WRK_RULE_SWT as DFLT_WRK_RULE_SWT",
    "JNR_DAYS___DFLT_WRK_RULE_SKEY as DFLT_WRK_RULE_SKEY",
    "JNR_DAYS___WRK_RULE_SKEY as WRK_RULE_SKEY",
    "JNR_DAYS___MONEY_AMT as MONEY_AMT",
    "JNR_DAYS___DRTN_AMT as DRTN_AMT",
    "JNR_DAYS___CORE_AMT as CORE_AMT",
    "JNR_DAYS___NON_CORE_AMT as NON_CORE_AMT",
    "JNR_DAYS___DRTN_DIFF_AMT as DRTN_DIFF_AMT",
    "JNR_DAYS___DRTN_HRS as DRTN_HRS",
    "JNR_DAYS___CORE_HRS as CORE_HRS",
    "JNR_DAYS___NON_CORE_HRS as NON_CORE_HRS",
    "JNR_DAYS___LOCKED_SWT as LOCKED_SWT",
    "JNR_DAYS___GRP_SCHD_SKEY as GRP_SCHD_SKEY",
    "JNR_DAYS___WEEK_DT as WEEK_DT",
    "JNR_DAYS___LBRACCT2_NAM as LBRACCT2_NAM",
)

# for each involved DataFrame, append the dataframe name to each column
JNR_WFA_TDTL_temp = JNR_WFA_TDTL.toDF(
    *["JNR_WFA_TDTL___" + col for col in JNR_WFA_TDTL.columns]
)

UPD_WFA_TDTL = JNR_WFA_TDTL_temp.selectExpr(
    "JNR_WFA_TDTL___TDTL_ID1 as TDTL_ID",
    "JNR_WFA_TDTL___RECORDED_DAT as RECORDED_DAT",
    "JNR_WFA_TDTL___TDTL_ID_target as TDTL_ID_target",
    "JNR_WFA_TDTL___STRT_DTM as STRT_DTM",
    "JNR_WFA_TDTL___END_DTM as END_DTM",
    "JNR_WFA_TDTL___LOCATION_ID as LOCATION_ID",
    "JNR_WFA_TDTL___EMPLOYEE_ID as EMPLOYEE_ID",
    "JNR_WFA_TDTL___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
    "JNR_WFA_TDTL___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
    "JNR_WFA_TDTL___WFA_DEPT_ID as WFA_DEPT_ID",
    "JNR_WFA_TDTL___WFA_DEPT_DESC as WFA_DEPT_DESC",
    "JNR_WFA_TDTL___WFA_TASK_ID as WFA_TASK_ID",
    "JNR_WFA_TDTL___WFA_TASK_DESC as WFA_TASK_DESC",
    "JNR_WFA_TDTL___LOCATION_ID_org_prev as LOCATION_ID_org_prev",
    "JNR_WFA_TDTL___WFA_BUSN_AREA_ID_org_prev as WFA_BUSN_AREA_ID_org_prev",
    "JNR_WFA_TDTL___WFA_BUSN_AREA_DESC_org_prev as WFA_BUSN_AREA_DESC_org_prev",
    "JNR_WFA_TDTL___WFA_DEPT_ID_org_prev as WFA_DEPT_ID_org_prev",
    "JNR_WFA_TDTL___WFA_DEPT_DESC_org_prev as WFA_DEPT_DESC_org_prev",
    "JNR_WFA_TDTL___WFA_TASK_ID_org_prev as WFA_TASK_ID_org_prev",
    "JNR_WFA_TDTL___WFA_TASK_DESC_org_prev as WFA_TASK_DESC_org_prev",
    "JNR_WFA_TDTL___TDTL_SKEY as TDTL_SKEY",
    "JNR_WFA_TDTL___RECORDED_FOR_DAT as RECORDED_FOR_DAT",
    "JNR_WFA_TDTL___ADJ_SWT as ADJ_SWT",
    "JNR_WFA_TDTL___EDT_SWT as EDT_SWT",
    "JNR_WFA_TDTL___UNSCHD_STRT_DTM as UNSCHD_STRT_DTM",
    "JNR_WFA_TDTL___UNSCHD_END_DTM as UNSCHD_END_DTM",
    "JNR_WFA_TDTL___STRT_TZONE_SKEY as STRT_TZONE_SKEY",
    "JNR_WFA_TDTL___LBRACCT_SKEY as LBRACCT_SKEY",
    "JNR_WFA_TDTL___HM_LBRACCT_SWT as HM_LBRACCT_SWT",
    "JNR_WFA_TDTL___HM_LBRACCT_SKEY as HM_LBRACCT_SKEY",
    "JNR_WFA_TDTL___FROM_LBRACCT_SKEY as FROM_LBRACCT_SKEY",
    "JNR_WFA_TDTL___PAYCD_SKEY as PAYCD_SKEY",
    "JNR_WFA_TDTL___CORE_HRS_SWT as CORE_HRS_SWT",
    "JNR_WFA_TDTL___FROM_PAYCD_SKEY as FROM_PAYCD_SKEY",
    "JNR_WFA_TDTL___SUPV_SKEY as SUPV_SKEY",
    "JNR_WFA_TDTL___PRI_JOB_SKEY as PRI_JOB_SKEY",
    "JNR_WFA_TDTL___JOB_SKEY as JOB_SKEY",
    "JNR_WFA_TDTL___PRI_ORG_SKEY as PRI_ORG_SKEY",
    "JNR_WFA_TDTL___ORG_SKEY as ORG_SKEY",
    "JNR_WFA_TDTL___PAYPER_SKEY as PAYPER_SKEY",
    "JNR_WFA_TDTL___HDAY_SKEY as HDAY_SKEY",
    "JNR_WFA_TDTL___STRT_PNCHEVNT_SKEY as STRT_PNCHEVNT_SKEY",
    "JNR_WFA_TDTL___END_PNCHEVNT_SKEY as END_PNCHEVNT_SKEY",
    "JNR_WFA_TDTL___STRT_DSRC_SKEY as STRT_DSRC_SKEY",
    "JNR_WFA_TDTL___END_DSRC_SKEY as END_DSRC_SKEY",
    "JNR_WFA_TDTL___DSRC_SKEY as DSRC_SKEY",
    "JNR_WFA_TDTL___EMPSTAT_SKEY as EMPSTAT_SKEY",
    "JNR_WFA_TDTL___AGE_NBR as AGE_NBR",
    "JNR_WFA_TDTL___TENURE_MO_NBR as TENURE_MO_NBR",
    "JNR_WFA_TDTL___DFLT_PAY_RULE_SKEY as DFLT_PAY_RULE_SKEY",
    "JNR_WFA_TDTL___DFLT_WRK_RULE_SWT as DFLT_WRK_RULE_SWT",
    "JNR_WFA_TDTL___DFLT_WRK_RULE_SKEY as DFLT_WRK_RULE_SKEY",
    "JNR_WFA_TDTL___WRK_RULE_SKEY as WRK_RULE_SKEY",
    "JNR_WFA_TDTL___MONEY_AMT as MONEY_AMT",
    "JNR_WFA_TDTL___DRTN_AMT as DRTN_AMT",
    "JNR_WFA_TDTL___CORE_AMT as CORE_AMT",
    "JNR_WFA_TDTL___NON_CORE_AMT as NON_CORE_AMT",
    "JNR_WFA_TDTL___DRTN_DIFF_AMT as DRTN_DIFF_AMT",
    "JNR_WFA_TDTL___DRTN_HRS as DRTN_HRS",
    "JNR_WFA_TDTL___CORE_HRS as CORE_HRS",
    "JNR_WFA_TDTL___NON_CORE_HRS as NON_CORE_HRS",
    "JNR_WFA_TDTL___LOCKED_SWT as LOCKED_SWT",
    "JNR_WFA_TDTL___GRP_SCHD_SKEY as GRP_SCHD_SKEY",
    "IF (JNR_WFA_TDTL___TDTL_ID_target IS NULL, CURRENT_TIMESTAMP, JNR_WFA_TDTL___LOAD_DT_target) as LOAD_TSTMP",
    "CURRENT_TIMESTAMP as UPDATE_TSTMP",
    "JNR_WFA_TDTL___WEEK_DT as WEEK_DT",
    "JNR_WFA_TDTL___LBRACCT_ID as LBRACCT_ID",
    "JNR_WFA_TDTL___HM_LBRACCT_ID as HM_LBRACCT_ID",
    "JNR_WFA_TDTL___FROM_LBRACCT_ID as FROM_LBRACCT_ID",
    "JNR_WFA_TDTL___PAYCD_ID as PAYCD_ID",
    "JNR_WFA_TDTL___FROM_PAYCD_ID as FROM_PAYCD_ID",
    "JNR_WFA_TDTL___LBRACCT2_NAM as WORK_LOCATION_ID",
    "IF(JNR_WFA_TDTL___TDTL_ID_target is null, 0, 1) as pyspark_data_action",
)

# COMMAND ----------
# Processing node Shortcut_to_WFA_TDTL1, type TARGET
# COLUMN COUNT: 72


Shortcut_to_WFA_TDTL1 = UPD_WFA_TDTL.selectExpr(
    "CAST(RECORDED_DAT AS TIMESTAMP) as DAY_DT",
    "TDTL_ID as TDTL_ID",
    "CAST(STRT_DTM AS TIMESTAMP) as STRT_DTM",
    "CAST(END_DTM AS TIMESTAMP) as END_DTM",
    "CAST(LOCATION_ID AS BIGINT) as LOCATION_ID",
    "EMPLOYEE_ID as EMPLOYEE_ID",
    "WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
    "CAST(WFA_BUSN_AREA_DESC AS STRING) as WFA_BUSN_AREA_DESC",
    "WFA_DEPT_ID as WFA_DEPT_ID",
    "CAST(WFA_DEPT_DESC AS STRING) as WFA_DEPT_DESC",
    "WFA_TASK_ID as WFA_TASK_ID",
    "CAST(WFA_TASK_DESC AS STRING) as WFA_TASK_DESC",
    "CAST(LOCATION_ID_org_prev AS BIGINT) as PRI_LOCATION_ID",
    "WFA_BUSN_AREA_ID_org_prev as PRI_WFA_BUSN_AREA_ID",
    "CAST(WFA_BUSN_AREA_DESC_org_prev AS STRING) as PRI_WFA_BUSN_AREA_DESC",
    "WFA_DEPT_ID_org_prev as PRI_WFA_DEPT_ID",
    "CAST(WFA_DEPT_DESC_org_prev AS STRING) as PRI_WFA_DEPT_DESC",
    "WFA_TASK_ID_org_prev as PRI_WFA_TASK_ID",
    "CAST(WFA_TASK_DESC_org_prev AS STRING) as PRI_WFA_TASK_DESC",
    "TDTL_SKEY as TSITEM_ID",
    "CAST(RECORDED_FOR_DAT AS TIMESTAMP) as RECORDED_FOR_DAT",
    "CAST(ADJ_SWT AS BIGINT) as ADJ_SWT",
    "CAST(EDT_SWT AS BIGINT) as EDT_SWT",
    "null as EMP_SKEY",
    "CAST(UNSCHD_STRT_DTM AS TIMESTAMP) as UNSCHD_STRT_DTM",
    "CAST(UNSCHD_END_DTM AS TIMESTAMP) as UNSCHD_END_DTM",
    "STRT_TZONE_SKEY as STRT_TZONE_SKEY",
    "LBRACCT_SKEY as LBRACCT_SKEY",
    "LBRACCT_ID as LBRACCT_ID",
    "CAST(HM_LBRACCT_SWT AS BIGINT) as HM_LBRACCT_SWT",
    "HM_LBRACCT_SKEY as HM_LBRACCT_SKEY",
    "HM_LBRACCT_ID as HM_LBRACCT_ID",
    "FROM_LBRACCT_SKEY as FROM_LBRACCT_SKEY",
    "FROM_LBRACCT_ID as FROM_LBRACCT_ID",
    "PAYCD_SKEY as PAYCD_SKEY",
    "PAYCD_ID as PAYCD_ID",
    "CAST(CORE_HRS_SWT AS BIGINT) as CORE_HRS_SWT",
    "FROM_PAYCD_SKEY as FROM_PAYCD_SKEY",
    "FROM_PAYCD_ID as FROM_PAYCD_ID",
    "SUPV_SKEY as SUPV_SKEY",
    "PRI_JOB_SKEY as PRI_JOB_SKEY",
    "JOB_SKEY as JOB_SKEY",
    "PRI_ORG_SKEY as PRI_ORG_SKEY",
    "ORG_SKEY as ORG_SKEY",
    "PAYPER_SKEY as PAYPER_SKEY",
    "HDAY_SKEY as HDAY_SKEY",
    "STRT_PNCHEVNT_SKEY as STRT_PNCHEVNT_SKEY",
    "END_PNCHEVNT_SKEY as END_PNCHEVNT_SKEY",
    "STRT_DSRC_SKEY as STRT_DSRC_SKEY",
    "END_DSRC_SKEY as END_DSRC_SKEY",
    "DSRC_SKEY as DSRC_SKEY",
    "EMPSTAT_SKEY as EMPSTAT_SKEY",
    "AGE_NBR as AGE_NBR",
    "TENURE_MO_NBR as TENURE_MO_NBR",
    "DFLT_PAY_RULE_SKEY as DFLT_PAY_RULE_SKEY",
    "CAST(DFLT_WRK_RULE_SWT AS BIGINT) as DFLT_WRK_RULE_SWT",
    "DFLT_WRK_RULE_SKEY as DFLT_WRK_RULE_SKEY",
    "WRK_RULE_SKEY as WRK_RULE_SKEY",
    "CAST(MONEY_AMT AS DECIMAL(16,6)) as MONEY_AMT",
    "CAST(DRTN_AMT AS DECIMAL(16,6)) as DRTN_AMT",
    "CAST(CORE_AMT AS DECIMAL(16,6)) as CORE_AMT",
    "CAST(NON_CORE_AMT AS DECIMAL(16,6)) as NON_CORE_AMT",
    "CAST(DRTN_DIFF_AMT AS DECIMAL(16,6)) as DRTN_DIFF_AMT",
    "CAST(DRTN_HRS AS DECIMAL(16,6)) as DRTN_HRS",
    "CAST(CORE_HRS AS DECIMAL(16,6)) as CORE_HRS",
    "CAST(NON_CORE_HRS AS DECIMAL(16,6)) as NON_CORE_HRS",
    "CAST(LOCKED_SWT AS BIGINT) as LOCKED_SWT",
    "GRP_SCHD_SKEY as GRP_SCHD_SKEY",
    "WORK_LOCATION_ID as WORK_LOCATION_ID",
    "CAST(WEEK_DT AS TIMESTAMP) as WEEK_DT",
    "CAST(LOAD_TSTMP AS TIMESTAMP) as UPDATE_DT",
    "CAST(UPDATE_TSTMP AS TIMESTAMP) as LOAD_DT",
    "pyspark_data_action as pyspark_data_action",
)
Shortcut_to_WFA_TDTL1.createOrReplaceTempView("Shortcut_to_WFA_TDTL1")
       
try:
    primary_key = (
        """source.DAY_DT = target.DAY_DT AND source.TDTL_ID = target.TDTL_ID"""
    )
    refined_perf_table = f"{empl_protected}.legacy_WFA_TDTL"
    executeMerge(Shortcut_to_WFA_TDTL1, refined_perf_table, primary_key)
    logger.info(f"Merge with {refined_perf_table} completed]")
    logPrevRunDt("WFA_TDTL", "WFA_TDTL", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
    logPrevRunDt("WFA_TDTL", "WFA_TDTL", "Failed", str(e), f"{raw}.log_run_details")
    raise e