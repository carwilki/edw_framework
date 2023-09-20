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

args = parser.parse_args()
env = args.env

if env is None or env == "":
    raise ValueError("env is not set")

refine = getEnvPrefix(env) + "refine"
raw = getEnvPrefix(env) + "raw"
legacy = getEnvPrefix(env) + "legacy"
enterprise = getEnvPrefix(env) + "enterprise"
empl_protected = getEnvPrefix(env) + "empl_protected"


# COMMAND ----------
# Processing node LKP_WFA_LBRACCT_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

# LKP_WFA_LBRACCT_SRC = spark.sql(
#     f"""SELECT
# LBRACCT_ID,
# LBRACCT_SKEY
# FROM {legacy}.WFA_LBRACCT"""
# )
# # Conforming fields names to the component layout
# LKP_WFA_LBRACCT_SRC = (
#     LKP_WFA_LBRACCT_SRC.withColumnRenamed(LKP_WFA_LBRACCT_SRC.columns[0], "LBRACCT_ID")
#     .withColumnRenamed(LKP_WFA_LBRACCT_SRC.columns[1], "LBRACCT_SKEY")
#     .withColumnRenamed(LKP_WFA_LBRACCT_SRC.columns[1], "in_LBRACCT_SKEY")
# )

# COMMAND ----------
# Processing node LKP_WFA_PAYCD_SRC, type SOURCE Prerequisite Lookup Object
# COLUMN COUNT: 3

# LKP_WFA_PAYCD_SRC = spark.sql(
#     f"""SELECT
# PAYCD_ID,
# PAYCD_SKEY
# FROM {legacy}.WFA_PAYCD"""
# )
# # Conforming fields names to the component layout
# LKP_WFA_PAYCD_SRC = (
#     LKP_WFA_PAYCD_SRC.withColumnRenamed(LKP_WFA_PAYCD_SRC.columns[0], "PAYCD_ID")
#     .withColumnRenamed(LKP_WFA_PAYCD_SRC.columns[1], "PAYCD_SKEY")
#     .withColumnRenamed(LKP_WFA_PAYCD_SRC.columns[1], "i_PAYCD_SKEY")
# )

# COMMAND ----------
# Processing node LKP_SITE_PROFILE_SRC, type SOURCE Cached data from connected lookup object
# COLUMN COUNT: 3

LKP_SITE_PROFILE_SRC = spark.sql(
    f"""SELECT
LOCATION_ID,
STORE_NBR
FROM {legacy}.SITE_PROFILE"""
)
# Conforming fields names to the component layout
LKP_SITE_PROFILE_SRC = (
    LKP_SITE_PROFILE_SRC.withColumnRenamed(
        LKP_SITE_PROFILE_SRC.columns[0], "LOCATION_ID"
    )
    .withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[1], "STORE_NBR")
    .withColumnRenamed(LKP_SITE_PROFILE_SRC.columns[1], "LBRACCT2_NAM1")
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_ORG_FACT_PRE_new, type SOURCE
# COLUMN COUNT: 8

SQ_Shortcut_to_WFA_ORG_FACT_PRE_new = spark.sql(
    f"""SELECT
WFA_ORG_FACT_PRE.LOCATION_ID,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_ID,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_DESC,
WFA_ORG_FACT_PRE.WFA_DEPT_ID,
WFA_ORG_FACT_PRE.WFA_DEPT_DESC,
WFA_ORG_FACT_PRE.WFA_TASK_ID,
WFA_ORG_FACT_PRE.WFA_TASK_DESC,
WFA_ORG_FACT_PRE.ORG_SKEY
FROM {raw}.WFA_ORG_FACT_PRE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_ORG_ACTUAL, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_ORG_FACT_PRE_new_temp = SQ_Shortcut_to_WFA_ORG_FACT_PRE_new.toDF(
    *[
        "SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___" + col
        for col in SQ_Shortcut_to_WFA_ORG_FACT_PRE_new.columns
    ]
)

EXP_ORG_ACTUAL = SQ_Shortcut_to_WFA_ORG_FACT_PRE_new_temp.selectExpr(
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___LOCATION_ID as LOCATION_ID",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___WFA_DEPT_ID as WFA_DEPT_ID",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___WFA_DEPT_DESC as WFA_DEPT_DESC",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___WFA_TASK_ID as WFA_TASK_ID",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___WFA_TASK_DESC as WFA_TASK_DESC",
    "BIGINT(SQ_Shortcut_to_WFA_ORG_FACT_PRE_new___ORG_SKEY) as ORG_SKEY",
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_EMPLOYEE_PROFILE, type SOURCE
# COLUMN COUNT: 1

SQ_Shortcut_to_EMPLOYEE_PROFILE = spark.sql(
    f"""SELECT
legacy_EMPLOYEE_PROFILE.EMPLOYEE_ID
FROM {empl_protected}.legacy_EMPLOYEE_PROFILE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_EMP_PRE, type SOURCE
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_EMP_PRE = spark.sql(
    f"""SELECT
WFA_EMP_PRE.EMP_SKEY,
WFA_EMP_PRE.PRSN_NBR_TXT
FROM {raw}.WFA_EMP_PRE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_EMP, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_EMP_PRE_temp = SQ_Shortcut_to_WFA_EMP_PRE.toDF(
    *[
        "SQ_Shortcut_to_WFA_EMP_PRE___" + col
        for col in SQ_Shortcut_to_WFA_EMP_PRE.columns
    ]
)

EXP_EMP = SQ_Shortcut_to_WFA_EMP_PRE_temp.selectExpr(
    "SQ_Shortcut_to_WFA_EMP_PRE___sys_row_id as sys_row_id",
    "BIGINT(SQ_Shortcut_to_WFA_EMP_PRE___EMP_SKEY) as EMP_SKEY",
    "cast(SQ_Shortcut_to_WFA_EMP_PRE___PRSN_NBR_TXT as int) as PRSN_NBR_TXT",
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev, type SOURCE
# COLUMN COUNT: 8

SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev = spark.sql(
    f"""SELECT
WFA_ORG_FACT_PRE.LOCATION_ID,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_ID,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_DESC,
WFA_ORG_FACT_PRE.WFA_DEPT_ID,
WFA_ORG_FACT_PRE.WFA_DEPT_DESC,
WFA_ORG_FACT_PRE.WFA_TASK_ID,
WFA_ORG_FACT_PRE.WFA_TASK_DESC,
WFA_ORG_FACT_PRE.ORG_SKEY
FROM {raw}.WFA_ORG_FACT_PRE"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TDTL, type SOURCE
# COLUMN COUNT: 3

SQ_Shortcut_to_WFA_TDTL = spark.sql(
    f"""SELECT
legacy_WFA_TDTL.DAY_DT,
legacy_WFA_TDTL.TDTL_ID,
legacy_WFA_TDTL.LOAD_DT
FROM {empl_protected}.legacy_WFA_TDTL"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_DAYS, type SOURCE
# COLUMN COUNT: 2

SQ_Shortcut_to_DAYS = spark.sql(
    f"""SELECT
DAYS.DAY_DT,
DAYS.WEEK_DT
FROM {enterprise}.DAYS"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TDTL_DIFF_PRE, type SOURCE
# COLUMN COUNT: 48

SQ_Shortcut_to_WFA_TDTL_DIFF_PRE = spark.sql(
    f"""SELECT
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
HM_WFA_LBRACCT.LBRACCT_ID AS HM_LBRACCT_ID,
FROM_WFA_LBRACCT.LBRACCT_ID AS FROM_LBRACCT_ID
FROM {empl_protected}.raw_WFA_TDTL_DIFF_PRE
LEFT JOIN {legacy}.WFA_PAYCD ON raw_WFA_TDTL_DIFF_PRE.PAYCD_SKEY = WFA_PAYCD.PAYCD_SKEY
LEFT JOIN {legacy}.WFA_PAYCD FROM_WFA_PAYCD ON raw_WFA_TDTL_DIFF_PRE.FROM_PAYCD_SKEY = FROM_WFA_PAYCD.PAYCD_SKEY
LEFT JOIN {legacy}.WFA_LBRACCT ON raw_WFA_TDTL_DIFF_PRE.LBRACCT_SKEY = WFA_LBRACCT.LBRACCT_SKEY
LEFT JOIN {legacy}.WFA_LBRACCT HM_WFA_LBRACCT ON raw_WFA_TDTL_DIFF_PRE.HM_LBRACCT_SKEY = HM_WFA_LBRACCT.LBRACCT_SKEY
LEFT JOIN {legacy}.WFA_LBRACCT FROM_WFA_LBRACCT ON raw_WFA_TDTL_DIFF_PRE.FROM_LBRACCT_SKEY = FROM_WFA_LBRACCT.LBRACCT_SKEY"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_LBRACCT, type SOURCE
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_LBRACCT = spark.sql(
    f"""SELECT
WFA_LBRACCT.LBRACCT_SKEY,
WFA_LBRACCT.LBRACCT2_NAM
FROM {legacy}.WFA_LBRACCT"""
).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXPTRANS1, type EXPRESSION
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_LBRACCT_temp = SQ_Shortcut_to_WFA_LBRACCT.toDF(
    *[
        "SQ_Shortcut_to_WFA_LBRACCT___" + col
        for col in SQ_Shortcut_to_WFA_LBRACCT.columns
    ]
)

EXPTRANS1 = SQ_Shortcut_to_WFA_LBRACCT_temp.selectExpr(
    "SQ_Shortcut_to_WFA_LBRACCT___sys_row_id as sys_row_id",
    "BIGINT(SQ_Shortcut_to_WFA_LBRACCT___LBRACCT_SKEY) as LBRACCT_SKEY1",
    "SQ_Shortcut_to_WFA_LBRACCT___LBRACCT2_NAM as LBRACCT2_NAM",
)

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
    .filter("DAY_DT > CURRENT_DATE - 36")
    .withColumn("sys_row_id", monotonically_increasing_id())
)

# COMMAND ----------
# Processing node EXP_ORG_PREV, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev_temp = SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev.toDF(
    *[
        "SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___" + col
        for col in SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev.columns
    ]
)

EXP_ORG_PREV = SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev_temp.selectExpr(
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___sys_row_id as sys_row_id",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___LOCATION_ID as LOCATION_ID",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___WFA_DEPT_ID as WFA_DEPT_ID",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___WFA_DEPT_DESC as WFA_DEPT_DESC",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___WFA_TASK_ID as WFA_TASK_ID",
    "SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___WFA_TASK_DESC as WFA_TASK_DESC",
    "BIGINT(SQ_Shortcut_to_WFA_ORG_FACT_PRE_prev___ORG_SKEY) as ORG_SKEY",
)

# COMMAND ----------
# Processing node JNR_ORG_ACTUAL, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 56

# for each involved DataFrame, append the dataframe name to each column
EXP_ORG_ACTUAL_temp = EXP_ORG_ACTUAL.toDF(
    *["EXP_ORG_ACTUAL___" + col for col in EXP_ORG_ACTUAL.columns]
)
SQ_Shortcut_to_WFA_TDTL_DIFF_PRE_temp = SQ_Shortcut_to_WFA_TDTL_DIFF_PRE.toDF(
    *[
        "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___" + col
        for col in SQ_Shortcut_to_WFA_TDTL_DIFF_PRE.columns
    ]
)

JNR_ORG_ACTUAL = EXP_ORG_ACTUAL_temp.join(
    SQ_Shortcut_to_WFA_TDTL_DIFF_PRE_temp,
    [
        EXP_ORG_ACTUAL_temp.EXP_ORG_ACTUAL___ORG_SKEY
        == SQ_Shortcut_to_WFA_TDTL_DIFF_PRE_temp.SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___ORG_SKEY
    ],
    "inner",
).selectExpr(
    "EXP_ORG_ACTUAL___ORG_SKEY as ORG_SKEY",
    "EXP_ORG_ACTUAL___LOCATION_ID as LOCATION_ID",
    "EXP_ORG_ACTUAL___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
    "EXP_ORG_ACTUAL___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
    "EXP_ORG_ACTUAL___WFA_DEPT_ID as WFA_DEPT_ID",
    "EXP_ORG_ACTUAL___WFA_DEPT_DESC as WFA_DEPT_DESC",
    "EXP_ORG_ACTUAL___WFA_TASK_ID as WFA_TASK_ID",
    "EXP_ORG_ACTUAL___WFA_TASK_DESC as WFA_TASK_DESC",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___TDTL_ID as TDTL_ID",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___RECORDED_DAT as RECORDED_DAT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___TDTL_SKEY as TDTL_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___RECORDED_FOR_DAT as RECORDED_FOR_DAT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___ADJ_SWT as ADJ_SWT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___EDT_SWT as EDT_SWT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___EMP_SKEY as EMP_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___STRT_DTM as STRT_DTM",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___END_DTM as END_DTM",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___UNSCHD_STRT_DTM as UNSCHD_STRT_DTM",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___UNSCHD_END_DTM as UNSCHD_END_DTM",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___STRT_TZONE_SKEY as STRT_TZONE_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___LBRACCT_SKEY as LBRACCT_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___LBRACCT_ID as LBRACCT_ID",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___HM_LBRACCT_SWT as HM_LBRACCT_SWT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___HM_LBRACCT_SKEY as HM_LBRACCT_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___HM_LBRACCT_ID as HM_LBRACCT_ID",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___FROM_LBRACCT_SKEY as FROM_LBRACCT_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___FROM_LBRACCT_ID as FROM_LBRACCT_ID",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___PAYCD_SKEY as PAYCD_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___PAYCD_ID as PAYCD_ID",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___CORE_HRS_SWT as CORE_HRS_SWT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___FROM_PAYCD_SKEY as FROM_PAYCD_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___FROM_PAYCD_ID as FROM_PAYCD_ID",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___SUPV_SKEY as SUPV_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___PRI_JOB_SKEY as PRI_JOB_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___JOB_SKEY as JOB_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___PRI_ORG_SKEY as PRI_ORG_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___ORG_SKEY as ORG_SKEY1",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___PAYPER_SKEY as PAYPER_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___HDAY_SKEY as HDAY_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___STRT_PNCHEVNT_SKEY as STRT_PNCHEVNT_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___END_PNCHEVNT_SKEY as END_PNCHEVNT_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___STRT_DSRC_SKEY as STRT_DSRC_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___END_DSRC_SKEY as END_DSRC_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___DSRC_SKEY as DSRC_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___EMPSTAT_SKEY as EMPSTAT_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___AGE_NBR as AGE_NBR",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___TENURE_MO_NBR as TENURE_MO_NBR",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___DFLT_PAY_RULE_SKEY as DFLT_PAY_RULE_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___DFLT_WRK_RULE_SWT as DFLT_WRK_RULE_SWT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___DFLT_WRK_RULE_SKEY as DFLT_WRK_RULE_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___WRK_RULE_SKEY as WRK_RULE_SKEY",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___MONEY_AMT as MONEY_AMT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___DRTN_AMT as DRTN_AMT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___CORE_AMT as CORE_AMT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___NON_CORE_AMT as NON_CORE_AMT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___DRTN_DIFF_AMT as DRTN_DIFF_AMT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___DRTN_HRS as DRTN_HRS",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___CORE_HRS as CORE_HRS",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___NON_CORE_HRS as NON_CORE_HRS",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___LOCKED_SWT as LOCKED_SWT",
    "SQ_Shortcut_to_WFA_TDTL_DIFF_PRE___GRP_SCHD_SKEY as GRP_SCHD_SKEY",
)

# COMMAND ----------
# Processing node Jnr_Wfa_Lbracct, type JOINER
# COLUMN COUNT: 58

Jnr_Wfa_Lbracct = EXPTRANS1.join(
    JNR_ORG_ACTUAL,
    [EXPTRANS1.LBRACCT_SKEY1 == JNR_ORG_ACTUAL.LBRACCT_SKEY],
    "right_outer",
)

# COMMAND ----------
# Processing node JNR_ORG_PREV, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 64

# for each involved DataFrame, append the dataframe name to each column
EXP_ORG_PREV_temp = EXP_ORG_PREV.toDF(
    *["EXP_ORG_PREV___" + col for col in EXP_ORG_PREV.columns]
)
Jnr_Wfa_Lbracct_temp = Jnr_Wfa_Lbracct.toDF(
    *["Jnr_Wfa_Lbracct___" + col for col in Jnr_Wfa_Lbracct.columns]
)

JNR_ORG_PREV = EXP_ORG_PREV_temp.join(
    Jnr_Wfa_Lbracct_temp,
    [
        EXP_ORG_PREV_temp.EXP_ORG_PREV___ORG_SKEY
        == Jnr_Wfa_Lbracct_temp.Jnr_Wfa_Lbracct___PRI_ORG_SKEY
    ],
    "right_outer",
).selectExpr(
    "Jnr_Wfa_Lbracct___ORG_SKEY as ORG_SKEY",
    "Jnr_Wfa_Lbracct___LOCATION_ID as LOCATION_ID",
    "Jnr_Wfa_Lbracct___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
    "Jnr_Wfa_Lbracct___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
    "Jnr_Wfa_Lbracct___WFA_DEPT_ID as WFA_DEPT_ID",
    "Jnr_Wfa_Lbracct___WFA_DEPT_DESC as WFA_DEPT_DESC",
    "Jnr_Wfa_Lbracct___WFA_TASK_ID as WFA_TASK_ID",
    "Jnr_Wfa_Lbracct___WFA_TASK_DESC as WFA_TASK_DESC",
    "Jnr_Wfa_Lbracct___TDTL_ID as TDTL_ID",
    "Jnr_Wfa_Lbracct___RECORDED_DAT as RECORDED_DAT",
    "Jnr_Wfa_Lbracct___TDTL_SKEY as TDTL_SKEY",
    "Jnr_Wfa_Lbracct___RECORDED_FOR_DAT as RECORDED_FOR_DAT",
    "Jnr_Wfa_Lbracct___ADJ_SWT as ADJ_SWT",
    "Jnr_Wfa_Lbracct___EDT_SWT as EDT_SWT",
    "Jnr_Wfa_Lbracct___EMP_SKEY as EMP_SKEY",
    "Jnr_Wfa_Lbracct___STRT_DTM as STRT_DTM",
    "Jnr_Wfa_Lbracct___END_DTM as END_DTM",
    "Jnr_Wfa_Lbracct___UNSCHD_STRT_DTM as UNSCHD_STRT_DTM",
    "Jnr_Wfa_Lbracct___UNSCHD_END_DTM as UNSCHD_END_DTM",
    "Jnr_Wfa_Lbracct___STRT_TZONE_SKEY as STRT_TZONE_SKEY",
    "Jnr_Wfa_Lbracct___LBRACCT_SKEY as LBRACCT_SKEY",
    "Jnr_Wfa_Lbracct___LBRACCT_ID as LBRACCT_ID",
    "Jnr_Wfa_Lbracct___HM_LBRACCT_SWT as HM_LBRACCT_SWT",
    "Jnr_Wfa_Lbracct___HM_LBRACCT_SKEY as HM_LBRACCT_SKEY",
    "Jnr_Wfa_Lbracct___HM_LBRACCT_ID as HM_LBRACCT_ID",
    "Jnr_Wfa_Lbracct___FROM_LBRACCT_SKEY as FROM_LBRACCT_SKEY",
    "Jnr_Wfa_Lbracct___FROM_LBRACCT_ID as FROM_LBRACCT_ID",
    "Jnr_Wfa_Lbracct___PAYCD_SKEY as PAYCD_SKEY",
    "Jnr_Wfa_Lbracct___PAYCD_ID as PAYCD_ID",
    "Jnr_Wfa_Lbracct___CORE_HRS_SWT as CORE_HRS_SWT",
    "Jnr_Wfa_Lbracct___FROM_PAYCD_SKEY as FROM_PAYCD_SKEY",
    "Jnr_Wfa_Lbracct___FROM_PAYCD_ID as FROM_PAYCD_ID",
    "Jnr_Wfa_Lbracct___SUPV_SKEY as SUPV_SKEY",
    "Jnr_Wfa_Lbracct___PRI_JOB_SKEY as PRI_JOB_SKEY",
    "Jnr_Wfa_Lbracct___JOB_SKEY as JOB_SKEY",
    "Jnr_Wfa_Lbracct___PRI_ORG_SKEY as PRI_ORG_SKEY",
    "EXP_ORG_PREV___LOCATION_ID as LOCATION_ID_org_prev",
    "EXP_ORG_PREV___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID_org_prev",
    "EXP_ORG_PREV___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC_org_prev",
    "EXP_ORG_PREV___WFA_DEPT_ID as WFA_DEPT_ID_org_prev",
    "EXP_ORG_PREV___WFA_DEPT_DESC as WFA_DEPT_DESC_org_prev",
    "EXP_ORG_PREV___WFA_TASK_ID as WFA_TASK_ID_org_prev",
    "EXP_ORG_PREV___WFA_TASK_DESC as WFA_TASK_DESC_org_prev",
    "EXP_ORG_PREV___ORG_SKEY as ORG_SKEY_org_prev",
    "Jnr_Wfa_Lbracct___PAYPER_SKEY as PAYPER_SKEY",
    "Jnr_Wfa_Lbracct___HDAY_SKEY as HDAY_SKEY",
    "Jnr_Wfa_Lbracct___STRT_PNCHEVNT_SKEY as STRT_PNCHEVNT_SKEY",
    "Jnr_Wfa_Lbracct___END_PNCHEVNT_SKEY as END_PNCHEVNT_SKEY",
    "Jnr_Wfa_Lbracct___STRT_DSRC_SKEY as STRT_DSRC_SKEY",
    "Jnr_Wfa_Lbracct___END_DSRC_SKEY as END_DSRC_SKEY",
    "Jnr_Wfa_Lbracct___DSRC_SKEY as DSRC_SKEY",
    "Jnr_Wfa_Lbracct___EMPSTAT_SKEY as EMPSTAT_SKEY",
    "Jnr_Wfa_Lbracct___AGE_NBR as AGE_NBR",
    "Jnr_Wfa_Lbracct___TENURE_MO_NBR as TENURE_MO_NBR",
    "Jnr_Wfa_Lbracct___DFLT_PAY_RULE_SKEY as DFLT_PAY_RULE_SKEY",
    "Jnr_Wfa_Lbracct___DFLT_WRK_RULE_SWT as DFLT_WRK_RULE_SWT",
    "Jnr_Wfa_Lbracct___DFLT_WRK_RULE_SKEY as DFLT_WRK_RULE_SKEY",
    "Jnr_Wfa_Lbracct___WRK_RULE_SKEY as WRK_RULE_SKEY",
    "Jnr_Wfa_Lbracct___MONEY_AMT as MONEY_AMT",
    "Jnr_Wfa_Lbracct___DRTN_AMT as DRTN_AMT",
    "Jnr_Wfa_Lbracct___CORE_AMT as CORE_AMT",
    "Jnr_Wfa_Lbracct___NON_CORE_AMT as NON_CORE_AMT",
    "Jnr_Wfa_Lbracct___DRTN_DIFF_AMT as DRTN_DIFF_AMT",
    "Jnr_Wfa_Lbracct___DRTN_HRS as DRTN_HRS",
    "Jnr_Wfa_Lbracct___CORE_HRS as CORE_HRS",
    "Jnr_Wfa_Lbracct___NON_CORE_HRS as NON_CORE_HRS",
    "Jnr_Wfa_Lbracct___LOCKED_SWT as LOCKED_SWT",
    "Jnr_Wfa_Lbracct___GRP_SCHD_SKEY as GRP_SCHD_SKEY",
    "Jnr_Wfa_Lbracct___LBRACCT2_NAM as LBRACCT2_NAM",
)

# COMMAND ----------
# Processing node JNR_EMP, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 66

# for each involved DataFrame, append the dataframe name to each column
JNR_ORG_PREV_temp = JNR_ORG_PREV.toDF(
    *["JNR_ORG_PREV___" + col for col in JNR_ORG_PREV.columns]
)
EXP_EMP_temp = EXP_EMP.toDF(*["EXP_EMP___" + col for col in EXP_EMP.columns])

JNR_EMP = EXP_EMP_temp.join(
    JNR_ORG_PREV_temp,
    [EXP_EMP_temp.EXP_EMP___EMP_SKEY == JNR_ORG_PREV_temp.JNR_ORG_PREV___EMP_SKEY],
    "right_outer",
).selectExpr(
    "JNR_ORG_PREV___ORG_SKEY as ORG_SKEY",
    "JNR_ORG_PREV___LOCATION_ID as LOCATION_ID",
    "JNR_ORG_PREV___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
    "JNR_ORG_PREV___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
    "JNR_ORG_PREV___WFA_DEPT_ID as WFA_DEPT_ID",
    "JNR_ORG_PREV___WFA_DEPT_DESC as WFA_DEPT_DESC",
    "JNR_ORG_PREV___WFA_TASK_ID as WFA_TASK_ID",
    "JNR_ORG_PREV___WFA_TASK_DESC as WFA_TASK_DESC",
    "JNR_ORG_PREV___TDTL_ID as TDTL_ID",
    "JNR_ORG_PREV___RECORDED_DAT as RECORDED_DAT",
    "JNR_ORG_PREV___TDTL_SKEY as TDTL_SKEY",
    "JNR_ORG_PREV___RECORDED_FOR_DAT as RECORDED_FOR_DAT",
    "JNR_ORG_PREV___ADJ_SWT as ADJ_SWT",
    "JNR_ORG_PREV___EDT_SWT as EDT_SWT",
    "JNR_ORG_PREV___EMP_SKEY as EMP_SKEY",
    "JNR_ORG_PREV___STRT_DTM as STRT_DTM",
    "JNR_ORG_PREV___END_DTM as END_DTM",
    "JNR_ORG_PREV___UNSCHD_STRT_DTM as UNSCHD_STRT_DTM",
    "JNR_ORG_PREV___UNSCHD_END_DTM as UNSCHD_END_DTM",
    "JNR_ORG_PREV___STRT_TZONE_SKEY as STRT_TZONE_SKEY",
    "JNR_ORG_PREV___LBRACCT_SKEY as LBRACCT_SKEY",
    "JNR_ORG_PREV___LBRACCT_ID as LBRACCT_ID",
    "JNR_ORG_PREV___HM_LBRACCT_SWT as HM_LBRACCT_SWT",
    "JNR_ORG_PREV___HM_LBRACCT_SKEY as HM_LBRACCT_SKEY",
    "JNR_ORG_PREV___HM_LBRACCT_ID as HM_LBRACCT_ID",
    "JNR_ORG_PREV___FROM_LBRACCT_SKEY as FROM_LBRACCT_SKEY",
    "JNR_ORG_PREV___FROM_LBRACCT_ID as FROM_LBRACCT_ID",
    "JNR_ORG_PREV___PAYCD_SKEY as PAYCD_SKEY",
    "JNR_ORG_PREV___PAYCD_ID as PAYCD_ID",
    "JNR_ORG_PREV___CORE_HRS_SWT as CORE_HRS_SWT",
    "JNR_ORG_PREV___FROM_PAYCD_SKEY as FROM_PAYCD_SKEY",
    "JNR_ORG_PREV___FROM_PAYCD_ID as FROM_PAYCD_ID",
    "JNR_ORG_PREV___SUPV_SKEY as SUPV_SKEY",
    "JNR_ORG_PREV___PRI_JOB_SKEY as PRI_JOB_SKEY",
    "JNR_ORG_PREV___JOB_SKEY as JOB_SKEY",
    "JNR_ORG_PREV___PRI_ORG_SKEY as PRI_ORG_SKEY",
    "JNR_ORG_PREV___LOCATION_ID_org_prev as LOCATION_ID_org_prev",
    "JNR_ORG_PREV___WFA_BUSN_AREA_ID_org_prev as WFA_BUSN_AREA_ID_org_prev",
    "JNR_ORG_PREV___WFA_BUSN_AREA_DESC_org_prev as WFA_BUSN_AREA_DESC_org_prev",
    "JNR_ORG_PREV___WFA_DEPT_ID_org_prev as WFA_DEPT_ID_org_prev",
    "JNR_ORG_PREV___WFA_DEPT_DESC_org_prev as WFA_DEPT_DESC_org_prev",
    "JNR_ORG_PREV___WFA_TASK_ID_org_prev as WFA_TASK_ID_org_prev",
    "JNR_ORG_PREV___WFA_TASK_DESC_org_prev as WFA_TASK_DESC_org_prev",
    "JNR_ORG_PREV___ORG_SKEY_org_prev as ORG_SKEY_org_prev",
    "JNR_ORG_PREV___PAYPER_SKEY as PAYPER_SKEY",
    "JNR_ORG_PREV___HDAY_SKEY as HDAY_SKEY",
    "JNR_ORG_PREV___STRT_PNCHEVNT_SKEY as STRT_PNCHEVNT_SKEY",
    "JNR_ORG_PREV___END_PNCHEVNT_SKEY as END_PNCHEVNT_SKEY",
    "JNR_ORG_PREV___STRT_DSRC_SKEY as STRT_DSRC_SKEY",
    "JNR_ORG_PREV___END_DSRC_SKEY as END_DSRC_SKEY",
    "JNR_ORG_PREV___DSRC_SKEY as DSRC_SKEY",
    "JNR_ORG_PREV___EMPSTAT_SKEY as EMPSTAT_SKEY",
    "JNR_ORG_PREV___AGE_NBR as AGE_NBR",
    "JNR_ORG_PREV___TENURE_MO_NBR as TENURE_MO_NBR",
    "JNR_ORG_PREV___DFLT_PAY_RULE_SKEY as DFLT_PAY_RULE_SKEY",
    "JNR_ORG_PREV___DFLT_WRK_RULE_SWT as DFLT_WRK_RULE_SWT",
    "JNR_ORG_PREV___DFLT_WRK_RULE_SKEY as DFLT_WRK_RULE_SKEY",
    "JNR_ORG_PREV___WRK_RULE_SKEY as WRK_RULE_SKEY",
    "JNR_ORG_PREV___MONEY_AMT as MONEY_AMT",
    "JNR_ORG_PREV___DRTN_AMT as DRTN_AMT",
    "JNR_ORG_PREV___CORE_AMT as CORE_AMT",
    "JNR_ORG_PREV___NON_CORE_AMT as NON_CORE_AMT",
    "JNR_ORG_PREV___DRTN_DIFF_AMT as DRTN_DIFF_AMT",
    "JNR_ORG_PREV___DRTN_HRS as DRTN_HRS",
    "JNR_ORG_PREV___CORE_HRS as CORE_HRS",
    "JNR_ORG_PREV___NON_CORE_HRS as NON_CORE_HRS",
    "JNR_ORG_PREV___LOCKED_SWT as LOCKED_SWT",
    "JNR_ORG_PREV___GRP_SCHD_SKEY as GRP_SCHD_SKEY",
    "EXP_EMP___EMP_SKEY as EMP_SKEY_emp",
    "EXP_EMP___PRSN_NBR_TXT as PRSN_NBR_TXT_emp",
    "JNR_ORG_PREV___LBRACCT2_NAM as LBRACCT2_NAM",
)

# COMMAND ----------
# Processing node JNR_EMPL_PROFILE, type JOINER
# COLUMN COUNT: 67

JNR_EMPL_PROFILE = SQ_Shortcut_to_EMPLOYEE_PROFILE.join(
    JNR_EMP,
    [SQ_Shortcut_to_EMPLOYEE_PROFILE.EMPLOYEE_ID == JNR_EMP.PRSN_NBR_TXT_emp],
    "right_outer",
)

# COMMAND ----------
# Processing node JNR_DAYS, type JOINER
# COLUMN COUNT: 69

JNR_DAYS = SQ_Shortcut_to_DAYS.join(
    JNR_EMPL_PROFILE,
    [SQ_Shortcut_to_DAYS.DAY_DT == JNR_EMPL_PROFILE.RECORDED_DAT],
    "inner",
)

# COMMAND ----------
# Processing node JNR_WFA_TDTL, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 67

# for each involved DataFrame, append the dataframe name to each column
FIL_35DAYS_temp = FIL_35DAYS.toDF(
    *["FIL_35DAYS___" + col for col in FIL_35DAYS.columns]
)
JNR_DAYS_temp = JNR_DAYS.toDF(*["JNR_DAYS___" + col for col in JNR_DAYS.columns])

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
    "FIL_35DAYS___sys_row_id as sys_row_id",
    "JNR_DAYS___TDTL_ID as TDTL_ID1",
    "JNR_DAYS___RECORDED_DAT as RECORDED_DAT",
    "JNR_DAYS___STRT_DTM as STRT_DTM",
    "JNR_DAYS___END_DTM as END_DTM",
    "JNR_DAYS___LOCATION_ID as LOCATION_ID",
    "JNR_DAYS___EMPLOYEE_ID as EMPLOYEE_ID",
    "JNR_DAYS___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
    "JNR_DAYS___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
    "JNR_DAYS___WFA_DEPT_ID as WFA_DEPT_ID",
    "JNR_DAYS___WFA_DEPT_DESC as WFA_DEPT_DESC",
    "JNR_DAYS___WFA_TASK_ID as WFA_TASK_ID",
    "JNR_DAYS___WFA_TASK_DESC as WFA_TASK_DESC",
    "JNR_DAYS___LOCATION_ID_org_prev as LOCATION_ID_org_prev",
    "JNR_DAYS___WFA_BUSN_AREA_ID_org_prev as WFA_BUSN_AREA_ID_org_prev",
    "JNR_DAYS___WFA_BUSN_AREA_DESC_org_prev as WFA_BUSN_AREA_DESC_org_prev",
    "JNR_DAYS___WFA_DEPT_ID_org_prev as WFA_DEPT_ID_org_prev",
    "JNR_DAYS___WFA_DEPT_DESC_org_prev as WFA_DEPT_DESC_org_prev",
    "JNR_DAYS___WFA_TASK_ID_org_prev as WFA_TASK_ID_org_prev",
    "JNR_DAYS___WFA_TASK_DESC_org_prev as WFA_TASK_DESC_org_prev",
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

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column

EXPTRANS = JNR_WFA_TDTL.selectExpr(
    "sys_row_id as sys_row_id", "cast(LBRACCT2_NAM as int) as LBRACCT2_NAM2"
)

# COMMAND ----------
# Processing node LKP_SITE_PROFILE, type LOOKUP_FROM_PRECACHED_DATASET
# COLUMN COUNT: 3


LKP_SITE_PROFILE_lookup_result = EXPTRANS.join(
    LKP_SITE_PROFILE_SRC, (col("LBRACCT2_NAM1") == col("LBRACCT2_NAM2")), "left"
).withColumn(
    "row_num_LOCATION_ID",
    row_number().over(Window.partitionBy("sys_row_id").orderBy("LOCATION_ID")),
)
LKP_SITE_PROFILE = LKP_SITE_PROFILE_lookup_result.filter(
    col("row_num_LOCATION_ID") == 1
).select(LKP_SITE_PROFILE_lookup_result.sys_row_id, col("LOCATION_ID"))

# COMMAND ----------
# Processing node EXP_LOAD_DT, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 15

# for each involved DataFrame, append the dataframe name to each column
JNR_WFA_TDTL_temp = JNR_WFA_TDTL.toDF(
    *["JNR_WFA_TDTL___" + col for col in JNR_WFA_TDTL.columns]
)
LKP_SITE_PROFILE_temp = LKP_SITE_PROFILE.toDF(
    *["LKP_SITE_PROFILE___" + col for col in LKP_SITE_PROFILE.columns]
)
# LKP_temp = LKP.toDF(*["LKP___" + col for col in LKP.columns])

# Joining dataframes JNR_WFA_TDTL, LKP_SITE_PROFILE to form EXP_LOAD_DT
EXP_LOAD_DT_joined = JNR_WFA_TDTL_temp.join(
    LKP_SITE_PROFILE_temp,
    JNR_WFA_TDTL_temp.JNR_WFA_TDTL___sys_row_id
    == LKP_SITE_PROFILE_temp.LKP_SITE_PROFILE___sys_row_id,
    "inner",
)
EXP_LOAD_DT = EXP_LOAD_DT_joined.selectExpr(
    "JNR_WFA_TDTL___sys_row_id as sys_row_id",
    "IF (JNR_WFA_TDTL___TDTL_ID_target IS NULL, CURRENT_TIMESTAMP, JNR_WFA_TDTL___LOAD_DT_target) as LOAD_TSTMP1",
    "CURRENT_TIMESTAMP as UPDATE_TSTMP1",
    "LKP_SITE_PROFILE___LOCATION_ID as WORK_LOCATION_ID",
)

# COMMAND ----------
# Processing node UPD_WFA_TDTL, type UPDATE_STRATEGY
# COLUMN COUNT: 72

# Joining dataframes JNR_WFA_TDTL, EXP_LOAD_DT to form UPD_WFA_TDTL
UPD_WFA_TDTL_joined = JNR_WFA_TDTL.join(
    EXP_LOAD_DT, JNR_WFA_TDTL.sys_row_id == EXP_LOAD_DT.sys_row_id, "inner"
)
# for each involved DataFrame, append the dataframe name to each column
JNR_WFA_TDTL_temp = UPD_WFA_TDTL_joined.toDF(
    *["JNR_WFA_TDTL___" + col for col in UPD_WFA_TDTL_joined.columns]
)

UPD_WFA_TDTL = JNR_WFA_TDTL_temp.selectExpr(
    "JNR_WFA_TDTL___TDTL_ID_target as TDTL_ID",
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
    "JNR_WFA_TDTL___LOAD_TSTMP1 as LOAD_TSTMP",
    "JNR_WFA_TDTL___UPDATE_TSTMP1 as UPDATE_TSTMP",
    "JNR_WFA_TDTL___WEEK_DT as WEEK_DT",
    "JNR_WFA_TDTL___LBRACCT_ID as LBRACCT_ID",
    "JNR_WFA_TDTL___HM_LBRACCT_ID as HM_LBRACCT_ID",
    "JNR_WFA_TDTL___FROM_LBRACCT_ID as FROM_LBRACCT_ID",
    "JNR_WFA_TDTL___PAYCD_ID as PAYCD_ID",
    "JNR_WFA_TDTL___FROM_PAYCD_ID as FROM_PAYCD_ID",
    "JNR_WFA_TDTL___WORK_LOCATION_ID as WORK_LOCATION_ID",
    "IF(JNR_WFA_TDTL___TDTL_ID_target is null, 0, 1) as pyspark_data_action",
)
# .withColumn('pyspark_data_action', when((TDTL_ID_target.isNull()) ,(lit(0))) .otherwise(lit(1)))

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
