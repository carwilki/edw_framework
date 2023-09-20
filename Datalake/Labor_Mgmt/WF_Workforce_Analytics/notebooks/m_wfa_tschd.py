#Code converted on 2023-08-08 15:41:01
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
parser.add_argument('env', type=str, help='Env Variable')

# uncomment before checking in
args = parser.parse_args()
env = args.env
# remove before checking in
#env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
enterprise = getEnvPrefix(env) + 'enterprise'
empl_protected = getEnvPrefix(env) + 'empl_protected'

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_ORG_FACT_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_WFA_ORG_FACT_PRE = spark.sql(f"""SELECT
WFA_ORG_FACT_PRE.LOCATION_ID,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_ID,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_DESC,
WFA_ORG_FACT_PRE.WFA_DEPT_ID,
WFA_ORG_FACT_PRE.WFA_DEPT_DESC,
WFA_ORG_FACT_PRE.WFA_TASK_ID,
WFA_ORG_FACT_PRE.WFA_TASK_DESC,
WFA_ORG_FACT_PRE.ORG_SKEY
FROM {raw}.WFA_ORG_FACT_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TSCHD, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_WFA_TSCHD = spark.sql(f"""SELECT
legacy_WFA_TSCHD.DAY_DT,
legacy_WFA_TSCHD.TSCHD_ID,
legacy_WFA_TSCHD.LOAD_DT
FROM {empl_protected}.legacy_WFA_TSCHD
WHERE DAY_DT > CURRENT_DATE - 36""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE, type SOURCE 
# COLUMN COUNT: 24

SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE = spark.sql(f"""SELECT
raw_WFA_TSCHD_DIFF_PRE.TSCHD_DAT,
raw_WFA_TSCHD_DIFF_PRE.TSCHD_ID,
raw_WFA_TSCHD_DIFF_PRE.STRT_DTM,
raw_WFA_TSCHD_DIFF_PRE.END_DTM,
WFA_ORG_FACT_PRE.LOCATION_ID,
legacy_EMPLOYEE_PROFILE.EMPLOYEE_ID as EMP_ID ,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_ID,
WFA_ORG_FACT_PRE.WFA_BUSN_AREA_DESC,
WFA_ORG_FACT_PRE.WFA_DEPT_ID,
WFA_ORG_FACT_PRE.WFA_DEPT_DESC,
WFA_ORG_FACT_PRE.WFA_TASK_ID,
WFA_ORG_FACT_PRE.WFA_TASK_DESC,
WFA_PAYCD_PRE.PAYCD_ID,
raw_WFA_TSCHD_DIFF_PRE.CORE_HRS_SWT,
raw_WFA_TSCHD_DIFF_PRE.PRI_ORG_SKEY,
raw_WFA_TSCHD_DIFF_PRE.SHIFT_ID,
raw_WFA_TSCHD_DIFF_PRE.MONEY_AMT,
raw_WFA_TSCHD_DIFF_PRE.DRTN_AMT,
raw_WFA_TSCHD_DIFF_PRE.CORE_AMT,
raw_WFA_TSCHD_DIFF_PRE.NON_CORE_AMT,
raw_WFA_TSCHD_DIFF_PRE.DRTN_HRS,
raw_WFA_TSCHD_DIFF_PRE.CORE_HRS,
raw_WFA_TSCHD_DIFF_PRE.NON_CORE_HRS,
DAYS.WEEK_DT
FROM {enterprise}.DAYS, {empl_protected}.legacy_EMPLOYEE_PROFILE, {raw}.WFA_ORG_FACT_PRE, {raw}.WFA_PAYCD_PRE, {raw}.WFA_EMP_PRE, {empl_protected}.raw_WFA_TSCHD_DIFF_PRE
WHERE raw_wfa_tschd_diff_pre.org_skey = wfa_org_fact_pre.org_skey

    AND raw_wfa_tschd_diff_pre.emp_skey    = wfa_emp_pre.emp_skey

    AND raw_wfa_tschd_diff_pre.paycd_skey = wfa_paycd_pre.paycd_skey

    AND wfa_emp_pre.prsn_nbr_txt = legacy_employee_profile.employee_id

    AND raw_wfa_tschd_diff_pre.tschd_dat = days.day_dt""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TO_PREV_ORG, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 32

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE_temp = SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE.toDF(*["SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___" + col for col in SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE.columns])
SQ_Shortcut_to_WFA_ORG_FACT_PRE_temp = SQ_Shortcut_to_WFA_ORG_FACT_PRE.toDF(*["SQ_Shortcut_to_WFA_ORG_FACT_PRE___" + col for col in SQ_Shortcut_to_WFA_ORG_FACT_PRE.columns])

JNR_TO_PREV_ORG = SQ_Shortcut_to_WFA_ORG_FACT_PRE_temp.join(SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE_temp,[SQ_Shortcut_to_WFA_ORG_FACT_PRE_temp.SQ_Shortcut_to_WFA_ORG_FACT_PRE___ORG_SKEY == SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___PRI_ORG_SKEY],'right_outer').selectExpr(
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___TSCHD_DAT as TSCHD_DAT",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___TSCHD_ID as TSCHD_ID",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___STRT_DTM as STRT_DTM",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___END_DTM as END_DTM",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___LOCATION_ID as LOCATION_ID",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___EMP_ID as EMP_ID",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___WFA_DEPT_ID as WFA_DEPT_ID",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___WFA_DEPT_DESC as WFA_DEPT_DESC",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___WFA_TASK_ID as WFA_TASK_ID",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___WFA_TASK_DESC as WFA_TASK_DESC",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___PAYCD_ID as PAYCD_ID",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___CORE_HRS_SWT as CORE_HRS_SWT",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___PRI_ORG_SKEY as PRI_ORG_SKEY",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___SHIFT_ID as SHIFT_ID",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___MONEY_AMT as MONEY_AMT",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___DRTN_AMT as DRTN_AMT",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___CORE_AMT as CORE_AMT",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___NON_CORE_AMT as NON_CORE_AMT",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___DRTN_HRS as DRTN_HRS",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___CORE_HRS as CORE_HRS",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___NON_CORE_HRS as NON_CORE_HRS",
	"SQ_Shortcut_to_WFA_TSCHD_DIFF_PRE___WEEK_DT as WEEK_DT",
	"SQ_Shortcut_to_WFA_ORG_FACT_PRE___LOCATION_ID as PRI_LOCATION_ID",
	"SQ_Shortcut_to_WFA_ORG_FACT_PRE___WFA_BUSN_AREA_ID as PRI_WFA_BUSN_AREA_ID",
	"SQ_Shortcut_to_WFA_ORG_FACT_PRE___WFA_BUSN_AREA_DESC as PRI_WFA_BUSN_AREA_DESC",
	"SQ_Shortcut_to_WFA_ORG_FACT_PRE___WFA_DEPT_ID as PRI_WFA_DEPT_ID",
	"SQ_Shortcut_to_WFA_ORG_FACT_PRE___WFA_DEPT_DESC as PRI_WFA_DEPT_DESC",
	"SQ_Shortcut_to_WFA_ORG_FACT_PRE___WFA_TASK_ID as PRI_WFA_TASK_ID",
	"SQ_Shortcut_to_WFA_ORG_FACT_PRE___WFA_TASK_DESC as PRI_WFA_TASK_DESC",
	"SQ_Shortcut_to_WFA_ORG_FACT_PRE___ORG_SKEY as ORG_SKEY1")

# COMMAND ----------
# Processing node JNR_TRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_TSCHD_temp = SQ_Shortcut_to_WFA_TSCHD.toDF(*["SQ_Shortcut_to_WFA_TSCHD___" + col for col in SQ_Shortcut_to_WFA_TSCHD.columns])
JNR_TO_PREV_ORG_temp = JNR_TO_PREV_ORG.toDF(*["JNR_TO_PREV_ORG___" + col for col in JNR_TO_PREV_ORG.columns])

JNR_TRANS = SQ_Shortcut_to_WFA_TSCHD_temp.join(JNR_TO_PREV_ORG_temp,[SQ_Shortcut_to_WFA_TSCHD_temp.SQ_Shortcut_to_WFA_TSCHD___DAY_DT == JNR_TO_PREV_ORG_temp.JNR_TO_PREV_ORG___TSCHD_DAT, SQ_Shortcut_to_WFA_TSCHD_temp.SQ_Shortcut_to_WFA_TSCHD___TSCHD_ID == JNR_TO_PREV_ORG_temp.JNR_TO_PREV_ORG___TSCHD_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_WFA_TSCHD___sys_row_id as sys_row_id",
	"JNR_TO_PREV_ORG___TSCHD_DAT as TSCHD_DAT",
	"JNR_TO_PREV_ORG___TSCHD_ID as TSCHD_ID",
	"JNR_TO_PREV_ORG___STRT_DTM as STRT_DTM",
	"JNR_TO_PREV_ORG___END_DTM as END_DTM",
	"JNR_TO_PREV_ORG___LOCATION_ID as LOCATION_ID",
	"JNR_TO_PREV_ORG___EMP_ID as EMP_ID",
	"JNR_TO_PREV_ORG___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
	"JNR_TO_PREV_ORG___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
	"JNR_TO_PREV_ORG___WFA_DEPT_ID as WFA_DEPT_ID",
	"JNR_TO_PREV_ORG___WFA_DEPT_DESC as WFA_DEPT_DESC",
	"JNR_TO_PREV_ORG___WFA_TASK_ID as WFA_TASK_ID",
	"JNR_TO_PREV_ORG___WFA_TASK_DESC as WFA_TASK_DESC",
	"JNR_TO_PREV_ORG___PRI_LOCATION_ID as PRI_LOCATION_ID",
	"JNR_TO_PREV_ORG___PRI_WFA_BUSN_AREA_ID as PRI_WFA_BUSN_AREA_ID",
	"JNR_TO_PREV_ORG___PRI_WFA_BUSN_AREA_DESC as PRI_WFA_BUSN_AREA_DESC",
	"JNR_TO_PREV_ORG___PRI_WFA_DEPT_ID as PRI_WFA_DEPT_ID",
	"JNR_TO_PREV_ORG___PRI_WFA_DEPT_DESC as PRI_WFA_DEPT_DESC",
	"JNR_TO_PREV_ORG___PRI_WFA_TASK_ID as PRI_WFA_TASK_ID",
	"JNR_TO_PREV_ORG___PRI_WFA_TASK_DESC as PRI_WFA_TASK_DESC",
	"JNR_TO_PREV_ORG___PAYCD_ID as PAYCD_ID",
	"JNR_TO_PREV_ORG___CORE_HRS_SWT as CORE_HRS_SWT",
	"JNR_TO_PREV_ORG___SHIFT_ID as SHIFT_ID",
	"JNR_TO_PREV_ORG___MONEY_AMT as MONEY_AMT",
	"JNR_TO_PREV_ORG___DRTN_AMT as DRTN_AMT",
	"JNR_TO_PREV_ORG___CORE_AMT as CORE_AMT",
	"JNR_TO_PREV_ORG___NON_CORE_AMT as NON_CORE_AMT",
	"JNR_TO_PREV_ORG___DRTN_HRS as DRTN_HRS",
	"JNR_TO_PREV_ORG___CORE_HRS as CORE_HRS",
	"JNR_TO_PREV_ORG___NON_CORE_HRS as NON_CORE_HRS",
	"JNR_TO_PREV_ORG___WEEK_DT as WEEK_DT",
	"SQ_Shortcut_to_WFA_TSCHD___DAY_DT as DAY_DT",
	"SQ_Shortcut_to_WFA_TSCHD___TSCHD_ID as TSCHD_ID1",
	"SQ_Shortcut_to_WFA_TSCHD___LOAD_DT as LOAD_DT")

# COMMAND ----------
# Processing node EXP_TRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 34

# for each involved DataFrame, append the dataframe name to each column
JNR_TRANS_temp = JNR_TRANS.toDF(*["JNR_TRANS___" + col for col in JNR_TRANS.columns])

EXP_TRANS = JNR_TRANS_temp.selectExpr(
	"JNR_TRANS___sys_row_id as sys_row_id",
	"JNR_TRANS___TSCHD_DAT as TSCHD_DAT",
	"JNR_TRANS___TSCHD_ID as TSCHD_ID",
	"JNR_TRANS___STRT_DTM as STRT_DTM",
	"JNR_TRANS___END_DTM as END_DTM",
	"JNR_TRANS___LOCATION_ID as LOCATION_ID",
	"JNR_TRANS___EMP_ID as EMP_ID",
	"JNR_TRANS___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID",
	"JNR_TRANS___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC",
	"JNR_TRANS___WFA_DEPT_ID as WFA_DEPT_ID",
	"JNR_TRANS___WFA_DEPT_DESC as WFA_DEPT_DESC",
	"JNR_TRANS___WFA_TASK_ID as WFA_TASK_ID",
	"JNR_TRANS___WFA_TASK_DESC as WFA_TASK_DESC",
	"IF (JNR_TRANS___PRI_LOCATION_ID IS NULL, 0, JNR_TRANS___PRI_LOCATION_ID) as PRI_LOCATION_ID",
	"IF (JNR_TRANS___PRI_WFA_BUSN_AREA_ID IS NULL, 0, JNR_TRANS___PRI_WFA_BUSN_AREA_ID) as PRI_WFA_BUSN_AREA_ID",
	"IF (JNR_TRANS___PRI_WFA_BUSN_AREA_DESC IS NULL, 'NA', JNR_TRANS___PRI_WFA_BUSN_AREA_DESC) as PRI_WFA_BUSN_AREA_DESC",
	"IF (JNR_TRANS___PRI_WFA_DEPT_ID IS NULL, 0, JNR_TRANS___PRI_WFA_DEPT_ID) as PRI_WFA_DEPT_ID",
	"IF (JNR_TRANS___PRI_WFA_DEPT_DESC IS NULL, 'NA', JNR_TRANS___PRI_WFA_DEPT_DESC) as PRI_WFA_DEPT_DESC",
	"IF (JNR_TRANS___PRI_WFA_TASK_ID IS NULL, 0, JNR_TRANS___PRI_WFA_TASK_ID) as PRI_WFA_TASK_ID",
	"IF (JNR_TRANS___PRI_WFA_TASK_DESC IS NULL, 'NA', JNR_TRANS___PRI_WFA_TASK_DESC) as PRI_WFA_TASK_DESC",
	"JNR_TRANS___PAYCD_ID as PAYCD_ID",
	"JNR_TRANS___CORE_HRS_SWT as CORE_HRS_SWT",
	"JNR_TRANS___SHIFT_ID as SHIFT_ID",
	"JNR_TRANS___MONEY_AMT as MONEY_AMT",
	"JNR_TRANS___DRTN_AMT as DRTN_AMT",
	"JNR_TRANS___CORE_AMT as CORE_AMT",
	"JNR_TRANS___NON_CORE_AMT as NON_CORE_AMT",
	"JNR_TRANS___DRTN_HRS as DRTN_HRS",
	"JNR_TRANS___CORE_HRS as CORE_HRS",
	"JNR_TRANS___NON_CORE_HRS as NON_CORE_HRS",
	"JNR_TRANS___WEEK_DT as WEEK_DT",
	"IF (JNR_TRANS___TSCHD_ID1 IS NULL, 'I', 'U') as LOAD_FLAG",
	"DATE_TRUNC ('DAY',  CURRENT_TIMESTAMP ) as UPDATE_DT",
	"JNR_TRANS___TSCHD_ID1 as TSCHD_ID1",
	"IF (JNR_TRANS___LOAD_DT IS NULL, DATE_TRUNC ('DAY',  CURRENT_TIMESTAMP ), JNR_TRANS___LOAD_DT) as LOAD_DT"
)

# COMMAND ----------
# Processing node UPD_ins_upd, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 33

# for each involved DataFrame, append the dataframe name to each column
EXP_TRANS_temp = EXP_TRANS.toDF(*["EXP_TRANS___" + col for col in EXP_TRANS.columns])

UPD_ins_upd = EXP_TRANS_temp.selectExpr(
	"EXP_TRANS___TSCHD_DAT as TSCHD_DAT1",
	"EXP_TRANS___TSCHD_ID as TSCHD_ID1",
	"EXP_TRANS___STRT_DTM as STRT_DTM1",
	"EXP_TRANS___END_DTM as END_DTM1",
	"EXP_TRANS___LOCATION_ID as LOCATION_ID1",
	"EXP_TRANS___EMP_ID as EMPLOYEE_ID1",
	"EXP_TRANS___WFA_BUSN_AREA_ID as WFA_BUSN_AREA_ID1",
	"EXP_TRANS___WFA_BUSN_AREA_DESC as WFA_BUSN_AREA_DESC1",
	"EXP_TRANS___WFA_DEPT_ID as WFA_DEPT_ID1",
	"EXP_TRANS___WFA_DEPT_DESC as WFA_DEPT_DESC1",
	"EXP_TRANS___WFA_TASK_ID as WFA_TASK_ID1",
	"EXP_TRANS___WFA_TASK_DESC as WFA_TASK_DESC1",
	"EXP_TRANS___PRI_LOCATION_ID as PRI_LOCATION_ID1",
	"EXP_TRANS___PRI_WFA_BUSN_AREA_ID as PRI_WFA_BUSN_AREA_ID1",
	"EXP_TRANS___PRI_WFA_BUSN_AREA_DESC as PRI_WFA_BUSN_AREA_DESC1",
	"EXP_TRANS___PRI_WFA_DEPT_ID as PRI_WFA_DEPT_ID1",
	"EXP_TRANS___PRI_WFA_DEPT_DESC as PRI_WFA_DEPT_DESC1",
	"EXP_TRANS___PRI_WFA_TASK_ID as PRI_WFA_TASK_ID1",
	"EXP_TRANS___PRI_WFA_TASK_DESC as PRI_WFA_TASK_DESC1",
	"EXP_TRANS___PAYCD_ID as PAYCD_ID",
	"EXP_TRANS___CORE_HRS_SWT as CORE_HRS_SWT1",
	"EXP_TRANS___SHIFT_ID as SHIFT_ID1",
	"EXP_TRANS___MONEY_AMT as MONEY_AMT1",
	"EXP_TRANS___DRTN_AMT as DRTN_AMT1",
	"EXP_TRANS___CORE_AMT as CORE_AMT1",
	"EXP_TRANS___NON_CORE_AMT as NON_CORE_AMT1",
	"EXP_TRANS___DRTN_HRS as DRTN_HRS1",
	"EXP_TRANS___CORE_HRS as CORE_HRS1",
	"EXP_TRANS___NON_CORE_HRS as NON_CORE_HRS1",
	"EXP_TRANS___WEEK_DT as WEEK_DT1",
	"EXP_TRANS___LOAD_FLAG as LOAD_FLAG",
	"EXP_TRANS___UPDATE_DT as UPDATE_DT",
	"EXP_TRANS___LOAD_DT as LOAD_DT1",
	"if(EXP_TRANS___LOAD_FLAG == 'I' ,0, 1) as pyspark_data_action")

# COMMAND ----------
# Processing node Shortcut_to_WFA_TSCHD_ins_upd, type TARGET 
# COLUMN COUNT: 32


Shortcut_to_WFA_TSCHD_ins_upd = UPD_ins_upd.selectExpr(
	"CAST(TSCHD_DAT1 AS TIMESTAMP) as DAY_DT",
	"CAST(TSCHD_ID1 AS BIGINT) as TSCHD_ID",
	"CAST(STRT_DTM1 AS TIMESTAMP) as STRT_DTM",
	"CAST(END_DTM1 AS TIMESTAMP) as END_DTM",
	"CAST(LOCATION_ID1 AS BIGINT) as LOCATION_ID",
	"CAST(EMPLOYEE_ID1 AS BIGINT) as EMPLOYEE_ID",
	"WFA_BUSN_AREA_ID1 as WFA_BUSN_AREA_ID",
	"WFA_BUSN_AREA_DESC1 as WFA_BUSN_AREA_DESC",
	"WFA_DEPT_ID1 as WFA_DEPT_ID",
	"WFA_DEPT_DESC1 as WFA_DEPT_DESC",
	"WFA_TASK_ID1 as WFA_TASK_ID",
	"WFA_TASK_DESC1 as WFA_TASK_DESC",
	"CAST(PRI_LOCATION_ID1 AS BIGINT) as PRI_LOCATION_ID",
	"PRI_WFA_BUSN_AREA_ID1 as PRI_WFA_BUSN_AREA_ID",
	"PRI_WFA_BUSN_AREA_DESC1 as PRI_WFA_BUSN_AREA_DESC",
	"PRI_WFA_DEPT_ID1 as PRI_WFA_DEPT_ID",
	"PRI_WFA_DEPT_DESC1 as PRI_WFA_DEPT_DESC",
	"PRI_WFA_TASK_ID1 as PRI_WFA_TASK_ID",
	"PRI_WFA_TASK_DESC1 as PRI_WFA_TASK_DESC",
	"CAST(PAYCD_ID AS BIGINT) as PAYCD_ID",
	"CAST(CORE_HRS_SWT1 AS BIGINT) as CORE_HRS_SWT",
	"CAST(SHIFT_ID1 AS BIGINT) as SHIFT_ID",
	"CAST(MONEY_AMT1 AS DECIMAL(16,6)) as MONEY_AMT",
	"CAST(DRTN_AMT1 AS DECIMAL(16,6)) as DRTN_AMT",
	"CAST(CORE_AMT1 AS DECIMAL(16,6)) as CORE_AMT",
	"CAST(NON_CORE_AMT1 AS DECIMAL(16,6)) as NON_CORE_AMT",
	"CAST(DRTN_HRS1 AS DECIMAL(16,6)) as DRTN_HRS",
	"CAST(CORE_HRS1 AS DECIMAL(16,6)) as CORE_HRS",
	"CAST(NON_CORE_HRS1 AS DECIMAL(16,6)) as NON_CORE_HRS",
	"CAST(WEEK_DT1 AS TIMESTAMP) as WEEK_DT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT1 AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.DAY_DT = target.DAY_DT AND source.TSCHD_ID = target.TSCHD_ID"""
	refined_perf_table = f"{empl_protected}.legacy_WFA_TSCHD"
	executeMerge(Shortcut_to_WFA_TSCHD_ins_upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("WFA_TSCHD", "WFA_TSCHD", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("WFA_TSCHD", "WFA_TSCHD","Failed",str(e), f"{raw}.log_run_details")
	raise e
		