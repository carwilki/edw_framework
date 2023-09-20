#Code converted on 2023-08-08 15:41:53
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

args = parser.parse_args()
env = args.env

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'


# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_EMP_PRE, type SOURCE 
# COLUMN COUNT: 18

SQ_Shortcut_to_WFA_EMP_PRE = spark.sql(f"""SELECT
WFA_EMP_PRE.EMP_ID,
WFA_EMP_PRE.CO_HIRE_DAT,
WFA_EMP_PRE.MGR_SIGNOFF_DAT,
WFA_EMP_PRE.PRSN_NBR_TXT,
WFA_EMP_PRE.SR_RANK_DAT,
WFA_EMP_PRE.DFLT_TZONE_SKEY,
WFA_EMP_PRE.DFLT_TZONE_DES,
WFA_EMP_PRE.DVC_GRP_NAM,
WFA_EMP_PRE.HM_LBRACCT_SKEY,
WFA_EMP_PRE.EMP_HM_LBRACCT_EFF_DAT,
WFA_EMP_PRE.EMP_HM_LBRACCT_EXP_DAT,
WFA_EMP_PRE.EMPSTAT_SKEY,
WFA_EMP_PRE.EMP_EMPSTAT_EFF_DAT,
WFA_EMP_PRE.EMP_EMPSTAT_EXP_DAT,
WFA_EMP_PRE.BADGE_COD,
WFA_EMP_PRE.EMP_BADGE_EFF_DAT,
WFA_EMP_PRE.EMP_BADGE_EXP_DAT,
WFA_EMP_PRE.EM_MNR_SWT
FROM {raw}.WFA_EMP_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_EMP, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_EMP = spark.sql(f"""SELECT
WFA_EMP.EMP_ID,
WFA_EMP.LOAD_DT
FROM {legacy}.WFA_EMP""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 20

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_EMP_temp = SQ_Shortcut_to_WFA_EMP.toDF(*["SQ_Shortcut_to_WFA_EMP___" + col for col in SQ_Shortcut_to_WFA_EMP.columns])
SQ_Shortcut_to_WFA_EMP_PRE_temp = SQ_Shortcut_to_WFA_EMP_PRE.toDF(*["SQ_Shortcut_to_WFA_EMP_PRE___" + col for col in SQ_Shortcut_to_WFA_EMP_PRE.columns])

JNR_TRANS = SQ_Shortcut_to_WFA_EMP_temp.join(SQ_Shortcut_to_WFA_EMP_PRE_temp,[SQ_Shortcut_to_WFA_EMP_temp.SQ_Shortcut_to_WFA_EMP___EMP_ID == SQ_Shortcut_to_WFA_EMP_PRE_temp.SQ_Shortcut_to_WFA_EMP_PRE___EMP_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_WFA_EMP_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_WFA_EMP_PRE___EMP_ID as EMP_ID",
	"SQ_Shortcut_to_WFA_EMP_PRE___CO_HIRE_DAT as CO_HIRE_DAT",
	"SQ_Shortcut_to_WFA_EMP_PRE___MGR_SIGNOFF_DAT as MGR_SIGNOFF_DAT",
	"SQ_Shortcut_to_WFA_EMP_PRE___PRSN_NBR_TXT as PRSN_NBR_TXT",
	"SQ_Shortcut_to_WFA_EMP_PRE___SR_RANK_DAT as SR_RANK_DAT",
	"SQ_Shortcut_to_WFA_EMP_PRE___DFLT_TZONE_SKEY as DFLT_TZONE_SKEY",
	"SQ_Shortcut_to_WFA_EMP_PRE___DFLT_TZONE_DES as DFLT_TZONE_DES",
	"SQ_Shortcut_to_WFA_EMP_PRE___DVC_GRP_NAM as DVC_GRP_NAM",
	"SQ_Shortcut_to_WFA_EMP_PRE___HM_LBRACCT_SKEY as HM_LBRACCT_SKEY",
	"SQ_Shortcut_to_WFA_EMP_PRE___EMP_HM_LBRACCT_EFF_DAT as EMP_HM_LBRACCT_EFF_DAT",
	"SQ_Shortcut_to_WFA_EMP_PRE___EMP_HM_LBRACCT_EXP_DAT as EMP_HM_LBRACCT_EXP_DAT",
	"SQ_Shortcut_to_WFA_EMP_PRE___EMPSTAT_SKEY as EMPSTAT_SKEY",
	"SQ_Shortcut_to_WFA_EMP_PRE___EMP_EMPSTAT_EFF_DAT as EMP_EMPSTAT_EFF_DAT",
	"SQ_Shortcut_to_WFA_EMP_PRE___EMP_EMPSTAT_EXP_DAT as EMP_EMPSTAT_EXP_DAT",
	"SQ_Shortcut_to_WFA_EMP_PRE___BADGE_COD as BADGE_COD",
	"SQ_Shortcut_to_WFA_EMP_PRE___EMP_BADGE_EFF_DAT as EMP_BADGE_EFF_DAT",
	"SQ_Shortcut_to_WFA_EMP_PRE___EMP_BADGE_EXP_DAT as EMP_BADGE_EXP_DAT",
	"SQ_Shortcut_to_WFA_EMP_PRE___EM_MNR_SWT as EM_MNR_SWT",
	"SQ_Shortcut_to_WFA_EMP___EMP_ID as EMP_ID1",
	"SQ_Shortcut_to_WFA_EMP___LOAD_DT as LOAD_DT")

# COMMAND ----------
# Processing node EXP_TRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 22

# for each involved DataFrame, append the dataframe name to each column
JNR_TRANS_temp = JNR_TRANS.toDF(*["JNR_TRANS___" + col for col in JNR_TRANS.columns])

EXP_TRANS = JNR_TRANS_temp.selectExpr(

	"JNR_TRANS___sys_row_id as sys_row_id",
	"JNR_TRANS___EMP_ID as EMP_ID",
	"JNR_TRANS___CO_HIRE_DAT as CO_HIRE_DAT",
	"JNR_TRANS___MGR_SIGNOFF_DAT as MGR_SIGNOFF_DAT",
	"JNR_TRANS___PRSN_NBR_TXT as PRSN_NBR_TXT",
	"JNR_TRANS___SR_RANK_DAT as SR_RANK_DAT",
	"JNR_TRANS___DFLT_TZONE_SKEY as DFLT_TZONE_SKEY",
	"JNR_TRANS___DFLT_TZONE_DES as DFLT_TZONE_DES",
	"JNR_TRANS___DVC_GRP_NAM as DVC_GRP_NAM",
	"JNR_TRANS___HM_LBRACCT_SKEY as HM_LBRACCT_SKEY",
	"JNR_TRANS___EMP_HM_LBRACCT_EFF_DAT as EMP_HM_LBRACCT_EFF_DAT",
	"JNR_TRANS___EMP_HM_LBRACCT_EXP_DAT as EMP_HM_LBRACCT_EXP_DAT",
	"JNR_TRANS___EMPSTAT_SKEY as EMPSTAT_SKEY",
	"JNR_TRANS___EMP_EMPSTAT_EFF_DAT as EMP_EMPSTAT_EFF_DAT",
	"JNR_TRANS___EMP_EMPSTAT_EXP_DAT as EMP_EMPSTAT_EXP_DAT",
	"JNR_TRANS___BADGE_COD as BADGE_COD",
	"JNR_TRANS___EMP_BADGE_EFF_DAT as EMP_BADGE_EFF_DAT",
	"JNR_TRANS___EMP_BADGE_EXP_DAT as EMP_BADGE_EXP_DAT",
	"JNR_TRANS___EM_MNR_SWT as EM_MNR_SWT",
	"IF (JNR_TRANS___EMP_ID1 IS NULL, 'I', 'U') as LOAD_FLAG",
	"DATE_TRUNC ( 'DAY', CURRENT_TIMESTAMP ) as UPDATE_DT",
	"JNR_TRANS___EMP_ID1 as EMP_ID1",
	"IF (JNR_TRANS___LOAD_DT IS NULL, DATE_TRUNC ( 'DAY', CURRENT_TIMESTAMP ), JNR_TRANS___LOAD_DT) as LOAD_DT"
)

# COMMAND ----------
# Processing node UPD_ins_upd, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 21

# for each involved DataFrame, append the dataframe name to each column
EXP_TRANS_temp = EXP_TRANS.toDF(*["EXP_TRANS___" + col for col in EXP_TRANS.columns])

UPD_ins_upd = EXP_TRANS_temp.selectExpr(
	"EXP_TRANS___EMP_ID as EMP_ID1",
	"EXP_TRANS___CO_HIRE_DAT as CO_HIRE_DAT1",
	"EXP_TRANS___MGR_SIGNOFF_DAT as MGR_SIGNOFF_DAT1",
	"EXP_TRANS___PRSN_NBR_TXT as PRSN_NBR_TXT1",
	"EXP_TRANS___SR_RANK_DAT as SR_RANK_DAT1",
	"EXP_TRANS___DFLT_TZONE_SKEY as DFLT_TZONE_SKEY1",
	"EXP_TRANS___DFLT_TZONE_DES as DFLT_TZONE_DES1",
	"EXP_TRANS___DVC_GRP_NAM as DVC_GRP_NAM1",
	"EXP_TRANS___HM_LBRACCT_SKEY as HM_LBRACCT_SKEY1",
	"EXP_TRANS___EMP_HM_LBRACCT_EFF_DAT as EMP_HM_LBRACCT_EFF_DAT1",
	"EXP_TRANS___EMP_HM_LBRACCT_EXP_DAT as EMP_HM_LBRACCT_EXP_DAT1",
	"EXP_TRANS___EMPSTAT_SKEY as EMPSTAT_SKEY1",
	"EXP_TRANS___EMP_EMPSTAT_EFF_DAT as EMP_EMPSTAT_EFF_DAT1",
	"EXP_TRANS___EMP_EMPSTAT_EXP_DAT as EMP_EMPSTAT_EXP_DAT1",
	"EXP_TRANS___BADGE_COD as BADGE_COD1",
	"EXP_TRANS___EMP_BADGE_EFF_DAT as EMP_BADGE_EFF_DAT1",
	"EXP_TRANS___EMP_BADGE_EXP_DAT as EMP_BADGE_EXP_DAT1",
	"EXP_TRANS___EM_MNR_SWT as EM_MNR_SWT1",
	"EXP_TRANS___LOAD_FLAG as LOAD_FLAG",
	"EXP_TRANS___UPDATE_DT as UPDATE_DT",
	"EXP_TRANS___LOAD_DT as LOAD_DT1",
	"IF(EXP_TRANS___LOAD_FLAG == 'I', 0, 1) as pyspark_data_action") 
	# .withColumn('pyspark_data_action', when((EXP_TRANS_temp.LOAD_FLAG == lit('I')) ,(li?t(0))) .otherwise(lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WFA_EMP_ins_upd, type TARGET 
# COLUMN COUNT: 20


Shortcut_to_WFA_EMP_ins_upd = UPD_ins_upd.selectExpr(
	"CAST(EMP_ID1 AS DECIMAL(10,0)) as EMP_ID",
	"CAST(CO_HIRE_DAT1 AS TIMESTAMP) as CO_HIRE_DAT",
	"CAST(MGR_SIGNOFF_DAT1 AS TIMESTAMP) as MGR_SIGNOFF_DAT",
	"PRSN_NBR_TXT1 as PRSN_NBR_TXT",
	"CAST(SR_RANK_DAT1 AS TIMESTAMP) as SR_RANK_DAT",
	"CAST(DFLT_TZONE_SKEY1 AS DECIMAL(10,0)) as DFLT_TZONE_SKEY",
	"DFLT_TZONE_DES1 as DFLT_TZONE_DES",
	"DVC_GRP_NAM1 as DVC_GRP_NAM",
	"CAST(HM_LBRACCT_SKEY1 AS DECIMAL(10,0)) as HM_LBRACCT_SKEY",
	"CAST(EMP_HM_LBRACCT_EFF_DAT1 AS TIMESTAMP) as EMP_HM_LBRACCT_EFF_DAT",
	"CAST(EMP_HM_LBRACCT_EXP_DAT1 AS TIMESTAMP) as EMP_HM_LBRACCT_EXP_DAT",
	"CAST(EMPSTAT_SKEY1 AS DECIMAL(10,0)) as EMPSTAT_SKEY",
	"CAST(EMP_EMPSTAT_EFF_DAT1 AS TIMESTAMP) as EMP_EMPSTAT_EFF_DAT",
	"CAST(EMP_EMPSTAT_EXP_DAT1 AS TIMESTAMP) as EMP_EMPSTAT_EXP_DAT",
	"BADGE_COD1 as BADGE_COD",
	"CAST(EMP_BADGE_EFF_DAT1 AS TIMESTAMP) as EMP_BADGE_EFF_DAT",
	"CAST(EMP_BADGE_EXP_DAT1 AS TIMESTAMP) as EMP_BADGE_EXP_DAT",
	"CAST(EM_MNR_SWT1 AS BIGINT) as EM_MNR_SWT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT1 AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.EMP_ID = target.EMP_ID"""
	refined_perf_table = f"{legacy}.WFA_EMP"
	executeMerge(Shortcut_to_WFA_EMP_ins_upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("WFA_EMP", "WFA_EMP", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("WFA_EMP", "WFA_EMP","Failed",str(e), f"{raw}.log_run_details")
	raise e
		