#Code converted on 2023-08-08 15:41:37
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
# Processing node SQ_Shortcut_to_WFA_ORG_PRE, type SOURCE 
# COLUMN COUNT: 26

SQ_Shortcut_to_WFA_ORG_PRE = spark.sql(f"""SELECT
WFA_ORG_PRE.ORG_ID,
WFA_ORG_PRE.ORG_IDS_ID,
WFA_ORG_PRE.ORG_RPT_TO_ID,
WFA_ORG_PRE.ORG_LVL_NBR,
WFA_ORG_PRE.ORG_TYP_ID,
WFA_ORG_PRE.ORG_TYP_NAM,
WFA_ORG_PRE.ORG_TYP_DES,
WFA_ORG_PRE.ORG_TYP_DISP_NBR,
WFA_ORG_PRE.ORG_JOB_SWT,
WFA_ORG_PRE.ORG_RT_SWT,
WFA_ORG_PRE.ORG_EFF_DAT,
WFA_ORG_PRE.ORG_EXP_DAT,
WFA_ORG_PRE.ORG_LVL06_NAM,
WFA_ORG_PRE.ORG_LVL07_NAM,
WFA_ORG_PRE.ORG_LVL08_NAM,
WFA_ORG_PRE.ORG_LVL09_NAM,
WFA_ORG_PRE.ORG_LVL10_NAM,
WFA_ORG_PRE.REC_ACTV_SWT,
WFA_ORG_PRE.REC_EXP_DTM,
WFA_ORG_PRE.LBRLVL_ID,
WFA_ORG_PRE.LBRLVL_DISP_NBR,
WFA_ORG_PRE.LBRLVL_NAM,
WFA_ORG_PRE.LBRLVL_ENTRY_ID,
WFA_ORG_PRE.LBRLVL_ENTRY_NAM,
WFA_ORG_PRE.LBRLVL_ENTRY_DES,
WFA_ORG_PRE.LBRLVL_ENTRY_ACTV_SWT
FROM {raw}.WFA_ORG_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_ORG, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_ORG = spark.sql(f"""SELECT
WFA_ORG.ORG_ID,
WFA_ORG.LOAD_DT
FROM {legacy}.WFA_ORG""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_ORG_temp = SQ_Shortcut_to_WFA_ORG.toDF(*["SQ_Shortcut_to_WFA_ORG___" + col for col in SQ_Shortcut_to_WFA_ORG.columns])
SQ_Shortcut_to_WFA_ORG_PRE_temp = SQ_Shortcut_to_WFA_ORG_PRE.toDF(*["SQ_Shortcut_to_WFA_ORG_PRE___" + col for col in SQ_Shortcut_to_WFA_ORG_PRE.columns])

JNR_TRANS = SQ_Shortcut_to_WFA_ORG_temp.join(SQ_Shortcut_to_WFA_ORG_PRE_temp,[SQ_Shortcut_to_WFA_ORG_temp.SQ_Shortcut_to_WFA_ORG___ORG_ID == SQ_Shortcut_to_WFA_ORG_PRE_temp.SQ_Shortcut_to_WFA_ORG_PRE___ORG_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_WFA_ORG_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_ID as ORG_ID",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_IDS_ID as ORG_IDS_ID",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_RPT_TO_ID as ORG_RPT_TO_ID",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_LVL_NBR as ORG_LVL_NBR",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_TYP_ID as ORG_TYP_ID",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_TYP_NAM as ORG_TYP_NAM",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_TYP_DES as ORG_TYP_DES",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_TYP_DISP_NBR as ORG_TYP_DISP_NBR",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_JOB_SWT as ORG_JOB_SWT",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_RT_SWT as ORG_RT_SWT",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_EFF_DAT as ORG_EFF_DAT",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_EXP_DAT as ORG_EXP_DAT",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_LVL06_NAM as ORG_LVL06_NAM",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_LVL07_NAM as ORG_LVL07_NAM",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_LVL08_NAM as ORG_LVL08_NAM",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_LVL09_NAM as ORG_LVL09_NAM",
	"SQ_Shortcut_to_WFA_ORG_PRE___ORG_LVL10_NAM as ORG_LVL10_NAM",
	"SQ_Shortcut_to_WFA_ORG_PRE___REC_ACTV_SWT as REC_ACTV_SWT",
	"SQ_Shortcut_to_WFA_ORG_PRE___REC_EXP_DTM as REC_EXP_DTM",
	"SQ_Shortcut_to_WFA_ORG_PRE___LBRLVL_ID as LBRLVL_ID",
	"SQ_Shortcut_to_WFA_ORG_PRE___LBRLVL_DISP_NBR as LBRLVL_DISP_NBR",
	"SQ_Shortcut_to_WFA_ORG_PRE___LBRLVL_NAM as LBRLVL_NAM",
	"SQ_Shortcut_to_WFA_ORG_PRE___LBRLVL_ENTRY_ID as LBRLVL_ENTRY_ID",
	"SQ_Shortcut_to_WFA_ORG_PRE___LBRLVL_ENTRY_NAM as LBRLVL_ENTRY_NAM",
	"SQ_Shortcut_to_WFA_ORG_PRE___LBRLVL_ENTRY_DES as LBRLVL_ENTRY_DES",
	"SQ_Shortcut_to_WFA_ORG_PRE___LBRLVL_ENTRY_ACTV_SWT as LBRLVL_ENTRY_ACTV_SWT",
	"SQ_Shortcut_to_WFA_ORG___ORG_ID as ORG_ID1",
	"SQ_Shortcut_to_WFA_ORG___LOAD_DT as LOAD_DT")

# COMMAND ----------
# Processing node EXP_TRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 30

# for each involved DataFrame, append the dataframe name to each column
JNR_TRANS_temp = JNR_TRANS.toDF(*["JNR_TRANS___" + col for col in JNR_TRANS.columns])

EXP_TRANS = JNR_TRANS_temp.selectExpr(
	"JNR_TRANS___sys_row_id as sys_row_id",
	"JNR_TRANS___ORG_ID as ORG_ID",
	"JNR_TRANS___ORG_IDS_ID as ORG_IDS_ID",
	"JNR_TRANS___ORG_RPT_TO_ID as ORG_RPT_TO_ID",
	"JNR_TRANS___ORG_LVL_NBR as ORG_LVL_NBR",
	"JNR_TRANS___ORG_TYP_ID as ORG_TYP_ID",
	"JNR_TRANS___ORG_TYP_NAM as ORG_TYP_NAM",
	"JNR_TRANS___ORG_TYP_DES as ORG_TYP_DES",
	"JNR_TRANS___ORG_TYP_DISP_NBR as ORG_TYP_DISP_NBR",
	"JNR_TRANS___ORG_JOB_SWT as ORG_JOB_SWT",
	"JNR_TRANS___ORG_RT_SWT as ORG_RT_SWT",
	"JNR_TRANS___ORG_EFF_DAT as ORG_EFF_DAT",
	"JNR_TRANS___ORG_EXP_DAT as ORG_EXP_DAT",
	"JNR_TRANS___ORG_LVL06_NAM as ORG_LVL06_NAM",
	"JNR_TRANS___ORG_LVL07_NAM as ORG_LVL07_NAM",
	"JNR_TRANS___ORG_LVL08_NAM as ORG_LVL08_NAM",
	"JNR_TRANS___ORG_LVL09_NAM as ORG_LVL09_NAM",
	"JNR_TRANS___ORG_LVL10_NAM as ORG_LVL10_NAM",
	"JNR_TRANS___REC_ACTV_SWT as REC_ACTV_SWT",
	"JNR_TRANS___REC_EXP_DTM as REC_EXP_DTM",
	"JNR_TRANS___LBRLVL_ID as LBRLVL_ID",
	"JNR_TRANS___LBRLVL_DISP_NBR as LBRLVL_DISP_NBR",
	"JNR_TRANS___LBRLVL_NAM as LBRLVL_NAM",
	"JNR_TRANS___LBRLVL_ENTRY_ID as LBRLVL_ENTRY_ID",
	"JNR_TRANS___LBRLVL_ENTRY_NAM as LBRLVL_ENTRY_NAM",
	"JNR_TRANS___LBRLVL_ENTRY_DES as LBRLVL_ENTRY_DES",
	"JNR_TRANS___LBRLVL_ENTRY_ACTV_SWT as LBRLVL_ENTRY_ACTV_SWT",
	"IF (JNR_TRANS___ORG_ID1 IS NULL, 'I', 'U') as LOAD_FLAG",
	"DATE_TRUNC ('DAY', CURRENT_TIMESTAMP ) as UPDATE_DT",
	"JNR_TRANS___ORG_ID1 as ORG_ID1",
	"IF (JNR_TRANS___LOAD_DT IS NULL, DATE_TRUNC ('DAY', CURRENT_TIMESTAMP ), JNR_TRANS___LOAD_DT) as LOAD_DT"
)

# COMMAND ----------
# Processing node UPD_ins_upd, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 29

# for each involved DataFrame, append the dataframe name to each column
EXP_TRANS_temp = EXP_TRANS.toDF(*["EXP_TRANS___" + col for col in EXP_TRANS.columns])

UPD_ins_upd = EXP_TRANS_temp.selectExpr(
	"EXP_TRANS___ORG_ID as ORG_ID1",
	"EXP_TRANS___ORG_IDS_ID as ORG_IDS_ID1",
	"EXP_TRANS___ORG_RPT_TO_ID as ORG_RPT_TO_ID1",
	"EXP_TRANS___ORG_LVL_NBR as ORG_LVL_NBR1",
	"EXP_TRANS___ORG_TYP_ID as ORG_TYP_ID1",
	"EXP_TRANS___ORG_TYP_NAM as ORG_TYP_NAM1",
	"EXP_TRANS___ORG_TYP_DES as ORG_TYP_DES1",
	"EXP_TRANS___ORG_TYP_DISP_NBR as ORG_TYP_DISP_NBR1",
	"EXP_TRANS___ORG_JOB_SWT as ORG_JOB_SWT1",
	"EXP_TRANS___ORG_RT_SWT as ORG_RT_SWT1",
	"EXP_TRANS___ORG_EFF_DAT as ORG_EFF_DAT1",
	"EXP_TRANS___ORG_EXP_DAT as ORG_EXP_DAT1",
	"EXP_TRANS___ORG_LVL06_NAM as ORG_LVL06_NAM1",
	"EXP_TRANS___ORG_LVL07_NAM as ORG_LVL07_NAM1",
	"EXP_TRANS___ORG_LVL08_NAM as ORG_LVL08_NAM1",
	"EXP_TRANS___ORG_LVL09_NAM as ORG_LVL09_NAM1",
	"EXP_TRANS___ORG_LVL10_NAM as ORG_LVL10_NAM1",
	"EXP_TRANS___REC_ACTV_SWT as REC_ACTV_SWT1",
	"EXP_TRANS___REC_EXP_DTM as REC_EXP_DTM1",
	"EXP_TRANS___LBRLVL_ID as LBRLVL_ID1",
	"EXP_TRANS___LBRLVL_DISP_NBR as LBRLVL_DISP_NBR1",
	"EXP_TRANS___LBRLVL_NAM as LBRLVL_NAM1",
	"EXP_TRANS___LBRLVL_ENTRY_ID as LBRLVL_ENTRY_ID1",
	"EXP_TRANS___LBRLVL_ENTRY_NAM as LBRLVL_ENTRY_NAM1",
	"EXP_TRANS___LBRLVL_ENTRY_DES as LBRLVL_ENTRY_DES1",
	"EXP_TRANS___LBRLVL_ENTRY_ACTV_SWT as LBRLVL_ENTRY_ACTV_SWT1",
	"EXP_TRANS___LOAD_FLAG as LOAD_FLAG",
	"EXP_TRANS___UPDATE_DT as UPDATE_DT",
	"EXP_TRANS___LOAD_DT as LOAD_DT1",
 	"IF(EXP_TRANS___LOAD_FLAG == 'I', 0, 1) as pyspark_data_action") 
	# .withColumn('pyspark_data_action', when((EXP_TRANS_temp.LOAD_FLAG == lit('I')) ,(lit(0))) .otherwise(lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WFA_ORG_ins_upd, type TARGET 
# COLUMN COUNT: 28


Shortcut_to_WFA_ORG_ins_upd = UPD_ins_upd.selectExpr(
	"CAST(ORG_ID1 AS DECIMAL(10,0)) as ORG_ID",
	"CAST(ORG_IDS_ID1 AS DECIMAL(10,0)) as ORG_IDS_ID",
	"CAST(ORG_RPT_TO_ID1 AS DECIMAL(10,0)) as ORG_RPT_TO_ID",
	"CAST(ORG_LVL_NBR1 AS DECIMAL(10,0)) as ORG_LVL_NBR",
	"CAST(ORG_TYP_ID1 AS DECIMAL(10,0)) as ORG_TYP_ID",
	"ORG_TYP_NAM1 as ORG_TYP_NAM",
	"ORG_TYP_DES1 as ORG_TYP_DES",
	"CAST(ORG_TYP_DISP_NBR1 AS DECIMAL(10,0)) as ORG_TYP_DISP_NBR",
	"CAST(ORG_JOB_SWT1 AS BIGINT) as ORG_JOB_SWT",
	"CAST(ORG_RT_SWT1 AS BIGINT) as ORG_RT_SWT",
	"CAST(ORG_EFF_DAT1 AS TIMESTAMP) as ORG_EFF_DAT",
	"CAST(ORG_EXP_DAT1 AS TIMESTAMP) as ORG_EXP_DAT",
	"ORG_LVL06_NAM1 as ORG_LVL06_NAM",
	"ORG_LVL07_NAM1 as ORG_LVL07_NAM",
	"ORG_LVL08_NAM1 as ORG_LVL08_NAM",
	"ORG_LVL09_NAM1 as ORG_LVL09_NAM",
	"ORG_LVL10_NAM1 as ORG_LVL10_NAM",
	"CAST(REC_ACTV_SWT1 AS BIGINT) as REC_ACTV_SWT",
	"CAST(REC_EXP_DTM1 AS TIMESTAMP) as REC_EXP_DTM",
	"CAST(LBRLVL_ID1 AS DECIMAL(10,0)) as LBRLVL_ID",
	"CAST(LBRLVL_DISP_NBR1 AS DECIMAL(10,0)) as LBRLVL_DISP_NBR",
	"LBRLVL_NAM1 as LBRLVL_NAM",
	"CAST(LBRLVL_ENTRY_ID1 AS DECIMAL(10,0)) as LBRLVL_ENTRY_ID",
	"LBRLVL_ENTRY_NAM1 as LBRLVL_ENTRY_NAM",
	"LBRLVL_ENTRY_DES1 as LBRLVL_ENTRY_DES",
	"CAST(LBRLVL_ENTRY_ACTV_SWT1 AS BIGINT) as LBRLVL_ENTRY_ACTV_SWT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT1 AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.ORG_ID = target.ORG_ID"""
	refined_perf_table = f"{legacy}.WFA_ORG"
	executeMerge(Shortcut_to_WFA_ORG_ins_upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("WFA_ORG", "WFA_ORG", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("WFA_ORG", "WFA_ORG","Failed",str(e), f"{raw}.log_run_details")
	raise e
		