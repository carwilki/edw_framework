#Code converted on 2023-08-08 15:42:08
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

(username,password,connection_string) = or_kro_read_krap1(env)
# COMMAND ----------
# Processing node SQ_Shortcut_to_DIM_ORG, type SOURCE 
# COLUMN COUNT: 28

SQ_Shortcut_to_DIM_ORG = jdbcOracleConnection(f"""SELECT
DIM_ORG.ORG_ID,
DIM_ORG.ORG_SKEY,
DIM_ORG.ORG_IDS_ID,
DIM_ORG.ORG_RPT_TO_ID,
DIM_ORG.ORG_LVL_NBR,
DIM_ORG.ORG_TYP_ID,
DIM_ORG.ORG_TYP_NAM,
DIM_ORG.ORG_TYP_DES,
DIM_ORG.ORG_TYP_DISP_NBR,
DIM_ORG.ORG_JOB_SWT,
DIM_ORG.ORG_RT_SWT,
DIM_ORG.ORG_EFF_DAT,
DIM_ORG.ORG_EXP_DAT,
DIM_ORG.ORG_LVL06_NAM,
DIM_ORG.ORG_LVL07_NAM,
DIM_ORG.ORG_LVL08_NAM,
DIM_ORG.ORG_LVL09_NAM,
DIM_ORG.ORG_LVL10_NAM,
DIM_ORG.UPDT_DTM,
DIM_ORG.REC_ACTV_SWT,
DIM_ORG.REC_EXP_DTM,
DIM_ORG.LBRLVL_ID,
DIM_ORG.LBRLVL_DISP_NBR,
DIM_ORG.LBRLVL_NAM,
DIM_ORG.LBRLVL_ENTRY_ID,
DIM_ORG.LBRLVL_ENTRY_NAM,
DIM_ORG.LBRLVL_ENTRY_DES,
DIM_ORG.LBRLVL_ENTRY_ACTV_SWT
FROM ia.DIM_ORG
WHERE DIM_ORG.ORG_SKEY > 1""",username,password,connection_string).withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node FIL_IS_STORE_NBR, type FILTER 
# COLUMN COUNT: 28

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_DIM_ORG_temp = SQ_Shortcut_to_DIM_ORG.toDF(*["SQ_Shortcut_to_DIM_ORG___" + col for col in SQ_Shortcut_to_DIM_ORG.columns])

FIL_IS_STORE_NBR = SQ_Shortcut_to_DIM_ORG_temp.selectExpr(
	"SQ_Shortcut_to_DIM_ORG___ORG_ID as ORG_ID",
	"SQ_Shortcut_to_DIM_ORG___ORG_SKEY as ORG_SKEY",
	"SQ_Shortcut_to_DIM_ORG___ORG_IDS_ID as ORG_IDS_ID",
	"SQ_Shortcut_to_DIM_ORG___ORG_RPT_TO_ID as ORG_RPT_TO_ID",
	"SQ_Shortcut_to_DIM_ORG___ORG_LVL_NBR as ORG_LVL_NBR",
	"SQ_Shortcut_to_DIM_ORG___ORG_TYP_ID as ORG_TYP_ID",
	"SQ_Shortcut_to_DIM_ORG___ORG_TYP_NAM as ORG_TYP_NAM",
	"SQ_Shortcut_to_DIM_ORG___ORG_TYP_DES as ORG_TYP_DES",
	"SQ_Shortcut_to_DIM_ORG___ORG_TYP_DISP_NBR as ORG_TYP_DISP_NBR",
	"SQ_Shortcut_to_DIM_ORG___ORG_JOB_SWT as ORG_JOB_SWT",
	"SQ_Shortcut_to_DIM_ORG___ORG_RT_SWT as ORG_RT_SWT",
	"SQ_Shortcut_to_DIM_ORG___ORG_EFF_DAT as ORG_EFF_DAT",
	"SQ_Shortcut_to_DIM_ORG___ORG_EXP_DAT as ORG_EXP_DAT",
	"SQ_Shortcut_to_DIM_ORG___ORG_LVL06_NAM as ORG_LVL06_NAM",
	"SQ_Shortcut_to_DIM_ORG___ORG_LVL07_NAM as ORG_LVL07_NAM",
	"SQ_Shortcut_to_DIM_ORG___ORG_LVL08_NAM as ORG_LVL08_NAM",
	"SQ_Shortcut_to_DIM_ORG___ORG_LVL09_NAM as ORG_LVL09_NAM",
	"SQ_Shortcut_to_DIM_ORG___ORG_LVL10_NAM as ORG_LVL10_NAM",
	"SQ_Shortcut_to_DIM_ORG___UPDT_DTM as UPDT_DTM",
	"SQ_Shortcut_to_DIM_ORG___REC_ACTV_SWT as REC_ACTV_SWT",
	"SQ_Shortcut_to_DIM_ORG___REC_EXP_DTM as REC_EXP_DTM",
	"SQ_Shortcut_to_DIM_ORG___LBRLVL_ID as LBRLVL_ID",
	"SQ_Shortcut_to_DIM_ORG___LBRLVL_DISP_NBR as LBRLVL_DISP_NBR",
	"SQ_Shortcut_to_DIM_ORG___LBRLVL_NAM as LBRLVL_NAM",
	"SQ_Shortcut_to_DIM_ORG___LBRLVL_ENTRY_ID as LBRLVL_ENTRY_ID",
	"SQ_Shortcut_to_DIM_ORG___LBRLVL_ENTRY_NAM as LBRLVL_ENTRY_NAM",
	"SQ_Shortcut_to_DIM_ORG___LBRLVL_ENTRY_DES as LBRLVL_ENTRY_DES",
	"SQ_Shortcut_to_DIM_ORG___LBRLVL_ENTRY_ACTV_SWT as LBRLVL_ENTRY_ACTV_SWT").filter("ORG_LVL06_NAM is not null").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_WFA_ORG_PRE, type TARGET 
# COLUMN COUNT: 28


Shortcut_to_WFA_ORG_PRE = FIL_IS_STORE_NBR.selectExpr(
	"CAST(ORG_ID AS BIGINT) as ORG_ID",
	"CAST(ORG_SKEY AS BIGINT) as ORG_SKEY",
	"CAST(ORG_IDS_ID AS BIGINT) as ORG_IDS_ID",
	"CAST(ORG_RPT_TO_ID AS BIGINT) as ORG_RPT_TO_ID",
	"CAST(ORG_LVL_NBR AS BIGINT) as ORG_LVL_NBR",
	"CAST(ORG_TYP_ID AS BIGINT) as ORG_TYP_ID",
	"ORG_TYP_NAM as ORG_TYP_NAM",
	"ORG_TYP_DES as ORG_TYP_DES",
	"CAST(ORG_TYP_DISP_NBR AS BIGINT) as ORG_TYP_DISP_NBR",
	"CAST(ORG_JOB_SWT AS INT) as ORG_JOB_SWT",
	"CAST(ORG_RT_SWT AS INT) as ORG_RT_SWT",
	"CAST(ORG_EFF_DAT AS TIMESTAMP) as ORG_EFF_DAT",
	"CAST(ORG_EXP_DAT AS TIMESTAMP) as ORG_EXP_DAT",
	"ORG_LVL06_NAM as ORG_LVL06_NAM",
	"ORG_LVL07_NAM as ORG_LVL07_NAM",
	"ORG_LVL08_NAM as ORG_LVL08_NAM",
	"ORG_LVL09_NAM as ORG_LVL09_NAM",
	"ORG_LVL10_NAM as ORG_LVL10_NAM",
	"CAST(UPDT_DTM AS TIMESTAMP) as UPDT_DTM",
	"CAST(REC_ACTV_SWT AS INT) as REC_ACTV_SWT",
	"CAST(REC_EXP_DTM AS TIMESTAMP) as REC_EXP_DTM",
	"CAST(LBRLVL_ID AS BIGINT) as LBRLVL_ID",
	"CAST(LBRLVL_DISP_NBR AS BIGINT) as LBRLVL_DISP_NBR",
	"LBRLVL_NAM as LBRLVL_NAM",
	"CAST(LBRLVL_ENTRY_ID AS BIGINT) as LBRLVL_ENTRY_ID",
	"LBRLVL_ENTRY_NAM as LBRLVL_ENTRY_NAM",
	"LBRLVL_ENTRY_DES as LBRLVL_ENTRY_DES",
	"CAST(LBRLVL_ENTRY_ACTV_SWT AS INT) as LBRLVL_ENTRY_ACTV_SWT"
)
# overwriteDeltaPartition(Shortcut_to_WFA_ORG_PRE,'DC_NBR',dcnbr,f'{raw}.WFA_ORG_PRE')
Shortcut_to_WFA_ORG_PRE.write.mode("overwrite").saveAsTable(f'{raw}.WFA_ORG_PRE')