#Code converted on 2023-08-08 15:42:21
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



if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
empl_protected = getEnvPrefix(env) + 'empl_protected'


# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TSCHD_PREV_PRE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_WFA_TSCHD_PREV_PRE = spark.sql(f"""SELECT
raw_WFA_TSCHD_PREV_PRE.TSCHD_ID,
raw_WFA_TSCHD_PREV_PRE.TSCHD_DAT,
raw_WFA_TSCHD_PREV_PRE.EMP_SKEY,
raw_WFA_TSCHD_PREV_PRE.STRT_DTM,
raw_WFA_TSCHD_PREV_PRE.END_DTM,
raw_WFA_TSCHD_PREV_PRE.PAYCD_SKEY,
raw_WFA_TSCHD_PREV_PRE.CORE_HRS_SWT,
raw_WFA_TSCHD_PREV_PRE.PRI_ORG_SKEY,
raw_WFA_TSCHD_PREV_PRE.ORG_SKEY,
raw_WFA_TSCHD_PREV_PRE.SHIFT_ID,
raw_WFA_TSCHD_PREV_PRE.MONEY_AMT,
raw_WFA_TSCHD_PREV_PRE.DRTN_AMT,
raw_WFA_TSCHD_PREV_PRE.CORE_AMT,
raw_WFA_TSCHD_PREV_PRE.NON_CORE_AMT,
raw_WFA_TSCHD_PREV_PRE.DRTN_HRS,
raw_WFA_TSCHD_PREV_PRE.CORE_HRS,
raw_WFA_TSCHD_PREV_PRE.NON_CORE_HRS
FROM {empl_protected}.raw_WFA_TSCHD_PREV_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_TSCHD_PRE, type SOURCE 
# COLUMN COUNT: 17

SQ_Shortcut_to_WFA_TSCHD_PRE = spark.sql(f"""SELECT
raw_WFA_TSCHD_PRE.TSCHD_ID,
raw_WFA_TSCHD_PRE.TSCHD_DAT,
raw_WFA_TSCHD_PRE.EMP_SKEY,
raw_WFA_TSCHD_PRE.STRT_DTM,
raw_WFA_TSCHD_PRE.END_DTM,
raw_WFA_TSCHD_PRE.PAYCD_SKEY,
raw_WFA_TSCHD_PRE.CORE_HRS_SWT,
raw_WFA_TSCHD_PRE.PRI_ORG_SKEY,
raw_WFA_TSCHD_PRE.ORG_SKEY,
raw_WFA_TSCHD_PRE.SHIFT_ID,
raw_WFA_TSCHD_PRE.MONEY_AMT,
raw_WFA_TSCHD_PRE.DRTN_AMT,
raw_WFA_TSCHD_PRE.CORE_AMT,
raw_WFA_TSCHD_PRE.NON_CORE_AMT,
raw_WFA_TSCHD_PRE.DRTN_HRS,
raw_WFA_TSCHD_PRE.CORE_HRS,
raw_WFA_TSCHD_PRE.NON_CORE_HRS
FROM {empl_protected}.raw_WFA_TSCHD_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 34

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp = SQ_Shortcut_to_WFA_TSCHD_PREV_PRE.toDF(*["SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___" + col for col in SQ_Shortcut_to_WFA_TSCHD_PREV_PRE.columns])
SQ_Shortcut_to_WFA_TSCHD_PRE_temp = SQ_Shortcut_to_WFA_TSCHD_PRE.toDF(*["SQ_Shortcut_to_WFA_TSCHD_PRE___" + col for col in SQ_Shortcut_to_WFA_TSCHD_PRE.columns])

JNR_TRANS = SQ_Shortcut_to_WFA_TSCHD_PRE_temp.join(SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp,[SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___TSCHD_ID == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___TSCHD_ID, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___TSCHD_DAT == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___TSCHD_DAT, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___EMP_SKEY == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___EMP_SKEY, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___STRT_DTM == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___STRT_DTM, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___END_DTM == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___END_DTM, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___PAYCD_SKEY == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___PAYCD_SKEY, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___CORE_HRS_SWT == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___CORE_HRS_SWT, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___PRI_ORG_SKEY == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___PRI_ORG_SKEY, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___ORG_SKEY == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___ORG_SKEY, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___SHIFT_ID == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___SHIFT_ID, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___MONEY_AMT == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___MONEY_AMT, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___DRTN_AMT == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___DRTN_AMT, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___CORE_AMT == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___CORE_AMT, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___NON_CORE_AMT == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___NON_CORE_AMT, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___DRTN_HRS == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___DRTN_HRS, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___CORE_HRS == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___CORE_HRS, SQ_Shortcut_to_WFA_TSCHD_PREV_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___NON_CORE_HRS == SQ_Shortcut_to_WFA_TSCHD_PRE_temp.SQ_Shortcut_to_WFA_TSCHD_PRE___NON_CORE_HRS],'right_outer').selectExpr(
	"SQ_Shortcut_to_WFA_TSCHD_PRE___TSCHD_ID as TSCHD_ID",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___TSCHD_DAT as TSCHD_DAT",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___EMP_SKEY as EMP_SKEY",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___STRT_DTM as STRT_DTM",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___END_DTM as END_DTM",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___PAYCD_SKEY as PAYCD_SKEY",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___CORE_HRS_SWT as CORE_HRS_SWT",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___PRI_ORG_SKEY as PRI_ORG_SKEY",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___ORG_SKEY as ORG_SKEY",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___SHIFT_ID as SHIFT_ID",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___MONEY_AMT as MONEY_AMT",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___DRTN_AMT as DRTN_AMT",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___CORE_AMT as CORE_AMT",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___NON_CORE_AMT as NON_CORE_AMT",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___DRTN_HRS as DRTN_HRS",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___CORE_HRS as CORE_HRS",
	"SQ_Shortcut_to_WFA_TSCHD_PRE___NON_CORE_HRS as NON_CORE_HRS",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___TSCHD_ID as TSCHD_ID1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___TSCHD_DAT as TSCHD_DAT1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___EMP_SKEY as EMP_SKEY1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___STRT_DTM as STRT_DTM1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___END_DTM as END_DTM1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___PAYCD_SKEY as PAYCD_SKEY1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___CORE_HRS_SWT as CORE_HRS_SWT1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___PRI_ORG_SKEY as PRI_ORG_SKEY1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___ORG_SKEY as ORG_SKEY1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___SHIFT_ID as SHIFT_ID1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___MONEY_AMT as MONEY_AMT1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___DRTN_AMT as DRTN_AMT1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___CORE_AMT as CORE_AMT1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___NON_CORE_AMT as NON_CORE_AMT1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___DRTN_HRS as DRTN_HRS1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___CORE_HRS as CORE_HRS1",
	"SQ_Shortcut_to_WFA_TSCHD_PREV_PRE___NON_CORE_HRS as NON_CORE_HRS1")

# COMMAND ----------
# Processing node FIL_TRANS, type FILTER 
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
JNR_TRANS_temp = JNR_TRANS.toDF(*["JNR_TRANS___" + col for col in JNR_TRANS.columns])

FIL_TRANS = JNR_TRANS_temp.selectExpr(
	"JNR_TRANS___TSCHD_ID as TSCHD_ID",
	"JNR_TRANS___TSCHD_DAT as TSCHD_DAT",
	"JNR_TRANS___EMP_SKEY as EMP_SKEY",
	"JNR_TRANS___STRT_DTM as STRT_DTM",
	"JNR_TRANS___END_DTM as END_DTM",
	"JNR_TRANS___PAYCD_SKEY as PAYCD_SKEY",
	"JNR_TRANS___CORE_HRS_SWT as CORE_HRS_SWT",
	"JNR_TRANS___PRI_ORG_SKEY as PRI_ORG_SKEY",
	"JNR_TRANS___ORG_SKEY as ORG_SKEY",
	"JNR_TRANS___SHIFT_ID as SHIFT_ID",
	"JNR_TRANS___MONEY_AMT as MONEY_AMT",
	"JNR_TRANS___DRTN_AMT as DRTN_AMT",
	"JNR_TRANS___CORE_AMT as CORE_AMT",
	"JNR_TRANS___NON_CORE_AMT as NON_CORE_AMT",
	"JNR_TRANS___DRTN_HRS as DRTN_HRS",
	"JNR_TRANS___CORE_HRS as CORE_HRS",
	"JNR_TRANS___NON_CORE_HRS as NON_CORE_HRS",
	"JNR_TRANS___TSCHD_ID1 as TSCHD_ID1").filter("TSCHD_ID1 is null").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_WFA_TSCHD_DIFF_PRE, type TARGET 
# COLUMN COUNT: 17


Shortcut_to_WFA_TSCHD_DIFF_PRE = FIL_TRANS.selectExpr(
	"CAST(TSCHD_ID AS BIGINT) as TSCHD_ID",
	"CAST(TSCHD_DAT AS TIMESTAMP) as TSCHD_DAT",
	"CAST(EMP_SKEY AS BIGINT) as EMP_SKEY",
	"CAST(STRT_DTM AS TIMESTAMP) as STRT_DTM",
	"CAST(END_DTM AS TIMESTAMP) as END_DTM",
	"CAST(PAYCD_SKEY AS BIGINT) as PAYCD_SKEY",
	"CAST(CORE_HRS_SWT AS INT) as CORE_HRS_SWT",
	"CAST(PRI_ORG_SKEY AS BIGINT) as PRI_ORG_SKEY",
	"CAST(ORG_SKEY AS BIGINT) as ORG_SKEY",
	"CAST(SHIFT_ID AS BIGINT) as SHIFT_ID",
	"CAST(MONEY_AMT AS DECIMAL(16,6)) as MONEY_AMT",
	"CAST(DRTN_AMT AS DECIMAL(16,6)) as DRTN_AMT",
	"CAST(CORE_AMT AS DECIMAL(16,6)) as CORE_AMT",
	"CAST(NON_CORE_AMT AS DECIMAL(16,6)) as NON_CORE_AMT",
	"CAST(DRTN_HRS AS DECIMAL(16,6)) as DRTN_HRS",
	"CAST(CORE_HRS AS DECIMAL(16,6)) as CORE_HRS",
	"CAST(NON_CORE_HRS AS DECIMAL(16,6)) as NON_CORE_HRS"
)
# overwriteDeltaPartition(Shortcut_to_WFA_TSCHD_DIFF_PRE,'DC_NBR',dcnbr,f'{raw}.WFA_TSCHD_DIFF_PRE')
Shortcut_to_WFA_TSCHD_DIFF_PRE.write.mode("overwrite").saveAsTable(f'{empl_protected}.raw_WFA_TSCHD_DIFF_PRE')