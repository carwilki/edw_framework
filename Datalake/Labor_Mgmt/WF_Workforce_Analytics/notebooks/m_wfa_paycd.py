#Code converted on 2023-08-08 15:41:47
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
# Processing node SQ_Shortcut_to_WFA_PAYCD_PRE, type SOURCE 
# COLUMN COUNT: 14

SQ_Shortcut_to_WFA_PAYCD_PRE = spark.sql(f"""SELECT
WFA_PAYCD_PRE.PAYCD_ID,
WFA_PAYCD_PRE.PAYCD_SKEY,
WFA_PAYCD_PRE.PAYCD_NAM,
WFA_PAYCD_PRE.PAYCD_TYP_COD,
WFA_PAYCD_PRE.PAYCD_WAGE_ADD_AMT,
WFA_PAYCD_PRE.PAYCD_WAGE_MLT_AMT,
WFA_PAYCD_PRE.PAYCD_MONEY_SWT,
WFA_PAYCD_PRE.PAYCD_TOT_SWT,
WFA_PAYCD_PRE.PAYCD_EXCUSED_SWT,
WFA_PAYCD_PRE.PAYCD_OT_SWT,
WFA_PAYCD_PRE.PAYCD_CONSEC_OT_SWT,
WFA_PAYCD_PRE.PAYCD_VISIBLE_SWT,
WFA_PAYCD_PRE.PAYCAT_NAM,
WFA_PAYCD_PRE.CORE_HRS_SWT
FROM {raw}.WFA_PAYCD_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_WFA_PAYCD, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_WFA_PAYCD = spark.sql(f"""SELECT
WFA_PAYCD.PAYCD_ID,
WFA_PAYCD.LOAD_DT
FROM {legacy}.WFA_PAYCD""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 16

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_WFA_PAYCD_PRE_temp = SQ_Shortcut_to_WFA_PAYCD_PRE.toDF(*["SQ_Shortcut_to_WFA_PAYCD_PRE___" + col for col in SQ_Shortcut_to_WFA_PAYCD_PRE.columns])
SQ_Shortcut_to_WFA_PAYCD_temp = SQ_Shortcut_to_WFA_PAYCD.toDF(*["SQ_Shortcut_to_WFA_PAYCD___" + col for col in SQ_Shortcut_to_WFA_PAYCD.columns])

JNR_TRANS = SQ_Shortcut_to_WFA_PAYCD_temp.join(SQ_Shortcut_to_WFA_PAYCD_PRE_temp,[SQ_Shortcut_to_WFA_PAYCD_temp.SQ_Shortcut_to_WFA_PAYCD___PAYCD_ID == SQ_Shortcut_to_WFA_PAYCD_PRE_temp.SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_WFA_PAYCD_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_ID as PAYCD_ID",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_SKEY as PAYCD_SKEY",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_NAM as PAYCD_NAM",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_TYP_COD as PAYCD_TYP_COD",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_WAGE_ADD_AMT as PAYCD_WAGE_ADD_AMT",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_WAGE_MLT_AMT as PAYCD_WAGE_MLT_AMT",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_MONEY_SWT as PAYCD_MONEY_SWT",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_TOT_SWT as PAYCD_TOT_SWT",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_EXCUSED_SWT as PAYCD_EXCUSED_SWT",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_OT_SWT as PAYCD_OT_SWT",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_CONSEC_OT_SWT as PAYCD_CONSEC_OT_SWT",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCD_VISIBLE_SWT as PAYCD_VISIBLE_SWT",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___PAYCAT_NAM as PAYCAT_NAM",
	"SQ_Shortcut_to_WFA_PAYCD_PRE___CORE_HRS_SWT as CORE_HRS_SWT",
	"SQ_Shortcut_to_WFA_PAYCD___PAYCD_ID as PAYCD_ID1",
	"SQ_Shortcut_to_WFA_PAYCD___LOAD_DT as LOAD_DT")

# COMMAND ----------
# Processing node EXP_TRANS, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 18

# for each involved DataFrame, append the dataframe name to each column
JNR_TRANS_temp = JNR_TRANS.toDF(*["JNR_TRANS___" + col for col in JNR_TRANS.columns])

EXP_TRANS = JNR_TRANS_temp.selectExpr(
	"JNR_TRANS___sys_row_id as sys_row_id",
	"JNR_TRANS___PAYCD_ID as PAYCD_ID",
	"JNR_TRANS___PAYCD_SKEY as PAYCD_SKEY",
	"JNR_TRANS___PAYCD_NAM as PAYCD_NAM",
	"JNR_TRANS___PAYCD_TYP_COD as PAYCD_TYP_COD",
	"JNR_TRANS___PAYCD_WAGE_ADD_AMT as PAYCD_WAGE_ADD_AMT",
	"JNR_TRANS___PAYCD_WAGE_MLT_AMT as PAYCD_WAGE_MLT_AMT",
	"JNR_TRANS___PAYCD_MONEY_SWT as PAYCD_MONEY_SWT",
	"JNR_TRANS___PAYCD_TOT_SWT as PAYCD_TOT_SWT",
	"JNR_TRANS___PAYCD_EXCUSED_SWT as PAYCD_EXCUSED_SWT",
	"JNR_TRANS___PAYCD_OT_SWT as PAYCD_OT_SWT",
	"JNR_TRANS___PAYCD_CONSEC_OT_SWT as PAYCD_CONSEC_OT_SWT",
	"JNR_TRANS___PAYCD_VISIBLE_SWT as PAYCD_VISIBLE_SWT",
	"JNR_TRANS___PAYCAT_NAM as PAYCAT_NAM",
	"JNR_TRANS___CORE_HRS_SWT as CORE_HRS_SWT",
	"IF (JNR_TRANS___PAYCD_ID1 IS NULL, 'I', 'U') as LOAD_FLAG",
	"DATE_TRUNC ('DAY', CURRENT_TIMESTAMP ) as UPDATE_DT",
	"JNR_TRANS___PAYCD_ID1 as PAYCD_ID1",
	"IF (JNR_TRANS___LOAD_DT IS NULL, DATE_TRUNC ('DAY', CURRENT_TIMESTAMP ), JNR_TRANS___LOAD_DT) as LOAD_DT"
)

# COMMAND ----------
# Processing node UPD_ins_upd, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
EXP_TRANS_temp = EXP_TRANS.toDF(*["EXP_TRANS___" + col for col in EXP_TRANS.columns])

UPD_ins_upd = EXP_TRANS_temp.selectExpr(
	"EXP_TRANS___PAYCD_ID as PAYCD_ID1",
	"EXP_TRANS___PAYCD_SKEY as PAYCD_SKEY",
	"EXP_TRANS___PAYCD_NAM as PAYCD_NAM1",
	"EXP_TRANS___PAYCD_TYP_COD as PAYCD_TYP_COD1",
	"EXP_TRANS___PAYCD_WAGE_ADD_AMT as PAYCD_WAGE_ADD_AMT1",
	"EXP_TRANS___PAYCD_WAGE_MLT_AMT as PAYCD_WAGE_MLT_AMT1",
	"EXP_TRANS___PAYCD_MONEY_SWT as PAYCD_MONEY_SWT1",
	"EXP_TRANS___PAYCD_TOT_SWT as PAYCD_TOT_SWT1",
	"EXP_TRANS___PAYCD_EXCUSED_SWT as PAYCD_EXCUSED_SWT1",
	"EXP_TRANS___PAYCD_OT_SWT as PAYCD_OT_SWT1",
	"EXP_TRANS___PAYCD_CONSEC_OT_SWT as PAYCD_CONSEC_OT_SWT1",
	"EXP_TRANS___PAYCD_VISIBLE_SWT as PAYCD_VISIBLE_SWT1",
	"EXP_TRANS___PAYCAT_NAM as PAYCAT_NAM1",
	"EXP_TRANS___CORE_HRS_SWT as CORE_HRS_SWT1",
	"EXP_TRANS___LOAD_FLAG as LOAD_FLAG",
	"EXP_TRANS___UPDATE_DT as UPDATE_DT",
	"EXP_TRANS___LOAD_DT as LOAD_DT1",
 	"IF(EXP_TRANS___LOAD_FLAG == 'I', 0, 1) as pyspark_data_action") 
	# .withColumn('pyspark_data_action', when((EXP_TRANS_temp.LOAD_FLAG == lit('I')) ,(lit(0))) .otherwise(lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_WFA_PAYCD_ins_upd, type TARGET 
# COLUMN COUNT: 16


Shortcut_to_WFA_PAYCD_ins_upd = UPD_ins_upd.selectExpr(
	"CAST(PAYCD_ID1 AS DECIMAL(10,0)) as PAYCD_ID",
	"CAST(PAYCD_SKEY AS DECIMAL(10,0)) as PAYCD_SKEY",
	"PAYCD_NAM1 as PAYCD_NAM",
	"PAYCD_TYP_COD1 as PAYCD_TYP_COD",
	"CAST(PAYCD_WAGE_ADD_AMT1 AS DECIMAL(16,6)) as PAYCD_WAGE_ADD_AMT",
	"CAST(PAYCD_WAGE_MLT_AMT1 AS DECIMAL(16,6)) as PAYCD_WAGE_MLT_AMT",
	"CAST(PAYCD_MONEY_SWT1 AS BIGINT) as PAYCD_MONEY_SWT",
	"CAST(PAYCD_TOT_SWT1 AS BIGINT) as PAYCD_TOT_SWT",
	"CAST(PAYCD_EXCUSED_SWT1 AS BIGINT) as PAYCD_EXCUSED_SWT",
	"CAST(PAYCD_OT_SWT1 AS BIGINT) as PAYCD_OT_SWT",
	"CAST(PAYCD_CONSEC_OT_SWT1 AS BIGINT) as PAYCD_CONSEC_OT_SWT",
	"CAST(PAYCD_VISIBLE_SWT1 AS BIGINT) as PAYCD_VISIBLE_SWT",
	"PAYCAT_NAM1 as PAYCAT_NAM",
	"CAST(CORE_HRS_SWT1 AS BIGINT) as CORE_HRS_SWT",
	"CAST(UPDATE_DT AS TIMESTAMP) as UPDATE_DT",
	"CAST(LOAD_DT1 AS TIMESTAMP) as LOAD_DT",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.PAYCD_ID = target.PAYCD_ID"""
	refined_perf_table = f"{legacy}.WFA_PAYCD"
	executeMerge(Shortcut_to_WFA_PAYCD_ins_upd, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("WFA_PAYCD", "WFA_PAYCD", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("WFA_PAYCD", "WFA_PAYCD","Failed",str(e), f"{raw}.log_run_details")
	raise e
		