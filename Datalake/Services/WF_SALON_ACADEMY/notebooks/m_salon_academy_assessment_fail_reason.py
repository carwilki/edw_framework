#Code converted on 2023-07-26 09:54:11
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
# Processing node SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE = spark.sql(f"""SELECT
SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE.SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID,
SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE.SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC
FROM {raw}.SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON = spark.sql(f"""SELECT
SALON_ACADEMY_ASSESSMENT_FAIL_REASON.SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID,
SALON_ACADEMY_ASSESSMENT_FAIL_REASON.SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC,
SALON_ACADEMY_ASSESSMENT_FAIL_REASON.LOAD_TSTMP
FROM {legacy}.SALON_ACADEMY_ASSESSMENT_FAIL_REASON""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_temp = SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON.toDF(*["SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___" + col for col in SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON.columns])
SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE_temp = SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE.toDF(*["SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE___" + col for col in SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE.columns])

JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON = SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE_temp.join(SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_temp,[SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE_temp.SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID == SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_temp.SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID],'left_outer').selectExpr(
	"SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID",
	"SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_PRE___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC",
	"SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID as i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID",
	"SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC as i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC",
	"SQ_Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON, type FILTER 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_temp = JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON.toDF(*["JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___" + col for col in JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON.columns])

FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON = JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_temp.selectExpr(
	"JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID",
	"JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC",
	"JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID as i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID",
	"JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC as i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC",
	"JNR_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID IS NULL OR (NOT i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID IS NULL AND (CASE WHEN SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC IS NULL THEN '' ELSE SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC END) != (CASE WHEN i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC IS NULL THEN '' ELSE i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC END))").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON, type EXPRESSION 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_temp = FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON.toDF(*["FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___" + col for col in FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON.columns])

EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON = FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_temp.selectExpr(
	"FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___sys_row_id as sys_row_id",
	"FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID",
	"FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (FIL_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___i_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR"
)

# COMMAND ----------
# Processing node UPD_SALON_ACADEMY_ASSESSMENT_FAIL_REASON, type UPDATE_STRATEGY 
# COLUMN COUNT: 5

# for each involved DataFrame, append the dataframe name to each column
EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_temp = EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON.toDF(*["EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___" + col for col in EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON.columns])

UPD_SALON_ACADEMY_ASSESSMENT_FAIL_REASON = EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON_temp.selectExpr(
	"EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID",
	"EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC",
	"EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_SALON_ACADEMY_ASSESSMENT_FAIL_REASON___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col('o_UPDATE_VALIDATOR') ==(lit(1)) , lit(0)) .when(col('o_UPDATE_VALIDATOR') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON, type TARGET 
# COLUMN COUNT: 4


Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON = UPD_SALON_ACADEMY_ASSESSMENT_FAIL_REASON.selectExpr(
	"CAST(SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID AS BIGINT) as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID",
	"CAST(SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC AS STRING) as SALON_ACADEMY_ASSESSMENT_FAIL_REASON_DESC",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID = target.SALON_ACADEMY_ASSESSMENT_FAIL_REASON_ID"""
	refined_perf_table = f"{legacy}.SALON_ACADEMY_ASSESSMENT_FAIL_REASON"
	executeMerge(Shortcut_to_SALON_ACADEMY_ASSESSMENT_FAIL_REASON, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("SALON_ACADEMY_ASSESSMENT_FAIL_REASON", "SALON_ACADEMY_ASSESSMENT_FAIL_REASON", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("SALON_ACADEMY_ASSESSMENT_FAIL_REASON", "SALON_ACADEMY_ASSESSMENT_FAIL_REASON","Failed",str(e), f"{raw}.log_run_details")
	raise e
		