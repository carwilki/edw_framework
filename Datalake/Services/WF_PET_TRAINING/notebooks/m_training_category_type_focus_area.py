#Code converted on 2023-08-09 10:48:25
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
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'

# Set global variables
starttime = datetime.now() #start timestamp of the script


# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA = spark.sql(f"""SELECT
TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {legacy}.TRAINING_CATEGORY_TYPE_FOCUS_AREA""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE, type SOURCE 
# COLUMN COUNT: 5

SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE = spark.sql(f"""SELECT
CATEGORY_TYPE_FOCUS_AREA_ID,
CATEGORY_ID,
FOCUS_AREA_ID,
LAST_MODIFIED,
LOAD_TSTMP
FROM {raw}.TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNRTRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 8

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_temp = SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA.toDF(*["SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA___" + col for col in SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA.columns])
SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE_temp = SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE.toDF(*["SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE___" + col for col in SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE.columns])

JNRTRANS = SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_temp.join(SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE_temp,[SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_temp.SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA___TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID == SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE_temp.SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE___CATEGORY_TYPE_FOCUS_AREA_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE___CATEGORY_TYPE_FOCUS_AREA_ID as CATEGORY_TYPE_FOCUS_AREA_ID", \
	"SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE___CATEGORY_ID as CATEGORY_ID", \
	"SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE___FOCUS_AREA_ID as FOCUS_AREA_ID", \
	"SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE___LAST_MODIFIED as LAST_MODIFIED", \
	"SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_PRE___LOAD_TSTMP as LOAD_TSTMP", \
	"SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA___TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID as lkp_TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID", \
	"SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA___UPDATE_TSTMP as lkp_UPDATE_TSTMP", \
	"SQ_Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNRTRANS_temp = JNRTRANS.toDF(*["JNRTRANS___" + col for col in JNRTRANS.columns])

EXPTRANS = JNRTRANS_temp.selectExpr( \
	# "JNRTRANS___sys_row_id as sys_row_id", \
	"JNRTRANS___CATEGORY_TYPE_FOCUS_AREA_ID as CATEGORY_TYPE_FOCUS_AREA_ID", \
	"JNRTRANS___CATEGORY_ID as CATEGORY_ID", \
	"JNRTRANS___FOCUS_AREA_ID as FOCUS_AREA_ID", \
	"JNRTRANS___LAST_MODIFIED as LAST_MODIFIED", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (JNRTRANS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNRTRANS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (JNRTRANS___lkp_TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID IS NULL, 1, 2) as UPDATE_FLAG" \
)

# COMMAND ----------
# Processing node UPDTRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])

UPDTRANS = EXPTRANS_temp.selectExpr( \
	"EXPTRANS___CATEGORY_TYPE_FOCUS_AREA_ID as CATEGORY_TYPE_FOCUS_AREA_ID", \
	"EXPTRANS___CATEGORY_ID as CATEGORY_ID", \
	"EXPTRANS___FOCUS_AREA_ID as FOCUS_AREA_ID", \
	"EXPTRANS___LAST_MODIFIED as LAST_MODIFIED", \
	"EXPTRANS___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXPTRANS___LOAD_TSTMP as LOAD_TSTMP", \
	"EXPTRANS___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_1, type TARGET 
# COLUMN COUNT: 6


Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_1 = UPDTRANS.selectExpr( \
	"CAST(CATEGORY_TYPE_FOCUS_AREA_ID AS BIGINT) as TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID", \
	"CAST(CATEGORY_ID AS BIGINT) as TRAINING_CATEGORY_ID", \
	"CAST(FOCUS_AREA_ID AS BIGINT) as TRAINING_FOCUS_AREA_ID", \
	"CAST(LAST_MODIFIED AS TIMESTAMP) as LAST_MODIFIED", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_1.write.saveAsTable(f'{raw}.TRAINING_CATEGORY_TYPE_FOCUS_AREA', mode = 'overwrite')
# spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID = target.TRAINING_CATEGORY_TYPE_FOCUS_AREA_ID"""
  refined_perf_table = f"{legacy}.TRAINING_CATEGORY_TYPE_FOCUS_AREA"
  executeMerge(Shortcut_to_TRAINING_CATEGORY_TYPE_FOCUS_AREA_1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_CATEGORY_TYPE_FOCUS_AREA", "TRAINING_CATEGORY_TYPE_FOCUS_AREA", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_CATEGORY_TYPE_FOCUS_AREA", "TRAINING_CATEGORY_TYPE_FOCUS_AREA","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	