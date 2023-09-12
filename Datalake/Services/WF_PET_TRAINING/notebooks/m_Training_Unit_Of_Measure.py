#Code converted on 2023-08-09 10:48:23
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
# Processing node SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE = spark.sql(f"""SELECT
TRAINING_UNIT_OF_MEASURE_ID,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {legacy}.TRAINING_UNIT_OF_MEASURE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE = spark.sql(f"""SELECT
UNIT_OF_MEASURE_ID,
UNIT_OF_MEASURE,
UNIT_OF_MEASURE_PLURAL
FROM {raw}.TRAINING_UNIT_OF_MEASURE_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNRTRANS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_temp = SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE.toDF(*["SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE___" + col for col in SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE.columns])
SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE_temp = SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE.toDF(*["SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE___" + col for col in SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE.columns])

JNRTRANS = SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_temp.join(SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE_temp,[SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_temp.SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE___TRAINING_UNIT_OF_MEASURE_ID == SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE_temp.SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE___UNIT_OF_MEASURE_ID],'right_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE___UNIT_OF_MEASURE_ID as UNIT_OF_MEASURE_ID", \
	"SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE___UNIT_OF_MEASURE as UNIT_OF_MEASURE", \
	"SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE_PRE___UNIT_OF_MEASURE_PLURAL as UNIT_OF_MEASURE_PLURAL", \
	"SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE___TRAINING_UNIT_OF_MEASURE_ID as lkp_TRAINING_UNIT_OF_MEASURE_ID", \
	"SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE___UPDATE_TSTMP as lkp_UPDATE_TSTMP", \
	"SQ_Shortcut_to_TRAINING_UNIT_OF_MEASURE___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node EXPTRANS, type EXPRESSION 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
JNRTRANS_temp = JNRTRANS.toDF(*["JNRTRANS___" + col for col in JNRTRANS.columns])

EXPTRANS = JNRTRANS_temp.selectExpr( \
	# "JNRTRANS___sys_row_id as sys_row_id", \
	"JNRTRANS___UNIT_OF_MEASURE_ID as UNIT_OF_MEASURE_ID", \
	"JNRTRANS___UNIT_OF_MEASURE as UNIT_OF_MEASURE", \
	"JNRTRANS___UNIT_OF_MEASURE_PLURAL as UNIT_OF_MEASURE_PLURAL", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (JNRTRANS___lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNRTRANS___lkp_LOAD_TSTMP) as LOAD_TSTMP", \
	"IF (JNRTRANS___lkp_TRAINING_UNIT_OF_MEASURE_ID IS NULL, 1, 2) as UPDATE_FLAG" \
)

# COMMAND ----------
# Processing node UPDTRANS, type UPDATE_STRATEGY 
# COLUMN COUNT: 6

# for each involved DataFrame, append the dataframe name to each column
EXPTRANS_temp = EXPTRANS.toDF(*["EXPTRANS___" + col for col in EXPTRANS.columns])

UPDTRANS = EXPTRANS_temp.selectExpr( \
	"EXPTRANS___UNIT_OF_MEASURE_ID as UNIT_OF_MEASURE_ID", \
	"EXPTRANS___UNIT_OF_MEASURE as UNIT_OF_MEASURE", \
	"EXPTRANS___UNIT_OF_MEASURE_PLURAL as UNIT_OF_MEASURE_PLURAL", \
	"EXPTRANS___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXPTRANS___LOAD_TSTMP as LOAD_TSTMP", \
	"EXPTRANS___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_UNIT_OF_MEASURE1, type TARGET 
# COLUMN COUNT: 5


Shortcut_to_TRAINING_UNIT_OF_MEASURE1 = UPDTRANS.selectExpr( \
	"CAST(UNIT_OF_MEASURE_ID AS BIGINT) as TRAINING_UNIT_OF_MEASURE_ID", \
	"CAST(UNIT_OF_MEASURE AS STRING) as TRAINING_UNIT_OF_MEASURE", \
	"CAST(UNIT_OF_MEASURE_PLURAL AS STRING) as TRAINING_UNIT_OF_MEASURE_PLURAL", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# Shortcut_to_TRAINING_UNIT_OF_MEASURE1.write.saveAsTable(f'{raw}.TRAINING_UNIT_OF_MEASURE', mode = 'overwrite')
# spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.TRAINING_UNIT_OF_MEASURE_ID = target.TRAINING_UNIT_OF_MEASURE_ID"""
  refined_perf_table = f"{legacy}.TRAINING_UNIT_OF_MEASURE"
  executeMerge(Shortcut_to_TRAINING_UNIT_OF_MEASURE1, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_UNIT_OF_MEASURE", "TRAINING_UNIT_OF_MEASURE", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_UNIT_OF_MEASURE", "TRAINING_UNIT_OF_MEASURE","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	