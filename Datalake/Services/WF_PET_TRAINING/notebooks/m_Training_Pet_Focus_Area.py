#Code converted on 2023-08-09 10:48:06
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
# Processing node SQ_TRAINING_PET_FOCUS_AREA, type SOURCE 
# COLUMN COUNT: 3

SQ_TRAINING_PET_FOCUS_AREA = spark.sql(f"""SELECT
TRAINING_PET_FOCUS_AREA_ID,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {legacy}.TRAINING_PET_FOCUS_AREA""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_TRAINING_PET_FOCUS_AREA_PRE, type SOURCE 
# COLUMN COUNT: 4

SQ_TRAINING_PET_FOCUS_AREA_PRE = spark.sql(f"""SELECT
PET_FOCUS_AREA_ID,
PET_ID,
FOCUS_AREA_ID,
LAST_MODIFIED
FROM {raw}.TRAINING_PET_FOCUS_AREA_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_PET_FOCUS_AREA, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_TRAINING_PET_FOCUS_AREA_PRE_temp = SQ_TRAINING_PET_FOCUS_AREA_PRE.toDF(*["SQ_TRAINING_PET_FOCUS_AREA_PRE___" + col for col in SQ_TRAINING_PET_FOCUS_AREA_PRE.columns])
SQ_TRAINING_PET_FOCUS_AREA_temp = SQ_TRAINING_PET_FOCUS_AREA.toDF(*["SQ_TRAINING_PET_FOCUS_AREA___" + col for col in SQ_TRAINING_PET_FOCUS_AREA.columns])

JNR_PET_FOCUS_AREA = SQ_TRAINING_PET_FOCUS_AREA_temp.join(SQ_TRAINING_PET_FOCUS_AREA_PRE_temp,[SQ_TRAINING_PET_FOCUS_AREA_temp.SQ_TRAINING_PET_FOCUS_AREA___TRAINING_PET_FOCUS_AREA_ID == SQ_TRAINING_PET_FOCUS_AREA_PRE_temp.SQ_TRAINING_PET_FOCUS_AREA_PRE___PET_FOCUS_AREA_ID],'right_outer').selectExpr( \
	"SQ_TRAINING_PET_FOCUS_AREA_PRE___PET_FOCUS_AREA_ID as PET_FOCUS_AREA_ID", \
	"SQ_TRAINING_PET_FOCUS_AREA_PRE___PET_ID as PET_ID", \
	"SQ_TRAINING_PET_FOCUS_AREA_PRE___FOCUS_AREA_ID as FOCUS_AREA_ID", \
	"SQ_TRAINING_PET_FOCUS_AREA_PRE___LAST_MODIFIED as LAST_MODIFIED", \
	"SQ_TRAINING_PET_FOCUS_AREA___TRAINING_PET_FOCUS_AREA_ID as lkp_TRAINING_PET_FOCUS_AREA_ID", \
	"SQ_TRAINING_PET_FOCUS_AREA___UPDATE_TSTMP as lkp_UPDATE_TSTMP", \
	"SQ_TRAINING_PET_FOCUS_AREA___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node EXP_PET_FOCUS_AREA, type EXPRESSION . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_PET_FOCUS_AREA_temp = JNR_PET_FOCUS_AREA.toDF(*["JNR_PET_FOCUS_AREA___" + col for col in JNR_PET_FOCUS_AREA.columns])

EXP_PET_FOCUS_AREA = JNR_PET_FOCUS_AREA_temp.selectExpr( \
	"JNR_PET_FOCUS_AREA___PET_FOCUS_AREA_ID as PET_FOCUS_AREA_ID", \
	"JNR_PET_FOCUS_AREA___PET_ID as PET_ID", \
	"JNR_PET_FOCUS_AREA___FOCUS_AREA_ID as FOCUS_AREA_ID", \
	"JNR_PET_FOCUS_AREA___LAST_MODIFIED as LAST_MODIFIED", \
	"JNR_PET_FOCUS_AREA___lkp_TRAINING_PET_FOCUS_AREA_ID as lkp_TRAINING_PET_FOCUS_AREA_ID", \
	"JNR_PET_FOCUS_AREA___lkp_UPDATE_TSTMP as lkp_UPDATE_TSTMP") \
	.withColumn('lkp_LOAD_TSTMP', lit(None)) \
	.selectExpr( \
	# "JNR_PET_FOCUS_AREA___sys_row_id as sys_row_id", \
	"PET_FOCUS_AREA_ID", \
	"PET_ID", \
	"FOCUS_AREA_ID", \
	"LAST_MODIFIED", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"IF (lkp_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, lkp_LOAD_TSTMP) as LOAD_DATE", \
	"IF (lkp_TRAINING_PET_FOCUS_AREA_ID IS NULL, 1, 2) as UPDATE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_PET_FOCUS_AREA, type UPDATE_STRATEGY 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
EXP_PET_FOCUS_AREA_temp = EXP_PET_FOCUS_AREA.toDF(*["EXP_PET_FOCUS_AREA___" + col for col in EXP_PET_FOCUS_AREA.columns])

UPD_PET_FOCUS_AREA = EXP_PET_FOCUS_AREA_temp.selectExpr( \
	"EXP_PET_FOCUS_AREA___PET_FOCUS_AREA_ID as PET_FOCUS_AREA_ID", \
	"EXP_PET_FOCUS_AREA___PET_ID as PET_ID", \
	"EXP_PET_FOCUS_AREA___FOCUS_AREA_ID as FOCUS_AREA_ID", \
	"EXP_PET_FOCUS_AREA___LAST_MODIFIED as LAST_MODIFIED", \
	"EXP_PET_FOCUS_AREA___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_PET_FOCUS_AREA___LOAD_DATE as LOAD_DATE", \
	"EXP_PET_FOCUS_AREA___UPDATE_FLAG as UPDATE_FLAG") \
	.withColumn('pyspark_data_action', when(col('UPDATE_FLAG') ==(lit(1)) , lit(0)) .when(col('UPDATE_FLAG') ==(lit(2)) , lit(1)))

# COMMAND ----------
# Processing node TRAINING_PET_FOCUS_AREA_3, type TARGET 
# COLUMN COUNT: 6


TRAINING_PET_FOCUS_AREA_3 = UPD_PET_FOCUS_AREA.selectExpr( \
	"CAST(PET_FOCUS_AREA_ID AS BIGINT) as TRAINING_PET_FOCUS_AREA_ID", \
	"CAST(PET_ID AS BIGINT) as TRAINING_PET_ID", \
	"CAST(FOCUS_AREA_ID AS BIGINT) as TRAINING_FOCUS_AREA_ID", \
	"CAST(LAST_MODIFIED AS TIMESTAMP) as TRAINING_LAST_MODIFIED", \
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
	"CAST(LOAD_DATE AS TIMESTAMP) as LOAD_TSTMP", \
	"pyspark_data_action as pyspark_data_action" \
)
# TRAINING_PET_FOCUS_AREA_3.write.saveAsTable(f'{raw}.TRAINING_PET_FOCUS_AREA', mode = 'overwrite')
spark.sql("""set spark.sql.legacy.timeParserPolicy = LEGACY""")

try:
  primary_key = """source.TRAINING_PET_FOCUS_AREA_ID = target.TRAINING_PET_FOCUS_AREA_ID"""
  refined_perf_table = f"{legacy}.TRAINING_PET_FOCUS_AREA"
  executeMerge(TRAINING_PET_FOCUS_AREA_3, refined_perf_table, primary_key)
  logger.info(f"Merge with {refined_perf_table} completed]")
  logPrevRunDt("TRAINING_PET_FOCUS_AREA", "TRAINING_PET_FOCUS_AREA", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
  logPrevRunDt("TRAINING_PET_FOCUS_AREA", "TRAINING_PET_FOCUS_AREA","Failed",str(e), f"{raw}.log_run_details", )
  raise e
	