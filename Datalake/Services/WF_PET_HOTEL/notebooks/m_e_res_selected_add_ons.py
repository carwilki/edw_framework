#Code converted on 2023-07-28 11:37:15
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
# env = 'dev'

if env is None or env == '':
    raise ValueError('env is not set')

refine = getEnvPrefix(env) + 'refine'
raw = getEnvPrefix(env) + 'raw'
legacy = getEnvPrefix(env) + 'legacy'
cust_sensitive = getEnvPrefix(env) + 'cust_sensitive'

# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS, type SOURCE 
# COLUMN COUNT: 9

SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS = spark.sql(f"""SELECT
E_RES_SELECTED_ADD_ON_ID,
E_RES_PET_ID,
E_RES_ADD_ON_CATEGORY_NAME,
E_RES_ADD_ON_DESC,
ADD_ON_FREQ,
UNIT_PRICE_AMT,
TOTAL_PRICE_AMT,
UPDATE_TSTMP,
LOAD_TSTMP
FROM {legacy}.E_RES_SELECTED_ADD_ONS""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE, type SOURCE 
# COLUMN COUNT: 8

SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE = spark.sql(f"""SELECT
SELECTED_ADD_ON_ID,
PET_ID,
CATEGORY,
ADD_ON,
FREQUENCY,
UNIT_PRICE,
TOTAL_PRICE,
LOAD_TSTMP
FROM {raw}.E_RES_SELECTED_ADD_ONS_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node JNR_E_RES_SELECTED_ADD_ONS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_temp = SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS.toDF(*["SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___" + col for col in SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS.columns])
SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE_temp = SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE.toDF(*["SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___" + col for col in SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE.columns])

JNR_E_RES_SELECTED_ADD_ONS = SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_temp.join(SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE_temp,[SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_temp.SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___E_RES_SELECTED_ADD_ON_ID == SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE_temp.SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___SELECTED_ADD_ON_ID],'right_outer').selectExpr(
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___SELECTED_ADD_ON_ID as SELECTED_ADD_ON_ID",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___PET_ID as PET_ID",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___CATEGORY as CATEGORY",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___ADD_ON as ADD_ON",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___FREQUENCY as FREQUENCY",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___UNIT_PRICE as UNIT_PRICE",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___TOTAL_PRICE as TOTAL_PRICE",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS_PRE___LOAD_TSTMP as LOAD_TSTMP",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___E_RES_SELECTED_ADD_ON_ID as i_E_RES_SELECTED_ADD_ON_ID",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___E_RES_PET_ID as i_E_RES_PET_ID",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___E_RES_ADD_ON_CATEGORY_NAME as i_E_RES_ADD_ON_CATEGORY_NAME",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___E_RES_ADD_ON_DESC as i_E_RES_ADD_ON_DESC",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___ADD_ON_FREQ as i_ADD_ON_FREQ",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___UNIT_PRICE_AMT as i_UNIT_PRICE_AMT",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___TOTAL_PRICE_AMT as i_TOTAL_PRICE_AMT",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___UPDATE_TSTMP as i_UPDATE_TSTMP",
	"SQ_Shortcut_to_E_RES_SELECTED_ADD_ONS___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------

# Processing node FIL_E_RES_SELECTED_ADD_ONS, type FILTER 
# COLUMN COUNT: 17

# for each involved DataFrame, append the dataframe name to each column
JNR_E_RES_SELECTED_ADD_ONS_temp = JNR_E_RES_SELECTED_ADD_ONS.toDF(*["JNR_E_RES_SELECTED_ADD_ONS___" + col for col in JNR_E_RES_SELECTED_ADD_ONS.columns])

FIL_E_RES_SELECTED_ADD_ONS = JNR_E_RES_SELECTED_ADD_ONS_temp.selectExpr(
	"JNR_E_RES_SELECTED_ADD_ONS___SELECTED_ADD_ON_ID as SELECTED_ADD_ON_ID",
	"JNR_E_RES_SELECTED_ADD_ONS___PET_ID as PET_ID",
	"JNR_E_RES_SELECTED_ADD_ONS___CATEGORY as CATEGORY",
	"JNR_E_RES_SELECTED_ADD_ONS___ADD_ON as ADD_ON",
	"JNR_E_RES_SELECTED_ADD_ONS___FREQUENCY as FREQUENCY",
	"JNR_E_RES_SELECTED_ADD_ONS___UNIT_PRICE as UNIT_PRICE",
	"JNR_E_RES_SELECTED_ADD_ONS___TOTAL_PRICE as TOTAL_PRICE",
	"JNR_E_RES_SELECTED_ADD_ONS___LOAD_TSTMP as LOAD_TSTMP",
	"JNR_E_RES_SELECTED_ADD_ONS___i_E_RES_SELECTED_ADD_ON_ID as i_E_RES_SELECTED_ADD_ON_ID",
	"JNR_E_RES_SELECTED_ADD_ONS___i_E_RES_PET_ID as i_E_RES_PET_ID",
	"JNR_E_RES_SELECTED_ADD_ONS___i_E_RES_ADD_ON_CATEGORY_NAME as i_E_RES_ADD_ON_CATEGORY_NAME",
	"JNR_E_RES_SELECTED_ADD_ONS___i_E_RES_ADD_ON_DESC as i_E_RES_ADD_ON_DESC",
	"JNR_E_RES_SELECTED_ADD_ONS___i_ADD_ON_FREQ as i_ADD_ON_FREQ",
	"JNR_E_RES_SELECTED_ADD_ONS___i_UNIT_PRICE_AMT as i_UNIT_PRICE_AMT",
	"JNR_E_RES_SELECTED_ADD_ONS___i_TOTAL_PRICE_AMT as i_TOTAL_PRICE_AMT",
	"JNR_E_RES_SELECTED_ADD_ONS___i_UPDATE_TSTMP as i_UPDATE_TSTMP",
	"JNR_E_RES_SELECTED_ADD_ONS___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_E_RES_SELECTED_ADD_ON_ID IS NULL OR ( i_E_RES_SELECTED_ADD_ON_ID IS NOT NULL AND ( IF(PET_ID IS NULL, - 99999, PET_ID) != IF(i_E_RES_PET_ID IS NULL, - 99999, i_E_RES_PET_ID) OR IF(CATEGORY IS NULL, '', CATEGORY) != IF(i_E_RES_ADD_ON_CATEGORY_NAME IS NULL, '', i_E_RES_ADD_ON_CATEGORY_NAME) OR IF(ADD_ON IS NULL, '', ADD_ON) != IF(i_E_RES_ADD_ON_DESC IS NULL, '', i_E_RES_ADD_ON_DESC) OR IF(FREQUENCY IS NULL, '', FREQUENCY) != IF(i_ADD_ON_FREQ IS NULL, '', i_ADD_ON_FREQ) OR IF(UNIT_PRICE IS NULL, - 99999, UNIT_PRICE) != IF(i_UNIT_PRICE_AMT IS NULL, - 99999, i_UNIT_PRICE_AMT) OR IF(TOTAL_PRICE IS NULL, - 99999, TOTAL_PRICE) != IF(i_TOTAL_PRICE_AMT IS NULL, - 99999, i_TOTAL_PRICE_AMT) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_E_RES_SELECTED_ADD_ONS, type EXPRESSION 
# COLUMN COUNT: 12

# for each involved DataFrame, append the dataframe name to each column
FIL_E_RES_SELECTED_ADD_ONS_temp = FIL_E_RES_SELECTED_ADD_ONS.toDF(*["FIL_E_RES_SELECTED_ADD_ONS___" + col for col in FIL_E_RES_SELECTED_ADD_ONS.columns])

EXP_E_RES_SELECTED_ADD_ONS = FIL_E_RES_SELECTED_ADD_ONS_temp.selectExpr(
	"FIL_E_RES_SELECTED_ADD_ONS___sys_row_id as sys_row_id",
	"FIL_E_RES_SELECTED_ADD_ONS___SELECTED_ADD_ON_ID as SELECTED_ADD_ON_ID",
	"FIL_E_RES_SELECTED_ADD_ONS___PET_ID as PET_ID",
	"FIL_E_RES_SELECTED_ADD_ONS___CATEGORY as CATEGORY",
	"FIL_E_RES_SELECTED_ADD_ONS___ADD_ON as ADD_ON",
	"FIL_E_RES_SELECTED_ADD_ONS___FREQUENCY as FREQUENCY",
	"FIL_E_RES_SELECTED_ADD_ONS___UNIT_PRICE as UNIT_PRICE",
	"FIL_E_RES_SELECTED_ADD_ONS___TOTAL_PRICE as TOTAL_PRICE",
	"FIL_E_RES_SELECTED_ADD_ONS___i_E_RES_SELECTED_ADD_ON_ID as i_E_RES_SELECTED_ADD_ON_ID",
	"FIL_E_RES_SELECTED_ADD_ONS___i_LOAD_TSTMP as i_LOAD_TSTMP",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF(FIL_E_RES_SELECTED_ADD_ONS___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_E_RES_SELECTED_ADD_ONS___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF(FIL_E_RES_SELECTED_ADD_ONS___i_E_RES_SELECTED_ADD_ON_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR"
)

# COMMAND ----------

# Processing node UPD_E_RES_SELECTED_ADD_ONS, type UPDATE_STRATEGY 
# COLUMN COUNT: 10

# for each involved DataFrame, append the dataframe name to each column
EXP_E_RES_SELECTED_ADD_ONS_temp = EXP_E_RES_SELECTED_ADD_ONS.toDF(*["EXP_E_RES_SELECTED_ADD_ONS___" + col for col in EXP_E_RES_SELECTED_ADD_ONS.columns])

UPD_E_RES_SELECTED_ADD_ONS = EXP_E_RES_SELECTED_ADD_ONS_temp.selectExpr(
	"EXP_E_RES_SELECTED_ADD_ONS___SELECTED_ADD_ON_ID as SELECTED_ADD_ON_ID",
	"EXP_E_RES_SELECTED_ADD_ONS___PET_ID as PET_ID",
	"EXP_E_RES_SELECTED_ADD_ONS___CATEGORY as CATEGORY",
	"EXP_E_RES_SELECTED_ADD_ONS___ADD_ON as ADD_ON",
	"EXP_E_RES_SELECTED_ADD_ONS___FREQUENCY as FREQUENCY",
	"EXP_E_RES_SELECTED_ADD_ONS___UNIT_PRICE as UNIT_PRICE",
	"EXP_E_RES_SELECTED_ADD_ONS___TOTAL_PRICE as TOTAL_PRICE",
	"EXP_E_RES_SELECTED_ADD_ONS___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_E_RES_SELECTED_ADD_ONS___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_E_RES_SELECTED_ADD_ONS___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col("o_UPDATE_VALIDATOR") ==(lit(1)) , lit(0)).when(col("o_UPDATE_VALIDATOR") ==(lit(2)) , lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_E_RES_SELECTED_ADD_ONS1, type TARGET 
# COLUMN COUNT: 9


Shortcut_to_E_RES_SELECTED_ADD_ONS1 = UPD_E_RES_SELECTED_ADD_ONS.selectExpr(
	"CAST(SELECTED_ADD_ON_ID AS INT) as E_RES_SELECTED_ADD_ON_ID",
	"CAST(PET_ID AS INT) as E_RES_PET_ID",
	"CAST(CATEGORY AS STRING) as E_RES_ADD_ON_CATEGORY_NAME",
	"CAST(ADD_ON AS STRING) as E_RES_ADD_ON_DESC",
	"CAST(FREQUENCY AS STRING) as ADD_ON_FREQ",
	"CAST(UNIT_PRICE AS DECIMAL(15,2)) as UNIT_PRICE_AMT",
	"CAST(TOTAL_PRICE AS DECIMAL(15,2)) as TOTAL_PRICE_AMT",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.E_RES_SELECTED_ADD_ON_ID = target.E_RES_SELECTED_ADD_ON_ID"""
	refined_perf_table = f"{legacy}.E_RES_SELECTED_ADD_ONS"
	executeMerge(Shortcut_to_E_RES_SELECTED_ADD_ONS1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("E_RES_SELECTED_ADD_ONS", "E_RES_SELECTED_ADD_ONS", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("E_RES_SELECTED_ADD_ONS", "E_RES_SELECTED_ADD_ONS","Failed",str(e), f"{raw}.log_run_details", )
	raise e
