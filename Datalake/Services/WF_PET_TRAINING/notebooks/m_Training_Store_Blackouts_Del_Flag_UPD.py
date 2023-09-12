#Code converted on 2023-08-09 16:37:40
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
# Processing node SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE, type SOURCE 
# COLUMN COUNT: 3

SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE = spark.sql(f"""SELECT
STORE_NUMBER,
BLACK_OUT_START_DATE,
BLACK_OUT_END_DATE
FROM {raw}.TRAINING_STORE_BLACKOUTS_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS, type SOURCE 
# COLUMN COUNT: 4

SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS = spark.sql(f"""SELECT
STORE_NBR,
BLACK_OUT_START_DT,
BLACK_OUT_END_DT,
LOAD_TSTMP
FROM {legacy}.TRAINING_STORE_BLACKOUTS
WHERE DELETED_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_STORE_BLACKOUTS, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_temp = SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS.toDF(*["SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___" + col for col in SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS.columns])
SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE_temp = SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE.toDF(*["SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___" + col for col in SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE.columns])

JNR_TRAINING_STORE_BLACKOUTS = SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_temp.join(SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE_temp,[SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_temp.SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___STORE_NBR == SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE_temp.SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___STORE_NUMBER],'left_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___STORE_NUMBER as STORE_NUMBER", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___BLACK_OUT_START_DATE as BLACK_OUT_START_DATE", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS_PRE___BLACK_OUT_END_DATE as BLACK_OUT_END_DATE", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___STORE_NBR as lkp_STORE_NBR", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___BLACK_OUT_START_DT as lkp_BLACK_OUT_START_DT", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___BLACK_OUT_END_DT as lkp_BLACK_OUT_END_DT", \
	"SQ_Shortcut_to_TRAINING_STORE_BLACKOUTS___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 7

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_STORE_BLACKOUTS_temp = JNR_TRAINING_STORE_BLACKOUTS.toDF(*["JNR_TRAINING_STORE_BLACKOUTS___" + col for col in JNR_TRAINING_STORE_BLACKOUTS.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_STORE_BLACKOUTS_temp.selectExpr( \
	"JNR_TRAINING_STORE_BLACKOUTS___STORE_NUMBER as STORE_NUMBER", \
	"JNR_TRAINING_STORE_BLACKOUTS___BLACK_OUT_START_DATE as BLACK_OUT_START_DATE", \
	"JNR_TRAINING_STORE_BLACKOUTS___BLACK_OUT_END_DATE as BLACK_OUT_END_DATE", \
	"JNR_TRAINING_STORE_BLACKOUTS___lkp_STORE_NBR as lkp_STORE_NBR", \
	"JNR_TRAINING_STORE_BLACKOUTS___lkp_BLACK_OUT_START_DT as lkp_BLACK_OUT_START_DT", \
	"JNR_TRAINING_STORE_BLACKOUTS___lkp_BLACK_OUT_END_DT as lkp_BLACK_OUT_END_DT", \
	"JNR_TRAINING_STORE_BLACKOUTS___lkp_LOAD_TSTMP as lkp_LOAD_TSTMP").filter("lkp_STORE_NBR IS NOT NULL AND STORE_NUMBER IS NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___lkp_STORE_NBR as lkp_STORE_NBR", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"1 as DELETE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___lkp_STORE_NBR as lkp_STORE_NBR", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___DELETE_FLAG as DELETE_FLAG") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_STORE_BLACKOUTS11, type TARGET 
# COLUMN COUNT: 6

UPD_UPDATE.createOrReplaceTempView('UPD_UPDATE_TMP')
spark.sql(f"""
		MERGE INTO {legacy}.TRAINING_STORE_BLACKOUTS target
        USING UPD_UPDATE_TMP source
        ON source.lkp_STORE_NBR = target.STORE_NBR
        WHEN MATCHED THEN UPDATE SET target.DELETED_FLAG = source.DELETE_FLAG , target.UPDATE_TSTMP = source.UPDATE_TSTMP
""")


# Shortcut_to_TRAINING_STORE_BLACKOUTS11 = UPD_UPDATE.selectExpr( \
# 	"CAST(lkp_STORE_NBR AS BIGINT) as STORE_NBR", \
# 	"CAST(NULL AS DATE) as BLACK_OUT_START_DT", \
# 	"CAST(NULL AS DATE) as BLACK_OUT_END_DT", \
# 	"CAST(DELETE_FLAG AS TINYINT) as DELETED_FLAG", \
# 	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP", \
# 	"pyspark_data_action as pyspark_data_action" \
# )
# Shortcut_to_TRAINING_STORE_BLACKOUTS11.write.saveAsTable(f'{raw}.TRAINING_STORE_BLACKOUTS', mode = 'overwrite')