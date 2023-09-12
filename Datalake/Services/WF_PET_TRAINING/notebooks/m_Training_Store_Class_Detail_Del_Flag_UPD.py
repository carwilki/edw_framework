#Code converted on 2023-08-09 16:37:42
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
# Processing node SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL, type SOURCE 
# COLUMN COUNT: 7

SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL = spark.sql(f"""SELECT
TRAINING_STORE_CLASS_DETAIL_ID,
TRAINING_STORE_CLASS_ID,
TRAINING_CLASS_TYPE_ID,
STORE_NBR,
CLASS_TSTMP,
SRC_LAST_MODIFIED_TSTMP,
LOAD_TSTMP
FROM {legacy}.TRAINING_STORE_CLASS_DETAIL
WHERE DELETED_FLAG = 0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE, type SOURCE 
# COLUMN COUNT: 6

SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE = spark.sql(f"""SELECT
STORE_CLASS_DETAIL_ID,
STORE_CLASS_ID,
CLASS_DATE_TIME,
CLASS_TYPE_ID,
LAST_MODIFIED,
STORE_NUMBER
FROM {raw}.TRAINING_STORE_CLASS_DETAIL_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_TRAINING_STORE_CLASS_DETAIL, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 13

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_temp = SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL.toDF(*["SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL___" + col for col in SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL.columns])
SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE_temp = SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE.toDF(*["SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE___" + col for col in SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE.columns])

JNR_TRAINING_STORE_CLASS_DETAIL = SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_temp.join(SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE_temp,[SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_temp.SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL___TRAINING_STORE_CLASS_DETAIL_ID == SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE_temp.SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE___STORE_CLASS_DETAIL_ID],'left_outer').selectExpr( \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE___STORE_CLASS_DETAIL_ID as STORE_CLASS_DETAIL_ID", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE___STORE_CLASS_ID as STORE_CLASS_ID", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE___CLASS_DATE_TIME as CLASS_DATE_TIME", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE___CLASS_TYPE_ID as CLASS_TYPE_ID", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE___LAST_MODIFIED as LAST_MODIFIED", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL_PRE___STORE_NUMBER as STORE_NUMBER", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL___TRAINING_STORE_CLASS_DETAIL_ID as lkp_TRAINING_STORE_CLASS_DETAIL_ID", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL___TRAINING_STORE_CLASS_ID as lkp_TRAINING_STORE_CLASS_ID", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL___TRAINING_CLASS_TYPE_ID as lkp_TRAINING_CLASS_TYPE_ID", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL___STORE_NBR as lkp_STORE_NBR", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL___CLASS_TSTMP as lkp_CLASS_TSTMP", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL___SRC_LAST_MODIFIED_TSTMP as lkp_SRC_LAST_MODIFIED_TSTMP", \
	"SQ_Shortcut_to_TRAINING_STORE_CLASS_DETAIL___LOAD_TSTMP as lkp_LOAD_TSTMP")

# COMMAND ----------
# Processing node FIL_UNCHANGED_RECORDS, type FILTER 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
JNR_TRAINING_STORE_CLASS_DETAIL_temp = JNR_TRAINING_STORE_CLASS_DETAIL.toDF(*["JNR_TRAINING_STORE_CLASS_DETAIL___" + col for col in JNR_TRAINING_STORE_CLASS_DETAIL.columns])

FIL_UNCHANGED_RECORDS = JNR_TRAINING_STORE_CLASS_DETAIL_temp.selectExpr( \
	"JNR_TRAINING_STORE_CLASS_DETAIL___STORE_CLASS_DETAIL_ID as STORE_CLASS_DETAIL_ID", \
	"JNR_TRAINING_STORE_CLASS_DETAIL___lkp_TRAINING_STORE_CLASS_DETAIL_ID as lkp_TRAINING_STORE_CLASS_DETAIL_ID").filter("STORE_CLASS_DETAIL_ID IS NULL AND lkp_TRAINING_STORE_CLASS_DETAIL_ID IS NOT NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_UPDATE_VALIDATOR, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
FIL_UNCHANGED_RECORDS_temp = FIL_UNCHANGED_RECORDS.toDF(*["FIL_UNCHANGED_RECORDS___" + col for col in FIL_UNCHANGED_RECORDS.columns])

EXP_UPDATE_VALIDATOR = FIL_UNCHANGED_RECORDS_temp.selectExpr( \
	"FIL_UNCHANGED_RECORDS___sys_row_id as sys_row_id", \
	"FIL_UNCHANGED_RECORDS___STORE_CLASS_DETAIL_ID as STORE_CLASS_DETAIL_ID", \
	"FIL_UNCHANGED_RECORDS___lkp_TRAINING_STORE_CLASS_DETAIL_ID as lkp_TRAINING_STORE_CLASS_DETAIL_ID", \
	"CURRENT_TIMESTAMP as UPDATE_TSTMP", \
	"1 as DELETE_FLAG" \
)

# COMMAND ----------
# Processing node UPD_DEL_FLAG_UPDATE, type UPDATE_STRATEGY 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
EXP_UPDATE_VALIDATOR_temp = EXP_UPDATE_VALIDATOR.toDF(*["EXP_UPDATE_VALIDATOR___" + col for col in EXP_UPDATE_VALIDATOR.columns])

UPD_DEL_FLAG_UPDATE = EXP_UPDATE_VALIDATOR_temp.selectExpr( \
	"EXP_UPDATE_VALIDATOR___lkp_TRAINING_STORE_CLASS_DETAIL_ID as lkp_TRAINING_STORE_CLASS_DETAIL_ID", \
	"EXP_UPDATE_VALIDATOR___UPDATE_TSTMP as UPDATE_TSTMP", \
	"EXP_UPDATE_VALIDATOR___DELETE_FLAG as DELETE_FLAG") \
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_TRAINING_STORE_CLASS_DETAIL11, type TARGET 
# COLUMN COUNT: 9

UPD_DEL_FLAG_UPDATE.createOrReplaceTempView('UPD_DEL_FLAG_UPDATE_TMP')
spark.sql(f"""
		MERGE INTO {legacy}.TRAINING_STORE_CLASS_DETAIL target
        USING UPD_DEL_FLAG_UPDATE_TMP source
        ON source.lkp_TRAINING_STORE_CLASS_DETAIL_ID = target.TRAINING_STORE_CLASS_DETAIL_ID
        WHEN MATCHED THEN UPDATE SET target.DELETED_FLAG = source.DELETE_FLAG , target.UPDATE_TSTMP = source.UPDATE_TSTMP
""")



# Shortcut_to_TRAINING_STORE_CLASS_DETAIL11 = UPD_DEL_FLAG_UPDATE.selectExpr( \
# 	"CAST(lkp_TRAINING_STORE_CLASS_DETAIL_ID AS BIGINT) as TRAINING_STORE_CLASS_DETAIL_ID", \
# 	"CAST(NULL AS BIGINT) as TRAINING_STORE_CLASS_ID", \
# 	"CAST(NULL AS BIGINT) as TRAINING_CLASS_TYPE_ID", \
# 	"CAST(NULL AS BIGINT) as STORE_NBR", \
# 	"CAST(NULL AS TIMESTAMP) as CLASS_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as SRC_LAST_MODIFIED_TSTMP", \
# 	"CAST(DELETE_FLAG AS TINYINT) as DELETED_FLAG", \
# 	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP", \
# 	"CAST(NULL AS TIMESTAMP) as LOAD_TSTMP", \
# 	"pyspark_data_action as pyspark_data_action" \
# )
# Shortcut_to_TRAINING_STORE_CLASS_DETAIL11.write.saveAsTable(f'{raw}.TRAINING_STORE_CLASS_DETAIL', mode = 'overwrite')