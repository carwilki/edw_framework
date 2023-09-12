#Code converted on 2023-07-28 11:37:31
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


# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY, type SOURCE 
# COLUMN COUNT: 12

SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY = spark.sql(f"""SELECT
E_RES_ADD_ON_CATEGORY_ID,
E_RES_ADD_ON_CATEGORY_NAME,
E_RES_ADD_ON_CATEGORY_DESC,
ALLOW_MULTIPLE_SERVICES_FLAG,
IMAGE_URL,
SORT_ORDER_NBR,
DELETED_FLAG,
E_RES_CREATED_TSTMP,
E_RES_CREATED_BY,
E_RES_UPDATE_TSTMP,
E_RES_UPDATED_BY,
LOAD_TSTMP
FROM {legacy}.E_RES_ADD_ON_CATEGORY""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE, type SOURCE 
# COLUMN COUNT: 11

SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE = spark.sql(f"""SELECT
ADD_ON_CATEGORY_ID,
ADD_ON_CATEGORY_NAME,
ALLOW_MULTIPLE_SERVICES,
CREATED_AT,
UPDATED_BY,
CREATED_BY,
UPDATED_AT,
IS_DELETED,
IMAGE_URL,
DESCRIPTION,
SORT_ORDER
FROM {raw}.E_RES_ADD_ON_CATEGORY_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_E_RES_ADD_ON_CATEGORY_PRE, type EXPRESSION 
# COLUMN COUNT: 11

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE_temp = SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE.toDF(*["SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___" + col for col in SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE.columns])

EXP_E_RES_ADD_ON_CATEGORY_PRE = SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE_temp.selectExpr(
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___ADD_ON_CATEGORY_NAME as ADD_ON_CATEGORY_NAME",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___ALLOW_MULTIPLE_SERVICES as ALLOW_MULTIPLE_SERVICES",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___CREATED_AT as CREATED_AT",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___UPDATED_BY as UPDATED_BY",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___CREATED_BY as CREATED_BY",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___UPDATED_AT as UPDATED_AT",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___IS_DELETED as IS_DELETED",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___IMAGE_URL as IMAGE_URL",
	"regexp_replace(SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___DESCRIPTION, '\\((\\\\r|\\\\n)\\)', '$1') as o_DESCRIPTION", 
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_PRE___SORT_ORDER as SORT_ORDER"
)

# COMMAND ----------

# Processing node JNR_E_RES_ADD_ON_CATEGORY, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_temp = SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY.toDF(*["SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___" + col for col in SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY.columns])
EXP_E_RES_ADD_ON_CATEGORY_PRE_temp = EXP_E_RES_ADD_ON_CATEGORY_PRE.toDF(*["EXP_E_RES_ADD_ON_CATEGORY_PRE___" + col for col in EXP_E_RES_ADD_ON_CATEGORY_PRE.columns])

JNR_E_RES_ADD_ON_CATEGORY = SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_temp.join(EXP_E_RES_ADD_ON_CATEGORY_PRE_temp,[SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY_temp.SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___E_RES_ADD_ON_CATEGORY_ID == EXP_E_RES_ADD_ON_CATEGORY_PRE_temp.EXP_E_RES_ADD_ON_CATEGORY_PRE___ADD_ON_CATEGORY_ID],'right_outer').selectExpr(
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___ADD_ON_CATEGORY_NAME as ADD_ON_CATEGORY_NAME",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___ALLOW_MULTIPLE_SERVICES as ALLOW_MULTIPLE_SERVICES",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___CREATED_AT as CREATED_AT",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___UPDATED_BY as UPDATED_BY",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___CREATED_BY as CREATED_BY",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___UPDATED_AT as UPDATED_AT",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___IS_DELETED as IS_DELETED",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___IMAGE_URL as IMAGE_URL",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___o_DESCRIPTION as DESCRIPTION",
	"EXP_E_RES_ADD_ON_CATEGORY_PRE___SORT_ORDER as SORT_ORDER",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___E_RES_ADD_ON_CATEGORY_ID as i_E_RES_ADD_ON_CATEGORY_ID",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___E_RES_ADD_ON_CATEGORY_NAME as i_E_RES_ADD_ON_CATEGORY_NAME",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___E_RES_ADD_ON_CATEGORY_DESC as i_E_RES_ADD_ON_CATEGORY_DESC",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___ALLOW_MULTIPLE_SERVICES_FLAG as i_ALLOW_MULTIPLE_SERVICES_FLAG",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___IMAGE_URL as i_IMAGE_URL",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___SORT_ORDER_NBR as i_SORT_ORDER_NBR",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___DELETED_FLAG as i_DELETED_FLAG",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___E_RES_CREATED_TSTMP as i_E_RES_CREATED_TSTMP",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___E_RES_CREATED_BY as i_E_RES_CREATED_BY",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___E_RES_UPDATE_TSTMP as i_E_RES_UPDATE_TSTMP",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___E_RES_UPDATED_BY as i_E_RES_UPDATED_BY",
	"SQ_Shortcut_to_E_RES_ADD_ON_CATEGORY___LOAD_TSTMP as i_LOAD_TSTMP")

# COMMAND ----------

# Processing node FIL_E_RES_ADD_ON_CATEGORY, type FILTER 
# COLUMN COUNT: 23

# for each involved DataFrame, append the dataframe name to each column
JNR_E_RES_ADD_ON_CATEGORY_temp = JNR_E_RES_ADD_ON_CATEGORY.toDF(*["JNR_E_RES_ADD_ON_CATEGORY___" + col for col in JNR_E_RES_ADD_ON_CATEGORY.columns])

FIL_E_RES_ADD_ON_CATEGORY = JNR_E_RES_ADD_ON_CATEGORY_temp.selectExpr(
	"JNR_E_RES_ADD_ON_CATEGORY___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
	"JNR_E_RES_ADD_ON_CATEGORY___ADD_ON_CATEGORY_NAME as ADD_ON_CATEGORY_NAME",
	"JNR_E_RES_ADD_ON_CATEGORY___ALLOW_MULTIPLE_SERVICES as ALLOW_MULTIPLE_SERVICES",
	"JNR_E_RES_ADD_ON_CATEGORY___CREATED_AT as CREATED_AT",
	"JNR_E_RES_ADD_ON_CATEGORY___UPDATED_BY as UPDATED_BY",
	"JNR_E_RES_ADD_ON_CATEGORY___CREATED_BY as CREATED_BY",
	"JNR_E_RES_ADD_ON_CATEGORY___UPDATED_AT as UPDATED_AT",
	"JNR_E_RES_ADD_ON_CATEGORY___IS_DELETED as IS_DELETED",
	"JNR_E_RES_ADD_ON_CATEGORY___IMAGE_URL as IMAGE_URL",
	"JNR_E_RES_ADD_ON_CATEGORY___DESCRIPTION as DESCRIPTION",
	"JNR_E_RES_ADD_ON_CATEGORY___SORT_ORDER as SORT_ORDER",
	"JNR_E_RES_ADD_ON_CATEGORY___i_E_RES_ADD_ON_CATEGORY_ID as i_E_RES_ADD_ON_CATEGORY_ID",
	"JNR_E_RES_ADD_ON_CATEGORY___i_E_RES_ADD_ON_CATEGORY_NAME as i_E_RES_ADD_ON_CATEGORY_NAME",
	"JNR_E_RES_ADD_ON_CATEGORY___i_E_RES_ADD_ON_CATEGORY_DESC as i_E_RES_ADD_ON_CATEGORY_DESC",
	"JNR_E_RES_ADD_ON_CATEGORY___i_ALLOW_MULTIPLE_SERVICES_FLAG as i_ALLOW_MULTIPLE_SERVICES_FLAG",
	"JNR_E_RES_ADD_ON_CATEGORY___i_IMAGE_URL as i_IMAGE_URL",
	"JNR_E_RES_ADD_ON_CATEGORY___i_SORT_ORDER_NBR as i_SORT_ORDER_NBR",
	"JNR_E_RES_ADD_ON_CATEGORY___i_DELETED_FLAG as i_DELETED_FLAG",
	"JNR_E_RES_ADD_ON_CATEGORY___i_E_RES_CREATED_TSTMP as i_E_RES_CREATED_TSTMP",
	"JNR_E_RES_ADD_ON_CATEGORY___i_E_RES_CREATED_BY as i_E_RES_CREATED_BY",
	"JNR_E_RES_ADD_ON_CATEGORY___i_E_RES_UPDATE_TSTMP as i_E_RES_UPDATE_TSTMP",
	"JNR_E_RES_ADD_ON_CATEGORY___i_E_RES_UPDATED_BY as i_E_RES_UPDATED_BY",
	"JNR_E_RES_ADD_ON_CATEGORY___i_LOAD_TSTMP as i_LOAD_TSTMP").filter("i_E_RES_ADD_ON_CATEGORY_ID IS NULL OR ( i_E_RES_ADD_ON_CATEGORY_ID IS NOT NULL AND ( IF (ADD_ON_CATEGORY_NAME IS NULL, ' ', ADD_ON_CATEGORY_NAME) != IF (i_E_RES_ADD_ON_CATEGORY_NAME IS NULL, ' ', i_E_RES_ADD_ON_CATEGORY_NAME) OR IF (ALLOW_MULTIPLE_SERVICES IS NULL, 0, ALLOW_MULTIPLE_SERVICES) != IF (i_ALLOW_MULTIPLE_SERVICES_FLAG IS NULL, 0, i_ALLOW_MULTIPLE_SERVICES_FLAG) OR IF (IMAGE_URL IS NULL, ' ', IMAGE_URL) != IF (i_IMAGE_URL IS NULL, ' ', i_IMAGE_URL) OR IF (DESCRIPTION IS NULL, ' ', DESCRIPTION) != IF (i_E_RES_ADD_ON_CATEGORY_DESC IS NULL, ' ', i_E_RES_ADD_ON_CATEGORY_DESC) OR IF (SORT_ORDER IS NULL, - 99999, SORT_ORDER) != IF (i_SORT_ORDER_NBR IS NULL, - 99999, i_SORT_ORDER_NBR) OR IF (IS_DELETED IS NULL, 0, IS_DELETED) != IF (i_DELETED_FLAG IS NULL, 0, i_DELETED_FLAG) OR IF (CREATED_AT IS NULL, date'1900-01-01', CREATED_AT) != IF (i_E_RES_CREATED_TSTMP IS NULL, date'1900-01-01', i_E_RES_CREATED_TSTMP) OR IF (CREATED_BY IS NULL, ' ', CREATED_BY) != IF (i_E_RES_CREATED_BY IS NULL, ' ', i_E_RES_CREATED_BY) OR IF (UPDATED_BY IS NULL, ' ', UPDATED_BY) != IF (i_E_RES_UPDATED_BY IS NULL, ' ', i_E_RES_UPDATED_BY) OR IF (UPDATED_AT IS NULL, date'1900-01-01', UPDATED_AT) != IF (i_E_RES_UPDATE_TSTMP IS NULL, date'1900-01-01', i_E_RES_UPDATE_TSTMP) ) )").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------

# Processing node EXP_E_RES_ADD_ON_CATEGORY, type EXPRESSION 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
FIL_E_RES_ADD_ON_CATEGORY_temp = FIL_E_RES_ADD_ON_CATEGORY.toDF(*["FIL_E_RES_ADD_ON_CATEGORY___" + col for col in FIL_E_RES_ADD_ON_CATEGORY.columns])

EXP_E_RES_ADD_ON_CATEGORY = FIL_E_RES_ADD_ON_CATEGORY_temp.selectExpr(
	"FIL_E_RES_ADD_ON_CATEGORY___sys_row_id as sys_row_id",
	"FIL_E_RES_ADD_ON_CATEGORY___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
	"FIL_E_RES_ADD_ON_CATEGORY___ADD_ON_CATEGORY_NAME as ADD_ON_CATEGORY_NAME",
	"FIL_E_RES_ADD_ON_CATEGORY___ALLOW_MULTIPLE_SERVICES as ALLOW_MULTIPLE_SERVICES",
	"FIL_E_RES_ADD_ON_CATEGORY___CREATED_AT as CREATED_AT",
	"FIL_E_RES_ADD_ON_CATEGORY___UPDATED_BY as UPDATED_BY",
	"FIL_E_RES_ADD_ON_CATEGORY___CREATED_BY as CREATED_BY",
	"FIL_E_RES_ADD_ON_CATEGORY___UPDATED_AT as UPDATED_AT",
	"FIL_E_RES_ADD_ON_CATEGORY___IS_DELETED as IS_DELETED",
	"FIL_E_RES_ADD_ON_CATEGORY___IMAGE_URL as IMAGE_URL",
	"FIL_E_RES_ADD_ON_CATEGORY___DESCRIPTION as DESCRIPTION",
	"FIL_E_RES_ADD_ON_CATEGORY___SORT_ORDER as SORT_ORDER",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (FIL_E_RES_ADD_ON_CATEGORY___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, FIL_E_RES_ADD_ON_CATEGORY___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (FIL_E_RES_ADD_ON_CATEGORY___i_E_RES_ADD_ON_CATEGORY_ID IS NULL, 1, 2) as o_UPDATE_VALIDATOR"
)

# COMMAND ----------

# Processing node UPD_E_RES_ADD_ON_CATEGORY, type UPDATE_STRATEGY 
# COLUMN COUNT: 14

# for each involved DataFrame, append the dataframe name to each column
EXP_E_RES_ADD_ON_CATEGORY_temp = EXP_E_RES_ADD_ON_CATEGORY.toDF(*["EXP_E_RES_ADD_ON_CATEGORY___" + col for col in EXP_E_RES_ADD_ON_CATEGORY.columns])

UPD_E_RES_ADD_ON_CATEGORY = EXP_E_RES_ADD_ON_CATEGORY_temp.selectExpr(
	"EXP_E_RES_ADD_ON_CATEGORY___ADD_ON_CATEGORY_ID as ADD_ON_CATEGORY_ID",
	"EXP_E_RES_ADD_ON_CATEGORY___ADD_ON_CATEGORY_NAME as ADD_ON_CATEGORY_NAME",
	"EXP_E_RES_ADD_ON_CATEGORY___ALLOW_MULTIPLE_SERVICES as ALLOW_MULTIPLE_SERVICES",
	"EXP_E_RES_ADD_ON_CATEGORY___CREATED_AT as CREATED_AT",
	"EXP_E_RES_ADD_ON_CATEGORY___UPDATED_BY as UPDATED_BY",
	"EXP_E_RES_ADD_ON_CATEGORY___CREATED_BY as CREATED_BY",
	"EXP_E_RES_ADD_ON_CATEGORY___UPDATED_AT as UPDATED_AT",
	"EXP_E_RES_ADD_ON_CATEGORY___IS_DELETED as IS_DELETED",
	"EXP_E_RES_ADD_ON_CATEGORY___IMAGE_URL as IMAGE_URL",
	"EXP_E_RES_ADD_ON_CATEGORY___DESCRIPTION as DESCRIPTION",
	"EXP_E_RES_ADD_ON_CATEGORY___SORT_ORDER as SORT_ORDER",
	"EXP_E_RES_ADD_ON_CATEGORY___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_E_RES_ADD_ON_CATEGORY___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_E_RES_ADD_ON_CATEGORY___o_UPDATE_VALIDATOR as o_UPDATE_VALIDATOR") \
	.withColumn('pyspark_data_action', when(col("o_UPDATE_VALIDATOR") == (lit(1)) , lit(0)).when(col("o_UPDATE_VALIDATOR") ==(lit(2)) , lit(1)))

# COMMAND ----------

# Processing node Shortcut_to_E_RES_ADD_ON_CATEGORY1, type TARGET 
# COLUMN COUNT: 13


Shortcut_to_E_RES_ADD_ON_CATEGORY1 = UPD_E_RES_ADD_ON_CATEGORY.selectExpr(
	"CAST(ADD_ON_CATEGORY_ID AS INT) as E_RES_ADD_ON_CATEGORY_ID",
	"CAST(ADD_ON_CATEGORY_NAME AS STRING) as E_RES_ADD_ON_CATEGORY_NAME",
	"CAST(DESCRIPTION AS STRING) as E_RES_ADD_ON_CATEGORY_DESC",
	"CAST(ALLOW_MULTIPLE_SERVICES AS TINYINT) as ALLOW_MULTIPLE_SERVICES_FLAG",
	"CAST(IMAGE_URL AS STRING) as IMAGE_URL",
	"CAST(SORT_ORDER AS INT) as SORT_ORDER_NBR",
	"CAST(IS_DELETED AS TINYINT) as DELETED_FLAG",
	"CAST(CREATED_AT AS TIMESTAMP) as E_RES_CREATED_TSTMP",
	"CAST(CREATED_BY AS STRING) as E_RES_CREATED_BY",
	"CAST(UPDATED_AT AS TIMESTAMP) as E_RES_UPDATE_TSTMP",
	"CAST(UPDATED_BY AS STRING) as E_RES_UPDATED_BY",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP",
	"pyspark_data_action as pyspark_data_action"
)

try:
	primary_key = """source.E_RES_ADD_ON_CATEGORY_ID = target.E_RES_ADD_ON_CATEGORY_ID"""
	refined_perf_table = f"{legacy}.E_RES_ADD_ON_CATEGORY"
	executeMerge(Shortcut_to_E_RES_ADD_ON_CATEGORY1, refined_perf_table, primary_key)
	logger.info(f"Merge with {refined_perf_table} completed]")
	logPrevRunDt("E_RES_ADD_ON_CATEGORY", "E_RES_ADD_ON_CATEGORY", "Completed", "N/A", f"{raw}.log_run_details")
except Exception as e:
	logPrevRunDt("E_RES_ADD_ON_CATEGORY", "E_RES_ADD_ON_CATEGORY","Failed",str(e), f"{raw}.log_run_details", )
	raise e