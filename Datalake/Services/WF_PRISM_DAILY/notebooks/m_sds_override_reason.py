#Code converted on 2023-07-24 08:28:36
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
# Processing node SQ_Shortcut_to_SDS_OVERRIDE_REASON, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SDS_OVERRIDE_REASON = spark.sql(f"""SELECT
SDS_OVERRIDE_REASON_DESC,
LOAD_TSTMP
FROM {legacy}.SDS_OVERRIDE_REASON""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_TRIM2, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_OVERRIDE_REASON_temp = SQ_Shortcut_to_SDS_OVERRIDE_REASON.toDF(*["SQ_Shortcut_to_SDS_OVERRIDE_REASON___" + col for col in SQ_Shortcut_to_SDS_OVERRIDE_REASON.columns])

EXP_TRIM2 = SQ_Shortcut_to_SDS_OVERRIDE_REASON_temp.selectExpr(
	"SQ_Shortcut_to_SDS_OVERRIDE_REASON___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SDS_OVERRIDE_REASON___SDS_OVERRIDE_REASON_DESC as SDS_OVERRIDE_REASON_DESC",
	"UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_SDS_OVERRIDE_REASON___SDS_OVERRIDE_REASON_DESC ) ) ) as o_SDS_OVERRIDE_REASON_DESC",
	"SQ_Shortcut_to_SDS_OVERRIDE_REASON___LOAD_TSTMP as LOAD_TSTMP"
)

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_ORDER_ITEM, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_SDS_ORDER_ITEM = spark.sql(f"""SELECT DISTINCT
PRICE_MANUAL_ADJ_REASON
FROM {legacy}.SDS_ORDER_ITEM
WHERE (SDS_SYSTEM_MODIFIED_TSTMP >= CURRENT_DATE-1

OR SDS_LAST_MODIFIED_TSTMP >= CURRENT_DATE-1

OR SDS_CREATED_TSTMP >= CURRENT_DATE-1) AND PRICE_MANUAL_ADJ_REASON IS NOT NULL""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_TRIM1, type EXPRESSION 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_ORDER_ITEM_temp = SQ_Shortcut_to_SDS_ORDER_ITEM.toDF(*["SQ_Shortcut_to_SDS_ORDER_ITEM___" + col for col in SQ_Shortcut_to_SDS_ORDER_ITEM.columns])

EXP_TRIM1 = SQ_Shortcut_to_SDS_ORDER_ITEM_temp.selectExpr(
	"SQ_Shortcut_to_SDS_ORDER_ITEM___sys_row_id as sys_row_id",
	"SQ_Shortcut_to_SDS_ORDER_ITEM___PRICE_MANUAL_ADJ_REASON as PRICE_MANUAL_ADJ_REASON",
	"UPPER ( LTRIM ( RTRIM ( SQ_Shortcut_to_SDS_ORDER_ITEM___PRICE_MANUAL_ADJ_REASON ) ) ) as o_PRICE_MANUAL_ADJ_REASON"
)

# COMMAND ----------
# Processing node JNR_SDS_OVERRIDE_REASON, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
EXP_TRIM1_temp = EXP_TRIM1.toDF(*["EXP_TRIM1___" + col for col in EXP_TRIM1.columns])
EXP_TRIM2_temp = EXP_TRIM2.toDF(*["EXP_TRIM2___" + col for col in EXP_TRIM2.columns])

JNR_SDS_OVERRIDE_REASON = EXP_TRIM1_temp.join(EXP_TRIM2_temp,[EXP_TRIM1_temp.EXP_TRIM1___o_PRICE_MANUAL_ADJ_REASON == EXP_TRIM2_temp.EXP_TRIM2___o_SDS_OVERRIDE_REASON_DESC],'left_outer').selectExpr(
	"EXP_TRIM2___o_SDS_OVERRIDE_REASON_DESC as i_SDS_OVERRIDE_REASON_DESC",
	"EXP_TRIM2___LOAD_TSTMP as i_LOAD_TSTMP",
	"EXP_TRIM1___o_PRICE_MANUAL_ADJ_REASON as o_PRICE_MANUAL_ADJ_REASON",
	"EXP_TRIM1___PRICE_MANUAL_ADJ_REASON as PRICE_MANUAL_ADJ_REASON")

# COMMAND ----------
# Processing node EXP_SDS_OVERRIDE_REASON, type EXPRESSION 
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column

EXP_SDS_OVERRIDE_REASON = JNR_SDS_OVERRIDE_REASON.selectExpr(
	"JNR_SDS_OVERRIDE_REASON___sys_row_id as sys_row_id",
	"LTRIM ( RTRIM ( JNR_SDS_OVERRIDE_REASON___PRICE_MANUAL_ADJ_REASON ) ) as o_PRICE_MANUAL_ADJ_REASON",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP",
	"IF (JNR_SDS_OVERRIDE_REASON___i_LOAD_TSTMP IS NULL, CURRENT_TIMESTAMP, JNR_SDS_OVERRIDE_REASON___i_LOAD_TSTMP) as LOAD_TSTMP",
	"IF (JNR_SDS_OVERRIDE_REASON___i_SDS_OVERRIDE_REASON_DESC IS NULL, 1, 2) as o_UPDATE_VALIDATION"
)

# COMMAND ----------
# Processing node FLT_INSERTS_ONLY, type FILTER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 4

# for each involved DataFrame, append the dataframe name to each column
EXP_SDS_OVERRIDE_REASON_temp = EXP_SDS_OVERRIDE_REASON.toDF(*["EXP_SDS_OVERRIDE_REASON___" + col for col in EXP_SDS_OVERRIDE_REASON.columns])

FLT_INSERTS_ONLY = EXP_SDS_OVERRIDE_REASON_temp.selectExpr(
	"EXP_SDS_OVERRIDE_REASON___o_PRICE_MANUAL_ADJ_REASON as PRICE_MANUAL_ADJ_REASON",
	"EXP_SDS_OVERRIDE_REASON___UPDATE_TSTMP as UPDATE_TSTMP",
	"EXP_SDS_OVERRIDE_REASON___LOAD_TSTMP as LOAD_TSTMP",
	"EXP_SDS_OVERRIDE_REASON___o_UPDATE_VALIDATION as o_UPDATE_VALIDATION").filter("o_UPDATE_VALIDATION = 1").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node Shortcut_to_SDS_OVERRIDE_REASON_3, type TARGET 
# COLUMN COUNT: 4

	# "col("SDS_OVERRIDE_REASON_GID") as SDS_OVERRIDE_REASON_GID",
# 
Shortcut_to_SDS_OVERRIDE_REASON_3 = FLT_INSERTS_ONLY.selectExpr(
	"CAST(PRICE_MANUAL_ADJ_REASON AS STRING) as SDS_OVERRIDE_REASON_DESC",
	"CAST(UPDATE_TSTMP AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(LOAD_TSTMP AS TIMESTAMP) as LOAD_TSTMP"
)
Shortcut_to_SDS_OVERRIDE_REASON_3.write.saveAsTable(f'{legacy}.SDS_OVERRIDE_REASON')