#Code converted on 2023-07-24 08:21:53
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
# Processing node SQ_Shortcut_to_SDS_ORDER_ITEM_PRE, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_SDS_ORDER_ITEM_PRE = spark.sql(f"""SELECT DISTINCT
ORDER_ID
FROM {raw}.SDS_ORDER_ITEM_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_ORDER_ITEM_PRE1, type SOURCE 
# COLUMN COUNT: 1

SQ_Shortcut_to_SDS_ORDER_ITEM_PRE1 = spark.sql(f"""SELECT
ID
FROM {raw}.SDS_ORDER_ITEM_PRE""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL = spark.sql(f"""SELECT
SDS_ORDER_PRODUCT_SPECIAL_ID,
SDS_ORDER_ITEM_ID
FROM {legacy}.SDS_ORDER_PRODUCT_SPECIAL
WHERE HARD_DELETED_FLAG=0""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node SQ_Shortcut_to_SDS_ORDER_ITEM, type SOURCE 
# COLUMN COUNT: 2

SQ_Shortcut_to_SDS_ORDER_ITEM = spark.sql(f"""SELECT
SDS_ORDER_ITEM_ID,
SDS_ORDER_ID
FROM {legacy}.SDS_ORDER_ITEM""").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_SDS_ORDER_ITEM_PRE, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
SQ_Shortcut_to_SDS_ORDER_ITEM_PRE_temp = SQ_Shortcut_to_SDS_ORDER_ITEM_PRE.toDF(*["SQ_Shortcut_to_SDS_ORDER_ITEM_PRE___" + col for col in SQ_Shortcut_to_SDS_ORDER_ITEM_PRE.columns])
SQ_Shortcut_to_SDS_ORDER_ITEM_temp = SQ_Shortcut_to_SDS_ORDER_ITEM.toDF(*["SQ_Shortcut_to_SDS_ORDER_ITEM___" + col for col in SQ_Shortcut_to_SDS_ORDER_ITEM.columns])

JNR_SDS_ORDER_ITEM_PRE = SQ_Shortcut_to_SDS_ORDER_ITEM_temp_PRE_temp.join(SQ_Shortcut_to_SDS_ORDER_ITEM_temp,[SQ_Shortcut_to_SDS_ORDER_ITEM_PRE_temp.SQ_Shortcut_to_SDS_ORDER_ITEM_PRE___ORDER_ID == SQ_Shortcut_to_SDS_ORDER_ITEM_temp.SQ_Shortcut_to_SDS_ORDER_ITEM___SDS_ORDER_ID],'inner').selectExpr(
	"SQ_Shortcut_to_SDS_ORDER_ITEM___SDS_ORDER_ITEM_ID as SDS_ORDER_ITEM_ID",
	"SQ_Shortcut_to_SDS_ORDER_ITEM___SDS_ORDER_ID as SDS_ORDER_ID",
	"SQ_Shortcut_to_SDS_ORDER_ITEM_PRE___ORDER_ID as i_ORDER_ID")

# COMMAND ----------
# Processing node JNR_SDS_ORDER_ITEM, type JOINER 
# COLUMN COUNT: 2

JNR_SDS_ORDER_ITEM = SQ_Shortcut_to_SDS_ORDER_ITEM_PRE1.join(JNR_SDS_ORDER_ITEM_PRE,[SQ_Shortcut_to_SDS_ORDER_ITEM_PRE1.ID == JNR_SDS_ORDER_ITEM_PRE.SDS_ORDER_ITEM_ID],'right_outer')

# COMMAND ----------
# Processing node FIL_SDS_ORDER_ITEM, type FILTER 
# COLUMN COUNT: 2

# for each involved DataFrame, append the dataframe name to each column
JNR_SDS_ORDER_ITEM_temp = JNR_SDS_ORDER_ITEM.toDF(*["JNR_SDS_ORDER_ITEM___" + col for col in JNR_SDS_ORDER_ITEM.columns])

FIL_SDS_ORDER_ITEM = JNR_SDS_ORDER_ITEM_temp.selectExpr(
	"JNR_SDS_ORDER_ITEM___SDS_ORDER_ITEM_ID as SDS_ORDER_ITEM_ID",
	"JNR_SDS_ORDER_ITEM___ID as ID").filter("ID IS NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node JNR_ORDER_PRODUCT_SPECIAL, type JOINER . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
FIL_SDS_ORDER_ITEM_temp = FIL_SDS_ORDER_ITEM.toDF(*["FIL_SDS_ORDER_ITEM___" + col for col in FIL_SDS_ORDER_ITEM.columns])
SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL_temp = SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL.toDF(*["SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL___" + col for col in SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL.columns])

JNR_ORDER_PRODUCT_SPECIAL = SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL_temp.join(FIL_SDS_ORDER_ITEM_temp,[SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL_temp.SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL___SDS_ORDER_ITEM_ID == FIL_SDS_ORDER_ITEM_temp.FIL_SDS_ORDER_ITEM___SDS_ORDER_ITEM_ID],'right_outer').selectExpr(
	"FIL_SDS_ORDER_ITEM___SDS_ORDER_ITEM_ID as SDS_ORDER_ITEM_ID",
	"SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL___SDS_ORDER_ITEM_ID as SDS_ORDER_ITEM_ID1",
	"SQ_Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL___SDS_ORDER_PRODUCT_SPECIAL_ID as SDS_ORDER_PRODUCT_SPECIAL_ID")

# COMMAND ----------
# Processing node FLT_ORDER_PRODUCT_SPECIAL, type FILTER 
# COLUMN COUNT: 1

# for each involved DataFrame, append the dataframe name to each column
JNR_ORDER_PRODUCT_SPECIAL_temp = JNR_ORDER_PRODUCT_SPECIAL.toDF(*["JNR_ORDER_PRODUCT_SPECIAL___" + col for col in JNR_ORDER_PRODUCT_SPECIAL.columns])

FLT_ORDER_PRODUCT_SPECIAL = JNR_ORDER_PRODUCT_SPECIAL_temp.selectExpr(
	"JNR_ORDER_PRODUCT_SPECIAL___SDS_ORDER_PRODUCT_SPECIAL_ID as SDS_ORDER_PRODUCT_SPECIAL_ID").filter("SDS_ORDER_PRODUCT_SPECIAL_ID IS NOT NULL").withColumn("sys_row_id", monotonically_increasing_id())

# COMMAND ----------
# Processing node EXP_SDS_ORDER_PRODUCT_SPECIAL, type EXPRESSION 
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
FLT_ORDER_PRODUCT_SPECIAL_temp = FLT_ORDER_PRODUCT_SPECIAL.toDF(*["FLT_ORDER_PRODUCT_SPECIAL___" + col for col in FLT_ORDER_PRODUCT_SPECIAL.columns])

EXP_SDS_ORDER_PRODUCT_SPECIAL = FLT_ORDER_PRODUCT_SPECIAL_temp.selectExpr(
	"FLT_ORDER_PRODUCT_SPECIAL___sys_row_id as sys_row_id",
	"FLT_ORDER_PRODUCT_SPECIAL___SDS_ORDER_PRODUCT_SPECIAL_ID as SDS_ORDER_PRODUCT_SPECIAL_ID",
	"1 as HARD_DELETED_FLAG",
	"CURRENT_TIMESTAMP as UPDATE_TSTMP"
)

# COMMAND ----------
# Processing node UPD_SDS_ORDER_PRODUCT_SPECIAL, type UPDATE_STRATEGY . Note: using additional SELECT to rename incoming columns
# COLUMN COUNT: 3

# for each involved DataFrame, append the dataframe name to each column
EXP_SDS_ORDER_PRODUCT_SPECIAL_temp = EXP_SDS_ORDER_PRODUCT_SPECIAL.toDF(*["EXP_SDS_ORDER_PRODUCT_SPECIAL___" + col for col in EXP_SDS_ORDER_PRODUCT_SPECIAL.columns])

UPD_SDS_ORDER_PRODUCT_SPECIAL = EXP_SDS_ORDER_PRODUCT_SPECIAL_temp.selectExpr(
	"EXP_SDS_ORDER_PRODUCT_SPECIAL___SDS_ORDER_PRODUCT_SPECIAL_ID as SDS_ORDER_PRODUCT_SPECIAL_ID3",
	"EXP_SDS_ORDER_PRODUCT_SPECIAL___HARD_DELETED_FLAG as HARD_DELETED_FLAG3",
	"EXP_SDS_ORDER_PRODUCT_SPECIAL___UPDATE_TSTMP as UPDATE_TSTMP3")\
	.withColumn('pyspark_data_action', lit(1))

# COMMAND ----------
# Processing node Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL1, type TARGET 
# COLUMN COUNT: 28

#TODO check if this is needed a delete ?
Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL1 = UPD_SDS_ORDER_PRODUCT_SPECIAL.selectExpr(
	"CAST(SDS_ORDER_PRODUCT_SPECIAL_ID3 AS STRING) as SDS_ORDER_PRODUCT_SPECIAL_ID",
	"CAST(lit(None) AS STRING) as SDS_ORDER_PRODUCT_SPECIAL_NAME",
	"CAST(lit(None) AS STRING) as SDS_ORDER_ITEM_ID",
	"CAST(lit(None) AS STRING) as SDS_SPECIAL_ID",
	"CAST(lit(None) AS STRING) as SDS_SPECIAL_CD",
	"CAST(lit(None) AS STRING) as MANUALLY_ENTERED_CD",
	"CAST(lit(None) AS STRING) as PREMIUM_DISCOUNT",
	"CAST(lit(None) AS DECIMAL(18,3)) as ADJUSTMENT_AMT",
	"CAST(lit(None) AS DECIMAL(18,3)) as ADJUSTMENT_PCT",
	"CAST(lit(None) AS DECIMAL(18,3)) as APPLIED_PCT_DISCOUNT_AMT",
	"CAST(lit(None) AS TINYINT) as EVALUATION_ORDER_NBR",
	"CAST(lit(None) AS DECIMAL(18,2)) as AUTO_CALCULATED_PRICE",
	"CAST(lit(None) AS TINYINT) as MANUAL_ADJUSTED_FLAG",
	"CAST(lit(None) AS TINYINT) as HIDE_FROM_CUSTOMER_ONLINE_INVOICE_FLAG",
	"CAST(lit(None) AS TINYINT) as APPLY_TO_ALL_PRODUCTS_FLAG",
	"CAST(lit(None) AS STRING) as DUPLICATION",
	"CAST(lit(None) AS TINYINT) as FOR_QUOTE_FLAG",
	"CAST(lit(None) AS STRING) as SDS_OWNER_ID",
	"CAST(lit(None) AS STRING) as CURRENCY_ISO_CD",
	"CAST(lit(None) AS TINYINT) as DELETED_FLAG",
	"CAST(HARD_DELETED_FLAG3 AS TINYINT) as HARD_DELETED_FLAG",
	"CAST(lit(None) AS TIMESTAMP) as SDS_SYSTEM_MODIFIED_TSTMP",
	"CAST(lit(None) AS TIMESTAMP) as SDS_LAST_MODIFIED_TSTMP",
	"CAST(lit(None) AS STRING) as SDS_LAST_MODIFIED_BY_ID",
	"CAST(lit(None) AS TIMESTAMP) as SDS_CREATED_TSTMP",
	"CAST(lit(None) AS STRING) as SDS_CREATED_BY_ID",
	"CAST(UPDATE_TSTMP3 AS TIMESTAMP) as UPDATE_TSTMP",
	"CAST(lit(None) AS TIMESTAMP) as LOAD_TSTMP",
	"UPD_SDS_ORDER_PRODUCT_SPECIAL.pyspark_data_action as pyspark_data_action"
)
Shortcut_to_SDS_ORDER_PRODUCT_SPECIAL1.write.saveAsTable(f'{legacy}.SDS_ORDER_PRODUCT_SPECIAL')